//! Dev and test server lifecycle.
//!
//! Two things here were previously worked around rather than fixed.
//!
//! **The child never inherits the JVM's stdio.** Core spawns the server with `Stdio::inherit()`
//! by default, so an orphaned server holds the dead JVM's stdout and stderr -- and Gradle waits
//! for pipe EOF after a test worker exits, which is why one leaked server hung an entire build
//! with every test green. Output goes to a caller-named file, or to `null`.
//!
//! Not `Stdio::piped()`: `EphemeralServer` keeps the child private and exposes no handles, so
//! nothing could drain the pipes, and a server that outran the pipe buffer would block forever --
//! trading a build hang for a worse one. A file has no such limit and keeps the logs.
//!
//! The pid registry on the JVM side is still worth keeping, since nothing in-process survives
//! `kill -9`, but it is no longer load-bearing for that failure.
//!
//! **The pid is public Rust API.** `EphemeralServer::child_process_id()` is and was public; the
//! C bridge simply did not expose it, which is what the carried fork patch existed to add.

use std::sync::Arc;
use std::time::Duration;

use temporalio_sdk_core::ephemeral_server::{
    EphemeralExe, EphemeralExeVersion, EphemeralServer, TemporalDevServerConfig, TestServerConfig,
};
use tokio::sync::Mutex;
use tokio_util::task::TaskTracker;
use tokio_util::task::task_tracker::TaskTrackerToken;

use crate::error::{KtError, KtResult};

pub struct EphemeralEntry {
    // The reservation follows the live child, including the gap between handle removal and free/Drop.
    server: Mutex<Option<(EphemeralServer, TaskTrackerToken)>>,
    tokio: tokio::runtime::Handle,
    tasks: TaskTracker,
    /// Captured at start so it stays readable after shutdown consumes the server.
    ///
    /// The C bridge read it straight off the server, which is why its accessor was documented as
    /// unsafe to call concurrently with shutdown.
    pub pid: u32,
    pub target: String,
    pub has_test_service: bool,
}

impl EphemeralEntry {
    pub fn info(&self) -> crate::proto::EphemeralServerInfo {
        crate::proto::EphemeralServerInfo {
            target: self.target.clone(),
            pid: self.pid,
            has_test_service: self.has_test_service,
        }
    }

    pub async fn shutdown(&self) -> Result<(), String> {
        // Keep the child in the entry until it is reaped. Cancellation must leave it available
        // for runtime shutdown, and concurrent shutdowns must wait for the same child to exit.
        let mut server = self.server.lock().await;
        let result = match server.as_mut() {
            Some((server, _reservation)) => server.shutdown().await.map_err(|e| format!("{e:#}")),
            None => Ok(()), // idempotent
        };
        server.take();
        result
    }

    /// FFI free runs on a JVM thread; keep close synchronous without blocking a Tokio worker.
    pub fn shutdown_blocking(&self) -> Result<(), String> {
        self.tokio.block_on(self.shutdown())
    }

    /// Removing a handle must still reap the child before its owning runtime stops.
    pub fn free(self: &Arc<Self>) {
        let entry = self.clone();
        self.tasks.spawn_on(
            async move {
                if let Err(error) = entry.shutdown().await {
                    tracing::warn!(%error, "could not reap ephemeral server");
                }
            },
            &self.tokio,
        );
    }
}

impl Drop for EphemeralEntry {
    fn drop(&mut self) {
        // Discarded creation results can drop their final Arc on Tokio. Never block that thread,
        // and retain only its Handle: an Arc<RuntimeEntry> could drop Core on its own worker.
        if let Some((mut server, _reservation)) = self.server.get_mut().take() {
            self.tasks.spawn_on(
                async move {
                    if let Err(error) = server.shutdown().await {
                        tracing::warn!(%error, "could not reap discarded ephemeral server");
                    }
                },
                &self.tokio,
            );
        }
    }
}

fn exe(options: &crate::proto::EphemeralServerOptions) -> EphemeralExe {
    if !options.existing_path.is_empty() {
        return EphemeralExe::ExistingPath(options.existing_path.clone());
    }
    let version = if options.download_version.is_empty() || options.download_version == "default" {
        EphemeralExeVersion::SDKDefault {
            sdk_name: if options.sdk_name.is_empty() {
                "sdk-kotlin".into()
            } else {
                options.sdk_name.clone()
            },
            sdk_version: if options.sdk_version.is_empty() {
                "0.1.0".into()
            } else {
                options.sdk_version.clone()
            },
        }
    } else {
        EphemeralExeVersion::Fixed(options.download_version.clone())
    };
    EphemeralExe::CachedDownload {
        version,
        dest_dir: (!options.download_dest_dir.is_empty())
            .then(|| options.download_dest_dir.clone()),
        ttl: options
            .download_ttl_seconds
            .or(Some(60 * 60 * 24))
            .filter(|seconds| *seconds > 0)
            .map(Duration::from_secs),
    }
}

/// Starts a server with its stdio redirected away from the JVM.
pub async fn start(
    options: crate::proto::EphemeralServerOptions,
    tokio: tokio::runtime::Handle,
    tasks: TaskTracker,
) -> Result<Arc<EphemeralEntry>, String> {
    let port = port(&options).map_err(|e| e.to_string())?;
    let (out, err) = redirect(&options)?;
    let server = if options.test_server {
        let config = TestServerConfig::builder()
            .exe(exe(&options))
            .maybe_port(port)
            .extra_args(options.extra_args.clone())
            .build();
        config.start_server_with_output(out, err).await
    } else {
        let config = TemporalDevServerConfig::builder()
            .exe(exe(&options))
            .namespace(if options.namespace.is_empty() {
                "default".to_string()
            } else {
                options.namespace.clone()
            })
            .ip(if options.ip.is_empty() {
                "127.0.0.1".to_string()
            } else {
                options.ip.clone()
            })
            .maybe_port(port)
            .ui(options.ui)
            .extra_args(options.extra_args.clone())
            .build();
        config.start_server_with_output(out, err).await
    }
    .map_err(|e| format!("{e:#}"))?;

    // Before this return Core owns a private Child inside its startup future. Cancelling that
    // future still uses Core's kill_on_drop; only completed starts can be explicitly reaped here.
    let pid = server.child_process_id().unwrap_or(0);
    let target = server.target.clone();
    let has_test_service = server.has_test_service;

    Ok(Arc::new(EphemeralEntry {
        server: Mutex::new(Some((server, tasks.token()))),
        tokio,
        tasks,
        pid,
        target,
        has_test_service,
    }))
}

fn port(options: &crate::proto::EphemeralServerOptions) -> KtResult<Option<u16>> {
    let port = u16::try_from(options.port)
        .map_err(|_| KtError::InvalidArgument("port must be between 0 and 65535".into()))?;
    Ok((port > 0).then_some(port))
}

/// Where the child's output goes.
///
/// Never the JVM's own stdout/stderr: an orphaned child holding those is what hangs a Gradle
/// build long after every test has passed.
fn redirect(
    options: &crate::proto::EphemeralServerOptions,
) -> Result<(std::process::Stdio, std::process::Stdio), String> {
    if options.log_file.is_empty() {
        return Ok((std::process::Stdio::null(), std::process::Stdio::null()));
    }
    let open = || {
        std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&options.log_file)
            .map_err(|e| format!("could not open {}: {e}", options.log_file))
    };
    Ok((open()?.into(), open()?.into()))
}

pub fn decode_options(bytes: &[u8]) -> KtResult<crate::proto::EphemeralServerOptions> {
    let options: crate::proto::EphemeralServerOptions = prost::Message::decode(bytes)?;
    port(&options)?;
    Ok(options)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        abi::{KtCompletion, KtKind},
        handle::{Entry, HANDLES, KIND_EPHEMERAL},
        queue::{Pending, Queue},
    };

    #[test]
    fn download_ttl_preserves_default_zero_and_explicit_lifetimes() {
        for (seconds, expected) in [
            (None, Some(86_400)),
            (Some(0), None),
            (Some(123), Some(123)),
        ] {
            let options = crate::proto::EphemeralServerOptions {
                download_ttl_seconds: seconds,
                ..Default::default()
            };
            let EphemeralExe::CachedDownload { ttl, .. } = exe(&options) else {
                panic!("expected a cached download");
            };
            assert_eq!(ttl, expected.map(Duration::from_secs));
        }
    }

    #[test]
    fn a_port_outside_the_tcp_range_is_rejected_instead_of_selecting_a_random_port() {
        let options = crate::proto::EphemeralServerOptions {
            port: 65_536,
            ..Default::default()
        };
        assert!(matches!(
            decode_options(&prost::Message::encode_to_vec(&options)),
            Err(KtError::InvalidArgument(_))
        ));
    }

    #[test]
    fn an_unclaimed_server_completion_releases_its_handle_but_poll_transfers_ownership() {
        let runtime = crate::runtime::new_runtime(crate::proto::RuntimeOptions::default()).unwrap();
        let new_handle = || {
            HANDLES.insert(Entry::Ephemeral(Arc::new(EphemeralEntry {
                // A stopped server still has a live bridge handle; no process is needed to test ownership.
                server: Mutex::new(None),
                tokio: runtime.core.tokio_handle().clone(),
                tasks: runtime.tasks.clone(),
                pid: 1,
                target: String::new(),
                has_test_service: false,
            })))
        };
        let result = |handle| Pending::ack(1).kind(KtKind::EphemeralStarted).aux0(handle);

        let handle = new_handle();
        drop(result(handle));
        assert!(HANDLES.ephemeral(handle).is_err());

        let queue = Queue::new();
        let handle = new_handle();
        queue.sender().push(result(handle));
        let poller = queue.poller();
        let mut out = std::mem::MaybeUninit::<KtCompletion>::uninit();
        assert_eq!(unsafe { poller.poll(out.as_mut_ptr(), 1, 0) }.unwrap(), 1);
        assert!(HANDLES.ephemeral(handle).is_ok());
        HANDLES.remove_of_kind(handle, KIND_EPHEMERAL).unwrap();

        queue.close();
        let handle = new_handle();
        queue.sender().push(result(handle));
        assert!(HANDLES.ephemeral(handle).is_err());

        let queued = Queue::new();
        let handle = new_handle();
        queued.sender().push(result(handle));
        drop(queued);
        assert!(HANDLES.ephemeral(handle).is_err());
    }
}
