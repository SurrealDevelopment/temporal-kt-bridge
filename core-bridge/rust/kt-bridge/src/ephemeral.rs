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

use parking_lot::Mutex;
use temporalio_sdk_core::ephemeral_server::{
    EphemeralExe, EphemeralExeVersion, EphemeralServer, TemporalDevServerConfig, TestServerConfig,
};

use crate::error::{KtError, KtResult};

pub struct EphemeralEntry {
    server: Mutex<Option<EphemeralServer>>,
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
        let mut server = self.server.lock().take();
        match server.as_mut() {
            Some(server) => server.shutdown().await.map_err(|e| format!("{e:#}")),
            None => Ok(()), // idempotent
        }
    }
}

/// Shuts the child down if the entry is dropped without an explicit shutdown, so an unwound or
/// forgotten path cannot leak a server process.
///
/// Best-effort and detached: `Drop` cannot await, and blocking here would stall whichever thread
/// released the last reference.
impl Drop for EphemeralEntry {
    fn drop(&mut self) {
        if let Some(mut server) = self.server.lock().take()
            && let Ok(handle) = tokio::runtime::Handle::try_current()
        {
            handle.spawn(async move {
                let _ = server.shutdown().await;
            });
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
        ttl: Some(Duration::from_secs(60 * 60 * 24)),
    }
}

/// Starts a server with its stdio piped back to the JVM.
pub async fn start(
    options: crate::proto::EphemeralServerOptions,
) -> Result<Arc<EphemeralEntry>, String> {
    let (out, err) = redirect(&options)?;
    let server = if options.test_server {
        let config = TestServerConfig::builder()
            .exe(exe(&options))
            .maybe_port(port(&options))
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
            .maybe_port(port(&options))
            .ui(options.ui)
            .extra_args(options.extra_args.clone())
            .build();
        config.start_server_with_output(out, err).await
    }
    .map_err(|e| format!("{e:#}"))?;

    let pid = server.child_process_id().unwrap_or(0);
    let target = server.target.clone();
    let has_test_service = server.has_test_service;

    Ok(Arc::new(EphemeralEntry {
        server: Mutex::new(Some(server)),
        pid,
        target,
        has_test_service,
    }))
}

fn port(options: &crate::proto::EphemeralServerOptions) -> Option<u16> {
    (options.port > 0 && options.port <= u16::MAX as u32).then_some(options.port as u16)
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
    prost::Message::decode(bytes).map_err(KtError::from)
}
