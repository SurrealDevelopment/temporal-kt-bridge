//! Runtime lifecycle, and the pending-request table that makes shutdown deterministic.

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::Mutex;
use temporalio_sdk_core::{CoreRuntime, RuntimeOptions as CoreRuntimeOptions, TokioRuntimeBuilder};
use tokio_util::sync::CancellationToken;

use crate::abi::{KT_ERR_SHUTDOWN, KtKind};
use crate::error::{KtError, KtResult};
use crate::queue::{Pending, Queue, Sender};

pub struct RuntimeEntry {
    pub core: CoreRuntime,
    pub queue: Queue,
    /// Cancellation tokens for in-flight operations, keyed by request id.
    ///
    /// Exists to uphold the one-completion invariant: on shutdown every entry still here is
    /// answered with `KT_ERR_SHUTDOWN`, so the JVM never waits on a completion that will not come.
    /// The previous bridge had no such guarantee and blocked on a 60-second latch instead.
    pending: Mutex<HashMap<u64, CancellationToken>>,
}

impl RuntimeEntry {
    pub fn sender(&self) -> Sender {
        self.queue.sender()
    }

    /// Registers an operation and returns its cancellation token.
    pub fn register(&self, req_id: u64) -> CancellationToken {
        let token = CancellationToken::new();
        self.pending.lock().insert(req_id, token.clone());
        token
    }

    /// Retires an operation. Returns false if it was already retired, which is how a cancellation
    /// racing a natural completion stays benign: whichever arrives second sends nothing.
    pub fn retire(&self, req_id: u64) -> bool {
        self.pending.lock().remove(&req_id).is_some()
    }

    pub fn cancel(&self, req_id: u64) {
        if let Some(token) = self.pending.lock().get(&req_id) {
            token.cancel();
        }
    }

    /// Answers every outstanding request, then wakes all pollers.
    fn drain_pending_for_shutdown(&self) {
        let outstanding: Vec<u64> = {
            let mut pending = self.pending.lock();
            let ids = pending.keys().copied().collect();
            pending.clear();
            ids
        };
        let sender = self.queue.sender();
        for req_id in outstanding {
            sender.push(Pending::error(req_id, KT_ERR_SHUTDOWN, "runtime is shutting down"));
        }
        // Sticky close, so a pump re-entering poll after shutdown returns instead of parking.
        self.queue.close();
    }
}

pub fn new_runtime(config: crate::proto::RuntimeOptions) -> KtResult<Arc<RuntimeEntry>> {
    let mut builder = TokioRuntimeBuilder::default();
    // Every Core thread was previously the default `tokio-runtime-worker`, which makes a thread
    // dump or profile unreadable when several runtimes are alive.
    builder.inner.thread_name_fn(|| {
        use std::sync::atomic::{AtomicUsize, Ordering};
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        format!("temporal-core-{}", NEXT.fetch_add(1, Ordering::Relaxed))
    });

    let mut options = CoreRuntimeOptions::default();
    if let Some(telemetry) = config.telemetry.as_ref()
        && !telemetry.log_filter.is_empty()
    {
        options = apply_log_filter(options, &telemetry.log_filter);
    }

    let core = CoreRuntime::new(options, builder).map_err(KtError::from)?;
    Ok(Arc::new(RuntimeEntry {
        core,
        queue: Queue::new(),
        pending: Mutex::new(HashMap::new()),
    }))
}

fn apply_log_filter(
    options: CoreRuntimeOptions,
    _filter: &str,
) -> CoreRuntimeOptions {
    // Telemetry construction moved between 0.6 and 0.8; wiring the filter through is handled with
    // the log-forwarding work rather than guessed at here.
    options
}

pub fn free_runtime(entry: Arc<RuntimeEntry>) {
    entry.drain_pending_for_shutdown();
}

pub fn runtime_info(_entry: &RuntimeEntry) -> crate::proto::RuntimeInfo {
    crate::proto::RuntimeInfo {
        core_version: env!("CARGO_PKG_VERSION").to_string(),
        prometheus_bind_address: String::new(),
    }
}

/// Marks a completion as belonging to a pushed event rather than a request.
pub const PUSHED: u64 = 0;

pub fn push_kind(sender: &Sender, kind: KtKind, payload: Vec<u8>, aux0: u64) {
    sender.push(Pending::ack(PUSHED).kind(kind).payload(payload).aux0(aux0));
}
