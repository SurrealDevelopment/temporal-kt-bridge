//! Runtime lifecycle, and the pending-request table that makes shutdown deterministic.

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::Mutex;
use temporalio_sdk_core::{CoreRuntime, RuntimeOptions as CoreRuntimeOptions, TokioRuntimeBuilder};
use tokio_util::sync::CancellationToken;

use crate::abi::{KT_ERR_CANCELLED, KT_ERR_PANIC, KT_ERR_SHUTDOWN};
use crate::error::{KtError, KtResult};
use futures_util::FutureExt;

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
            sender.push(Pending::error(
                req_id,
                KT_ERR_SHUTDOWN,
                "runtime is shutting down",
            ));
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

fn apply_log_filter(options: CoreRuntimeOptions, _filter: &str) -> CoreRuntimeOptions {
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

/// Runs an async operation so that it produces exactly one terminal completion.
///
/// This is where the invariant in [`crate::queue`] is actually enforced, rather than merely
/// intended. Whatever `op` does -- succeed, fail, panic, or lose a race with cancellation or
/// shutdown -- the caller's `req_id` is answered once and only once:
///
///   * the operation races its own cancellation token, so `kt_cancel` produces `KT_ERR_CANCELLED`
///     rather than leaving the JVM waiting;
///   * `retire` is the single point of truth, so whichever of completion and cancellation arrives
///     second sends nothing;
///   * a panic inside `op` is caught here, because a panicking Tokio task would otherwise be
///     swallowed by the JoinSet and the request would hang forever;
///   * on shutdown, anything still registered is drained with `KT_ERR_SHUTDOWN`.
pub fn spawn_request<F>(entry: &Arc<RuntimeEntry>, req_id: u64, op: F)
where
    F: std::future::Future<Output = Pending> + Send + 'static,
{
    let token = entry.register(req_id);
    let sender = entry.sender();
    let owner = entry.clone();
    entry.core.tokio_handle().spawn(async move {
        let result = tokio::select! {
            biased;
            _ = token.cancelled() => Pending::error(req_id, KT_ERR_CANCELLED, "cancelled"),
            outcome = std::panic::AssertUnwindSafe(op).catch_unwind() => match outcome {
                Ok(pending) => pending,
                Err(payload) => Pending::error(
                    req_id,
                    KT_ERR_PANIC,
                    format!("panic: {}", crate::panic::panic_message(&payload)),
                ),
            },
        };
        // Only the first of completion / cancellation / shutdown gets to answer.
        if owner.retire(req_id) {
            sender.push(result);
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::abi::{KT_ERR_CANCELLED, KT_ERR_PANIC, KT_OK};

    fn runtime() -> Arc<RuntimeEntry> {
        new_runtime(crate::proto::RuntimeOptions::default()).expect("runtime")
    }

    fn drain(entry: &RuntimeEntry, poller: &crate::queue::PollerEntry) -> Vec<(u64, i32)> {
        let mut out = vec![
            crate::abi::KtCompletion {
                req_id: 0,
                kind: 0,
                status: 0,
                payload: 0,
                payload_len: 0,
                aux0: 0,
                aux1: 0,
            };
            16
        ];
        let _ = entry;
        let n = unsafe { poller.poll(out.as_mut_ptr(), 16, 500) }.unwrap();
        out[..n as usize]
            .iter()
            .map(|r| (r.req_id, r.status))
            .collect()
    }

    #[test]
    fn a_successful_request_yields_exactly_one_completion() {
        let entry = runtime();
        let poller = entry.queue.poller();
        spawn_request(&entry, 1, async { Pending::ack(1) });
        assert_eq!(drain(&entry, &poller), vec![(1, KT_OK)]);
        // Nothing further, and the pending table is empty again.
        assert_eq!(drain(&entry, &poller), vec![]);
        assert!(!entry.retire(1), "the request must already be retired");
    }

    #[test]
    fn a_panicking_request_answers_rather_than_hanging() {
        // A panicking Tokio task is otherwise swallowed by the JoinSet and the JVM waits forever.
        let entry = runtime();
        let poller = entry.queue.poller();
        spawn_request(&entry, 2, async { panic!("boom") });
        assert_eq!(drain(&entry, &poller), vec![(2, KT_ERR_PANIC)]);
    }

    #[test]
    fn cancelling_answers_once_and_the_late_result_is_dropped() {
        let entry = runtime();
        let poller = entry.queue.poller();
        spawn_request(&entry, 3, async {
            tokio::time::sleep(std::time::Duration::from_millis(400)).await;
            Pending::ack(3)
        });
        std::thread::sleep(std::time::Duration::from_millis(50));
        entry.cancel(3);
        assert_eq!(drain(&entry, &poller), vec![(3, KT_ERR_CANCELLED)]);
        // The operation's own result must not arrive afterwards.
        std::thread::sleep(std::time::Duration::from_millis(500));
        assert_eq!(
            drain(&entry, &poller),
            vec![],
            "a cancelled request must answer exactly once"
        );
    }

    #[test]
    fn shutdown_answers_everything_still_outstanding() {
        // This is what removes the JVM's 60-second "await pending callbacks" latch: no
        // continuation is left waiting for a completion that will never come.
        let entry = runtime();
        let poller = entry.queue.poller();
        for req_id in 10..14 {
            spawn_request(&entry, req_id, async move {
                tokio::time::sleep(std::time::Duration::from_secs(30)).await;
                Pending::ack(req_id)
            });
        }
        std::thread::sleep(std::time::Duration::from_millis(50));
        free_runtime(entry.clone());

        let mut answered: Vec<u64> = drain(&entry, &poller)
            .into_iter()
            .map(|(id, _)| id)
            .collect();
        answered.sort_unstable();
        assert_eq!(answered, vec![10, 11, 12, 13]);
    }

    #[test]
    fn cancelling_an_unknown_request_is_harmless() {
        let entry = runtime();
        entry.cancel(9_999);
    }
}
