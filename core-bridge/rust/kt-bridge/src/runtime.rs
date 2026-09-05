//! Runtime lifecycle, and the pending-request table that makes shutdown deterministic.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::telemetry::BridgeMeter;
use parking_lot::Mutex;
use temporalio_common::telemetry::metrics::CoreMeter;
use temporalio_common::telemetry::{CoreTelemetry, Logger, TelemetryOptions};
use temporalio_sdk_core::{CoreRuntime, RuntimeOptions as CoreRuntimeOptions, TokioRuntimeBuilder};
use tokio_util::sync::CancellationToken;

use crate::abi::{KT_ERR_CANCELLED, KT_ERR_PANIC, KT_ERR_SHUTDOWN};
use crate::error::{KtError, KtResult};
use futures_util::FutureExt;

use crate::queue::{Pending, Queue, Sender};

pub struct RuntimeEntry {
    pub core: CoreRuntime,
    pub queue: Queue,
    pub metrics: Option<Arc<BridgeMeter>>,
    /// Retained until a successful FFI buffer copy; a size probe must not consume samples.
    pub(crate) metric_batch: Mutex<Option<Vec<u8>>>,
    /// Cancellation tokens for in-flight operations, keyed by request id.
    ///
    /// Exists to uphold the one-completion invariant: on shutdown every entry still here is
    /// answered with `KT_ERR_SHUTDOWN`, so the JVM never waits on a completion that will not come.
    /// The previous bridge had no such guarantee and blocked on a 60-second latch instead.
    pending: Arc<Mutex<HashMap<u64, CancellationToken>>>,
    /// Set by shutdown. A request registered after this point would otherwise never be answered:
    /// the drain has already run, so nothing would retire it.
    closed: AtomicBool,
}

impl RuntimeEntry {
    pub fn sender(&self) -> Sender {
        self.queue.sender()
    }

    /// Rejects new operations once shutdown starts, and preserves an earlier duplicate id.
    pub fn register(&self, req_id: u64) -> KtResult<CancellationToken> {
        let mut pending = self.pending.lock();
        // Checked under the lock so it cannot interleave with the drain, which takes the same
        // lock: either this entry is drained, or the call is rejected. Never neither.
        if self.closed.load(Ordering::Acquire) {
            return Err(KtError::Shutdown);
        }
        if pending.contains_key(&req_id) {
            return Err(KtError::InvalidArgument(format!(
                "duplicate req_id {req_id}"
            )));
        }
        let token = CancellationToken::new();
        pending.insert(req_id, token.clone());
        Ok(token)
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
        // Retirement and enqueue use this same lock, so shutdown cannot close the queue
        // between them and lose a terminal completion.
        let mut pending = self.pending.lock();
        self.closed.store(true, Ordering::Release);
        let sender = self.queue.sender();
        for (req_id, token) in pending.drain() {
            token.cancel();
            sender.push(Pending::error(
                req_id,
                KT_ERR_SHUTDOWN,
                "runtime is shutting down",
            ));
        }
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

    let telemetry = config.telemetry.as_ref();
    let metrics = telemetry
        .filter(|options| options.buffer_metrics)
        .map(|_| Arc::new(BridgeMeter::default()));
    let logging = telemetry
        .filter(|options| options.forward_logs)
        .map(|options| Logger::Forward {
            filter: if options.log_filter.is_empty() {
                "WARN".into()
            } else {
                options.log_filter.clone()
            },
        });
    let heartbeat = config.worker_heartbeat_interval_millis.unwrap_or(60_000);
    let options = CoreRuntimeOptions::builder()
        .telemetry_options(
            TelemetryOptions::builder()
                .maybe_logging(logging)
                .maybe_metrics(metrics.clone().map(|meter| meter as Arc<dyn CoreMeter>))
                .build(),
        )
        .heartbeat_interval((heartbeat != 0).then(|| std::time::Duration::from_millis(heartbeat)))
        .build()
        .map_err(KtError::InvalidArgument)?;

    let core = CoreRuntime::new(options, builder).map_err(KtError::from)?;
    Ok(Arc::new(RuntimeEntry {
        core,
        queue: Queue::new(),
        metrics,
        metric_batch: Mutex::new(None),
        pending: Arc::new(Mutex::new(HashMap::new())),
        closed: AtomicBool::new(false),
    }))
}

pub fn drain_metrics(entry: &RuntimeEntry) -> crate::proto::MetricBatch {
    let mut batch = entry
        .metrics
        .as_ref()
        .map(|meter| meter.drain())
        .unwrap_or_default();
    batch.logs.extend(
        entry
            .core
            .telemetry()
            .fetch_buffered_logs()
            .into_iter()
            .map(|log| crate::proto::LogRecord {
                target: log.target,
                message: log.message,
                level: log.level.to_string(),
                fields_json: serde_json::to_string(&log.fields)
                    .expect("log fields serialize as JSON"),
            }),
    );
    batch
}

pub fn free_runtime(entry: Arc<RuntimeEntry>) {
    entry.drain_pending_for_shutdown();
}

pub fn runtime_info(_entry: &RuntimeEntry) -> crate::proto::RuntimeInfo {
    crate::proto::RuntimeInfo {
        core_version: env!("TEMPORAL_SDK_CORE_VERSION").to_string(),
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
pub fn spawn_request<F>(entry: &Arc<RuntimeEntry>, req_id: u64, op: F) -> KtResult
where
    F: std::future::Future<Output = Pending> + Send + 'static,
{
    let sender = entry.sender();
    let token = entry.register(req_id)?;
    // Tasks own only request bookkeeping. Even a temporary Weak::upgrade can become the last
    // RuntimeEntry reference during free and drop Tokio from inside its own worker thread.
    let pending = entry.pending.clone();
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
        let mut pending = pending.lock();
        if pending.remove(&req_id).is_some() {
            sender.push(result);
        }
        // Otherwise result drops here and releases any handle the caller never received.
    });
    Ok(())
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
        spawn_request(&entry, 1, async { Pending::ack(1) }).unwrap();
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
        spawn_request(&entry, 2, async { panic!("boom") }).unwrap();
        assert_eq!(drain(&entry, &poller), vec![(2, KT_ERR_PANIC)]);
    }

    #[test]
    fn cancelling_answers_once_and_the_late_result_is_dropped() {
        let entry = runtime();
        let poller = entry.queue.poller();
        spawn_request(&entry, 3, async {
            tokio::time::sleep(std::time::Duration::from_millis(400)).await;
            Pending::ack(3)
        })
        .unwrap();
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
            })
            .unwrap();
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
    fn duplicate_ids_preserve_the_original_cancellation_and_shutdown_rejects_new_requests() {
        let entry = runtime();
        let token = entry.register(1).unwrap();
        assert!(matches!(
            entry.register(1),
            Err(KtError::InvalidArgument(_))
        ));
        entry.cancel(1);
        assert!(token.is_cancelled());
        free_runtime(entry.clone());
        assert!(matches!(
            spawn_request(&entry, 2, async { Pending::ack(2) }),
            Err(KtError::Shutdown)
        ));
    }

    #[test]
    fn shutdown_does_not_wait_on_or_drop_the_runtime_from_a_request_task() {
        let entry = runtime();
        let weak = Arc::downgrade(&entry);
        spawn_request(&entry, 1, std::future::pending()).unwrap();
        free_runtime(entry);
        assert!(weak.upgrade().is_none());
    }

    #[test]
    fn cancelling_an_unknown_request_is_harmless() {
        let entry = runtime();
        entry.cancel(9_999);
    }
}
