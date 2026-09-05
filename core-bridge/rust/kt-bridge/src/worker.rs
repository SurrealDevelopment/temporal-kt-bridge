//! Worker lifecycle and the Rust-owned poll loops.
//!
//! The most consequential difference from the C bridge is that **Core's poll loops live here**,
//! not in Kotlin.
//!
//! `WorkerActivityTasks::shutdown()` only completes once the language side has polled until Core
//! returns [`PollError::ShutDown`]. Under the C bridge that contract leaked all the way up into
//! `ManagedWorker`, which grew a cancellation-safe re-poll loop purely to satisfy it -- and any
//! future path that cancelled the poll coroutine reintroduced an unkillable hang. Here three
//! Tokio tasks own the loops and exit only on `ShutDown`. Kotlin consumes a channel; cancelling
//! that consumer cannot break Core, because Kotlin no longer drives the contract at all.
//!
//! Backpressure needs no explicit bound: Core gates polling on slot reservation, so in-flight
//! task count is capped by the worker's own slot configuration.

use std::sync::Arc;

use parking_lot::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use temporalio_common::worker::WorkerTaskTypes;
use temporalio_sdk_core::ResourceBasedSlotsOptions;
use temporalio_sdk_core::Worker as CoreWorker;
use temporalio_sdk_core::{
    PollError, ResourceBasedTunerConfig, ResourceSlotOptions, SlotSupplierOptions,
    TunerHolderOptions, WorkerVersioningStrategy,
};

use crate::abi::{KT_ERR_FAILED, KtKind};
use crate::error::{KtError, KtResult};
use crate::queue::{Pending, Sender};

/// A worker's lifecycle, expressed so that the finalized state has no worker to reach for.
///
/// The C bridge modelled this as `Option<Arc<Worker>>` where `None` meant finalized, which put
/// the burden on every entry point to remember to check. One did not: a heartbeat racing
/// `finalize_shutdown` unwrapped `None` and aborted the JVM. Here `Finalized` simply has no field
/// to unwrap, so the mistake is not expressible.
pub enum WorkerState {
    Running {
        core: Arc<CoreWorker>,
        started: bool,
    },
    Draining {
        core: Arc<CoreWorker>,
    },
    Finalized,
}

pub struct WorkerEntry {
    pub state: Mutex<WorkerState>,
    pub sender: Sender,
    /// The runtime this worker belongs to.
    ///
    /// Held because synchronous Core calls made from a JVM thread still spawn internally --
    /// `record_activity_heartbeat` does -- and panic with "there is no reactor running" without a
    /// runtime context. Keeping the handle here means every entry point can enter it without the
    /// caller having to pass the runtime in.
    pub tokio: tokio::runtime::Handle,
    /// How many poll loops are still running.
    ///
    /// Shutdown must not finalize until every pump has seen `PollError::ShutDown`, because Core's
    /// own shutdown does not complete until lang has polled each stream to the end.
    pub live_pumps: Arc<AtomicUsize>,
    pub resources: Option<Arc<crate::jvm_slots::ResourceGate>>,
    shutdown_lock: tokio::sync::Mutex<()>,
    pumps: Mutex<Vec<tokio::task::AbortHandle>>,
}

impl WorkerEntry {
    pub fn new(core: Arc<CoreWorker>, sender: Sender, tokio: tokio::runtime::Handle) -> Self {
        Self {
            state: Mutex::new(WorkerState::Running {
                core,
                started: false,
            }),
            sender,
            tokio,
            live_pumps: Arc::new(AtomicUsize::new(0)),
            resources: None,
            shutdown_lock: tokio::sync::Mutex::new(()),
            pumps: Mutex::new(Vec::new()),
        }
    }
}

impl WorkerEntry {
    /// The only way to reach the Core worker. There is no `unwrap` anywhere in this file.
    pub fn core(&self) -> KtResult<Arc<CoreWorker>> {
        match &*self.state.lock() {
            WorkerState::Running { core, .. } | WorkerState::Draining { core } => Ok(core.clone()),
            WorkerState::Finalized => Err(KtError::WorkerShutDown),
        }
    }
}

/// Which stream a pushed task came from.
#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(u64)]
pub enum TaskKind {
    WorkflowActivation = 0,
    Activity = 1,
    Nexus = 2,
}

impl TaskKind {
    fn completion_kind(self) -> KtKind {
        match self {
            TaskKind::WorkflowActivation => KtKind::TaskWorkflowActivation,
            TaskKind::Activity => KtKind::TaskActivity,
            TaskKind::Nexus => KtKind::TaskNexus,
        }
    }
}

struct Leaving(Arc<AtomicUsize>);

impl Drop for Leaving {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

/// Runs one poll loop until Core reports shutdown.
///
/// Exiting only on [`PollError::ShutDown`] is what discharges Core's contract inside Rust. A
/// transport error is reported and ends this stream: Core retries non-fatal errors internally, so
/// anything surfacing here is terminal for the stream.
async fn pump(
    core: Arc<CoreWorker>,
    kind: TaskKind,
    sender: Sender,
    worker_handle: u64,
    leaving: Leaving,
) {
    // Constructed before spawn so cancellation before the first poll still decrements the count.
    let _leaving = leaving;
    // Rebound AFTER `_leaving` on purpose. Locals drop in reverse order and parameters drop after
    // all locals, so as a parameter `core` would outlive `_leaving`: the decrement would run
    // while this pump still held its Arc<CoreWorker>, and shutdown()'s Arc::try_unwrap could
    // fail spuriously and skip finalize_shutdown. As a later local it drops first.
    let core = core;
    use futures_util::FutureExt;
    let outcome = std::panic::AssertUnwindSafe(async {
        let mut reported = false;

        loop {
            let polled = match kind {
                TaskKind::WorkflowActivation => core
                    .poll_workflow_activation()
                    .await
                    .map(|task| prost::Message::encode_to_vec(&task)),
                TaskKind::Activity => core
                    .poll_activity_task()
                    .await
                    .map(|task| prost::Message::encode_to_vec(&task)),
                TaskKind::Nexus => core
                    .poll_nexus_task()
                    .await
                    .map(|task| prost::Message::encode_to_vec(&task)),
            };

            match polled {
                Ok(bytes) => sender.push(
                    Pending::ack(crate::runtime::PUSHED)
                        .kind(kind.completion_kind())
                        .payload(bytes)
                        .aux0(worker_handle)
                        .aux1(kind as u64),
                ),
                Err(PollError::ShutDown) => {
                    // Tell Kotlin the stream is finished so it can close its channel, rather than
                    // leaving a consumer suspended forever.
                    sender.push(
                        Pending::ack(crate::runtime::PUSHED)
                            .kind(KtKind::TaskStreamEnd)
                            .aux0(worker_handle)
                            .aux1(kind as u64),
                    );
                    return;
                }
                Err(err) => {
                    // Report it -- once -- but keep polling. Core's own shutdown for this stream is
                    // only released by poll() observing ShutDown, so a pump that quits on an error
                    // would leave finalize_shutdown waiting forever. Core streams yield Err items and
                    // continue; a bad namespace or credentials surface here as a stream of them, so
                    // the report is not repeated and the loop backs off between attempts.
                    if !reported {
                        reported = true;
                        core.initiate_shutdown();
                        // aux1 carries the stream kind: without it Kotlin knows a stream died but not
                        // which of its three channels to close.
                        sender.push(
                            Pending::error(crate::runtime::PUSHED, KT_ERR_FAILED, err.to_string())
                                .kind(KtKind::WorkerFailed)
                                .aux0(worker_handle)
                                .aux1(kind as u64),
                        );
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                }
            }
        }
    })
    .catch_unwind()
    .await;
    if let Err(payload) = outcome {
        sender.push(
            Pending::error(
                crate::runtime::PUSHED,
                crate::abi::KT_ERR_PANIC,
                format!(
                    "worker poll panic: {}",
                    crate::panic::panic_message(&payload)
                ),
            )
            .kind(KtKind::WorkerFailed)
            .aux0(worker_handle)
            .aux1(kind as u64),
        );
        sender.push(
            Pending::ack(crate::runtime::PUSHED)
                .kind(KtKind::TaskStreamEnd)
                .aux0(worker_handle)
                .aux1(kind as u64),
        );
        core.initiate_shutdown();
    }
}

pub fn start(
    entry: &Arc<WorkerEntry>,
    runtime: &Arc<crate::runtime::RuntimeEntry>,
    worker_handle: u64,
) -> KtResult {
    let mut guard = entry.state.lock();
    let core = match &*guard {
        WorkerState::Running { core, started } => {
            if *started {
                return Ok(()); // idempotent
            }
            core.clone()
        }
        WorkerState::Draining { .. } | WorkerState::Finalized => {
            return Err(KtError::WorkerShutDown);
        }
    };
    if let WorkerState::Running { started, .. } = &mut *guard {
        *started = true;
    }
    // Keep the state lock until every task is recorded: free must not miss tasks spawned after
    // it has already invalidated the worker.
    let mut pumps = entry.pumps.lock();
    entry.live_pumps.store(3, Ordering::Release);
    let _entered = runtime.core.tokio_handle().enter();
    for kind in [
        TaskKind::WorkflowActivation,
        TaskKind::Activity,
        TaskKind::Nexus,
    ] {
        pumps.push(
            tokio::spawn(pump(
                core.clone(),
                kind,
                entry.sender.clone(),
                worker_handle,
                Leaving(entry.live_pumps.clone()),
            ))
            .abort_handle(),
        );
    }
    Ok(())
}

/// Force-close a worker whose caller will no longer deliver task completions. Initiating
/// shutdown alone cannot stop polls blocked on those missing completions.
pub fn free(entry: &WorkerEntry) {
    let mut state = entry.state.lock();
    let previous = std::mem::replace(&mut *state, WorkerState::Finalized);
    for pump in entry.pumps.lock().drain(..) {
        pump.abort();
    }
    if let WorkerState::Running { core, .. } | WorkerState::Draining { core } = previous {
        let _entered = entry.tokio.enter();
        core.initiate_shutdown();
    }
}

/// Builds and validates Core's worker config from the protobuf options.
pub fn worker_config(
    options: &crate::proto::WorkerOptions,
) -> KtResult<temporalio_sdk_core::WorkerConfig> {
    worker_config_with_resource_gate(options, None)
}

pub fn worker_config_with_resource_gate(
    options: &crate::proto::WorkerOptions,
    resource_gate: Option<Arc<crate::jvm_slots::ResourceGate>>,
) -> KtResult<temporalio_sdk_core::WorkerConfig> {
    use std::collections::{HashMap, HashSet};
    use temporalio_sdk_core::WorkflowErrorType;

    if options.namespace.trim().is_empty() || options.task_queue.trim().is_empty() {
        return Err(KtError::InvalidArgument(
            "namespace and task_queue must not be blank".into(),
        ));
    }
    let ratio = options.nonsticky_to_sticky_poll_ratio.unwrap_or(0.2);
    if !ratio.is_finite() || !(0.0..=1.0).contains(&ratio) {
        return Err(KtError::InvalidArgument(
            "nonsticky_to_sticky_poll_ratio must be between 0 and 1".into(),
        ));
    }
    if options
        .nondeterminism_as_workflow_fail_for_types
        .iter()
        .any(|s| s.trim().is_empty())
    {
        return Err(KtError::InvalidArgument(
            "nondeterminism workflow type names must not be blank".into(),
        ));
    }
    let errors = || HashSet::from([WorkflowErrorType::Nondeterminism]);
    let per_type: HashMap<_, _> = options
        .nondeterminism_as_workflow_fail_for_types
        .iter()
        .map(|name| (name.clone(), errors()))
        .collect();

    temporalio_sdk_core::WorkerConfig::builder()
        .tuner(tuner(options, resource_gate)?)
        .namespace(options.namespace.clone())
        .task_queue(options.task_queue.clone())
        .max_cached_workflows(options.max_cached_workflows as usize)
        .task_types(WorkerTaskTypes {
            enable_workflows: !options.no_workflows,
            enable_local_activities: !options.no_local_activities,
            enable_remote_activities: !options.no_remote_activities,
            enable_nexus: options.enable_nexus,
        })
        .versioning_strategy(versioning(options)?)
        .maybe_client_identity_override(
            (!options.identity.is_empty()).then(|| options.identity.clone()),
        )
        .maybe_workflow_task_poller_behavior(poller(options.workflow_poller_behavior.as_ref())?)
        .maybe_activity_task_poller_behavior(poller(options.activity_poller_behavior.as_ref())?)
        .maybe_nexus_task_poller_behavior(poller(options.nexus_poller_behavior.as_ref())?)
        .max_heartbeat_throttle_interval(duration_millis(
            options
                .max_heartbeat_throttle_interval_millis
                .unwrap_or(60_000),
        )?)
        .default_heartbeat_throttle_interval(duration_millis(
            options
                .default_heartbeat_throttle_interval_millis
                .unwrap_or(30_000),
        )?)
        .maybe_max_worker_activities_per_second(rate(options.max_activities_per_second)?)
        .maybe_max_task_queue_activities_per_second(rate(
            options.max_task_queue_activities_per_second,
        )?)
        .nonsticky_to_sticky_poll_ratio(ratio)
        .sticky_queue_schedule_to_start_timeout(duration_millis(
            options
                .sticky_queue_schedule_to_start_timeout_millis
                .unwrap_or(10_000),
        )?)
        .maybe_graceful_shutdown_period(
            options
                .graceful_shutdown_period_millis
                .map(duration_millis)
                .transpose()?,
        )
        .workflow_failure_errors(if options.nondeterminism_as_workflow_fail {
            errors()
        } else {
            HashSet::new()
        })
        .workflow_types_to_failure_errors(per_type)
        .max_eager_activity_reservations_per_workflow_task(
            options
                .max_eager_activity_reservations_per_workflow_task
                .unwrap_or(3) as usize,
        )
        .disable_payload_error_limit(options.disable_payload_error_limit)
        .build()
        .map_err(|e| KtError::InvalidArgument(format!("invalid worker config: {e}")))
}

fn duration_millis(value: u64) -> KtResult<std::time::Duration> {
    // The JVM API uses signed milliseconds. Reject values that could not originate there.
    if value > i64::MAX as u64 {
        return Err(KtError::InvalidArgument(
            "duration exceeds signed 64-bit milliseconds".into(),
        ));
    }
    Ok(std::time::Duration::from_millis(value))
}

fn rate(value: Option<f64>) -> KtResult<Option<f64>> {
    if value.is_some_and(|v| !v.is_finite() || v < 0.0) {
        return Err(KtError::InvalidArgument(
            "activity rate limits must be finite and nonnegative".into(),
        ));
    }
    Ok(value.filter(|v| *v > 0.0))
}

fn poller(
    options: Option<&crate::proto::PollerBehavior>,
) -> KtResult<Option<temporalio_sdk_core::PollerBehavior>> {
    use crate::proto::poller_behavior::Strategy;
    use temporalio_sdk_core::PollerBehavior;
    options
        .map(|options| match options.strategy.as_ref() {
            Some(Strategy::SimpleMaximum(maximum)) => {
                Ok(PollerBehavior::SimpleMaximum(*maximum as usize))
            }
            Some(Strategy::Autoscaling(a)) => Ok(PollerBehavior::Autoscaling {
                minimum: a.minimum as usize,
                maximum: a.maximum as usize,
                initial: a.initial as usize,
            }),
            None => Err(KtError::InvalidArgument(
                "poller behavior must select a strategy".into(),
            )),
        })
        .transpose()
}

fn versioning(options: &crate::proto::WorkerOptions) -> KtResult<WorkerVersioningStrategy> {
    use temporalio_common::worker::{
        VersioningBehavior, WorkerDeploymentOptions, WorkerDeploymentVersion,
    };
    let Some(deployment) = options.deployment_options.as_ref() else {
        return Ok(WorkerVersioningStrategy::None {
            build_id: options.build_id.clone(),
        });
    };
    if deployment.deployment_name.trim().is_empty() || deployment.build_id.trim().is_empty() {
        return Err(KtError::InvalidArgument(
            "deployment name and build ID must not be blank".into(),
        ));
    }
    let behavior = match deployment.default_versioning_behavior {
        0 => None,
        1 => Some(VersioningBehavior::Pinned),
        2 => Some(VersioningBehavior::AutoUpgrade),
        _ => {
            return Err(KtError::InvalidArgument(
                "unknown default versioning behavior".into(),
            ));
        }
    };
    Ok(WorkerVersioningStrategy::WorkerDeploymentBased(
        WorkerDeploymentOptions::new(
            WorkerDeploymentVersion::builder()
                .deployment_name(deployment.deployment_name.clone())
                .build_id(deployment.build_id.clone())
                .build(),
        )
        .use_worker_versioning(deployment.use_worker_versioning)
        .maybe_default_versioning_behavior(behavior)
        .build(),
    ))
}

fn tuner(
    options: &crate::proto::WorkerOptions,
    resource_gate: Option<Arc<crate::jvm_slots::ResourceGate>>,
) -> KtResult<Arc<dyn temporalio_sdk_core::WorkerTuner + Send + Sync>> {
    fn slot<K: temporalio_sdk_core::SlotKind + Send + Sync + 'static>(
        fixed_slots: Option<u32>,
        limits: Option<&crate::proto::ResourceSlotLimits>,
        resource_gate: Option<&Arc<crate::jvm_slots::ResourceGate>>,
        kind: u32,
        sticky: bool,
    ) -> KtResult<SlotSupplierOptions<K>> {
        match (fixed_slots, limits) {
            (Some(_), Some(_)) => Err(KtError::InvalidArgument(
                "a slot supplier cannot be both fixed and resource-based".into(),
            )),
            (_, Some(l)) => {
                if l.maximum_slots == 0 || l.minimum_slots > l.maximum_slots {
                    return Err(KtError::InvalidArgument(
                        "resource slots require minimum <= maximum and maximum > 0".into(),
                    ));
                }
                if let Some(gate) = resource_gate {
                    duration_millis(l.ramp_throttle_millis)?;
                    return Ok(SlotSupplierOptions::Custom(crate::jvm_slots::supplier(
                        gate.clone(),
                        l,
                        kind,
                        sticky,
                    )));
                }
                Ok(SlotSupplierOptions::ResourceBased(
                    ResourceSlotOptions::new(
                        l.minimum_slots as usize,
                        l.maximum_slots as usize,
                        duration_millis(l.ramp_throttle_millis)?,
                    ),
                ))
            }
            (Some(0), _) => Err(KtError::InvalidArgument(
                "fixed slot counts must be positive".into(),
            )),
            (slots, _) => Ok(SlotSupplierOptions::FixedSize {
                slots: slots.unwrap_or(100) as usize,
            }),
        }
    }
    if !options.no_workflows && options.max_cached_workflows > 0 {
        let workflow_limit = options
            .workflow_resource_limits
            .as_ref()
            .map(|l| l.maximum_slots)
            .or(options.max_concurrent_workflow_tasks)
            .unwrap_or(100);
        if workflow_limit < 2 {
            return Err(KtError::InvalidArgument(
                "cached workflows require at least 2 workflow slots".into(),
            ));
        }
    }
    let jvm = options
        .resource_tuner
        .as_ref()
        .is_some_and(|rb| rb.jvm_resource_based);
    let resource_gate = if jvm {
        Some(resource_gate.unwrap_or_else(|| Arc::new(crate::jvm_slots::ResourceGate::new())))
    } else {
        None
    };
    let resource_based_config = options
        .resource_tuner
        .as_ref()
        .map(|rb| {
            if !rb.target_memory_usage.is_finite()
                || !(0.0..=1.0).contains(&rb.target_memory_usage)
                || !rb.target_cpu_usage.is_finite()
                || !(0.0..=1.0).contains(&rb.target_cpu_usage)
            {
                return Err(KtError::InvalidArgument(
                    "resource targets must be between 0 and 1".into(),
                ));
            }
            if [
                rb.memory_p_gain,
                rb.memory_i_gain,
                rb.memory_d_gain,
                rb.memory_output_threshold,
                rb.cpu_p_gain,
                rb.cpu_i_gain,
                rb.cpu_d_gain,
                rb.cpu_output_threshold,
            ]
            .iter()
            .any(|v| !v.is_finite())
            {
                return Err(KtError::InvalidArgument(
                    "resource PID gains and thresholds must be finite".into(),
                ));
            }
            Ok(ResourceBasedTunerConfig::Options(
                ResourceBasedSlotsOptions::builder()
                    .target_mem_usage(rb.target_memory_usage)
                    .target_cpu_usage(rb.target_cpu_usage)
                    .mem_p_gain(rb.memory_p_gain)
                    .mem_i_gain(rb.memory_i_gain)
                    .mem_d_gain(rb.memory_d_gain)
                    .mem_output_threshold(rb.memory_output_threshold)
                    .cpu_p_gain(rb.cpu_p_gain)
                    .cpu_i_gain(rb.cpu_i_gain)
                    .cpu_d_gain(rb.cpu_d_gain)
                    .cpu_output_threshold(rb.cpu_output_threshold)
                    .build(),
            ))
        })
        .transpose()?
        .filter(|_| !jvm);
    let holder = TunerHolderOptions::builder()
        .maybe_resource_based_config(resource_based_config)
        .workflow_slot_options(slot(
            options.max_concurrent_workflow_tasks,
            options.workflow_resource_limits.as_ref(),
            resource_gate.as_ref(),
            0,
            !options.no_workflows && options.max_cached_workflows > 0,
        )?)
        .activity_slot_options(slot(
            options.max_concurrent_activities,
            options.activity_resource_limits.as_ref(),
            resource_gate.as_ref(),
            1,
            false,
        )?)
        .local_activity_slot_options(slot(
            options.max_concurrent_local_activities,
            options.local_activity_resource_limits.as_ref(),
            resource_gate.as_ref(),
            2,
            false,
        )?)
        .nexus_slot_options(slot(
            options.max_concurrent_nexus_tasks,
            options.nexus_resource_limits.as_ref(),
            resource_gate.as_ref(),
            3,
            false,
        )?)
        .build()
        .map_err(|e| KtError::InvalidArgument(format!("invalid slot options: {e}")))?
        .build_tuner_holder()
        .map_err(KtError::from)?;
    Ok(Arc::new(holder))
}

/// Shuts the worker down without ever leaving Core's poll contract unsatisfied.
///
/// Order matters and is the area with the highest historical defect density in this bridge:
///
///   1. `initiate_shutdown` tells Core to stop handing out work.
///   2. Wait for all three pumps to observe `PollError::ShutDown` and exit. Core's own shutdown
///      does not complete until every stream has been polled to the end, so finalizing before
///      this point is what used to hang `awaitShutdown` forever.
///   3. Move to `Finalized` -- which structurally has no worker to reach for -- and drop the last
///      reference so Core can finish.
///
/// `grace` bounds step 2. Exceeding it is reported rather than hidden, because silently
/// finalizing early is how a stranded activity completion goes unnoticed.
pub async fn shutdown(entry: Arc<WorkerEntry>, grace: std::time::Duration) -> Result<(), String> {
    // Concurrent shutdowns must not each hold a Core Arc while waiting for the other to release it.
    let _shutdown = entry.shutdown_lock.lock().await;
    let (core, started) = {
        let mut guard = entry.state.lock();
        match std::mem::replace(&mut *guard, WorkerState::Finalized) {
            WorkerState::Running { core, started } => {
                *guard = WorkerState::Draining { core: core.clone() };
                (core, started)
            }
            WorkerState::Draining { core } => {
                *guard = WorkerState::Draining { core: core.clone() };
                (core, true)
            }
            WorkerState::Finalized => return Ok(()), // idempotent
        }
    };

    core.initiate_shutdown();

    if !started {
        // No pumps were ever spawned, so nothing will poll the streams to ShutDown -- and Core's
        // finalize_shutdown waits on exactly that, forever. Dropping the never-polled worker
        // without finalizing is what the other SDKs do here.
        *entry.state.lock() = WorkerState::Finalized;
        drop(core);
        return Ok(());
    }

    let deadline = std::time::Instant::now() + grace;
    while entry.live_pumps.load(Ordering::Acquire) > 0 {
        if std::time::Instant::now() >= deadline {
            // Stay in Draining: the pumps are still delivering tasks, and flipping to Finalized
            // now would make every completion fail with WorkerShutDown while Core waits for
            // exactly those completions -- a deadlock. Finalize in the background once the pumps
            // really have ended, and tell the caller the truth about the grace period.
            let bg = entry.clone();
            entry.tokio.spawn(async move {
                while bg.live_pumps.load(Ordering::Acquire) > 0 {
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                }
                let _shutdown = bg.shutdown_lock.lock().await;
                if let Ok(core) = bg.core() {
                    let _ = finalize(&bg, core, std::time::Instant::now() + grace).await;
                }
            });
            return Err("shut down before every poll stream reported ShutDown".to_string());
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }

    // A concurrent force-close has already invalidated and aborted the worker.
    if matches!(*entry.state.lock(), WorkerState::Finalized) {
        return Ok(());
    }
    finalize(
        &entry,
        core,
        deadline.max(std::time::Instant::now() + grace),
    )
    .await
}

/// Moves to `Finalized` and lets Core finish, once every other reference is gone.
async fn finalize(
    entry: &WorkerEntry,
    core: Arc<CoreWorker>,
    deadline: std::time::Instant,
) -> Result<(), String> {
    *entry.state.lock() = WorkerState::Finalized;

    // A pump that has just decremented live_pumps may still be a few instructions from dropping
    // its Arc, and an in-flight complete()/heartbeat holds a clone for the length of the call.
    // Neither is a leak, so wait for them rather than declaring the worker unfinalizable on the
    // first attempt.
    let mut core = core;
    loop {
        match Arc::try_unwrap(core) {
            Ok(worker) => {
                worker.finalize_shutdown().await;
                return Ok(());
            }
            Err(shared) => {
                if std::time::Instant::now() >= deadline {
                    // Returned, never panicked: the C bridge's equivalent unwrapped here and
                    // aborted the JVM.
                    *entry.state.lock() = WorkerState::Draining { core: shared };
                    return Err("worker still referenced elsewhere; not finalized".to_string());
                }
                core = shared;
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        }
    }
}

/// Completes a task of the given kind from its encoded protobuf.
pub async fn complete(core: &Arc<CoreWorker>, task_kind: u32, bytes: &[u8]) -> Result<(), String> {
    match task_kind {
        0 => {
            let completion = prost::Message::decode(bytes).map_err(|e| e.to_string())?;
            core.complete_workflow_activation(completion)
                .await
                .map_err(|e| e.to_string())
        }
        1 => {
            let completion = prost::Message::decode(bytes).map_err(|e| e.to_string())?;
            core.complete_activity_task(completion)
                .await
                .map_err(|e| e.to_string())
        }
        2 => {
            let completion = prost::Message::decode(bytes).map_err(|e| e.to_string())?;
            core.complete_nexus_task(completion)
                .await
                .map_err(|e| e.to_string())
        }
        other => Err(format!("unknown task kind {other}")),
    }
}

/// Records an activity heartbeat.
///
/// Runs inside the runtime: this is called synchronously from a JVM thread, and Core spawns
/// internally, which panics with "there is no reactor running" without a context. That panic was
/// contained by `kt_export!` -- so the call merely returned an error -- but it poisoned a
/// `LazyLock` inside Core, and every later worker operation in the process then failed with
/// "LazyLock instance has previously been poisoned". A contained panic is not a harmless one when
/// it leaves shared state broken.
pub fn heartbeat(entry: &WorkerEntry, core: &Arc<CoreWorker>, bytes: &[u8]) -> KtResult {
    let heartbeat: temporalio_common::protos::coresdk::ActivityHeartbeat =
        prost::Message::decode(bytes)?;
    entry
        .tokio
        .block_on(async { core.record_activity_heartbeat(heartbeat) });
    Ok(())
}
