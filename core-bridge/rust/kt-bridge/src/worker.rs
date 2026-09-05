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
            live_pumps: Arc::new(AtomicUsize::new(3)),
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
    live: Arc<AtomicUsize>,
) {
    // Decrement on every exit path, including an unwind, so shutdown cannot wait forever on a
    // pump that has already gone.
    struct Leaving(Arc<AtomicUsize>);
    impl Drop for Leaving {
        fn drop(&mut self) {
            self.0.fetch_sub(1, Ordering::AcqRel);
        }
    }
    let _leaving = Leaving(live);

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
                // aux1 carries the stream kind: without it Kotlin knows a stream died but not
                // which of its three channels to close.
                sender.push(
                    Pending::error(crate::runtime::PUSHED, KT_ERR_FAILED, err.to_string())
                        .kind(KtKind::WorkerFailed)
                        .aux0(worker_handle)
                        .aux1(kind as u64),
                );
                // Still end the stream, so the consumer closes rather than waiting forever.
                sender.push(
                    Pending::ack(crate::runtime::PUSHED)
                        .kind(KtKind::TaskStreamEnd)
                        .aux0(worker_handle)
                        .aux1(kind as u64),
                );
                return;
            }
        }
    }
}

/// Spawns the three poll loops.
///
/// Two things here are load-bearing and were wrong in the first draft:
///
///   * `JoinSet::spawn` calls `tokio::spawn`, which panics with "there is no reactor running"
///     unless a runtime is entered. This runs on the JVM's calling thread, so the handle must be
///     entered explicitly.
///   * The pumps are *detached*, not held in a `JoinSet` owned by the state. A `JoinSet` aborts
///     its tasks on drop, so replacing `Running` with `Draining` would have killed all three
///     mid-`poll_*` -- precisely the "lang stopped polling before PollError::ShutDown" violation
///     this module exists to make impossible. Each pump instead owns an `Arc<CoreWorker>` and
///     runs to `ShutDown` on its own; `finalize` waits for them via the completion counter.
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
    drop(guard);

    let _entered = runtime.core.tokio_handle().enter();
    for kind in [
        TaskKind::WorkflowActivation,
        TaskKind::Activity,
        TaskKind::Nexus,
    ] {
        tokio::spawn(pump(
            core.clone(),
            kind,
            entry.sender.clone(),
            worker_handle,
            entry.live_pumps.clone(),
        ));
    }
    Ok(())
}

/// Builds Core's worker config from the protobuf options.
pub fn worker_config(
    options: &crate::proto::WorkerOptions,
) -> KtResult<temporalio_sdk_core::WorkerConfig> {
    if options.namespace.is_empty() {
        return Err(KtError::InvalidArgument("namespace is empty".into()));
    }
    if options.task_queue.is_empty() {
        return Err(KtError::InvalidArgument("task_queue is empty".into()));
    }
    // `task_types` has no default and Core rejects an empty set, so it must be stated. The
    // builder is typestate-based, so options cannot be applied by conditional reassignment --
    // hence `maybe_*` and one chained expression.
    let task_types = WorkerTaskTypes {
        enable_workflows: !options.no_workflows,
        enable_local_activities: !options.no_local_activities,
        enable_remote_activities: !options.no_remote_activities,
        enable_nexus: false,
    };

    // Slot limits use Core's own FixedSize supplier. The C bridge instead implemented a custom
    // SlotSupplier in the JVM, which cost 7 FFM upcalls on the path in front of every poll plus a
    // PID controller and a resource monitor -- roughly 500 lines to approximate what Core already
    // does. Core also offers a ResourceBased supplier if these ever need to be adaptive.
    let tuner = tuner(options)?;

    temporalio_sdk_core::WorkerConfig::builder()
        .tuner(tuner)
        .namespace(options.namespace.clone())
        .task_queue(options.task_queue.clone())
        .max_cached_workflows(options.max_cached_workflows as usize)
        .task_types(task_types)
        // Required with no default. Versioning is a distinct feature with its own JVM surface;
        // until that is wired through, workers are explicitly unversioned rather than implicitly
        // defaulted, so a versioning bug cannot hide behind a silent default.
        .versioning_strategy(WorkerVersioningStrategy::None {
            build_id: options.build_id.clone(),
        })
        .maybe_client_identity_override(
            (!options.identity.is_empty()).then(|| options.identity.clone()),
        )
        .build()
        .map_err(|e| KtError::InvalidArgument(format!("invalid worker config: {e}")))
}

/// Slot limits: Core's resource-based supplier when the caller asked for one, otherwise fixed
/// sizes defaulting to Core's own values.
fn tuner(
    options: &crate::proto::WorkerOptions,
) -> KtResult<Arc<dyn temporalio_sdk_core::WorkerTuner + Send + Sync>> {
    fn fixed<K: temporalio_sdk_core::SlotKind>(
        slots: u32,
        default: usize,
    ) -> SlotSupplierOptions<K> {
        SlotSupplierOptions::FixedSize {
            slots: if slots > 0 { slots as usize } else { default },
        }
    }

    fn limits(l: Option<&crate::proto::ResourceSlotLimits>) -> Option<ResourceSlotOptions> {
        l.map(|l| {
            ResourceSlotOptions::new(
                l.minimum_slots as usize,
                if l.maximum_slots > 0 {
                    l.maximum_slots as usize
                } else {
                    10_000
                },
                std::time::Duration::from_millis(l.ramp_throttle_millis),
            )
        })
    }

    /// Resource-based when this slot type carries limits, fixed otherwise, so a worker can mix
    /// the two.
    fn slot<K: temporalio_sdk_core::SlotKind>(
        limits: Option<ResourceSlotOptions>,
        fixed_slots: u32,
        default: usize,
    ) -> SlotSupplierOptions<K> {
        match limits {
            Some(l) => SlotSupplierOptions::ResourceBased(l),
            None => fixed(fixed_slots, default),
        }
    }

    let holder = if let Some(rb) = options.resource_tuner.as_ref() {
        let opts = ResourceBasedSlotsOptions::builder()
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
            .build();

        TunerHolderOptions::builder()
            .resource_based_config(ResourceBasedTunerConfig::Options(opts))
            .workflow_slot_options(slot(
                limits(options.workflow_resource_limits.as_ref()),
                options.max_concurrent_workflow_tasks,
                100,
            ))
            .activity_slot_options(slot(
                limits(options.activity_resource_limits.as_ref()),
                options.max_concurrent_activities,
                100,
            ))
            .local_activity_slot_options(slot(
                limits(options.local_activity_resource_limits.as_ref()),
                options.max_concurrent_local_activities,
                100,
            ))
            .build()
            .map_err(|e| KtError::InvalidArgument(format!("invalid slot options: {e}")))?
            .build_tuner_holder()
            .map_err(KtError::from)?
    } else {
        TunerHolderOptions::builder()
            .workflow_slot_options(fixed(options.max_concurrent_workflow_tasks, 100))
            .activity_slot_options(fixed(options.max_concurrent_activities, 100))
            .local_activity_slot_options(fixed(options.max_concurrent_local_activities, 100))
            .build()
            .map_err(|e| KtError::InvalidArgument(format!("invalid slot options: {e}")))?
            .build_tuner_holder()
            .map_err(KtError::from)?
    };
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
    let core = {
        let mut guard = entry.state.lock();
        match std::mem::replace(&mut *guard, WorkerState::Finalized) {
            WorkerState::Running { core, .. } | WorkerState::Draining { core } => {
                *guard = WorkerState::Draining { core: core.clone() };
                core
            }
            WorkerState::Finalized => return Ok(()), // idempotent
        }
    };

    core.initiate_shutdown();

    let deadline = std::time::Instant::now() + grace;
    let mut timed_out = false;
    while entry.live_pumps.load(Ordering::Acquire) > 0 {
        if std::time::Instant::now() >= deadline {
            timed_out = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }

    *entry.state.lock() = WorkerState::Finalized;

    match Arc::try_unwrap(core) {
        Ok(worker) => {
            worker.finalize_shutdown().await;
            if timed_out {
                Err("shut down before every poll stream reported ShutDown".to_string())
            } else {
                Ok(())
            }
        }
        // Returned, never panicked: the C bridge's equivalent unwrapped and aborted the JVM.
        Err(_) => Err("worker still referenced elsewhere; not finalized".to_string()),
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
