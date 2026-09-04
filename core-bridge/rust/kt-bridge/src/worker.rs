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
use temporalio_sdk_core::Worker as CoreWorker;
use temporalio_sdk_core::PollError;
use tokio::task::JoinSet;

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
        pumps: JoinSet<()>,
    },
    Draining {
        core: Arc<CoreWorker>,
    },
    Finalized,
}

pub struct WorkerEntry {
    pub state: Mutex<WorkerState>,
    pub sender: Sender,
    /// This worker's own handle, so pushed task completions can say which worker they came from.
    pub handle: Mutex<u64>,
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
pub enum TaskKind {
    WorkflowActivation,
    Activity,
    Nexus,
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
) {
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
                    .aux0(worker_handle),
            ),
            Err(PollError::ShutDown) => {
                // Tell Kotlin the stream is finished so it can close its channel, rather than
                // leaving a consumer suspended forever.
                sender.push(
                    Pending::ack(crate::runtime::PUSHED)
                        .kind(KtKind::TaskStreamEnd)
                        .aux0(worker_handle),
                );
                return;
            }
            Err(err) => {
                sender.push(
                    Pending::error(crate::runtime::PUSHED, KT_ERR_FAILED, err.to_string())
                        .kind(KtKind::WorkerFailed)
                        .aux0(worker_handle),
                );
                return;
            }
        }
    }
}

/// Spawns the three poll loops.
pub fn start(entry: &Arc<WorkerEntry>, worker_handle: u64) -> KtResult {
    let mut guard = entry.state.lock();
    let core = match &mut *guard {
        WorkerState::Running { core, pumps } => {
            if !pumps.is_empty() {
                return Ok(()); // already started; starting twice is a no-op, not an error
            }
            core.clone()
        }
        WorkerState::Draining { .. } | WorkerState::Finalized => {
            return Err(KtError::WorkerShutDown);
        }
    };

    if let WorkerState::Running { pumps, .. } = &mut *guard {
        for kind in [TaskKind::WorkflowActivation, TaskKind::Activity, TaskKind::Nexus] {
            pumps.spawn(pump(core.clone(), kind, entry.sender.clone(), worker_handle));
        }
    }
    Ok(())
}
