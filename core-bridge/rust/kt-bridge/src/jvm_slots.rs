//! JVM makes resource-pressure decisions; Rust owns permits, wakeups and ramp throttling.

use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use temporalio_sdk_core::{
    SlotKind, SlotMarkUsedContext, SlotReleaseContext, SlotReservationContext, SlotSupplier,
    SlotSupplierPermit,
};
use tokio::sync::Notify;

#[derive(Default)]
pub struct ResourceGate {
    allowed: AtomicU32,
    changed: Notify,
    reserved: [AtomicU32; 4],
    pending: [AtomicU32; 4],
}

impl ResourceGate {
    pub fn new() -> Self {
        Self::default()
    }

    /// One admission bit for each of workflow, activity, local activity and Nexus slots.
    pub fn update(&self, allowed: u32) {
        self.allowed.store(allowed, Ordering::Release);
        self.changed.notify_waiters();
    }

    pub fn stats(&self) -> [u32; 8] {
        std::array::from_fn(|index| {
            let counters = if index % 2 == 0 {
                &self.reserved
            } else {
                &self.pending
            };
            counters[index / 2].load(Ordering::Acquire)
        })
    }

    fn allows(&self, kind: u32) -> bool {
        self.allowed.load(Ordering::Acquire) & (1 << kind) != 0
    }
}

struct State {
    issued: [usize; 2],
    last_issued: Instant,
}

struct Permit {
    state: Arc<Mutex<State>>,
    gate: Arc<ResourceGate>,
    queue: usize,
    kind: usize,
}

impl Drop for Permit {
    fn drop(&mut self) {
        self.state.lock().issued[self.queue] -= 1;
        self.gate.reserved[self.kind].fetch_sub(1, Ordering::AcqRel);
        self.gate.changed.notify_waiters();
    }
}

struct PendingReserve<'a>(&'a AtomicU32);

impl Drop for PendingReserve<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

struct JvmSlots<K> {
    gate: Arc<ResourceGate>,
    state: Arc<Mutex<State>>,
    minimum: usize,
    maximum: usize,
    ramp: Duration,
    kind: u32,
    sticky: bool,
    marker: PhantomData<K>,
}

pub fn supplier<K: SlotKind + Send + Sync + 'static>(
    gate: Arc<ResourceGate>,
    limits: &crate::proto::ResourceSlotLimits,
    kind: u32,
    sticky: bool,
) -> Arc<dyn SlotSupplier<SlotKind = K> + Send + Sync> {
    Arc::new(JvmSlots {
        gate,
        state: Arc::new(Mutex::new(State {
            issued: [0, 0],
            last_issued: Instant::now(),
        })),
        minimum: limits.minimum_slots as usize,
        maximum: limits.maximum_slots as usize,
        ramp: Duration::from_millis(limits.ramp_throttle_millis),
        kind,
        sticky,
        marker: PhantomData,
    })
}

#[async_trait::async_trait]
impl<K: SlotKind + Send + Sync> SlotSupplier for JvmSlots<K> {
    type SlotKind = K;

    async fn reserve_slot(&self, ctx: &dyn SlotReservationContext) -> SlotSupplierPermit {
        let pending = &self.gate.pending[self.kind as usize];
        pending.fetch_add(1, Ordering::AcqRel);
        let _pending = PendingReserve(pending);
        loop {
            let notified = self.gate.changed.notified();
            tokio::pin!(notified);
            // Register before checking the state so a concurrent release/update cannot be missed.
            notified.as_mut().enable();
            if let Some(permit) = self.try_reserve_slot(ctx) {
                return permit;
            }
            let wait = self
                .ramp
                .saturating_sub(self.state.lock().last_issued.elapsed());
            tokio::select! {
                _ = notified => {},
                _ = tokio::time::sleep(wait), if !wait.is_zero() => {},
            }
        }
    }

    fn try_reserve_slot(&self, ctx: &dyn SlotReservationContext) -> Option<SlotSupplierPermit> {
        let queue = usize::from(self.sticky && ctx.is_sticky());
        let mut state = self.state.lock();
        let issued = state.issued.iter().sum::<usize>();
        if issued >= self.maximum {
            return None;
        }
        // Keep one permit available for each workflow queue so sticky polling cannot starve new work.
        if self.sticky && state.issued[1 - queue] == 0 && issued + 1 == self.maximum {
            return None;
        }
        let required = issued < self.minimum || (self.sticky && state.issued[queue] == 0);
        if !required && (!self.gate.allows(self.kind) || state.last_issued.elapsed() < self.ramp) {
            return None;
        }
        state.issued[queue] += 1;
        self.gate.reserved[self.kind as usize].fetch_add(1, Ordering::AcqRel);
        state.last_issued = Instant::now();
        Some(SlotSupplierPermit::with_user_data(Permit {
            state: self.state.clone(),
            gate: self.gate.clone(),
            queue,
            kind: self.kind as usize,
        }))
    }

    fn mark_slot_used(&self, _: &dyn SlotMarkUsedContext<SlotKind = K>) {}

    // Core drops the permit after this call; Drop also releases reservations cancelled before use.
    fn release_slot(&self, _: &dyn SlotReleaseContext<SlotKind = K>) {}

    fn available_slots(&self) -> Option<usize> {
        Some(self.maximum - self.state.lock().issued.iter().sum::<usize>())
    }

    fn slot_supplier_kind(&self) -> String {
        "JvmResourceBased".to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use temporalio_common::worker::WorkerDeploymentVersion;
    use temporalio_sdk_core::{ActivitySlotKind, WorkflowSlotKind};

    struct Context(bool);
    impl SlotReservationContext for Context {
        fn task_queue(&self) -> &str {
            "queue"
        }
        fn worker_identity(&self) -> &str {
            "worker"
        }
        fn worker_deployment_version(&self) -> &Option<WorkerDeploymentVersion> {
            &None
        }
        fn num_issued_slots(&self) -> usize {
            0
        }
        fn is_sticky(&self) -> bool {
            self.0
        }
    }

    fn limits(min: u32, max: u32, ramp: u64) -> crate::proto::ResourceSlotLimits {
        crate::proto::ResourceSlotLimits {
            minimum_slots: min,
            maximum_slots: max,
            ramp_throttle_millis: ramp,
        }
    }

    #[tokio::test]
    async fn jvm_pressure_bounds_ramp_and_cancelled_reservations() {
        let gate = Arc::new(ResourceGate::new());
        let slots = supplier::<ActivitySlotKind>(gate.clone(), &limits(1, 2, 20), 1, false);
        let ctx = Context(false);
        let started = Instant::now();
        let first = slots
            .try_reserve_slot(&ctx)
            .expect("minimum bypasses pressure");
        assert!(slots.try_reserve_slot(&ctx).is_none());
        gate.update(1); // Workflow admission must not allow an activity reservation.
        assert!(slots.try_reserve_slot(&ctx).is_none());
        gate.update(2);
        let second = tokio::time::timeout(Duration::from_secs(1), slots.reserve_slot(&ctx))
            .await
            .unwrap();
        assert!(
            started.elapsed() >= Duration::from_millis(20),
            "ramp applies"
        );
        assert!(slots.try_reserve_slot(&ctx).is_none(), "hard maximum");
        assert!(
            tokio::time::timeout(Duration::from_millis(5), slots.reserve_slot(&ctx))
                .await
                .is_err()
        );
        assert_eq!(
            slots.available_slots(),
            Some(0),
            "cancelled reservation does not change count"
        );
        assert_eq!(&gate.stats()[2..4], &[2, 0]);
        drop(second);
        drop(first);
        assert_eq!(slots.available_slots(), Some(2));
        gate.update(0);
        let minimum = slots.try_reserve_slot(&ctx).unwrap();
        let waiting = slots.reserve_slot(&ctx);
        tokio::pin!(waiting);
        assert!(
            tokio::time::timeout(Duration::from_millis(5), waiting.as_mut())
                .await
                .is_err()
        );
        assert_eq!(&gate.stats()[2..4], &[1, 1]);
        gate.update(2);
        let granted = tokio::time::timeout(Duration::from_secs(1), waiting)
            .await
            .unwrap();
        drop(granted);
        drop(minimum);
    }

    #[test]
    fn cached_workflows_keep_a_slot_for_both_queues_under_pressure() {
        let slots = supplier::<WorkflowSlotKind>(
            Arc::new(ResourceGate::new()),
            &limits(0, 2, 100),
            0,
            true,
        );
        let nonsticky = slots.try_reserve_slot(&Context(false)).unwrap();
        assert!(slots.try_reserve_slot(&Context(false)).is_none());
        let sticky = slots.try_reserve_slot(&Context(true)).unwrap();
        assert_eq!(slots.available_slots(), Some(0));
        drop(nonsticky);
        assert!(slots.try_reserve_slot(&Context(true)).is_none());
        let next_nonsticky = slots.try_reserve_slot(&Context(false)).unwrap();
        drop(sticky);
        drop(next_nonsticky);
        assert_eq!(slots.available_slots(), Some(2));
    }
}
