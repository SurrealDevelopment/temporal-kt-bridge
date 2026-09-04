//! The completion queue: how results reach the JVM.
//!
//! Nothing in this crate calls into the JVM. Instead the JVM runs dedicated pump threads that
//! block in `kt_poller_poll` and drain batches of [`KtCompletion`]. That removes FFM upcalls
//! entirely, and with them a whole family of failures the previous bridge worked around one at a
//! time: exceptions constructed on Rust callback threads crashing the JVM in `fillInStackTrace`,
//! upcall stubs freed while a late callback was still in flight, and JVM work executing on Tokio
//! worker threads.
//!
//! Two invariants hold the design together:
//!
//!   1. **Every `req_id` produces exactly one terminal completion** -- including cancelled
//!      operations and runtime shutdown, where the pending table is drained with
//!      `KT_ERR_SHUTDOWN`. The JVM never has to guess whether more is coming, so shutdown needs
//!      no timeout and no "await pending callbacks" latch.
//!   2. **Payload memory belongs to the poller and lives until its next poll.** One implicit
//!      release per batch, instead of a free call per result.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use parking_lot::{Condvar, Mutex};

use crate::abi::{KtCompletion, KtKind};

/// A completion plus the bytes it refers to, before those bytes are copied into a batch buffer.
pub struct Pending {
    pub req_id: u64,
    pub kind: KtKind,
    pub status: i32,
    pub payload: Vec<u8>,
    pub aux0: u64,
    pub aux1: u64,
}

impl Pending {
    pub fn ack(req_id: u64) -> Self {
        Self { req_id, kind: KtKind::Ack, status: crate::abi::KT_OK, payload: Vec::new(), aux0: 0, aux1: 0 }
    }

    pub fn error(req_id: u64, status: i32, message: impl Into<String>) -> Self {
        Self {
            req_id,
            kind: KtKind::Ack,
            status,
            payload: message.into().into_bytes(),
            aux0: 0,
            aux1: 0,
        }
    }

    pub fn kind(mut self, kind: KtKind) -> Self {
        self.kind = kind;
        self
    }

    pub fn payload(mut self, payload: Vec<u8>) -> Self {
        self.payload = payload;
        self
    }

    pub fn aux0(mut self, aux0: u64) -> Self {
        self.aux0 = aux0;
        self
    }

    pub fn aux1(mut self, aux1: u64) -> Self {
        self.aux1 = aux1;
        self
    }
}

/// Scratch space a poller reuses for the payloads of one batch.
///
/// Grown on demand and never shrunk, so steady state is allocation-free. The previous bridge paid
/// two mallocs, two frees and an extra FFI call per task just to hand over bytes.
#[derive(Default)]
struct BatchBuf {
    bytes: Vec<u8>,
    used: usize,
}

impl BatchBuf {
    fn reset(&mut self) {
        self.used = 0;
    }

    /// Copies `src` into the slab and returns `(pointer, len)`.
    ///
    /// The slab is reserved up front for the whole batch so it cannot reallocate mid-batch, which
    /// would invalidate pointers already handed out in this same batch.
    fn put(&mut self, src: &[u8]) -> (u64, u64) {
        let start = self.used;
        self.bytes[start..start + src.len()].copy_from_slice(src);
        self.used += src.len();
        (self.bytes[start..].as_ptr() as u64, src.len() as u64)
    }
}

struct Shared {
    queue: Mutex<Vec<Pending>>,
    signal: Condvar,
    woken: AtomicBool,
}

/// One poller, owned by exactly one JVM pump thread.
pub struct PollerEntry {
    shared: Arc<Shared>,
    /// Batch scratch. A `Mutex` only to satisfy `Sync`; contention would mean two threads share a
    /// poller, which the Kotlin side does not do.
    buf: Mutex<BatchBuf>,
    pub runtime: u64,
}

/// The queue a runtime pushes into. Cloned into every Rust task that reports back.
#[derive(Clone)]
pub struct Sender {
    shared: Arc<Shared>,
    next_req_id: Arc<AtomicU64>,
}

impl Sender {
    pub fn push(&self, pending: Pending) {
        let mut queue = self.shared.queue.lock();
        queue.push(pending);
        self.shared.signal.notify_one();
    }

    /// Ids for work the bridge starts on its own, kept disjoint from caller-supplied ids by
    /// starting above the range the JVM allocates from.
    pub fn internal_req_id(&self) -> u64 {
        self.next_req_id.fetch_add(1, Ordering::Relaxed)
    }
}

pub struct Queue {
    shared: Arc<Shared>,
    next_req_id: Arc<AtomicU64>,
}

impl Default for Queue {
    fn default() -> Self {
        Self::new()
    }
}

impl Queue {
    pub fn new() -> Self {
        Self {
            shared: Arc::new(Shared {
                queue: Mutex::new(Vec::new()),
                signal: Condvar::new(),
                woken: AtomicBool::new(false),
            }),
            // Well above anything the JVM allocates, so internal and caller ids cannot collide.
            next_req_id: Arc::new(AtomicU64::new(1 << 48)),
        }
    }

    pub fn sender(&self) -> Sender {
        Sender {
            shared: self.shared.clone(),
            next_req_id: self.next_req_id.clone(),
        }
    }

    pub fn poller(&self, runtime: u64) -> PollerEntry {
        PollerEntry {
            shared: self.shared.clone(),
            buf: Mutex::new(BatchBuf::default()),
            runtime,
        }
    }

    /// Unblocks every waiting poller, used on shutdown.
    pub fn wake_all(&self) {
        self.shared.woken.store(true, Ordering::SeqCst);
        self.shared.signal.notify_all();
    }
}

impl PollerEntry {
    /// Blocks until at least one completion is available, the timeout elapses, or the poller is
    /// woken; then fills `out` with up to `cap` records.
    ///
    /// Returns the number written. Waiting happens in a real `Condvar` park -- no spinning and no
    /// polling interval -- and batching is opportunistic: it never waits to fill a batch, so a
    /// lone completion is not delayed.
    ///
    /// # Safety
    /// `out` must point to `cap` writable `KtCompletion` slots.
    pub unsafe fn poll(&self, out: *mut KtCompletion, cap: u32, timeout_millis: i32) -> u32 {
        if cap == 0 {
            return 0;
        }

        let drained: Vec<Pending> = {
            let mut queue = self.shared.queue.lock();
            if queue.is_empty() && !self.shared.woken.swap(false, Ordering::SeqCst) {
                if timeout_millis < 0 {
                    self.shared.signal.wait(&mut queue);
                } else {
                    let timeout = std::time::Duration::from_millis(timeout_millis as u64);
                    self.shared.signal.wait_for(&mut queue, timeout);
                }
            }
            let take = (cap as usize).min(queue.len());
            queue.drain(..take).collect()
        };

        if drained.is_empty() {
            return 0;
        }

        let mut buf = self.buf.lock();
        buf.reset();
        // Reserve the whole batch before writing any of it: a reallocation partway through would
        // dangle the pointers already stored in earlier records of this batch.
        let total: usize = drained.iter().map(|p| p.payload.len()).sum();
        if buf.bytes.len() < total {
            buf.bytes.resize(total.next_power_of_two().max(64 * 1024), 0);
        }

        for (index, pending) in drained.iter().enumerate() {
            let (ptr, len) = if pending.payload.is_empty() {
                (0, 0)
            } else {
                buf.put(&pending.payload)
            };
            let record = KtCompletion {
                req_id: pending.req_id,
                kind: pending.kind as u32,
                status: pending.status,
                payload: ptr,
                payload_len: len,
                aux0: pending.aux0,
                aux1: pending.aux1,
            };
            unsafe { out.add(index).write(record) };
        }
        drained.len() as u32
    }

    pub fn wake(&self) {
        self.shared.woken.store(true, Ordering::SeqCst);
        self.shared.signal.notify_all();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn drain(poller: &PollerEntry, cap: u32) -> Vec<KtCompletion> {
        let mut out = vec![
            KtCompletion { req_id: 0, kind: 0, status: 0, payload: 0, payload_len: 0, aux0: 0, aux1: 0 };
            cap as usize
        ];
        let n = unsafe { poller.poll(out.as_mut_ptr(), cap, 0) };
        out.truncate(n as usize);
        out
    }

    #[test]
    fn a_pushed_completion_round_trips_with_its_payload() {
        let queue = Queue::new();
        let poller = queue.poller(0);
        queue.sender().push(Pending::ack(7).payload(b"hello".to_vec()));

        let got = drain(&poller, 8);
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].req_id, 7);
        let bytes = unsafe {
            std::slice::from_raw_parts(got[0].payload as *const u8, got[0].payload_len as usize)
        };
        assert_eq!(bytes, b"hello");
    }

    #[test]
    fn a_batch_preserves_every_payload() {
        // Payloads share one slab, so a reallocation partway through would dangle the pointers
        // already written into earlier records of the same batch.
        let queue = Queue::new();
        let poller = queue.poller(0);
        let sender = queue.sender();
        for i in 0..64u64 {
            sender.push(Pending::ack(i).payload(vec![i as u8; 1024]));
        }

        let got = drain(&poller, 64);
        assert_eq!(got.len(), 64);
        for (i, record) in got.iter().enumerate() {
            let bytes = unsafe {
                std::slice::from_raw_parts(record.payload as *const u8, record.payload_len as usize)
            };
            assert_eq!(bytes.len(), 1024);
            assert!(bytes.iter().all(|&b| b == i as u8), "payload {i} was corrupted by a later one");
        }
    }

    #[test]
    fn cap_bounds_the_batch_and_the_rest_stays_queued() {
        let queue = Queue::new();
        let poller = queue.poller(0);
        let sender = queue.sender();
        for i in 0..10u64 {
            sender.push(Pending::ack(i));
        }
        assert_eq!(drain(&poller, 4).len(), 4);
        assert_eq!(drain(&poller, 4).len(), 4);
        assert_eq!(drain(&poller, 4).len(), 2);
    }

    #[test]
    fn an_empty_queue_returns_nothing_without_blocking() {
        let queue = Queue::new();
        let poller = queue.poller(0);
        assert_eq!(drain(&poller, 4).len(), 0);
    }

    #[test]
    fn wake_releases_a_blocked_poll() {
        // A poll that could not be woken would make shutdown hang, which is the failure this
        // whole design is meant to remove.
        let queue = Queue::new();
        let poller = Arc::new(queue.poller(0));
        let waker = poller.clone();
        let handle = std::thread::spawn(move || {
            let mut out = [KtCompletion {
                req_id: 0, kind: 0, status: 0, payload: 0, payload_len: 0, aux0: 0, aux1: 0,
            }; 1];
            unsafe { waker.poll(out.as_mut_ptr(), 1, -1) }
        });
        std::thread::sleep(std::time::Duration::from_millis(100));
        poller.wake();
        assert_eq!(handle.join().unwrap(), 0, "a woken poll returns empty rather than hanging");
    }

    #[test]
    fn a_blocked_poll_receives_a_later_push() {
        let queue = Queue::new();
        let poller = Arc::new(queue.poller(0));
        let sender = queue.sender();
        let reader = poller.clone();
        let handle = std::thread::spawn(move || {
            let mut out = [KtCompletion {
                req_id: 0, kind: 0, status: 0, payload: 0, payload_len: 0, aux0: 0, aux1: 0,
            }; 1];
            let n = unsafe { reader.poll(out.as_mut_ptr(), 1, -1) };
            (n, out[0].req_id)
        });
        std::thread::sleep(std::time::Duration::from_millis(100));
        sender.push(Pending::ack(99));
        assert_eq!(handle.join().unwrap(), (1, 99));
    }
}
