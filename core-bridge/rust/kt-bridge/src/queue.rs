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

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use parking_lot::{Condvar, Mutex};

use crate::abi::{KtCompletion, KtKind};
use crate::error::{KtError, KtResult};

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
        Self {
            req_id,
            kind: KtKind::Ack,
            status: crate::abi::KT_OK,
            payload: Vec::new(),
            aux0: 0,
            aux1: 0,
        }
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

/// Queue state, all of it behind one mutex.
///
/// `wake_seq` and `closed` live *inside* the mutex deliberately. An earlier version kept `woken`
/// as an `AtomicBool` set outside it, which lost wakeups: a poller could observe the queue empty
/// and the flag clear while still holding the guard, a waker could then set the flag and
/// `notify_all` with nobody parked (a `Condvar` signal is not sticky), and the poller would go on
/// to `wait` forever. That is the exact hang this design exists to remove, so the state a waiter
/// tests and the signal that wakes it must be published under the same lock.
struct State {
    queue: VecDeque<Pending>,
    /// Wakes delivered but not yet consumed.
    ///
    /// A counter rather than an epoch a waiter samples on entry: with an epoch, a wake published
    /// *before* the poll starts is indistinguishable from no wake at all, and the poller parks on
    /// a signal that has already been and gone. Consuming a pending wake makes it sticky until
    /// somebody actually acts on it.
    wakes: u64,
    /// Sticky. Once the runtime is gone, every later poll must return immediately rather than
    /// park: nothing will ever push again, and the poller outlives the runtime.
    closed: bool,
}

struct Shared {
    state: Mutex<State>,
    signal: Condvar,
}

/// One poller, owned by exactly one JVM pump thread.
pub struct PollerEntry {
    shared: Arc<Shared>,
    /// Batch scratch, reused across polls.
    buf: Mutex<BatchBuf>,
    /// Enforces single-threaded use rather than merely documenting it.
    ///
    /// Payload pointers handed out by one poll stay valid until *this poller's* next poll. Two
    /// threads polling the same poller would let one reset and reallocate the slab under the
    /// other's still-valid pointers -- silent cross-FFI corruption. A comment cannot prevent
    /// that; a handle is just a u64 any thread can pass.
    in_poll: AtomicBool,
}

/// The queue a runtime pushes into. Cloned into every Rust task that reports back.
#[derive(Clone)]
pub struct Sender {
    shared: Arc<Shared>,
}

impl Sender {
    pub fn push(&self, pending: Pending) {
        let mut state = self.shared.state.lock();
        if state.closed {
            return;
        }
        state.queue.push_back(pending);
        self.shared.signal.notify_all();
    }
}

pub struct Queue {
    shared: Arc<Shared>,
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
                state: Mutex::new(State {
                    queue: VecDeque::new(),
                    wakes: 0,
                    closed: false,
                }),
                signal: Condvar::new(),
            }),
        }
    }

    pub fn sender(&self) -> Sender {
        Sender {
            shared: self.shared.clone(),
        }
    }

    pub fn poller(&self) -> PollerEntry {
        PollerEntry {
            shared: self.shared.clone(),
            buf: Mutex::new(BatchBuf::default()),
            in_poll: AtomicBool::new(false),
        }
    }

    /// Closes the queue permanently and releases every waiter.
    ///
    /// Sticky, so a pump that re-enters `poll` after shutdown returns immediately instead of
    /// parking forever with no producer left alive.
    pub fn close(&self) {
        let mut state = self.shared.state.lock();
        state.closed = true;
        self.shared.signal.notify_all();
    }

    pub fn is_closed(&self) -> bool {
        self.shared.state.lock().closed
    }
}

impl PollerEntry {
    /// Blocks until at least one completion is available, the timeout elapses, the poller is
    /// woken, or the queue is closed; then fills `out` with up to `cap` records.
    ///
    /// Returns the number written. A return of 0 is legitimate for any of those reasons and the
    /// caller is expected to loop.
    ///
    /// # Safety
    /// `out` must point to at least `cap` writable, 8-byte-aligned [`KtCompletion`] slots, and
    /// must remain valid for the duration of the call. Payload pointers in the returned records
    /// borrow this poller's slab and stay valid only until this poller's next `poll`.
    pub unsafe fn poll(
        &self,
        out: *mut KtCompletion,
        cap: u32,
        timeout_millis: i32,
    ) -> KtResult<u32> {
        if cap == 0 {
            return Ok(0);
        }
        if out.is_null() {
            return Err(KtError::InvalidArgument("out is null".into()));
        }
        // See `in_poll`: reject rather than corrupt.
        if self.in_poll.swap(true, Ordering::Acquire) {
            return Err(KtError::InvalidArgument(
                "this poller is already being polled by another thread; each poller belongs to one pump thread".into(),
            ));
        }
        let _guard = InPoll(&self.in_poll);

        let drained: Vec<Pending> = {
            let mut state = self.shared.state.lock();
            let deadline = (timeout_millis > 0).then(|| {
                std::time::Instant::now() + std::time::Duration::from_millis(timeout_millis as u64)
            });

            // `loop`, not `if`: a Condvar may wake spuriously, so the predicate -- not the
            // signal -- decides when there is something to do.
            loop {
                if !state.queue.is_empty() || state.closed {
                    break;
                }
                if state.wakes > 0 {
                    state.wakes -= 1;
                    break;
                }
                if timeout_millis == 0 {
                    break;
                }
                match deadline {
                    None => self.shared.signal.wait(&mut state),
                    Some(deadline) => {
                        if self
                            .shared
                            .signal
                            .wait_until(&mut state, deadline)
                            .timed_out()
                        {
                            break;
                        }
                    }
                }
            }

            let take = (cap as usize).min(state.queue.len());
            state.queue.drain(..take).collect()
        };

        if drained.is_empty() {
            return Ok(0);
        }

        let mut buf = self.buf.lock();
        buf.reset();
        let total: usize = drained.iter().map(|p| p.payload.len()).sum();
        if buf.bytes.len() < total {
            buf.bytes
                .resize(total.next_power_of_two().max(64 * 1024), 0);
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
        Ok(drained.len() as u32)
    }

    /// Releases a parked poll. Idempotent and safe from any thread.
    pub fn wake(&self) {
        let mut state = self.shared.state.lock();
        state.wakes += 1;
        self.shared.signal.notify_all();
    }
}

/// Clears the in-poll flag even if the body returns early or unwinds.
struct InPoll<'a>(&'a AtomicBool);

impl Drop for InPoll<'_> {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn drain(poller: &PollerEntry, cap: u32) -> Vec<KtCompletion> {
        let mut out = vec![
            KtCompletion {
                req_id: 0,
                kind: 0,
                status: 0,
                payload: 0,
                payload_len: 0,
                aux0: 0,
                aux1: 0
            };
            cap as usize
        ];
        let n = unsafe { poller.poll(out.as_mut_ptr(), cap, 0) }.unwrap();
        out.truncate(n as usize);
        out
    }

    #[test]
    fn a_pushed_completion_round_trips_with_its_payload() {
        let queue = Queue::new();
        let poller = queue.poller();
        queue
            .sender()
            .push(Pending::ack(7).payload(b"hello".to_vec()));

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
        let poller = queue.poller();
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
            assert!(
                bytes.iter().all(|&b| b == i as u8),
                "payload {i} was corrupted by a later one"
            );
        }
    }

    #[test]
    fn cap_bounds_the_batch_and_the_rest_stays_queued() {
        let queue = Queue::new();
        let poller = queue.poller();
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
        let poller = queue.poller();
        assert_eq!(drain(&poller, 4).len(), 0);
    }

    /// A wake published *while the waiter is between checking and parking* must still be seen.
    ///
    /// This is the interleaving that deadlocked the previous implementation, which set an
    /// AtomicBool and notified without holding the queue mutex: the notify hit zero parked
    /// waiters and was dropped, and the waiter then parked forever. Hammering it exercises the
    /// window rather than relying on a sleep to miss it.
    #[test]
    fn a_wake_racing_the_park_is_not_lost() {
        for _ in 0..500 {
            let queue = Queue::new();
            let poller = Arc::new(queue.poller());
            let waker = poller.clone();
            let handle = std::thread::spawn(move || {
                let mut out = [KtCompletion {
                    req_id: 0,
                    kind: 0,
                    status: 0,
                    payload: 0,
                    payload_len: 0,
                    aux0: 0,
                    aux1: 0,
                }; 1];
                unsafe { waker.poll(out.as_mut_ptr(), 1, -1) }.unwrap()
            });
            // No sleep: land the wake anywhere in the check-then-park window.
            poller.wake();
            assert_eq!(handle.join().unwrap(), 0);
        }
    }

    /// Closing must be sticky: a pump loops, so releasing it once is not enough.
    #[test]
    fn a_poll_after_close_returns_immediately_rather_than_parking() {
        let queue = Queue::new();
        let poller = queue.poller();
        queue.close();
        for _ in 0..3 {
            // -1 would park forever if `closed` were one-shot like the old `woken` flag.
            let mut out = [KtCompletion {
                req_id: 0,
                kind: 0,
                status: 0,
                payload: 0,
                payload_len: 0,
                aux0: 0,
                aux1: 0,
            }; 1];
            assert_eq!(unsafe { poller.poll(out.as_mut_ptr(), 1, -1) }.unwrap(), 0);
        }
    }

    #[test]
    fn a_second_thread_polling_one_poller_is_rejected_not_allowed_to_corrupt() {
        let queue = Queue::new();
        let poller = Arc::new(queue.poller());
        let blocker = poller.clone();
        let held = std::thread::spawn(move || {
            let mut out = [KtCompletion {
                req_id: 0,
                kind: 0,
                status: 0,
                payload: 0,
                payload_len: 0,
                aux0: 0,
                aux1: 0,
            }; 1];
            unsafe { blocker.poll(out.as_mut_ptr(), 1, 400) }.unwrap()
        });
        std::thread::sleep(std::time::Duration::from_millis(80));
        let mut out = [KtCompletion {
            req_id: 0,
            kind: 0,
            status: 0,
            payload: 0,
            payload_len: 0,
            aux0: 0,
            aux1: 0,
        }; 1];
        // Would otherwise reset and possibly reallocate the slab under the first thread's
        // still-valid payload pointers.
        assert!(unsafe { poller.poll(out.as_mut_ptr(), 1, 0) }.is_err());
        held.join().unwrap();
    }

    #[test]
    fn wake_releases_a_blocked_poll() {
        // A poll that could not be woken would make shutdown hang, which is the failure this
        // whole design is meant to remove.
        let queue = Queue::new();
        let poller = Arc::new(queue.poller());
        let waker = poller.clone();
        let handle = std::thread::spawn(move || {
            let mut out = [KtCompletion {
                req_id: 0,
                kind: 0,
                status: 0,
                payload: 0,
                payload_len: 0,
                aux0: 0,
                aux1: 0,
            }; 1];
            unsafe { waker.poll(out.as_mut_ptr(), 1, -1) }.unwrap()
        });
        std::thread::sleep(std::time::Duration::from_millis(100));
        poller.wake();
        assert_eq!(
            handle.join().unwrap(),
            0,
            "a woken poll returns empty rather than hanging"
        );
    }

    #[test]
    fn a_blocked_poll_receives_a_later_push() {
        let queue = Queue::new();
        let poller = Arc::new(queue.poller());
        let sender = queue.sender();
        let reader = poller.clone();
        let handle = std::thread::spawn(move || {
            let mut out = [KtCompletion {
                req_id: 0,
                kind: 0,
                status: 0,
                payload: 0,
                payload_len: 0,
                aux0: 0,
                aux1: 0,
            }; 1];
            let n = unsafe { reader.poll(out.as_mut_ptr(), 1, -1) }.unwrap();
            (n, out[0].req_id)
        });
        std::thread::sleep(std::time::Duration::from_millis(100));
        sender.push(Pending::ack(99));
        assert_eq!(handle.join().unwrap(), (1, 99));
    }
}
