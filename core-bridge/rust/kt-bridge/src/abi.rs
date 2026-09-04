//! The ABI surface shared with the JVM.
//!
//! Exactly one struct crosses the boundary, and it contains only naturally-aligned scalars.
//! Everything else is passed as scalars and `(ptr, len)` argument pairs, and all configuration
//! travels as protobuf bytes. That is deliberate: the previous C bridge required ~50 `#[repr(C)]`
//! structs to be mirrored by hand in Java `MemoryLayout` declarations, where a stale offset
//! compiled cleanly and made Rust read past the end of a JVM-allocated struct. With one
//! all-scalar struct there is nothing left to drift.

/// Bumped only when the shape below changes incompatibly.
pub const KT_ABI_VERSION: u32 = 1;

/// "KTB1", so a probe against the wrong library fails on the first word.
pub const KT_ABI_MAGIC: u32 = 0x4B54_4231;

pub const KT_OK: i32 = 0;

// Negative values are bridge errors. Positive values, where a call defines them, are gRPC status
// codes. Keep in step with KtErr on the Kotlin side.
pub const KT_ERR_PANIC: i32 = -1;
pub const KT_ERR_INVALID_ARGUMENT: i32 = -2;
pub const KT_ERR_STALE_HANDLE: i32 = -3;
pub const KT_ERR_WRONG_HANDLE_KIND: i32 = -4;
pub const KT_ERR_SHUTDOWN: i32 = -5;
pub const KT_ERR_WORKER_SHUT_DOWN: i32 = -6;
pub const KT_ERR_CANCELLED: i32 = -7;
pub const KT_ERR_FAILED: i32 = -8;
pub const KT_ERR_BUFFER_TOO_SMALL: i32 = -9;

/// Discriminants for [`KtCompletion::kind`].
///
/// A completion either answers a request (`req_id != 0`) or is pushed by the bridge on its own
/// (`req_id == 0`), which is how tasks, logs and metrics arrive.
#[repr(u32)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum KtKind {
    /// Terminal answer to a request that carries no payload of its own.
    Ack = 0,
    ClientConnected = 1,
    Rpc = 2,
    /// Pushed: a workflow activation. `aux0` is the worker handle.
    TaskWorkflowActivation = 3,
    /// Pushed: an activity task. `aux0` is the worker handle.
    TaskActivity = 4,
    /// Pushed: a nexus task. `aux0` is the worker handle.
    TaskNexus = 5,
    /// Pushed: a task stream ended because the worker is shutting down. `aux0` is the worker.
    TaskStreamEnd = 6,
    /// Pushed: an internal task panicked or died; the worker is no longer usable.
    WorkerFailed = 7,
    EphemeralStarted = 8,
    /// Pushed: a line of a dev/test server's stdout or stderr.
    ServerLog = 9,
    /// Pushed: a Core log record. `aux0` is the level, `aux1` the timestamp in millis.
    Log = 10,
}

impl KtKind {
    pub const COUNT: u32 = 11;
}

/// The only struct in this ABI.
///
/// 48 bytes, no padding on any supported target. Never add a field: add a `KtKind` and use the
/// aux slots, or bump [`KT_ABI_VERSION`].
#[repr(C)]
#[derive(Clone, Copy)]
pub struct KtCompletion {
    /// Caller-supplied request id, or 0 for a pushed event.
    pub req_id: u64,
    /// A [`KtKind`] discriminant.
    pub kind: u32,
    /// [`KT_OK`], a negative bridge error, or (for RPCs) a positive gRPC status code.
    pub status: i32,
    /// Pointer into the poller's batch buffer. Valid until the next poll *on that poller*, and
    /// never freed by the caller.
    pub payload: u64,
    pub payload_len: u64,
    /// Kind-specific: a handle, a log level, a metric instrument id.
    pub aux0: u64,
    /// Kind-specific: a timestamp, a metric attribute-set id.
    pub aux1: u64,
}

const _: () = {
    assert!(size_of::<KtCompletion>() == 48);
    assert!(align_of::<KtCompletion>() == 8);
};

/// Self-description the JVM checks at class-init.
///
/// Returned as a flat `u32` array rather than a struct so that reading it cannot itself depend on
/// a struct layout being right. A mismatch means the native library does not match the JAR, which
/// otherwise shows up much later as corrupted memory.
pub fn abi_probe_values() -> [u32; 12] {
    [
        KT_ABI_MAGIC,
        KT_ABI_VERSION,
        size_of::<KtCompletion>() as u32,
        core::mem::offset_of!(KtCompletion, req_id) as u32,
        core::mem::offset_of!(KtCompletion, kind) as u32,
        core::mem::offset_of!(KtCompletion, status) as u32,
        core::mem::offset_of!(KtCompletion, payload) as u32,
        core::mem::offset_of!(KtCompletion, payload_len) as u32,
        core::mem::offset_of!(KtCompletion, aux0) as u32,
        core::mem::offset_of!(KtCompletion, aux1) as u32,
        usize::BITS,
        KtKind::COUNT,
    ]
}
