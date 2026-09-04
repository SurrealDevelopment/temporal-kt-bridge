//! FFI bridge between temporal-kt (JVM) and the Temporal Rust SDK-Core.
//!
//! Designed for a JDK 25 `java.lang.foreign` consumer rather than for a generic C consumer. Three
//! choices follow from that and shape everything else:
//!
//!   * **No upcalls.** Results reach the JVM through a completion queue that JVM pump threads
//!     drain (see [`queue`]). Nothing here ever calls into the JVM.
//!   * **One ABI struct.** Everything else is scalars, `(ptr, len)` pairs, and protobuf config
//!     (see [`abi`]), so there are no struct layouts to mirror by hand and drift.
//!   * **Handles, not pointers.** Use-after-free is a returned error, not undefined behaviour
//!     (see [`handle`]).
//!
//! Every entry point is defined with [`kt_export!`], which contains panics.

pub mod abi;
pub mod client;
pub mod ephemeral;
pub mod error;
pub mod handle;
pub mod panic;
pub mod queue;
pub mod runtime;
pub mod worker;

/// Generated from `proto/kt_bridge.proto`.
pub mod proto {
    include!(concat!(env!("OUT_DIR"), "/kt_bridge.rs"));
}

use std::ptr;

use abi::*;

use error::{KtError, KtResult};
use handle::{Entry, HANDLES};


/// Copies `src` into a caller-provided buffer, reporting the length it needed.
///
/// # Safety
/// `out_len` must be writable and `u32`-aligned. `out` must be null, or writable for `cap` bytes.
///
/// `out_len` always receives the full length, so a caller that guessed too small can retry with
/// the right size rather than silently truncating.
unsafe fn write_out(src: &[u8], out: *mut u8, cap: u32, out_len: *mut u32) -> KtResult {
    if out_len.is_null() {
        return Err(KtError::InvalidArgument("out_len is null".into()));
    }
    unsafe { out_len.write(src.len() as u32) };
    if src.len() > cap as usize {
        return Err(KtError::BufferTooSmall);
    }
    if !src.is_empty() {
        if out.is_null() {
            return Err(KtError::InvalidArgument("out is null".into()));
        }
        unsafe { ptr::copy_nonoverlapping(src.as_ptr(), out, src.len()) };
    }
    Ok(())
}

/// Borrows a caller-provided buffer.
///
/// # Safety
/// `ptr` must be null or point to `len` readable bytes that outlive the returned slice. The
/// lifetime is caller-chosen and unchecked, so the result must not escape the call.
///
/// A null pointer with a non-zero length is rejected rather than silently treated as empty: that
/// combination is always a caller bug, and decoding it as a default message would hide it.
unsafe fn slice<'a>(ptr: *const u8, len: u32) -> KtResult<&'a [u8]> {
    if len == 0 {
        return Ok(&[]);
    }
    if ptr.is_null() {
        return Err(KtError::InvalidArgument(
            "null pointer with non-zero length".into(),
        ));
    }
    Ok(unsafe { std::slice::from_raw_parts(ptr, len as usize) })
}


// ---------------------------------------------------------------------------------------------
// ABI and diagnostics
// ---------------------------------------------------------------------------------------------

/// Writes the ABI self-description and returns how many `u32`s it has.
///
/// Deliberately not a `kt_export!`: it returns a count rather than a status, and it must be
/// callable before anything else so a mismatched library fails at class-init with a readable diff
/// instead of corrupting memory later. It cannot fail or panic.
///
/// # Safety
/// `out` must be null, or writable for `cap` `u32`s and aligned for `u32`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kt_abi_probe(out: *mut u32, cap: u32) -> u32 {
    let values = abi_probe_values();
    if !out.is_null() {
        let n = (cap as usize).min(values.len());
        unsafe { ptr::copy_nonoverlapping(values.as_ptr(), out, n) };
    }
    values.len() as u32
}

/// Copies the calling thread's last error message out. Returns its full length.
///
/// The message is thread-local, so this must be called on the thread whose call failed.
///
/// # Safety
/// `out` must be null, or writable for `cap` bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kt_last_error(out: *mut u8, cap: u32) -> u32 {
    panic::with_last_error(|message| {
        let bytes = message.as_bytes();
        if !out.is_null() {
            let n = (cap as usize).min(bytes.len());
            unsafe { ptr::copy_nonoverlapping(bytes.as_ptr(), out, n) };
        }
        bytes.len() as u32
    })
}

// ---------------------------------------------------------------------------------------------
// Runtime
// ---------------------------------------------------------------------------------------------

kt_export! {
    /// Creates a Core runtime from a `kt_bridge.RuntimeOptions` protobuf.
    fn kt_runtime_new(cfg: *const u8, cfg_len: u32, out_runtime: *mut u64) {
        if out_runtime.is_null() {
            return Err(KtError::InvalidArgument("out_runtime is null".into()));
        }
        let options: proto::RuntimeOptions = prost::Message::decode(unsafe { slice(cfg, cfg_len) }?)?;
        let entry = runtime::new_runtime(options)?;
        let handle = HANDLES.insert(Entry::Runtime(entry));
        unsafe { out_runtime.write(handle) };
        Ok(())
    }
}

kt_export! {
    /// Writes a `kt_bridge.RuntimeInfo` protobuf describing the runtime.
    fn kt_runtime_info(runtime: u64, out: *mut u8, cap: u32, out_len: *mut u32) {
        let entry = HANDLES.runtime(runtime)?;
        let info = runtime::runtime_info(&entry);
        let mut bytes = Vec::new();
        prost::Message::encode(&info, &mut bytes).map_err(|e| KtError::Failed(e.to_string()))?;
        unsafe { write_out(&bytes, out, cap, out_len) }
    }
}

kt_export! {
    /// Terminal. Answers every outstanding request with `KT_ERR_SHUTDOWN` and wakes all pollers,
    /// so no JVM continuation is left waiting for a completion that will never arrive.
    fn kt_runtime_free(runtime: u64) {
        match HANDLES.remove_of_kind(runtime, handle::KIND_RUNTIME)? {
            Entry::Runtime(entry) => {
                runtime::free_runtime(entry);
                Ok(())
            }
            _ => Err(KtError::WrongHandleKind),
        }
    }
}

// ---------------------------------------------------------------------------------------------
// Client
// ---------------------------------------------------------------------------------------------

kt_export! {
    /// Connects a client. Returns immediately; the result arrives as a completion for `req_id`
    /// with `kind = ClientConnected` and the handle in `aux0`, or a non-zero status and the error
    /// text as the payload.
    fn kt_client_connect(runtime: u64, cfg: *const u8, cfg_len: u32, req_id: u64) {
        if req_id == 0 {
            return Err(KtError::InvalidArgument("req_id 0 is reserved for pushed events".into()));
        }
        let entry = HANDLES.runtime(runtime)?;
        let config: proto::ClientOptions = prost::Message::decode(unsafe { slice(cfg, cfg_len) }?)?;
        let options = client::connection_options(&config)?;
        let namespace = config.namespace.clone();

        runtime::spawn_request(&entry, req_id, async move {
            match client::connect(options, namespace).await {
                Ok(client) => {
                    let handle = HANDLES.insert(Entry::Client(client));
                    queue::Pending::ack(req_id).kind(KtKind::ClientConnected).aux0(handle)
                }
                Err(message) => queue::Pending::error(req_id, KT_ERR_FAILED, message),
            }
        });
        Ok(())
    }
}

kt_export! {
    fn kt_client_free(client: u64) {
        HANDLES.remove_of_kind(client, handle::KIND_CLIENT)?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------------------------
// Worker
// ---------------------------------------------------------------------------------------------

kt_export! {
    /// Creates a worker. Synchronous, because Core's `init_worker` is.
    fn kt_worker_new(runtime: u64, client: u64, cfg: *const u8, cfg_len: u32, out_worker: *mut u64) {
        if out_worker.is_null() {
            return Err(KtError::InvalidArgument("out_worker is null".into()));
        }
        let rt = HANDLES.runtime(runtime)?;
        let cl = HANDLES.client(client)?;
        let options: proto::WorkerOptions = prost::Message::decode(unsafe { slice(cfg, cfg_len) }?)?;
        let config = worker::worker_config(&options)?;

        // init_worker needs a Tokio context.
        let _entered = rt.core.tokio_handle().enter();
        let core = temporalio_sdk_core::init_worker(&rt.core, config, cl.connection.clone())
            .map_err(KtError::from)?;

        let entry = std::sync::Arc::new(worker::WorkerEntry::new(
            std::sync::Arc::new(core),
            rt.sender(),
        ));
        let handle = HANDLES.insert(Entry::Worker(entry));
        unsafe { out_worker.write(handle) };
        Ok(())
    }
}

kt_export! {
    /// Starts the poll loops. Tasks then arrive as pushed completions with this worker's handle
    /// in `aux0` and the stream kind in `aux1`. Idempotent.
    fn kt_worker_start(runtime: u64, worker: u64) {
        let rt = HANDLES.runtime(runtime)?;
        let entry = HANDLES.worker(worker)?;
        worker::start(&entry, &rt, worker)
    }
}

kt_export! {
    /// Completes a task. `task_kind`: 0 workflow activation, 1 activity, 2 nexus.
    fn kt_worker_complete(
        runtime: u64,
        worker: u64,
        task_kind: u32,
        proto_bytes: *const u8,
        proto_len: u32,
        req_id: u64,
    ) {
        if req_id == 0 {
            return Err(KtError::InvalidArgument("req_id 0 is reserved for pushed events".into()));
        }
        let rt = HANDLES.runtime(runtime)?;
        let entry = HANDLES.worker(worker)?;
        let core = entry.core()?;
        let bytes = unsafe { slice(proto_bytes, proto_len) }?.to_vec();

        runtime::spawn_request(&rt, req_id, async move {
            match worker::complete(&core, task_kind, &bytes).await {
                Ok(()) => queue::Pending::ack(req_id),
                Err(message) => queue::Pending::error(req_id, KT_ERR_FAILED, message),
            }
        });
        Ok(())
    }
}

kt_export! {
    /// Records an activity heartbeat. Synchronous: Core batches internally.
    ///
    /// Returns KT_ERR_WORKER_SHUT_DOWN rather than panicking if the worker is finalized -- the
    /// exact race that used to abort the JVM with SIGABRT.
    fn kt_worker_heartbeat(worker: u64, proto_bytes: *const u8, proto_len: u32) {
        let entry = HANDLES.worker(worker)?;
        let core = entry.core()?;
        worker::heartbeat(&core, unsafe { slice(proto_bytes, proto_len) }?)
    }
}

kt_export! {
    /// Shuts the worker down: stop accepting work, wait for every poll stream to report
    /// ShutDown, then finalize. Bounded by `grace_millis`.
    fn kt_worker_shutdown(runtime: u64, worker: u64, grace_millis: u64, req_id: u64) {
        if req_id == 0 {
            return Err(KtError::InvalidArgument("req_id 0 is reserved for pushed events".into()));
        }
        let rt = HANDLES.runtime(runtime)?;
        let entry = HANDLES.worker(worker)?;
        let grace = std::time::Duration::from_millis(grace_millis.max(1));

        runtime::spawn_request(&rt, req_id, async move {
            match worker::shutdown(entry, grace).await {
                Ok(()) => queue::Pending::ack(req_id),
                Err(message) => queue::Pending::error(req_id, KT_ERR_FAILED, message),
            }
        });
        Ok(())
    }
}

kt_export! {
    fn kt_worker_free(worker: u64) {
        HANDLES.remove_of_kind(worker, handle::KIND_WORKER)?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------------------------
// Completion queue
// ---------------------------------------------------------------------------------------------

kt_export! {
    fn kt_poller_new(runtime: u64, _flags: u32, out_poller: *mut u64) {
        if out_poller.is_null() {
            return Err(KtError::InvalidArgument("out_poller is null".into()));
        }
        let entry = HANDLES.runtime(runtime)?;
        let poller = entry.queue.poller();
        let handle = HANDLES.insert(Entry::Poller(std::sync::Arc::new(poller)));
        unsafe { out_poller.write(handle) };
        Ok(())
    }
}

kt_export! {
    /// Blocks until at least one completion is available, `timeout_millis` elapses (-1 blocks
    /// indefinitely, 0 does not block), or `kt_poller_wake` is called.
    ///
    /// Payload pointers in the returned records stay valid until the next poll on this poller.
    ///
    /// The JVM side must call this from a platform thread, never a virtual one: a blocking
    /// downcall pins and blocks its carrier. It must also never be bound with
    /// `Linker.Option.critical()`, which would keep the thread in Java state and stall every GC
    /// for the duration of the block.
    fn kt_poller_poll(
        poller: u64,
        out: *mut KtCompletion,
        cap: u32,
        timeout_millis: i32,
        out_count: *mut u32,
    ) {
        if out.is_null() || out_count.is_null() {
            return Err(KtError::InvalidArgument("out/out_count is null".into()));
        }
        let entry = HANDLES.poller(poller)?;
        let count = unsafe { entry.poll(out, cap, timeout_millis) }?;
        unsafe { out_count.write(count) };
        Ok(())
    }
}

kt_export! {
    /// Unblocks a poll. Idempotent and safe from any thread.
    fn kt_poller_wake(poller: u64) {
        HANDLES.poller(poller)?.wake();
        Ok(())
    }
}

kt_export! {
    fn kt_poller_free(poller: u64) {
        HANDLES.remove_of_kind(poller, handle::KIND_POLLER)?;
        Ok(())
    }
}

kt_export! {
    /// Cancels an in-flight operation. It still produces exactly one completion, with
    /// `KT_ERR_CANCELLED`. A no-op if it already completed.
    fn kt_cancel(runtime: u64, req_id: u64) {
        HANDLES.runtime(runtime)?.cancel(req_id);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_null_pointer_with_a_nonzero_length_is_rejected() {
        // Silently treating this as empty made `kt_runtime_new(NULL, 4096, out)` -- always a
        // caller bug -- decode as a default config and return KT_OK.
        let result = unsafe { slice(std::ptr::null(), 4096) };
        assert!(matches!(result, Err(KtError::InvalidArgument(_))));
    }

    #[test]
    fn a_zero_length_is_empty_regardless_of_pointer() {
        assert_eq!(unsafe { slice(std::ptr::null(), 0) }.unwrap(), b"");
    }

    #[test]
    fn the_abi_probe_reports_the_layout_the_jvm_expects() {
        let mut out = [0u32; 16];
        let n = unsafe { kt_abi_probe(out.as_mut_ptr(), out.len() as u32) };
        assert_eq!(n, 12);
        assert_eq!(out[0], abi::KT_ABI_MAGIC);
        assert_eq!(out[2], 48, "KtCompletion must stay 48 bytes");
        assert_eq!(out[10], 64, "pointer width");
    }

    #[test]
    fn the_abi_probe_reports_its_length_even_with_no_buffer() {
        assert_eq!(unsafe { kt_abi_probe(std::ptr::null_mut(), 0) }, 12);
    }
}
