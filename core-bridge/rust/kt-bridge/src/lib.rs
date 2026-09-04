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

unsafe fn slice<'a>(ptr: *const u8, len: u32) -> &'a [u8] {
    if len == 0 || ptr.is_null() {
        &[]
    } else {
        unsafe { std::slice::from_raw_parts(ptr, len as usize) }
    }
}

// ---------------------------------------------------------------------------------------------
// ABI and diagnostics
// ---------------------------------------------------------------------------------------------

/// Writes the ABI self-description and returns how many `u32`s it has.
///
/// Deliberately not a `kt_export!`: it returns a count rather than a status, and it must be
/// callable before anything else so a mismatched library fails at class-init with a readable diff
/// instead of corrupting memory later.
#[unsafe(no_mangle)]
pub extern "C" fn kt_abi_probe(out: *mut u32, cap: u32) -> u32 {
    let values = abi_probe_values();
    if !out.is_null() {
        let n = (cap as usize).min(values.len());
        unsafe { ptr::copy_nonoverlapping(values.as_ptr(), out, n) };
    }
    values.len() as u32
}

/// Copies the calling thread's last error message out. Returns its full length.
#[unsafe(no_mangle)]
pub extern "C" fn kt_last_error(out: *mut u8, cap: u32) -> u32 {
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
        let options: proto::RuntimeOptions = prost::Message::decode(unsafe { slice(cfg, cfg_len) })?;
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
        let config: proto::ClientOptions = prost::Message::decode(unsafe { slice(cfg, cfg_len) })?;
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
