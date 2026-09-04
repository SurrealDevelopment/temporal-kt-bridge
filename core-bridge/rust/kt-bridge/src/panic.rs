//! Panic containment at the FFI boundary.
//!
//! A panic escaping an `extern "C"` function aborts the process. That is not hypothetical here:
//! the previous C bridge had exactly one `catch_unwind` in 6,800 lines, and an activity heartbeat
//! racing worker shutdown unwrapped a `None` and took the whole JVM down with SIGABRT.
//!
//! Every entry point in this crate goes through [`kt_export!`], so containment is structural
//! rather than remembered. Three things enforce that:
//!   1. the `compile_error!` below, since a panic=abort profile would silently defeat every guard;
//!   2. a CI grep for bare `extern "C"` outside this macro;
//!   3. internal tasks are `JoinSet`-tracked and report a `WorkerFailed` completion on JoinError,
//!      so a panicked poll pump surfaces as an exception instead of a silent hang.

#[cfg(panic = "abort")]
compile_error!(
    "kt-bridge requires panic = \"unwind\": every extern \"C\" entry point relies on catch_unwind \
     to keep a panic from aborting the host JVM."
);

use std::cell::RefCell;
use std::panic::AssertUnwindSafe;

use crate::abi::{KT_ERR_PANIC, KT_OK};
use crate::error::KtError;

thread_local! {
    /// Last error message for the calling thread, retrievable via `kt_last_error`.
    ///
    /// Error *text* cannot ride the return value (an `i32`), and allocating a buffer per failed
    /// call would mean an ownership contract for the failure path. A thread-local keeps failures
    /// allocation-free for the caller.
    static LAST_ERROR: RefCell<String> = const { RefCell::new(String::new()) };
}

pub fn set_last_error(message: &str) {
    LAST_ERROR.with(|slot| {
        let mut slot = slot.borrow_mut();
        slot.clear();
        slot.push_str(message);
    });
}

pub fn with_last_error<R>(f: impl FnOnce(&str) -> R) -> R {
    LAST_ERROR.with(|slot| f(slot.borrow().as_str()))
}

/// Runs `f`, converting both errors and panics into a status code.
pub fn guard(f: impl FnOnce() -> Result<(), KtError>) -> i32 {
    match std::panic::catch_unwind(AssertUnwindSafe(f)) {
        Ok(Ok(())) => KT_OK,
        Ok(Err(err)) => {
            set_last_error(&err.to_string());
            err.code()
        }
        Err(payload) => {
            let message = panic_message(&payload);
            tracing::error!(panic = %message, "kt-bridge contained a panic at the FFI boundary");
            set_last_error(&format!("panic: {message}"));
            KT_ERR_PANIC
        }
    }
}

pub fn panic_message(payload: &Box<dyn std::any::Any + Send>) -> String {
    if let Some(s) = payload.downcast_ref::<&str>() {
        (*s).to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "non-string panic payload".to_string()
    }
}

/// Defines an `extern "C"` entry point whose body is panic-contained and returns `i32`.
///
/// The body returns `Result<(), KtError>`; the macro maps it to a status code.
#[macro_export]
macro_rules! kt_export {
    (
        $(#[$meta:meta])*
        fn $name:ident($($arg:ident : $ty:ty),* $(,)?) $body:block
    ) => {
        $(#[$meta])*
        ///
        /// # Safety
        /// Every pointer argument must either be null or point to memory valid for the stated
        /// length for the duration of the call. `out`-style pointers must additionally be
        /// writable and correctly aligned for their type. The callee never retains a pointer past
        /// the call, so the caller may free or reuse the memory as soon as it returns.
        #[unsafe(no_mangle)]
        // `unsafe` because these dereference caller-provided raw pointers; the exported symbol
        // and the C ABI are unchanged, so the JVM side is unaffected.
        pub unsafe extern "C" fn $name($($arg: $ty),*) -> i32 {
            $crate::panic::guard(move || -> ::std::result::Result<(), $crate::error::KtError> {
                $body
            })
        }
    };
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::abi::{KT_ERR_PANIC, KT_ERR_STALE_HANDLE, KT_OK};

    #[test]
    fn success_is_ok() {
        assert_eq!(guard(|| Ok(())), KT_OK);
    }

    #[test]
    fn an_error_becomes_its_code_and_is_retrievable() {
        assert_eq!(guard(|| Err(KtError::StaleHandle)), KT_ERR_STALE_HANDLE);
        assert!(with_last_error(|m| m.contains("stale handle")));
    }

    #[test]
    fn a_panic_is_contained_rather_than_aborting_the_process() {
        // The whole reason this crate exists in this shape: a panic crossing extern "C" aborts
        // the host JVM. If this test ever fails it does so by killing the test process.
        assert_eq!(guard(|| panic!("boom")), KT_ERR_PANIC);
        assert!(
            with_last_error(|m| m.contains("boom")),
            "panic text must reach kt_last_error"
        );
    }

    #[test]
    fn a_non_string_panic_payload_is_still_contained() {
        assert_eq!(guard(|| std::panic::panic_any(42u32)), KT_ERR_PANIC);
    }
}
