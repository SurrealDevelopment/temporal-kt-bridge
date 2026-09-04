//! Generation-counted handle table.
//!
//! Handles are opaque `u64`s, never pointers. The JVM therefore cannot dereference anything, and
//! a handle used after free resolves to [`KtError::StaleHandle`] instead of touching freed memory.
//! The generation counter is what distinguishes "freed" from "this slot was reused": without it,
//! a stale handle would silently address whatever object landed in the slot next.
//!
//! Layout: `[kind:u8 | generation:u24 | index:u32]`. Handle 0 is never valid, so a zeroed or
//! default-initialised field on the JVM side fails loudly rather than aliasing slot 0.

use std::sync::Arc;

use parking_lot::RwLock;

use crate::error::{KtError, KtResult};

const KIND_SHIFT: u32 = 56;
const GEN_SHIFT: u32 = 32;
const GEN_MASK: u64 = 0x00FF_FFFF;

pub struct Slot {
    generation: u32,
    entry: Option<Entry>,
}

#[derive(Default)]
pub struct Table {
    slots: Vec<Slot>,
    free: Vec<u32>,
}

impl Default for HandleTable {
    fn default() -> Self {
        Self::new()
    }
}

pub struct HandleTable {
    inner: RwLock<Table>,
}

impl HandleTable {
    pub const fn new() -> Self {
        Self {
            inner: RwLock::new(Table {
                slots: Vec::new(),
                free: Vec::new(),
            }),
        }
    }

    pub fn insert(&self, entry: Entry) -> u64 {
        let kind = entry.kind() as u64;
        let mut table = self.inner.write();
        let index = match table.free.pop() {
            Some(index) => {
                table.slots[index as usize].entry = Some(entry);
                index
            }
            None => {
                table.slots.push(Slot {
                    generation: 1,
                    entry: Some(entry),
                });
                (table.slots.len() - 1) as u32
            }
        };
        let generation = table.slots[index as usize].generation as u64;
        (kind << KIND_SHIFT) | ((generation & GEN_MASK) << GEN_SHIFT) | index as u64
    }

    /// Removes the entry and invalidates every copy of the handle.
    ///
    /// The generation bump is why a double free is an error rather than a second removal of
    /// whatever now occupies the slot.
    /// `expected_kind` is checked, not just the index and generation.
    ///
    /// Without it a transposed argument -- `kt_poller_free(runtimeHandle)` -- removed the runtime,
    /// returned KT_OK, and skipped shutdown entirely, leaving every pump parked forever. Kind
    /// safety used to come only from matching the `Entry` variant on the read path, which the
    /// remove path never did.
    pub fn remove_of_kind(&self, handle: u64, expected_kind: u8) -> KtResult<Entry> {
        // Handle 0 is not a wrong-kind handle, it is not a handle at all -- a zeroed or
        // default-initialised field on the JVM side. Report that rather than a kind mismatch,
        // which would send the reader looking for a transposed argument.
        if handle == 0 {
            return Err(KtError::StaleHandle);
        }
        let index = (handle as u32) as usize;
        let generation = ((handle >> GEN_SHIFT) & GEN_MASK) as u32;
        if (handle >> KIND_SHIFT) as u8 != expected_kind {
            return Err(KtError::WrongHandleKind);
        }
        let mut table = self.inner.write();
        let slot = table.slots.get_mut(index).ok_or(KtError::StaleHandle)?;
        if slot.generation != generation {
            return Err(KtError::StaleHandle);
        }
        let entry = slot.entry.take().ok_or(KtError::StaleHandle)?;
        // Mask to the 24 bits that are actually encoded in the handle. Bumping the full u32 and
        // clamping with `.max(1)` only avoided generation 0 at the 2^32 wrap, while `insert`
        // encodes `generation & GEN_MASK`: after 2^24 cycles a slot issued a handle whose encoded
        // generation was 0 while the slot held 0x1000000, so the very first use of a freshly
        // issued handle came back StaleHandle and the entry leaked.
        slot.generation = ((slot.generation + 1) & (GEN_MASK as u32)).max(1);
        table.free.push(index as u32);
        Ok(entry)
    }

    fn with<R>(&self, handle: u64, f: impl FnOnce(&Entry) -> KtResult<R>) -> KtResult<R> {
        let index = (handle as u32) as usize;
        let generation = ((handle >> GEN_SHIFT) & GEN_MASK) as u32;
        let table = self.inner.read();
        let slot = table.slots.get(index).ok_or(KtError::StaleHandle)?;
        if slot.generation != generation {
            return Err(KtError::StaleHandle);
        }
        match &slot.entry {
            Some(entry) => f(entry),
            None => Err(KtError::StaleHandle),
        }
    }
}

/// Everything the table can hold.
///
/// One table rather than one per type: with separate tables a client handle passed where a worker
/// is expected would index a different table and silently succeed. Here the kind tag makes it
/// [`KtError::WrongHandleKind`].
pub enum Entry {
    Runtime(Arc<crate::runtime::RuntimeEntry>),
    Client(Arc<crate::client::ClientEntry>),
    Worker(Arc<crate::worker::WorkerEntry>),
    Ephemeral(Arc<crate::ephemeral::EphemeralEntry>),
    Poller(Arc<crate::queue::PollerEntry>),
}

pub const KIND_RUNTIME: u8 = 1;
pub const KIND_CLIENT: u8 = 2;
pub const KIND_WORKER: u8 = 3;
pub const KIND_EPHEMERAL: u8 = 4;
pub const KIND_POLLER: u8 = 5;

impl Entry {
    fn kind(&self) -> u8 {
        match self {
            Entry::Runtime(_) => KIND_RUNTIME,
            Entry::Client(_) => KIND_CLIENT,
            Entry::Worker(_) => KIND_WORKER,
            Entry::Ephemeral(_) => KIND_EPHEMERAL,
            Entry::Poller(_) => KIND_POLLER,
        }
    }
}

macro_rules! typed_getter {
    ($name:ident, $variant:ident, $ty:ty) => {
        impl HandleTable {
            pub fn $name(&self, handle: u64) -> KtResult<Arc<$ty>> {
                self.with(handle, |entry| match entry {
                    Entry::$variant(value) => Ok(value.clone()),
                    _ => Err(KtError::WrongHandleKind),
                })
            }
        }
    };
}

typed_getter!(runtime, Runtime, crate::runtime::RuntimeEntry);
typed_getter!(client, Client, crate::client::ClientEntry);
typed_getter!(worker, Worker, crate::worker::WorkerEntry);
typed_getter!(ephemeral, Ephemeral, crate::ephemeral::EphemeralEntry);
typed_getter!(poller, Poller, crate::queue::PollerEntry);

/// The process-wide table. Handles outlive individual calls, so it cannot be per-call state.
pub static HANDLES: HandleTable = HandleTable::new();

#[cfg(test)]
mod tests {
    use super::*;
    use crate::queue::Queue;

    fn poller_entry() -> Arc<crate::queue::PollerEntry> {
        Arc::new(Queue::new().poller())
    }

    #[test]
    fn resolves_a_live_handle() {
        let table = HandleTable::new();
        let handle = table.insert(Entry::Poller(poller_entry()));
        assert!(table.poller(handle).is_ok());
    }

    #[test]
    fn handle_zero_is_never_valid() {
        // A zeroed or default-initialised field on the JVM side must fail loudly rather than
        // alias slot 0.
        let table = HandleTable::new();
        table.insert(Entry::Poller(poller_entry()));
        assert!(matches!(table.poller(0), Err(KtError::StaleHandle)));
    }

    #[test]
    fn a_freed_handle_is_stale_rather_than_undefined() {
        let table = HandleTable::new();
        let handle = table.insert(Entry::Poller(poller_entry()));
        table.remove_of_kind(handle, KIND_POLLER).unwrap();
        assert!(matches!(table.poller(handle), Err(KtError::StaleHandle)));
    }

    #[test]
    fn double_free_is_an_error_not_a_second_removal() {
        let table = HandleTable::new();
        let handle = table.insert(Entry::Poller(poller_entry()));
        table.remove_of_kind(handle, KIND_POLLER).unwrap();
        assert!(matches!(table.remove_of_kind(handle, KIND_POLLER), Err(KtError::StaleHandle)));
    }

    #[test]
    fn a_reused_slot_does_not_answer_to_the_old_handle() {
        // The generation counter earns its keep here: without it the stale handle would resolve
        // to whatever object landed in the recycled slot.
        let table = HandleTable::new();
        let first = table.insert(Entry::Poller(poller_entry()));
        table.remove_of_kind(first, KIND_POLLER).unwrap();
        let second = table.insert(Entry::Poller(poller_entry()));
        assert_ne!(first, second, "slot reuse must change the handle");
        assert!(matches!(table.poller(first), Err(KtError::StaleHandle)));
        assert!(table.poller(second).is_ok());
    }

    /// Freeing must check the kind too, not only index and generation.
    ///
    /// It did not, so `kt_poller_free(runtimeHandle)` removed the runtime, returned KT_OK, and
    /// skipped shutdown -- leaving every pump parked forever on a runtime that no longer existed.
    #[test]
    fn freeing_with_the_wrong_kind_is_rejected_and_leaves_the_entry_alive() {
        let table = HandleTable::new();
        let handle = table.insert(Entry::Poller(poller_entry()));
        assert!(matches!(
            table.remove_of_kind(handle, KIND_RUNTIME),
            Err(KtError::WrongHandleKind)
        ));
        assert!(table.poller(handle).is_ok(), "a rejected free must not remove the entry");
    }

    /// Only 24 bits of the generation are encoded in a handle, so the bump must wrap in 24 bits.
    ///
    /// Bumping the full u32 meant that after 2^24 cycles a slot issued a handle encoding
    /// generation 0 while holding 0x1000000: the first use of a brand-new handle returned
    /// StaleHandle and the entry could never be freed.
    #[test]
    fn a_slot_stays_usable_across_the_generation_wrap() {
        let table = HandleTable::new();
        // Drive one slot past 2^24 reuses by forcing the counter near the boundary.
        {
            let mut inner = table.inner.write();
            inner.slots.push(Slot { generation: 0x00FF_FFFF, entry: None });
            inner.free.push(0);
        }
        for _ in 0..4 {
            let handle = table.insert(Entry::Poller(poller_entry()));
            assert!(table.poller(handle).is_ok(), "a freshly issued handle must resolve");
            table.remove_of_kind(handle, KIND_POLLER).unwrap();
        }
    }

    #[test]
    fn the_wrong_handle_kind_is_rejected() {
        let table = HandleTable::new();
        let poller = table.insert(Entry::Poller(poller_entry()));
        assert!(matches!(table.worker(poller), Err(KtError::WrongHandleKind)));
    }
}
