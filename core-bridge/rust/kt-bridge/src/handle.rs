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
    pub fn remove(&self, handle: u64) -> KtResult<Entry> {
        let index = (handle as u32) as usize;
        let generation = ((handle >> GEN_SHIFT) & GEN_MASK) as u32;
        let mut table = self.inner.write();
        let slot = table.slots.get_mut(index).ok_or(KtError::StaleHandle)?;
        if slot.generation != generation {
            return Err(KtError::StaleHandle);
        }
        let entry = slot.entry.take().ok_or(KtError::StaleHandle)?;
        slot.generation = slot.generation.wrapping_add(1).max(1);
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

impl Entry {
    fn kind(&self) -> u8 {
        match self {
            Entry::Runtime(_) => 1,
            Entry::Client(_) => 2,
            Entry::Worker(_) => 3,
            Entry::Ephemeral(_) => 4,
            Entry::Poller(_) => 5,
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
        Arc::new(Queue::new().poller(0))
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
        table.remove(handle).unwrap();
        assert!(matches!(table.poller(handle), Err(KtError::StaleHandle)));
    }

    #[test]
    fn double_free_is_an_error_not_a_second_removal() {
        let table = HandleTable::new();
        let handle = table.insert(Entry::Poller(poller_entry()));
        table.remove(handle).unwrap();
        assert!(matches!(table.remove(handle), Err(KtError::StaleHandle)));
    }

    #[test]
    fn a_reused_slot_does_not_answer_to_the_old_handle() {
        // The generation counter earns its keep here: without it the stale handle would resolve
        // to whatever object landed in the recycled slot.
        let table = HandleTable::new();
        let first = table.insert(Entry::Poller(poller_entry()));
        table.remove(first).unwrap();
        let second = table.insert(Entry::Poller(poller_entry()));
        assert_ne!(first, second, "slot reuse must change the handle");
        assert!(matches!(table.poller(first), Err(KtError::StaleHandle)));
        assert!(table.poller(second).is_ok());
    }

    #[test]
    fn the_wrong_handle_kind_is_rejected() {
        let table = HandleTable::new();
        let poller = table.insert(Entry::Poller(poller_entry()));
        assert!(matches!(table.worker(poller), Err(KtError::WrongHandleKind)));
    }
}
