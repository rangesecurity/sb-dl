//! client utilities from solana-storage-bigtable

use {
    solana_sdk::clock::Slot,
    log::warn,
};

// Convert a slot to its bucket representation whereby lower slots are always lexically ordered
// before higher slots
pub fn slot_to_key(slot: Slot) -> String {
    format!("{slot:016x}")
}

pub fn slot_to_blocks_key(slot: Slot) -> String {
    slot_to_key(slot)
}

pub fn slot_to_entries_key(slot: Slot) -> String {
    slot_to_key(slot)
}

pub fn slot_to_tx_by_addr_key(slot: Slot) -> String {
    slot_to_key(!slot)
}

// Reverse of `slot_to_key`
pub fn key_to_slot(key: &str) -> Option<Slot> {
    match Slot::from_str_radix(key, 16) {
        Ok(slot) => Some(slot),
        Err(err) => {
            // bucket data is probably corrupt
            warn!("Failed to parse object key as a slot: {}: {}", key, err);
            None
        }
    }
}