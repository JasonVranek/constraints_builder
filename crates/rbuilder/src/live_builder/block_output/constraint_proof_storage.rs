use crate::building::constraint_proofs::ConstraintProof;
use ahash::HashMap;
use alloy_primitives::B256;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// Keep proofs for the last 10 slots
const MAX_SLOTS_TO_KEEP: usize = 10;

/// Proofs for all constraint transactions in a single block
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockProofs {
    pub block_hash: B256,
    pub block_number: u64,
    pub proofs: Vec<ConstraintProof>,
}

/// Proofs for all blocks in a single slot
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SlotProofs {
    pub slot: u64,
    pub blocks: Vec<BlockProofs>,
}

/// Response structure for the RPC endpoint - returns last 10 slots
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProofsResponse {
    pub slots: Vec<SlotProofs>,
}

/// Thread-safe storage for constraint proofs
#[derive(Debug, Clone)]
pub struct ConstraintProofStorage {
    // Map: slot -> block_hash -> BlockProofs
    storage: Arc<RwLock<HashMap<u64, HashMap<B256, BlockProofs>>>>,
}

impl ConstraintProofStorage {
    pub fn new() -> Self {
        Self {
            storage: Arc::new(RwLock::new(HashMap::default())),
        }
    }

    /// Store a proof for a constraint transaction
    pub fn add_proof(
        &self,
        slot: u64,
        block_hash: B256,
        block_number: u64,
        proof: ConstraintProof,
    ) {
        let mut storage = self.storage.write();

        // Get or create slot entry
        let slot_entry = storage.entry(slot).or_insert_with(HashMap::default);

        // Get or create block entry
        let block_entry = slot_entry.entry(block_hash).or_insert_with(|| BlockProofs {
            block_hash,
            block_number,
            proofs: Vec::new(),
        });

        // Add the proof
        block_entry.proofs.push(proof);

        // Cleanup: keep only the last MAX_SLOTS_TO_KEEP slots
        if storage.len() > MAX_SLOTS_TO_KEEP {
            // Find the oldest slot to remove
            if let Some(&min_slot) = storage.keys().min() {
                storage.remove(&min_slot);
            }
        }
    }

    /// Get proofs for the last 10 slots
    pub fn get_recent_proofs(&self) -> ProofsResponse {
        let storage = self.storage.read();

        // Get all slots and sort them
        let mut slots: Vec<u64> = storage.keys().copied().collect();
        slots.sort_unstable();
        slots.reverse(); // Most recent first

        // Build response
        let slot_proofs: Vec<SlotProofs> = slots
            .into_iter()
            .filter_map(|slot| {
                storage.get(&slot).map(|blocks| {
                    let block_proofs: Vec<BlockProofs> = blocks.values().cloned().collect();
                    SlotProofs {
                        slot,
                        blocks: block_proofs,
                    }
                })
            })
            .collect();

        ProofsResponse {
            slots: slot_proofs,
        }
    }
}

impl Default for ConstraintProofStorage {
    fn default() -> Self {
        Self::new()
    }
}
