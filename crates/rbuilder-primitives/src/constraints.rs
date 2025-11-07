use alloy_primitives::B256;
use ethereum_consensus::{bellatrix::presets::minimal::Transaction, ssz::prelude::*};

pub const MAX_CONSTRAINTS_PER_BLOCK: usize = 256;

// Could be signed as in reference
#[derive(Debug, Clone, Serializable, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct Constraints {
    pub message: ConstraintsMessage,
}

// Could include indices
#[derive(Debug, Clone, Serializable, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct ConstraintsMessage {
    pub block: u64,
    pub transactions: List<Transaction, MAX_CONSTRAINTS_PER_BLOCK>,
}

/// Unique identifier for a constraint, based on its content hash
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ConstraintId(B256);

impl Constraints {
    /// Generate a unique ID for this constraint based on its content
    pub fn id(&self) -> ConstraintId {
        // Compute hash of the serialized constraint using SSZ encoding
        let bytes = serialize(self).expect("Constraint SSZ serialization should not fail");
        ConstraintId(alloy_primitives::keccak256(&bytes))
    }

    /// Get the hash value from the constraint ID
    pub fn hash(&self) -> B256 {
        self.id().0
    }
}
