use alloy_primitives::{B256, U256};
use eth_trie::{EthTrie, MemoryDB, Trie};
use ethereum_types::H256;
use reth_primitives::TransactionSigned;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// Result type for constraint proof operations
pub type Result<T> = std::result::Result<T, ConstraintProofError>;

/// Errors that can occur during constraint proof generation
#[derive(Debug, thiserror::Error)]
pub enum ConstraintProofError {
    #[error("Trie error: {0}")]
    TrieError(#[from] eth_trie::TrieError),
    #[error("Transaction not found at index {0}")]
    TxNotFound(usize),
    #[error("Transaction hash {0} not found in block")]
    TxHashNotFound(B256),
    #[error("Invalid proof")]
    InvalidProof,
}

/// Represents a Merkle inclusion proof for a constraint transaction
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConstraintProof {
    /// Transaction hash
    pub tx_hash: B256,
    /// Index of the transaction in the block
    pub tx_index: usize,
    /// Merkle proof nodes (hex-encoded)
    #[serde(with = "hex_vec")]
    pub proof: Vec<Vec<u8>>,
    /// Transaction root hash
    pub transactions_root: B256,
    /// Block number
    pub block_number: u64,
    /// Slot number
    pub slot: u64,
}

/// Serde helper for hex encoding Vec<Vec<u8>>
mod hex_vec {
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(data: &Vec<Vec<u8>>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        use serde::ser::SerializeSeq;
        let mut seq = serializer.serialize_seq(Some(data.len()))?;
        for item in data {
            seq.serialize_element(&format!("0x{}", hex::encode(item)))?;
        }
        seq.end()
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Vec<Vec<u8>>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let strings: Vec<String> = Vec::deserialize(deserializer)?;
        strings
            .into_iter()
            .map(|s| {
                let s = s.strip_prefix("0x").unwrap_or(&s);
                hex::decode(s).map_err(serde::de::Error::custom)
            })
            .collect()
    }
}

/// Builder for transaction Merkle Patricia Trie
pub struct TransactionTrieBuilder {
    trie: EthTrie<MemoryDB>,
    transactions: Vec<B256>,
}

impl TransactionTrieBuilder {
    /// Create a new trie builder
    pub fn new() -> Self {
        let memdb = Arc::new(MemoryDB::new(true));
        let trie = EthTrie::new(memdb);
        Self {
            trie,
            transactions: Vec::new(),
        }
    }

    /// Build the transaction trie from a list of signed transactions
    pub fn build(transactions: &[TransactionSigned]) -> Result<Self> {
        let mut builder = Self::new();

        for (idx, tx) in transactions.iter().enumerate() {
            // Key is RLP-encoded index
            let key = alloy_rlp::encode(U256::from(idx));

            // Value is RLP-encoded signed transaction
            let tx_bytes = alloy_rlp::encode(tx);

            builder.trie.insert(key.as_slice(), &tx_bytes)?;
            builder.transactions.push(*tx.hash());
        }

        Ok(builder)
    }

    /// Get the root hash of the trie
    pub fn root(&mut self) -> Result<B256> {
        let root = self.trie.root_hash()?;
        Ok(B256::from_slice(root.as_bytes()))
    }

    /// Generate a proof for a transaction at the given index
    pub fn get_proof(&mut self, tx_index: usize) -> Result<Vec<Vec<u8>>> {
        if tx_index >= self.transactions.len() {
            return Err(ConstraintProofError::TxNotFound(tx_index));
        }

        let key = alloy_rlp::encode(U256::from(tx_index));
        let proof = self.trie.get_proof(key.as_slice())?;
        Ok(proof)
    }

    /// Find the index of a transaction by its hash
    pub fn find_tx_index(&self, tx_hash: &B256) -> Result<usize> {
        self.transactions
            .iter()
            .position(|hash| hash == tx_hash)
            .ok_or(ConstraintProofError::TxHashNotFound(*tx_hash))
    }

    /// Verify a proof for a transaction at the given index
    pub fn verify_proof(
        &self,
        tx_index: usize,
        proof: &[Vec<u8>],
        root: &B256,
    ) -> Result<Vec<u8>> {
        let key = alloy_rlp::encode(U256::from(tx_index));
        let root_hash = H256::from_slice(root.as_slice());

        match self.trie.verify_proof(root_hash, key.as_slice(), proof.to_vec()) {
            Ok(Some(value)) => Ok(value),
            Ok(None) => Err(ConstraintProofError::InvalidProof),
            Err(e) => Err(ConstraintProofError::TrieError(e)),
        }
    }

    /// Get the transaction hash at the given index
    pub fn get_tx_hash(&self, tx_index: usize) -> Result<B256> {
        self.transactions
            .get(tx_index)
            .copied()
            .ok_or(ConstraintProofError::TxNotFound(tx_index))
    }
}

impl Default for TransactionTrieBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use reth_primitives::{Transaction, TxLegacy};

    #[test]
    fn test_build_trie_and_generate_proof() {
        // Create some test transactions
        let tx1 = TransactionSigned::from_transaction_and_signature(
            Transaction::Legacy(TxLegacy {
                nonce: 0,
                gas_price: 1000000000,
                gas_limit: 21000,
                to: alloy_primitives::TxKind::Call(alloy_primitives::Address::ZERO),
                value: alloy_primitives::U256::from(100),
                input: Default::default(),
                chain_id: Some(1),
            }),
            alloy_primitives::Signature::test_signature(),
        );

        let tx2 = TransactionSigned::from_transaction_and_signature(
            Transaction::Legacy(TxLegacy {
                nonce: 1,
                gas_price: 1000000000,
                gas_limit: 21000,
                to: alloy_primitives::TxKind::Call(alloy_primitives::Address::ZERO),
                value: alloy_primitives::U256::from(200),
                input: Default::default(),
                chain_id: Some(1),
            }),
            alloy_primitives::Signature::test_signature(),
        );

        let transactions = vec![tx1.clone(), tx2.clone()];

        // Build trie
        let mut builder = TransactionTrieBuilder::build(&transactions).unwrap();

        // Get root
        let root = builder.root().unwrap();
        assert_ne!(root, B256::ZERO);

        // Generate proof for first transaction
        let proof = builder.get_proof(0).unwrap();
        assert!(!proof.is_empty());

        // Find transaction index
        let tx1_hash = tx1.hash();
        let index = builder.find_tx_index(&tx1_hash).unwrap();
        assert_eq!(index, 0);

        // Verify proof
        let verified = builder.verify_proof(0, &proof, &root);
        assert!(verified.is_ok());
    }
}
