use alloy_consensus::TxEnvelope;
use tracing::info;

use fabric_constraints::types::ConstraintProofs;
use fabric_inclusion::proofs::TransactionTrieBuilder;

/// Prove transaction inclusion for a list of transactions and a list of constraint transaction hashes
pub fn prove_transaction_inclusion(
    transactions: &[TxEnvelope],
    constraint_tx_hashes: &[alloy_primitives::B256],
    block_number: u64,
) -> eyre::Result<ConstraintProofs> {
    info!(
        block_number,
        constraint_count = constraint_tx_hashes.len(),
        "Started transaction inclusion proof generation"
    );

    // Build transaction trie
    let mut prover = TransactionTrieBuilder::build(&transactions)?;

    // Prove constraints
    let proof = prover.prove_batch(constraint_tx_hashes)?;

    Ok(proof)
}
