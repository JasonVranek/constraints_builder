use crate::{
    building::{
        constraint_proofs::{ConstraintProof, TransactionTrieBuilder},
        BuiltBlockTrace,
    },
    live_builder::{
        block_output::{
            bidding_service_interface::RelaySet,
            constraint_proof_storage::ConstraintProofStorage
        },
        payload_events::MevBoostSlotData
    },
};
use alloy_eips::eip2718::Decodable2718;
use alloy_primitives::U256;
use alloy_rpc_types_beacon::relay::SubmitBlockRequest as AlloySubmitBlockRequest;
use reth_primitives::TransactionSigned;
use std::sync::Arc;
use tracing::{debug, error, info};

use super::bidding_service_interface::BidObserver;

/// Observer that generates Merkle inclusion proofs for constraint transactions
/// after blocks are submitted to relays.
#[derive(Debug, Clone)]
pub struct ConstraintProofObserver {
    storage: ConstraintProofStorage,
}

impl ConstraintProofObserver {
    pub fn new(storage: ConstraintProofStorage) -> Self {
        Self { storage }
    }
}

impl BidObserver for ConstraintProofObserver {
    fn block_submitted(
        &self,
        slot_data: &MevBoostSlotData,
        submit_block_request: Arc<AlloySubmitBlockRequest>,
        built_block_trace: Arc<BuiltBlockTrace>,
        _builder_name: String,
        _best_bid_value: U256,
        relays: &RelaySet,
    ) {
        // Check if there are any appended constraint transactions
        if built_block_trace.appended_constraint_txs.is_empty() {
            return;
        }

        // Clone necessary data for the background task
        let constraint_tx_hashes = built_block_trace.appended_constraint_txs.clone();
        let block_number = slot_data.block();
        let slot = slot_data.slot();
        let relay_count = relays.relays().len();
        let storage = self.storage.clone();

        debug!(
            block_number,
            slot,
            constraint_count = constraint_tx_hashes.len(),
            relays = relay_count,
            "Spawning background task to generate constraint proofs"
        );

        // Spawn background task for proof generation
        tokio::spawn(async move {
            generate_and_log_constraint_proofs(
                submit_block_request,
                constraint_tx_hashes,
                block_number,
                slot,
                storage,
            )
            .await;
        });
    }
}

/// Background task that generates and logs Merkle inclusion proofs for constraint transactions
async fn generate_and_log_constraint_proofs(
    submit_block_request: Arc<AlloySubmitBlockRequest>,
    constraint_tx_hashes: Vec<alloy_primitives::B256>,
    block_number: u64,
    slot: u64,
    storage: ConstraintProofStorage,
) {
    info!(
        block_number,
        slot,
        constraint_count = constraint_tx_hashes.len(),
        "Started constraint proof generation"
    );

    // Extract block hash
    let block_hash = extract_block_hash(&submit_block_request);

    // Extract transactions from the execution payload
    let transactions = match extract_transactions(&submit_block_request) {
        Ok(txs) => txs,
        Err(e) => {
            error!(
                block_number,
                slot,
                error = %e,
                "Failed to extract transactions from execution payload"
            );
            return;
        }
    };

    debug!(
        block_number,
        slot,
        total_transactions = transactions.len(),
        "Extracted transactions from block"
    );

    // Build transaction trie
    let mut trie_builder = match TransactionTrieBuilder::build(&transactions) {
        Ok(builder) => builder,
        Err(e) => {
            error!(
                block_number,
                slot,
                error = %e,
                "Failed to build transaction trie"
            );
            return;
        }
    };

    // Get the transaction root
    let transactions_root = match trie_builder.root() {
        Ok(root) => root,
        Err(e) => {
            error!(
                block_number,
                slot,
                error = %e,
                "Failed to compute transaction root"
            );
            return;
        }
    };

    debug!(
        block_number,
        slot,
        %transactions_root,
        "Built transaction trie"
    );

    // Generate proofs for each constraint transaction
    let mut success_count = 0;
    let mut failure_count = 0;

    for constraint_tx_hash in constraint_tx_hashes {
        match generate_single_proof(
            &mut trie_builder,
            constraint_tx_hash,
            transactions_root,
            block_number,
            slot,
        ) {
            Ok(proof) => {
                // Store proof in storage
                storage.add_proof(slot, block_hash, block_number, proof.clone());

                // Log proof as JSON
                let proof_json = match serde_json::to_string(&proof) {
                    Ok(json) => json,
                    Err(e) => {
                        error!(
                            block_number,
                            slot,
                            %constraint_tx_hash,
                            error = %e,
                            "Failed to serialize proof to JSON"
                        );
                        failure_count += 1;
                        continue;
                    }
                };

                info!(
                    constraint_proof = %proof_json,
                    "Generated constraint transaction inclusion proof"
                );
                success_count += 1;
            }
            Err(e) => {
                error!(
                    block_number,
                    slot,
                    %constraint_tx_hash,
                    error = %e,
                    "Failed to generate proof for constraint transaction"
                );
                failure_count += 1;
            }
        }
    }

    if failure_count == 0 {
        info!(
            block_number,
            slot,
            success_count,
            "Successfully generated all constraint proofs"
        );
    } else {
        error!(
            block_number,
            slot,
            success_count,
            failure_count,
            "Completed constraint proof generation with failures"
        );
    }
}

/// Generate a single Merkle inclusion proof for a constraint transaction
fn generate_single_proof(
    trie_builder: &mut TransactionTrieBuilder,
    tx_hash: alloy_primitives::B256,
    transactions_root: alloy_primitives::B256,
    block_number: u64,
    slot: u64,
) -> Result<ConstraintProof, String> {
    // Find the transaction index
    let tx_index = trie_builder
        .find_tx_index(&tx_hash)
        .map_err(|e| format!("Transaction not found in block: {}", e))?;

    // Generate the proof
    let proof = trie_builder
        .get_proof(tx_index)
        .map_err(|e| format!("Failed to generate proof: {}", e))?;

    Ok(ConstraintProof {
        tx_hash,
        tx_index,
        proof,
        transactions_root,
        block_number,
        slot,
    })
}

/// Extract signed transactions from the execution payload
fn extract_transactions(
    submit_block_request: &AlloySubmitBlockRequest,
) -> Result<Vec<TransactionSigned>, String> {
    // Extract transaction bytes from the appropriate variant
    let tx_bytes_list = match submit_block_request {
        AlloySubmitBlockRequest::Electra(request) => {
            &request.execution_payload.payload_inner.payload_inner.transactions
        }
        AlloySubmitBlockRequest::Fulu(request) => {
            &request.execution_payload.payload_inner.payload_inner.transactions
        }
        AlloySubmitBlockRequest::Deneb(request) => {
            &request.execution_payload.payload_inner.payload_inner.transactions
        }
        AlloySubmitBlockRequest::Capella(request) => {
            &request.execution_payload.payload_inner.transactions
        }
    };

    // Decode transactions
    let mut transactions = Vec::new();

    for tx_bytes in tx_bytes_list {
        // Decode directly as reth TransactionSigned
        let tx = TransactionSigned::decode_2718(&mut tx_bytes.as_ref())
            .map_err(|e| format!("Failed to decode transaction: {}", e))?;
        transactions.push(tx);
    }

    if transactions.is_empty() {
        return Err("No transactions in execution payload".to_string());
    }

    Ok(transactions)
}

/// Extract block hash from the execution payload
fn extract_block_hash(submit_block_request: &AlloySubmitBlockRequest) -> alloy_primitives::B256 {
    match submit_block_request {
        AlloySubmitBlockRequest::Electra(request) => {
            request.execution_payload.payload_inner.payload_inner.block_hash
        }
        AlloySubmitBlockRequest::Fulu(request) => {
            request.execution_payload.payload_inner.payload_inner.block_hash
        }
        AlloySubmitBlockRequest::Deneb(request) => {
            request.execution_payload.payload_inner.payload_inner.block_hash
        }
        AlloySubmitBlockRequest::Capella(request) => {
            request.execution_payload.payload_inner.block_hash
        }
    }
}
