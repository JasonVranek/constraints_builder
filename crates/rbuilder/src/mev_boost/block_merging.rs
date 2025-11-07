use alloy_primitives::Address;
use serde::{Deserialize, Serialize};
use ssz_derive::{Decode, Encode};

use rbuilder_primitives::mev_boost::SubmitBlockRequest;

/// Simple JSON/SSZ wrapper for helix block merging
#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
pub struct SignedBidSubmissionWithMergingData {
    pub submission: SubmitBlockRequest,
    pub merging_data: BlockMergingData,
}

#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
#[ssz(enum_behaviour = "transparent")]
#[serde(untagged)]
pub enum Order {
    Tx(TransactionOrder),
    Bundle(BundleOrder),
}

/// Vector of transaction indices.
pub type TxIndices = Vec<usize>;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Encode, Decode)]
#[serde(deny_unknown_fields)]
pub struct TransactionOrder {
    /// Index into block.transactions
    pub index: usize,
    /// If the transaction is allowed to revert
    pub can_revert: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, Encode, Decode)]
#[serde(deny_unknown_fields)]
/// References a bundle of transactions via their indices.
/// All indices are for the block the transactions come from.
pub struct BundleOrder {
    /// Signals txs that are part of the bundle and ordering of txs.
    /// Indices are for the block body the transactions come from.
    pub txs: TxIndices,
    /// Txs that may revert.
    /// Indices are for the [txs](Self::txs) array.
    pub reverting_txs: TxIndices,
    /// Txs that are allowed to be omitted, but not revert.
    /// Indices are for the [txs](Self::txs) array.
    pub dropping_txs: TxIndices,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize, Encode, Decode)]
#[serde(deny_unknown_fields)]
pub struct BlockMergingData {
    pub allow_appending: bool,
    pub builder_address: Address,
    pub merge_orders: Vec<Order>,
}

impl BlockMergingData {
    pub fn is_default(&self) -> bool {
        !self.allow_appending && self.merge_orders.is_empty()
    }

    /// Generate block merging data for all transactions in the block.
    /// All transactions are marked as available for merging and can revert,
    /// giving the relay maximum flexibility for block merging.
    pub fn for_all_transactions(builder_address: Address, transaction_count: usize) -> Self {
        let merge_orders = (0..transaction_count)
            .map(|index| Order::Tx(TransactionOrder {
                index,
                can_revert: true, // Allow revert for maximum relay flexibility
            }))
            .collect();

        Self {
            allow_appending: true,
            builder_address,
            merge_orders,
        }
    }
}

/// Extract fee recipient and transaction count from SubmitBlockRequest
/// Centralized logic to avoid duplication between SSZ and JSON paths
pub fn extract_submission_metadata(request: &SubmitBlockRequest) -> (Address, usize) {
    use alloy_rpc_types_beacon::relay::SubmitBlockRequest as AlloySubmitBlockRequest;
    match request.request.as_ref() {
        AlloySubmitBlockRequest::Capella(req) => {
            (req.execution_payload.payload_inner.fee_recipient, req.execution_payload.payload_inner.transactions.len())
        },
        AlloySubmitBlockRequest::Deneb(req) => {
            (req.execution_payload.payload_inner.payload_inner.fee_recipient, req.execution_payload.payload_inner.payload_inner.transactions.len())
        },
        AlloySubmitBlockRequest::Electra(req) => {
            (req.execution_payload.payload_inner.payload_inner.fee_recipient, req.execution_payload.payload_inner.payload_inner.transactions.len())
        },
        AlloySubmitBlockRequest::Fulu(req) => {
            (req.execution_payload.payload_inner.payload_inner.fee_recipient, req.execution_payload.payload_inner.payload_inner.transactions.len())
        },
    }
}