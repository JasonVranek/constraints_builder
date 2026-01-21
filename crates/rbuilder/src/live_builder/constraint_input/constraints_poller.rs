use std::time::Duration;

use super::ConstraintInputConfig;
use crate::telemetry::inc_order_input_rpc_errors;
use eyre::Context;
use fabric_constraints::{
    client::{ConstraintsClient, HttpConstraintsClient},
    types::ConstraintsMessage,
};
use fabric_inclusion::types::InclusionPayload;
use rbuilder_primitives::{
    serialize::{RawTx, TxEncoding},
    Bundle, BundleVersion, Metadata, Order,
};
use tokio::{sync::mpsc, task::JoinHandle};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};
use url::Url;
use uuid::Uuid;

/// Convert each transaction in constraint to individual bundles for order pipeline
fn constraint_to_individual_bundles(
    constraints_message: &ConstraintsMessage,
) -> eyre::Result<Vec<Bundle>> {
    let mut bundles = Vec::new();

    for constraint in constraints_message.constraints.iter() {
        let payload = InclusionPayload::abi_decode(&constraint.payload)?;
        let tx = payload.decode_transaction()?;

        let tx_with_blobs = RawTx {
            tx: payload.signed_tx,
        }
        .decode(TxEncoding::NoBlobData)
        .context("Failed to decode constraint transaction")?
        .tx_with_blobs;

        let bundle = Bundle {
            version: BundleVersion::V2,
            block: None,
            min_timestamp: None,
            max_timestamp: None,
            txs: vec![tx_with_blobs],    // Single transaction per bundle
            reverting_tx_hashes: vec![], // Constraints must succeed
            dropping_tx_hashes: vec![],
            hash: *tx.hash(), // Use constraint tx hash as base for bundle hash
            uuid: Uuid::new_v4(),
            replacement_data: None,
            signer: None,
            refund_identity: None,
            metadata: Metadata::new_received_now(),
            refund: None,
            external_hash: None,
        };
        bundles.push(bundle);
    }

    Ok(bundles)
}

/// Calculate the current slot based on system time
fn get_current_slot(genesis_timestamp: u64) -> u64 {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("Failed to get current time")
        .as_secs();
    (now - genesis_timestamp) / 12
}

/// Get slots we should poll for constraints.
/// We poll both the current slot AND the next slot because:
/// - If we're in slot N-1 and building for slot N, we need slot N constraints
/// - If we're already in slot N and building for slot N, we also need slot N constraints
/// - Polling both ensures we don't miss constraints due to timing edge cases
fn get_slots_to_poll(genesis_timestamp: u64) -> (u64, u64) {
    let current_slot = get_current_slot(genesis_timestamp);
    (current_slot, current_slot + 1)
}

pub async fn run(
    config: ConstraintInputConfig,
    constraints_pool: mpsc::Sender<ConstraintsMessage>,
    global_cancel: CancellationToken,
) -> eyre::Result<JoinHandle<()>> {
    let results = constraints_pool.clone();
    let timeout = config.results_channel_timeout;

    // Parse the constraint server URL to extract host and port
    let url = Url::parse(&config.server_url)
        .context("Failed to parse constraint_server_url")?;
    let host = url
        .host_str()
        .ok_or_else(|| eyre::eyre!("constraint_server_url missing host"))?
        .to_string();
    let port = url
        .port()
        .ok_or_else(|| eyre::eyre!("constraint_server_url missing port"))?;

    // Client to call the constraints server
    let client = HttpConstraintsClient::new(host, port, None);

    let handle = tokio::spawn(async move {
        // Track slots we've successfully polled to avoid re-polling
        // We keep a small set since we only care about recent slots
        let mut polled_slots: std::collections::HashSet<u64> = std::collections::HashSet::new();
        let mut last_cleanup_slot: u64 = 0;

        loop {
            // Get both current slot and next slot to cover timing edge cases
            let (current_slot, next_slot) = get_slots_to_poll(config.genesis_timestamp);

            // Clean up old polled slots periodically (keep only last 5 slots)
            if current_slot > last_cleanup_slot + 5 {
                polled_slots.retain(|&s| s >= current_slot.saturating_sub(2));
                last_cleanup_slot = current_slot;
            }

            // Try to poll for both slots
            let slots_to_try = [current_slot, next_slot];
            let mut any_polled = false;

            for &slot in &slots_to_try {
                // Skip if we already successfully polled this slot
                if polled_slots.contains(&slot) {
                    continue;
                }

                // Call the constraints server to get the constraints
                match client.get_constraints(slot).await {
                    Ok(signed_constraints) => {
                        // We assume that there is only one constraint per slot
                        if let Some(signed_constraint) = signed_constraints.first() {
                            let constraints_message = &signed_constraint.message;

                            // Validate that returned constraints match the requested slot
                            if constraints_message.slot != slot {
                                warn!(
                                    requested_slot = slot,
                                    returned_slot = constraints_message.slot,
                                    constraint_count = constraints_message.constraints.len(),
                                    "Constraint slot mismatch! Server returned constraints for different slot than requested"
                                );
                                // Mark as polled anyway to avoid repeated requests
                                polled_slots.insert(slot);
                                continue;
                            }

                            // Send to constraint pipeline
                            match results
                                .send_timeout(constraints_message.clone(), timeout)
                                .await
                            {
                                Ok(()) => {
                                    info!(
                                        slot,
                                        current_slot,
                                        next_slot,
                                        constraint_count = constraints_message.constraints.len(),
                                        "Constraints found and sent to pool"
                                    );
                                }
                                Err(e) => {
                                    warn!(?e, slot, "Failed to send constraint to constraint pipeline");
                                    inc_order_input_rpc_errors("other");
                                }
                            }

                            // Mark this slot as successfully polled
                            polled_slots.insert(slot);
                            any_polled = true;
                        } else {
                            // No constraints for this slot (might not exist yet for next_slot)
                            if slot == current_slot {
                                // Only log warning for current slot - next slot might not be ready
                                warn!(slot, "No constraints found for current slot");
                            }
                        }
                    }
                    Err(e) => {
                        // Only log error for current slot to reduce noise
                        if slot == current_slot {
                            warn!(?e, slot, "Failed to get constraints from server");
                        }
                    }
                }
            }

            // Backoff before next poll cycle
            if any_polled {
                tokio::time::sleep(Duration::from_millis(200)).await;
            } else {
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        }
    });

    Ok(tokio::spawn(async move {
        info!("Constraint poller: started");
        global_cancel.cancelled().await;
        handle.abort();
        info!("Constraint poller: finished");
    }))
}
