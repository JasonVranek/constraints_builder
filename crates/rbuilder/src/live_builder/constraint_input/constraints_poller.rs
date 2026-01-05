use std::time::Duration;

use super::ConstraintInputConfig;
use crate::{
    live_builder::order_input::ReplaceableOrderPoolCommand, telemetry::inc_order_input_rpc_errors,
};
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

fn get_next_slot(genesis_timestamp: u64) -> u64 {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("Failed to get current time")
        .as_secs();
    let slot = (now - genesis_timestamp) / 12;
    slot + 1
}

pub async fn run(
    config: ConstraintInputConfig,
    results: mpsc::Sender<ConstraintsMessage>,
    order_sender: mpsc::Sender<ReplaceableOrderPoolCommand>,
    global_cancel: CancellationToken,
) -> eyre::Result<JoinHandle<()>> {
    let results = results.clone();
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
        // Track the last slot we successfully polled to avoid immediate re-polling
        let mut last_polled_slot: Option<u64> = None;

        loop {
            // Get the next slot
            let slot = get_next_slot(config.genesis_timestamp);

            // Skip if we already successfully polled this slot
            // It is assumed there is only one SignedConstraint object per slot
            if last_polled_slot == Some(slot) {
                tokio::time::sleep(Duration::from_millis(500)).await;
                continue;
            }

            // Call the constraints server to get the constraints
            match client.get_constraints(slot).await {
                Ok(signed_constraints) => {
                    // We assume that there is only one constraint per slot
                    if let Some(signed_constraint) = signed_constraints.first() {
                        let constraints_message = &signed_constraint.message;

                        match constraint_to_individual_bundles(constraints_message) {
                            Ok(constraint_bundles) => {
                                for bundle in constraint_bundles {
                                    let order = Order::Bundle(bundle);
                                    let order_command = ReplaceableOrderPoolCommand::Order(order);
                                    if let Err(e) =
                                        order_sender.send_timeout(order_command, timeout).await
                                    {
                                        warn!(
                                            ?e,
                                            "Failed to send constraint bundle to order pipeline"
                                        );
                                    }
                                }
                            }
                            Err(e) => {
                                warn!(
                                    ?e,
                                    slot, "Failed to convert constraint to individual bundles"
                                );
                            }
                        }

                        // Always send to constraint pipeline for fallback
                        match results
                            .send_timeout(constraints_message.clone(), timeout)
                            .await
                        {
                            Ok(()) => {},
                            Err(e) => {
                                warn!(?e, slot, "Failed to send constraint to constraint pipeline");
                                inc_order_input_rpc_errors("other");
                            }
                        }
                        info!("{} constraints found for slot {}", constraints_message.constraints.len(), slot);

                        // Mark this slot as successfully polled
                        last_polled_slot = Some(slot);
                    } else {
                        // Backoff before retrying
                        warn!(slot, "No constraints found for slot");
                        tokio::time::sleep(Duration::from_millis(500)).await;
                        continue;
                    }
                }
                Err(e) => {
                    warn!(?e, "Failed to get constraints");
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
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
