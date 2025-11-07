use super::ConstraintInputConfig;
use crate::{
    live_builder::order_input::ReplaceableOrderPoolCommand,
    telemetry::inc_order_input_rpc_errors
};
use rbuilder_primitives::{
    constraints::Constraints,
    Bundle, BundleVersion, Metadata, Order,
    serialize::{RawTx, TxEncoding}
};
use alloy_primitives::Bytes;
use eyre::Context;
use jsonrpsee::{server::Server, types::ErrorObject, RpcModule};
use std::net::{SocketAddr, SocketAddrV4};
use tokio::{
    sync::{mpsc, mpsc::error::SendTimeoutError},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, trace, warn};
use uuid::Uuid;

const SUBMIT_CONSTRAINTS: &str = "submitConstraints";

/// Convert each transaction in constraint to individual bundles for order pipeline
fn constraint_to_individual_bundles(constraint: &Constraints) -> eyre::Result<Vec<Bundle>> {
    let mut bundles = Vec::new();
    
    for raw_tx_bytes in constraint.message.transactions.iter() {
        let tx_bytes = Bytes::from(raw_tx_bytes.as_ref().to_vec());
        let tx_with_blobs = RawTx { tx: tx_bytes }
            .decode(TxEncoding::NoBlobData)
            .context("Failed to decode constraint transaction")?
            .tx_with_blobs;

        let bundle = Bundle {
            version: BundleVersion::V2,
            block: Some(constraint.message.block),
            min_timestamp: None,
            max_timestamp: None,
            txs: vec![tx_with_blobs], // Single transaction per bundle
            reverting_tx_hashes: vec![], // Constraints must succeed
            dropping_tx_hashes: vec![],
            hash: constraint.hash(), // Use constraint hash as base for bundle hash
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

pub async fn start_constraint_rpc(
    config: ConstraintInputConfig,
    results: mpsc::Sender<Constraints>,
    order_sender: mpsc::Sender<ReplaceableOrderPoolCommand>,
    global_cancel: CancellationToken,
) -> eyre::Result<JoinHandle<()>> {
    let addr = SocketAddr::V4(SocketAddrV4::new(config.server_ip, config.server_port));
    let timeout = config.results_channel_timeout;

    let server = Server::builder()
        .max_connections(config.serve_max_connections)
        .http_only()
        .build(addr)
        .await?;

    let mut module = RpcModule::new(());
    module.register_async_method(SUBMIT_CONSTRAINTS, move |params, _| {
        let results = results.clone();
        let order_sender = order_sender.clone();
        async move {
            let constraints: Constraints = params.one().map_err(|err| {
                warn!(?err, "Failed to parse Constraints");
                inc_order_input_rpc_errors(SUBMIT_CONSTRAINTS);
                ErrorObject::owned(-32602, "invalid constraints", None::<()>)
            })?;

            trace!(block = constraints.message.block, "Received Constraints");
            let block = constraints.message.block;

            // Convert constraint to individual bundles and send to order pipeline
            match constraint_to_individual_bundles(&constraints) {
                Ok(constraint_bundles) => {
                    let mut sent_count = 0;
                    for bundle in constraint_bundles {
                        let order = Order::Bundle(bundle);
                        let order_command = ReplaceableOrderPoolCommand::Order(order);
                        if let Err(e) = order_sender.send_timeout(order_command, timeout).await {
                            warn!(?e, "Failed to send constraint bundle to order pipeline");
                        } else {
                            sent_count += 1;
                        }
                    }
                    debug!(block, sent_count, "Sent constraint transactions as individual bundles to order pipeline");
                }
                Err(e) => {
                    warn!(?e, block, "Failed to convert constraint to individual bundles");
                }
            }

            // Always send to constraint pipeline for fallback
            results
                .send_timeout(constraints, timeout)
                .await
                .map_err(|err| match err {
                    SendTimeoutError::Timeout(_) => {
                        warn!("submitConstraints timeout");
                        inc_order_input_rpc_errors("other");
                        ErrorObject::owned(-32000, "overloaded", None::<()>)
                    }
                    SendTimeoutError::Closed(_) => {
                        ErrorObject::owned(-32603, "internal error", None::<()>)
                    }
                })?;
            Ok::<String, ErrorObject>(format!("block_{block}"))
        }
    })?;

    let handle = server.start(module);

    Ok(tokio::spawn(async move {
        info!("Constraint RPC server: started on {}", addr);
        tokio::select! {
            _ = global_cancel.cancelled() => {},
            _ = handle.stopped() => {
                info!("Constraint RPC server stopped");
                global_cancel.cancel();
            },
        }
        info!("Constraint RPC server: finished");
    }))
}
