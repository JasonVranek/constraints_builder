use super::constraint_proof_storage::{ConstraintProofStorage, ProofsResponse};
use jsonrpsee::{server::Server, RpcModule};
use std::net::{SocketAddr, SocketAddrV4};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::info;

const GET_CONSTRAINT_PROOFS: &str = "getConstraintProofs";

/// Configuration for the constraint proof RPC server
#[derive(Debug, Clone)]
pub struct ConstraintProofRpcConfig {
    pub server_ip: std::net::Ipv4Addr,
    pub server_port: u16,
    pub max_connections: u32,
}

impl Default for ConstraintProofRpcConfig {
    fn default() -> Self {
        Self {
            server_ip: std::net::Ipv4Addr::new(127, 0, 0, 1),
            server_port: 8548,  // Next in sequence after constraint RPC (8547)
            max_connections: 100,
        }
    }
}

/// Start the constraint proof RPC server
pub async fn start_constraint_proof_rpc(
    config: ConstraintProofRpcConfig,
    storage: ConstraintProofStorage,
    cancel: CancellationToken,
) -> eyre::Result<JoinHandle<()>> {
    let addr = SocketAddr::V4(SocketAddrV4::new(config.server_ip, config.server_port));

    let server = Server::builder()
        .max_connections(config.max_connections)
        .http_only()
        .build(addr)
        .await?;

    info!(
        address = %addr,
        "Started constraint proof RPC server"
    );

    let mut module = RpcModule::new(storage.clone());

    // Register the getConstraintProofs method
    module.register_method(GET_CONSTRAINT_PROOFS, |_, storage| {
        let proofs = storage.get_recent_proofs();
        Ok::<ProofsResponse, jsonrpsee::types::ErrorObject>(proofs)
    })?;

    let server_handle = server.start(module);

    // Spawn task to handle shutdown
    let task = tokio::spawn(async move {
        cancel.cancelled().await;
        info!("Shutting down constraint proof RPC server");
        server_handle.stop().ok();
        server_handle.stopped().await;
    });

    Ok(task)
}
