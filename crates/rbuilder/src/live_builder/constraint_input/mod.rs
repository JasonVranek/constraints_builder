//! constraint_input fetches new constraints from a constraint server and manages a constraint pool
pub mod constraint_sink;
pub mod constraintpool;
pub mod rpc_server;

pub use self::{
    constraint_sink::{ConstraintPoolCommand, ConstraintSink},
    constraintpool::{ConstraintPool, ConstraintPoolSubscriptionId},
};
use rbuilder_primitives::constraints::Constraints;
use parking_lot::Mutex;
use std::{net::Ipv4Addr, sync::Arc, time::Duration};
use tokio::{sync::mpsc, task::JoinHandle};
use tokio_util::sync::CancellationToken;
use tracing::info;

/// Configuration for constraint input via RPC
#[derive(Debug, Clone)]
pub struct ConstraintInputConfig {
    pub enabled: bool,
    /// Constraint RPC port
    pub server_port: u16,
    /// Constraint RPC IP address  
    pub server_ip: Ipv4Addr,
    /// Constraint RPC max connections
    pub serve_max_connections: u32,
    /// Timeout when sending constraints to processing channel
    pub results_channel_timeout: Duration,
}

/// Default values for constraint input configuration
pub const DEFAULT_SERVE_MAX_CONNECTIONS: u32 = 4096;
pub const DEFAULT_RESULTS_CHANNEL_TIMEOUT: Duration = Duration::from_millis(50);
pub const CONSTRAINT_INPUT_BUFFER: usize = 10_000;

impl Default for ConstraintInputConfig {
    fn default() -> Self {
        use crate::live_builder::base_config::DEFAULT_CONSTRAINT_SERVER_PORT;
        Self {
            enabled: true,
            server_port: DEFAULT_CONSTRAINT_SERVER_PORT,
            server_ip: Ipv4Addr::new(127, 0, 0, 1),
            serve_max_connections: DEFAULT_SERVE_MAX_CONNECTIONS,
            results_channel_timeout: DEFAULT_RESULTS_CHANNEL_TIMEOUT,
        }
    }
}

impl ConstraintInputConfig {
    pub fn new(
        enabled: bool,
        server_port: u16,
        server_ip: Ipv4Addr,
        serve_max_connections: u32,
        results_channel_timeout: Duration,
    ) -> Self {
        Self {
            enabled,
            server_port,
            server_ip,
            serve_max_connections,
            results_channel_timeout,
        }
    }
}

#[derive(Debug)]
pub struct ConstraintPoolSubscriber {
    constraintpool: Arc<Mutex<ConstraintPool>>,
}

impl ConstraintPoolSubscriber {
    pub fn add_sink(
        &self,
        block: u64,
        sink: Box<dyn ConstraintSink>,
    ) -> ConstraintPoolSubscriptionId {
        self.constraintpool.lock().add_sink(block, sink)
    }

    pub fn remove_sink(
        &self,
        id: &ConstraintPoolSubscriptionId,
    ) -> Option<Box<dyn ConstraintSink>> {
        self.constraintpool.lock().remove_sink(id)
    }

    pub fn block_updated(&self, current_block: u64) {
        self.constraintpool.lock().block_updated(current_block);
    }

    pub fn get_constraints_for_block(&self, block: u64) -> Vec<Constraints> {
        self.constraintpool.lock().get_constraints_for_block(block)
    }
}

pub async fn start_constraintpool_jobs(
    config: ConstraintInputConfig,
    global_cancel: CancellationToken,
    constraint_sender: mpsc::Sender<ConstraintPoolCommand>,
    constraint_receiver: mpsc::Receiver<ConstraintPoolCommand>,
    order_sender: mpsc::Sender<crate::live_builder::order_input::ReplaceableOrderPoolCommand>,
) -> eyre::Result<(JoinHandle<()>, ConstraintPoolSubscriber)> {
    let constraint_pool = Arc::new(Mutex::new(ConstraintPool::new()));
    let subscriber = ConstraintPoolSubscriber {
        constraintpool: constraint_pool.clone(),
    };

    // Start RPC server if enabled
    let _rpc_handle = if config.enabled {
        let (constraint_tx, mut constraint_rx) =
            mpsc::channel::<Constraints>(CONSTRAINT_INPUT_BUFFER);

        let rpc_handle =
            rpc_server::start_constraint_rpc(config, constraint_tx, order_sender, global_cancel.clone()).await?;

        // Bridge RPC constraints to pool commands
        tokio::spawn({
            let constraint_sender = constraint_sender.clone();
            async move {
                while let Some(constraint) = constraint_rx.recv().await {
                    if constraint_sender
                        .send(ConstraintPoolCommand::Insert(constraint))
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
            }
        });

        Some(rpc_handle)
    } else {
        info!("Constraint RPC disabled");
        None
    };

    // Main constraint processing task
    let handle = tokio::spawn(async move {
        info!("ConstraintPoolJobs: started");
        let mut constraint_receiver = constraint_receiver;
        let mut new_commands = Vec::new();

        loop {
            tokio::select! {
                _ = global_cancel.cancelled() => { break; },
                n = constraint_receiver.recv_many(&mut new_commands, 100) => {
                    if n == 0 {
                        break;
                    }
                },
            };

            {
                let mut pool = constraint_pool.lock();
                pool.process_commands(std::mem::take(&mut new_commands));
            }
        }
        info!("ConstraintPoolJobs: finished");
    });

    Ok((handle, subscriber))
}
