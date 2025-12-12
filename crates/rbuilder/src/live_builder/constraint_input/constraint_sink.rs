// constraint_sink.rs — lean & mirrors order sink

use fabric_constraints::types::ConstraintsMessage;
use core::fmt::Debug;
use tokio::sync::mpsc;
use tracing::info;

/// Receiver of constraint commands (immutable inserts; removals are by slot).
/// Methods return `bool` so the source can drop dead subscribers immediately.
pub trait ConstraintSink: Debug + Send {
    fn insert_constraint(&mut self, constraints: ConstraintsMessage) -> bool;
    fn remove_constraints_for_slot(&mut self, slot: u64) -> bool;
    fn is_alive(&self) -> bool;
}

/// Minimal sink that just logs events.
#[derive(Debug)]
pub struct ConstraintPrinter;

impl ConstraintSink for ConstraintPrinter {
    fn insert_constraint(&mut self, constraints: ConstraintsMessage) -> bool {
        info!(
            slot = ?constraints.slot,
            count = constraints.constraints.len(),
            "New constraints"
        );
        true
    }

    // Constraints are removed by slot, not by ID (differs from orders).
    fn remove_constraints_for_slot(&mut self, slot: u64) -> bool {
        info!(slot, "Removed constraints for slot");
        true
    }

    fn is_alive(&self) -> bool {
        true
    }
}

impl Drop for ConstraintPrinter {
    fn drop(&mut self) {
        println!("ConstraintPrinter Dropped");
    }
}

/// Commands for push→pull adaptation (parallels OrderPoolCommand).
#[derive(Debug, Clone)]
pub enum ConstraintPoolCommand {
    Insert(ConstraintsMessage),
    RemoveSlot(u64),
}

/// Channel-backed adapter implementing `ConstraintSink` (parallels OrderSender2OrderSink).
#[derive(Debug)]
pub struct ConstraintSender2ConstraintSink {
    sender: mpsc::UnboundedSender<ConstraintPoolCommand>,
}

impl ConstraintSender2ConstraintSink {
    /// Returns the sender (implements `ConstraintSink`) and the receiver to poll.
    pub fn new() -> (Self, mpsc::UnboundedReceiver<ConstraintPoolCommand>) {
        let (sender, receiver) = mpsc::unbounded_channel();
        (Self { sender }, receiver)
    }
}

impl ConstraintSink for ConstraintSender2ConstraintSink {
    fn insert_constraint(&mut self, constraint: ConstraintsMessage) -> bool {
        self.sender
            .send(ConstraintPoolCommand::Insert(constraint))
            .is_ok()
    }

    fn remove_constraints_for_slot(&mut self, slot: u64) -> bool {
        self.sender
            .send(ConstraintPoolCommand::RemoveSlot(slot))
            .is_ok()
    }

    fn is_alive(&self) -> bool {
        !self.sender.is_closed()
    }
}
