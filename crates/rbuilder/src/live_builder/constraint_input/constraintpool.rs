use rbuilder_primitives::constraints::{ConstraintId, Constraints};
use ahash::HashMap;
use lru::LruCache;
use std::num::NonZeroUsize;
use tokio::sync::mpsc;
use tracing::trace;

use super::constraint_sink::{
    ConstraintPoolCommand, ConstraintSender2ConstraintSink, ConstraintSink,
};

#[derive(Debug)]
pub struct ConstraintsForBlock {
    pub new_constraint_sub: mpsc::UnboundedReceiver<ConstraintPoolCommand>,
}

impl ConstraintsForBlock {
    /// Helper to create a [`ConstraintsForBlock`] "wrapped" with a [`ConstraintSender2ConstraintSink`].
    /// Give this `ConstraintsForBlock` to a pull stage and push on the returned sink.
    pub fn new_with_sink() -> (Self, ConstraintSender2ConstraintSink) {
        let (sink, sender) = ConstraintSender2ConstraintSink::new();
        (
            ConstraintsForBlock {
                new_constraint_sub: sender,
            },
            sink,
        )
    }
}

#[derive(Debug)]
struct SinkSubscription {
    sink: Box<dyn ConstraintSink>,
    block: u64,
}

/// Returned by `add_sink` so callers can later remove that subscription.
#[derive(Debug, Eq, Hash, PartialEq, Clone)]
pub struct ConstraintPoolSubscriptionId(u64);

/// Repository of ALL constraints received via `process_commands`.
/// No business logic here: we just store, dedupe, and replay per-block events.
/// Expiration is explicit: either via `RemoveBlock(block)` commands, or programmatic `block_updated`.
#[derive(Debug)]
pub struct ConstraintPool {
    /// Constraints by target block.
    constraints_by_block: HashMap<u64, Vec<Constraints>>,
    /// Deduplication cache for constraints using (ConstraintId, block) as key.
    known_constraints: LruCache<(ConstraintId, u64), ()>,
    /// Active downstream sinks keyed by subscription id.
    sinks: HashMap<ConstraintPoolSubscriptionId, SinkSubscription>,
    next_sink_id: u64,
}

impl Default for ConstraintPool {
    fn default() -> Self {
        Self::new()
    }
}

impl ConstraintPool {
    pub fn new() -> Self {
        Self {
            constraints_by_block: HashMap::default(),
            known_constraints: LruCache::new(NonZeroUsize::new(1_000).expect("non-zero")),
            sinks: HashMap::default(),
            next_sink_id: 0,
        }
    }

    /// Process a batch of commands (insertions or per-block removals).
    pub fn process_commands(&mut self, commands: Vec<ConstraintPoolCommand>) {
        for cmd in commands {
            self.process_command(cmd);
        }
    }

    /// Adds a sink and pushes the current state for the requested block.
    /// (No pruning here; expiry/cleanup happens via explicit events.)
    pub fn add_sink(
        &mut self,
        block: u64,
        mut sink: Box<dyn ConstraintSink>,
    ) -> ConstraintPoolSubscriptionId {
        if let Some(existing) = self.constraints_by_block.get(&block) {
            // Replay snapshot for this block
            for c in existing.iter().cloned() {
                // If replay fails, we still register; liveness will be checked on next events.
                let _ = sink.insert_constraint(c);
            }
        }

        let id = ConstraintPoolSubscriptionId(self.next_sink_id);
        self.next_sink_id += 1;
        self.sinks
            .insert(id.clone(), SinkSubscription { sink, block });
        id
    }

    /// Removes the sink. If present, returns it to the caller.
    pub fn remove_sink(
        &mut self,
        id: &ConstraintPoolSubscriptionId,
    ) -> Option<Box<dyn ConstraintSink>> {
        self.sinks.remove(id).map(|s| s.sink)
    }

    /// Should be called when current head/block is updated. Expires all blocks `< current_block`.
    /// This *notifies sinks* (no silent pruning).
    pub fn block_updated(&mut self, current_block: u64) {
        // Use the same cleanup logic for consistency
        self.cleanup_expired_constraints(current_block);

        // Optional: also drop subscriptions for past blocks entirely.
        self.sinks
            .retain(|_, sub| sub.sink.is_alive() && sub.block >= current_block);
    }

    // --- Internals ---------------------------------------------------------

    /// Single-command entry point: mutate internal state, then forward to interested sinks.
    fn process_command(&mut self, command: ConstraintPoolCommand) {
        // 1) Mutate state
        match &command {
            ConstraintPoolCommand::Insert(c) => self.process_constraint(c),
            ConstraintPoolCommand::RemoveBlock(block) => self.process_remove_block(*block),
        }

        // 2) Forward to matching sinks and drop dead ones
        let target_block = command_target_block(&command);
        self.sinks.retain(|_, sub| {
            if !sub.sink.is_alive() {
                return false;
            }
            if sub.block == target_block {
                let ok = match command.clone() {
                    ConstraintPoolCommand::Insert(c) => sub.sink.insert_constraint(c),
                    ConstraintPoolCommand::RemoveBlock(b) => {
                        sub.sink.remove_constraints_for_block(b)
                    }
                };
                if !ok {
                    return false;
                }
            }
            true
        });
    }

    /// Insert into in-memory store with dedup.
    fn process_constraint(&mut self, c: &Constraints) {
        let block = c.message.block;
        let constraint_id = c.id();

        if self.known_constraints.contains(&(constraint_id, block)) {
            trace!(block, ?constraint_id, "Constraint known, dropping");
            return;
        }

        self.constraints_by_block
            .entry(block)
            .or_default()
            .push(c.clone());
        self.known_constraints.put((constraint_id, block), ());
        trace!(block, ?constraint_id, "Inserted constraint");
    }

    fn process_remove_block(&mut self, block: u64) {
        let _ = self.constraints_by_block.remove(&block);
        trace!(block, "Removed constraints for block");
    }

    pub fn get_constraints_for_block(&mut self, block: u64) -> Vec<Constraints> {
        // Clean up expired constraints (blocks < current block) to prevent memory leaks
        self.cleanup_expired_constraints(block);
        
        self.constraints_by_block
            .get(&block)
            .cloned()
            .unwrap_or_default()
    }

    /// Clean up constraints for blocks that are older than the current block
    fn cleanup_expired_constraints(&mut self, current_block: u64) {
        let expired_blocks: Vec<u64> = self
            .constraints_by_block
            .keys()
            .copied()
            .filter(|&b| b < current_block)
            .collect();

        for block in expired_blocks {
            self.constraints_by_block.remove(&block);
            trace!(block, current_block, "Cleaned up expired constraints");
        }
    }
}

#[inline]
fn command_target_block(cmd: &ConstraintPoolCommand) -> u64 {
    match cmd {
        ConstraintPoolCommand::Insert(c) => c.message.block,
        ConstraintPoolCommand::RemoveBlock(b) => *b,
    }
}
