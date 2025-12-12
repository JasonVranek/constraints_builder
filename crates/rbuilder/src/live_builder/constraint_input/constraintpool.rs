use ahash::HashMap;
use lru::LruCache;
use std::num::NonZeroUsize;
use tokio::sync::mpsc;
use tracing::trace;
use fabric_constraints::types::ConstraintsMessage;

use super::constraint_sink::{
    ConstraintPoolCommand, ConstraintSender2ConstraintSink, ConstraintSink,
};

#[derive(Debug)]
pub struct ConstraintsForSlot {
    pub new_constraint_sub: mpsc::UnboundedReceiver<ConstraintPoolCommand>,
}

impl ConstraintsForSlot {
    /// Helper to create a [`ConstraintsForSlot`] "wrapped" with a [`ConstraintSender2ConstraintSink`].
    /// Give this `ConstraintsForSlot` to a pull stage and push on the returned sink.
    pub fn new_with_sink() -> (Self, ConstraintSender2ConstraintSink) {
        let (sink, sender) = ConstraintSender2ConstraintSink::new();
        (
            ConstraintsForSlot {
                new_constraint_sub: sender,
            },
            sink,
        )
    }
}

#[derive(Debug)]
struct SinkSubscription {
    sink: Box<dyn ConstraintSink>,
    slot: u64,
}

/// Returned by `add_sink` so callers can later remove that subscription.
#[derive(Debug, Eq, Hash, PartialEq, Clone)]
pub struct ConstraintPoolSubscriptionId(u64);

/// Repository of ALL constraints received via `process_commands`.
/// No business logic here: we just store, dedupe, and replay per-block events.
/// Expiration is explicit: either via `RemoveSlot(slot)` commands, or programmatic `slot_updated`.
#[derive(Debug)]
pub struct ConstraintPool {
    /// Constraints by target slot.
    constraints_by_slot: HashMap<u64, ConstraintsMessage>,
    /// Deduplication cache for constraints using (slot) as key.
    known_constraints: LruCache<u64, ()>,
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
            constraints_by_slot: HashMap::default(),
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
        slot: u64,
        mut sink: Box<dyn ConstraintSink>,
    ) -> ConstraintPoolSubscriptionId {
        if let Some(existing) = self.constraints_by_slot.get(&slot) {
            // Replay snapshot for this block
            let _ = sink.insert_constraint(existing.clone());
        }

        let id = ConstraintPoolSubscriptionId(self.next_sink_id);
        self.next_sink_id += 1;
        self.sinks
            .insert(id.clone(), SinkSubscription { sink, slot });
        id
    }

    /// Removes the sink. If present, returns it to the caller.
    pub fn remove_sink(
        &mut self,
        id: &ConstraintPoolSubscriptionId,
    ) -> Option<Box<dyn ConstraintSink>> {
        self.sinks.remove(id).map(|s| s.sink)
    }

    /// Should be called when current head/slot is updated. Expires all slots `< current_slot`.
    /// This *notifies sinks* (no silent pruning).
    pub fn slot_updated(&mut self, current_slot: u64) {
        // Use the same cleanup logic for consistency
        self.cleanup_expired_constraints(current_slot);

        // Optional: also drop subscriptions for past slots entirely.
        self.sinks
            .retain(|_, sub| sub.sink.is_alive() && sub.slot >= current_slot);
    }

    // --- Internals ---------------------------------------------------------

    /// Single-command entry point: mutate internal state, then forward to interested sinks.
    fn process_command(&mut self, command: ConstraintPoolCommand) {
        // 1) Mutate state
        match &command {
            ConstraintPoolCommand::Insert(c) => self.process_constraints(c),
            ConstraintPoolCommand::RemoveSlot(slot) => self.process_remove_slot(*slot),
        }

        // 2) Forward to matching sinks and drop dead ones
        let target_slot = command_target_slot(&command);
        self.sinks.retain(|_, sub| {
            if !sub.sink.is_alive() {
                return false;
            }
            if sub.slot == target_slot {
                let ok = match command.clone() {
                    ConstraintPoolCommand::Insert(c) => sub.sink.insert_constraint(c),
                    ConstraintPoolCommand::RemoveSlot(s) => {
                        sub.sink.remove_constraints_for_slot(s)
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
    fn process_constraints(&mut self, c: &ConstraintsMessage) {
        let slot = c.slot;

        if self.known_constraints.contains(&slot) {
            trace!(slot, "Constraint known, dropping");
            return;
        }

        self.constraints_by_slot.insert(slot, c.clone());
        self.known_constraints.put(slot, ());
        trace!(slot, "Inserted constraint");
    }

    fn process_remove_slot(&mut self, slot: u64) {
        let _ = self.constraints_by_slot.remove(&slot);
        trace!(slot, "Removed constraints for slot");
    }

    pub fn get_constraints_for_slot(&mut self, slot: u64) -> ConstraintsMessage {
        // Clean up expired constraints (blocks < current block) to prevent memory leaks
        self.cleanup_expired_constraints(slot);
        
        self.constraints_by_slot
            .get(&slot)
            .cloned()
            .unwrap_or_default()
    }

    /// Clean up constraints for slots that are older than the current slot
    fn cleanup_expired_constraints(&mut self, current_slot: u64) {
        let expired_slots: Vec<u64> = self
            .constraints_by_slot
            .keys()
            .copied()
            .filter(|&s| s < current_slot)
            .collect();

        for slot in expired_slots {
            self.constraints_by_slot.remove(&slot);
            trace!(slot, current_slot, "Cleaned up expired constraints");
        }
    }
}

#[inline]
fn command_target_slot(cmd: &ConstraintPoolCommand) -> u64 {
    match cmd {
        ConstraintPoolCommand::Insert(c) => c.slot,
        ConstraintPoolCommand::RemoveSlot(s) => *s,
    }
}
