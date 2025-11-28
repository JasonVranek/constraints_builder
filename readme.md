# rbuilder (experimental-constraints-merging)

A fork of [flashbots/rbuilder](https://github.com/flashbots/rbuilder) adding constraint support and block merging for preconfirmation workflows.

## Overview

This branch extends rbuilder with:

- **Constraints** — Accept preconfirmation constraints via RPC and include them in blocks
- **Merkle Proofs** — Generate inclusion proofs for constraint transactions
- **Block Merging** — Submit blocks with merging metadata to compatible relays

## Constraints

Constraints are transactions that must be included in a block:

1. Received via JSON-RPC on `constraint_rpc_port` (default: `8547`)
2. Stored in a constraint pool indexed by target block number
3. Appended to blocks during building
4. Tracked in `BuiltBlockTrace.appended_constraint_txs`

## Merkle Inclusion Proofs

After block submission, merkle proofs are generated for each constraint transaction.

| Component | Description |
|-----------|-------------|
| **Trie** | Ethereum MPT with RLP-encoded tx index as key, RLP-encoded signed tx as value |
| **Proof** | Path from transaction leaf to transactions root |
| **Storage** | In-memory, retains last 10 slots |
| **RPC** | `getConstraintProofs` on `constraint_proof_rpc_port` (default: `9548`) |

Based on [merklefruit/trie-proofs](https://github.com/merklefruit/trie-proofs).

## Block Merging

### Merging Data

All transactions are marked as mergeable by default:

- `can_revert: true` — each transaction may revert
- `allow_appending: true` — relay may append additional transactions
- JSON only — SSZ not supported for merging payloads

### Relay Configuration

Enable per-relay via config:

```toml
[[relays]]
name = "helix"
url = "https://..."
supports_block_merging = true
```

When `supports_block_merging = true`:

- Submission wrapped in `SignedBidSubmissionWithMergingData`
- `x-mergeable: true` header added
- JSON encoding forced (SSZ disabled)

## Configuration

| Option | Default | Description |
|--------|---------|-------------|
| `constraint_rpc_port` | `8547` | Port for receiving constraints |
| `constraint_rpc_ip` | `0.0.0.0` | IP for constraint RPC server |
| `constraint_proof_rpc_port` | `9548` | Port for serving proofs |
| `constraint_proof_rpc_ip` | `0.0.0.0` | IP for proof RPC server |

## TODO

- [ ] **SignedCommitment verification** — Receive `SignedCommitment` objects from the relay and verify signatures before adding constraints to the pool

## License

Licensed under Apache 2.0 or MIT, consistent with upstream rbuilder.

## Acknowledgements

- [flashbots/rbuilder](https://github.com/flashbots/rbuilder) — upstream block builder
- [merklefruit/trie-proofs](https://github.com/merklefruit/trie-proofs) — merkle proof implementation reference
