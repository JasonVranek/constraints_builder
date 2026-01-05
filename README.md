# rbuilder (experimental-constraints-merging)

A fork of [flashbots/rbuilder](https://github.com/flashbots/rbuilder) adding constraint support and block merging for preconfirmation workflows.

## Overview

This branch extends rbuilder with:

- **Constraints** — Ingest `SignedConstraints` by polling the relay's `GET /constraints` endpoint. During block finalization, transactions are inserted based on the constraints, then Merkle inclusion proofs are generated for each. A modified `POST /blocks_with_proofs` is called to submit the block and merkle proofs to the relay. 
- **Block Merging** — Submit blocks with merging metadata to compatible relays

## Constraints

This implementation assumes a `Constraint` wraps an `InclusionPayload` that requires a specific transaction to be included in a specific slot:

1. Received via `constraints_poller`
2. Stored in a constraint pool indexed by target slot number
3. Appended to blocks during building
4. Tracked in `BuiltBlockTrace.appended_constraint_txs`

## Merkle Inclusion Proofs

After block submission, merkle proofs are generated for each constraint transaction.

| Component | Description |
|-----------|-------------|
| **Trie** | Ethereum MPT with RLP-encoded tx index as key, RLP-encoded signed tx as value |
| **Proof** | Path from transaction leaf to transactions root |

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
| `constraint_server_url` | `http://127.0.0.1:8547` | URL of the constraints server to poll |

## TODO

- [ ] **SignedConstraint verification** — Receive `SignedConstraint` objects from the relay and verify signatures before adding constraints to the pool

## License

Licensed under Apache 2.0 or MIT, consistent with upstream rbuilder.

## Acknowledgements

- [flashbots/rbuilder](https://github.com/flashbots/rbuilder) — upstream block builder
- [merklefruit/trie-proofs](https://github.com/merklefruit/trie-proofs) — merkle proof implementation reference
- [fabric](https://github.com/eth-fabric/fabric) - types and helpers to work with constraints