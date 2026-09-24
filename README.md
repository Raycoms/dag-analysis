# Sui Consensus DB Analysis

A Rust tool that reads a Sui consensus (Mysticeti) DB directly off disk and
computes per-block reference/overhead statistics — block size, reference
count and size, round-to-round reference overlap, transaction payload and
size, and a roaring-bitmap encoding of missing validators — then prints
summary statistics (mean, p1, p10, p50, p90, p99, max) across every block
in the DB.

This is the data source behind the case-study numbers in the paper's
evaluation section (Epoch 648 / 2025 and Epoch 1230 / 2026).

## Requirements

- Rust toolchain (via `cargo`).
- A Sui consensus DB, placed at:
  ```
  $HOME/Downloads/opt/sui/db/consensus_db/<slot>
  ```
  where `<slot>` is the epoch number (e.g. `648`, `1230`). This path is
  currently hardcoded in `main()` — move or symlink your DB there, or edit
  the `db_path` line if you'd rather point elsewhere.
- The DB must contain a `blocks` column family (RocksDB) or `blocks`
  keyspace (tidehunter) that the tool iterates over.

## Build

```bash
cargo build --release
```

## Run

```bash
cargo run --release -- <slot> <validator_set_size>
```

- `<slot>` — the epoch number; also selects which DB subdirectory to read
  (`.../consensus_db/<slot>`).
- `<validator_set_size>` — the number of validators in that epoch's
  committee, used to size the "missing validators" bitmap.

Example (the two epochs used in the paper):

```bash
cargo run --release -- 648 108    # Epoch 648, 2025, 108 validators
cargo run --release -- 1230 128   # Epoch 1230, 2026, 128 validators
```