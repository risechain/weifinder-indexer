# Weifinder Indexer

A blockchain indexer written in Rust that fetches and stores block data from EVM-compatible chains.

## Overview

Weifinder Indexer connects to an Ethereum-compatible blockchain via WebSocket RPC, fetches blocks in real-time, and stores them in a DuckLake database. It uses SQLite to track indexing progress through checkpoints, allowing it to resume from where it left off.

## Features

- Real-time block indexing via WebSocket subscriptions
- Concurrent block fetching with configurable rate limiting
- Checkpoint-based resumption for fault tolerance
- Reorg detection
- DuckDB/DuckLake for efficient block data storage
- SQLite for checkpoint persistence

## Architecture

The indexer consists of several components:

- **ChainIndexer**: Main orchestrator that coordinates block fetching, processing, and storage
- **BlockFetcher**: Concurrent block fetcher with rate limiting and backpressure control
- **ChainHeadWatcher**: Subscribes to new block headers to track chain tip
- **IndexerProvider**: Wrapper around the RPC provider with head tracking

## Requirements

- Rust 2024 edition
- SQLite (for checkpoints)
- DuckDB with DuckLake extension (for block data)
- Access to an EVM-compatible WebSocket RPC endpoint

## Configuration

The indexer is configured via the `Settings` struct:

| Setting | Description |
|---------|-------------|
| `db_url` | Path to SQLite database for checkpoints |
| `rpc_url` | WebSocket RPC endpoint URL |
| `fetcher_max_concurrency` | Maximum concurrent block fetch requests |
| `fetcher_max_rps` | Maximum requests per second |

## Usage

```bash
cargo run
```

By default, the indexer connects to `wss://testnet.riselabs.xyz/ws` and stores data in `data/indexer.db`.

## Database Schema

### Checkpoints (SQLite)

Tracks the last indexed block per chain:

```sql
CREATE TABLE checkpoints (
  chain_id INTEGER NOT NULL PRIMARY KEY,
  block_number INTEGER NOT NULL,
  block_hash TEXT NOT NULL
);
```

### Blocks (DuckDB)

Stores block data per chain:

```sql
CREATE TABLE blocks_{chain_id} (
  number UINT64 NOT NULL,
  hash VARCHAR NOT NULL,
  timestamp UINT64 NOT NULL,
  parent_hash VARCHAR NOT NULL,
  gas_used UINT64 NOT NULL,
  gas_limit UINT64 NOT NULL
);
```

## Dependencies

- **alloy**: Ethereum library for RPC and types
- **diesel**: ORM for SQLite checkpoint storage
- **duckdb**: Analytical database for block storage
- **tokio**: Async runtime
- **flume**: MPMC channels for block data flow
- **governor**: Rate limiting
- **tracing**: Logging and diagnostics

## Development Status

This project is in early development. Notable TODOs:

- Implement reorg handling at data store level
- Add configuration file/environment variable support
- Implement release build data connection setup
