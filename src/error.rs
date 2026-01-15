use alloy::transports::{RpcError, TransportErrorKind};
use tokio::sync::watch;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("RPC error: {0}")]
    RpcError(#[from] RpcError<TransportErrorKind>),
    #[error("Missing block {0}")]
    MissingBlock(u64),
    #[error("Block fetch error #{block_number}: {source}")]
    BlockFetchError {
        block_number: u64,
        #[source]
        source: RpcError<TransportErrorKind>,
    },
    #[error("Data store error: {0}")]
    DataStoreError(#[from] duckdb::Error),
    #[error("Missing transaction {transaction_hash} in block {block_hash}")]
    MissingTransaction {
        transaction_hash: String,
        block_hash: String,
    },
    #[error("Head watcher closed")]
    HeadWatcherClosed(#[source] watch::error::RecvError),
    #[error("Join error")]
    JoinError(#[from] tokio::task::JoinError),
    #[error("oneshot Recv error")]
    OneshotRecvError(#[from] tokio::sync::oneshot::error::RecvError),
    #[error("Checkpoint database error: {0}")]
    CheckpointDatabaseError(#[from] diesel::result::Error),
    #[error("Checkpoint database connection error: {0}")]
    CheckpointDatabaseConnectionError(#[from] diesel::result::ConnectionError),
    #[error("Checkpoint database migration error: {source}")]
    CheckpointDatabaseMigrationError {
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}
