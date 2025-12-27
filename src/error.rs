use alloy::transports::{RpcError, TransportErrorKind};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("RPC error")]
    RpcError(#[from] RpcError<TransportErrorKind>),
    #[error("Missing block {0}")]
    MissingBlock(u64),
    #[error("Block fetch error #{block_number}")]
    BlockFetchError {
        block_number: u64,
        #[source]
        source: RpcError<TransportErrorKind>,
    },
    #[error("Data store error")]
    DataStoreError(#[from] duckdb::Error),
}
