use std::{num::NonZeroUsize, sync::Arc, time::Instant};

use alloy::{hex::ToHexExt, rpc::types::Block};
use duckdb::{OptionalExt, params};
use flume::{SendError, Sender};
use metrics::{counter, histogram};
use tokio::sync::RwLock;
use tracing::error;

use crate::indexer::{Checkpoint, IndexerProvider};

pub struct BlockSaver {
    tx: Sender<Block>,
    last_checkpoint: Arc<RwLock<Option<Checkpoint>>>,
}

impl BlockSaver {
    pub fn run(
        provider: IndexerProvider,
        batch_save_size: NonZeroUsize,
    ) -> Result<Self, crate::Error> {
        let data_conn = duckdb::Connection::open_in_memory()?;
        let blocks_table = format!("blocks_{}", provider.chain_id());

        #[cfg(debug_assertions)]
        data_conn
            .execute_batch(
                r#"
                        INSTALL ducklake;
                        ATTACH 'ducklake:data/data.ducklake' AS data;
                        USE data;
                        "#,
            )
            .unwrap();

        data_conn
            .execute(
                &format!(
                    r#"
                        CREATE TABLE IF NOT EXISTS {blocks_table} (
                        number UINT64 NOT NULL,
                        hash VARCHAR NOT NULL,
                        timestamp UINT64 NOT NULL,
                        parent_hash VARCHAR NOT NULL,
                        gas_used UINT64 NOT NULL,
                        gas_limit UINT64 NOT NULL,
                        );
                        "#,
                ),
                [],
            )
            .unwrap();

        let last_checkpoint = data_conn
            .query_row(
                format!("SELECT number, hash FROM {blocks_table} ORDER BY number DESC LIMIT 1")
                    .as_str(),
                [],
                |row| {
                    row.get("number").and_then(|number: u64| {
                        row.get("hash").map(|hash: String| Checkpoint {
                            block_number: number,
                            block_hash: hash,
                        })
                    })
                },
            )
            .optional()?;

        #[cfg(not(debug_assertions))]
        todo!("Implement data connection setup for release builds");

        let last_saved_block_counter = counter!("indexer_last_saved_block");

        last_saved_block_counter.absolute(
            last_checkpoint
                .as_ref()
                .map(|c| c.block_number)
                .unwrap_or(0),
        );

        let (tx, rx) = flume::bounded::<Block>(batch_save_size.get() * 10);

        tokio::task::spawn_blocking(move || {
            let mut block_appender = match data_conn.appender(&blocks_table) {
                Ok(appender) => appender,
                Err(err) => {
                    error!("Failed to create block appender: {}", err);
                    return;
                }
            };

            let batch_size = batch_save_size.get();
            let mut blocks_in_appender = 0;
            let append_duration_histogram = histogram!("indexer_duckdb_append_duration_seconds");
            let flush_duration_histogram = histogram!("indexer_duckdb_flush_duration_seconds");

            while let Ok(block) = rx.recv() {
                // todo: use insert with data inlining instead when at the tip of the chain
                let append_start = Instant::now();
                if let Err(err) = block_appender.append_row(params![
                    block.number(),
                    block.hash().encode_hex_with_prefix(),
                    block.header.timestamp,
                    block.header.parent_hash.encode_hex_with_prefix(),
                    block.header.gas_used,
                    block.header.gas_limit,
                ]) {
                    error!("Failed to append block to appender: {}", err);
                    break;
                }
                append_duration_histogram.record(append_start.elapsed().as_secs_f64());

                blocks_in_appender += 1;

                if blocks_in_appender >= batch_size {
                    let flush_start = Instant::now();
                    if let Err(err) = block_appender.flush() {
                        error!("Failed to flush block appender: {}", err);
                        break;
                    }
                    flush_duration_histogram.record(flush_start.elapsed().as_secs_f64());
                    last_saved_block_counter.absolute(block.number());
                    blocks_in_appender = 0;
                }
            }
        });

        Ok(Self {
            tx,
            last_checkpoint: Arc::new(RwLock::new(last_checkpoint)),
        })
    }

    pub async fn last_saved_checkpoint(&self) -> Option<Checkpoint> {
        self.last_checkpoint.read().await.clone()
    }

    pub async fn save_block(&self, block: Block) -> Result<(), SendError<Block>> {
        self.tx.send_async(block).await
    }
}
