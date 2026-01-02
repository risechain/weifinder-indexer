use std::{num::NonZeroUsize, sync::Arc, time::Instant};

use alloy::{hex::ToHexExt, rpc::types::Block};
use duckdb::{OptionalExt, params};
use flume::{SendError, Sender};
use metrics::{counter, histogram};
use tokio::sync::RwLock;
use tracing::error;

use crate::indexer::Checkpoint;

pub struct BlockSaver {
    tx: Sender<Block>,
    last_checkpoint: Arc<RwLock<Option<Checkpoint>>>,
}

pub struct BlockSaverParams<'a> {
    pub batch_save_size: NonZeroUsize,
    pub catalog_db_url: &'a str,
    pub s3_endpoint: &'a str,
    pub s3_access_key_id: &'a str,
    pub s3_secret_access_key: &'a str,
}

impl BlockSaver {
    pub fn run(
        BlockSaverParams {
            batch_save_size,
            catalog_db_url,
            s3_endpoint,
            s3_access_key_id,
            s3_secret_access_key,
        }: BlockSaverParams,
    ) -> Result<Self, crate::Error> {
        let data_conn = duckdb::Connection::open_in_memory()?;
        let is_remote = !s3_endpoint.starts_with("localhost");

        data_conn
            .execute_batch(&format!(
                r#"
                    CREATE OR REPLACE SECRET secret (
                        TYPE s3,
                        PROVIDER config,
                        URL_STYLE path,
                        ENDPOINT '{s3_endpoint}',
                        USE_SSL {is_remote},
                        KEY_ID '{s3_access_key_id}',
                        SECRET '{s3_secret_access_key}'

                    );

                    ATTACH 'ducklake:postgres:{catalog_db_url}' AS data (
                        DATA_PATH 's3://indexer/data/',
                        DATA_INLINING_ROW_LIMIT 10
                    );
                    USE data;

                    CREATE TABLE IF NOT EXISTS blocks (
                        number UINT64 NOT NULL,
                        hash VARCHAR NOT NULL,
                        timestamp UINT64 NOT NULL,
                        parent_hash VARCHAR NOT NULL,
                        gas_used UINT64 NOT NULL,
                        gas_limit UINT64 NOT NULL,
                    );
                "#,
            ))
            .unwrap();

        let last_checkpoint = data_conn
            .query_row(
                "SELECT number, hash FROM blocks ORDER BY number DESC LIMIT 1",
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

        let last_saved_block_counter = counter!("indexer_last_saved_block");

        last_saved_block_counter.absolute(
            last_checkpoint
                .as_ref()
                .map(|c| c.block_number)
                .unwrap_or(0),
        );

        let (tx, rx) = flume::bounded::<Block>(batch_save_size.get() * 10);

        tokio::task::spawn_blocking(move || {
            let mut block_appender = match data_conn.appender("blocks") {
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
