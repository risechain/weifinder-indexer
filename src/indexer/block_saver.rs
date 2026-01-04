use std::{num::NonZeroUsize, sync::Arc, time::Instant};

use alloy::consensus::Transaction;
use chrono::DateTime;
use duckdb::{OptionalExt, params};
use flume::{SendError, Sender};
use metrics::{counter, histogram};
use op_alloy_rpc_types::OpTransactionReceipt;
use tokio::sync::RwLock;
use tracing::error;

use crate::indexer::{Checkpoint, types::OpBlock};

pub struct BlockSaver {
    tx: Sender<(OpBlock, Vec<OpTransactionReceipt>)>,
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
                        hash BLOB NOT NULL,
                        timestamp TIMESTAMP_S NOT NULL,
                        parent_hash BLOB NOT NULL,
                        gas_used UINT64 NOT NULL,
                        gas_limit UINT64 NOT NULL,
                        transactions_root BLOB NOT NULL,
                        state_root BLOB NOT NULL,
                        receipts_root BLOB NOT NULL,
                        size UINT64,
                    );

                    CREATE TABLE IF NOT EXISTS transactions (
                        hash BLOB NOT NULL,
                        index UINT64 NOT NULL,
                        block_number UINT64 NOT NULL,
                        from_address BLOB NOT NULL,
                        to_address BLOB,
                        effective_gas_price UINT64 NOT NULL,
                        gas_used UINT64 NOT NULL,
                        contract_address BLOB,
                        type UINT8 NOT NULL,
                        status BOOLEAN NOT NULL,
                        input BLOB NOT NULL,
                        nonce UINT64 NOT NULL,
                        value VARCHAR NOT NULL
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

        let (tx, rx) =
            flume::bounded::<(OpBlock, Vec<OpTransactionReceipt>)>(batch_save_size.get() * 10);

        tokio::task::spawn_blocking(move || {
            let mut block_appender = match data_conn.appender("blocks") {
                Ok(appender) => appender,
                Err(err) => {
                    error!("Failed to create block appender: {}", err);
                    return;
                }
            };
            let mut transaction_appender = match data_conn.appender("transactions") {
                Ok(appender) => appender,
                Err(err) => {
                    error!("Failed to create transaction appender: {}", err);
                    return;
                }
            };

            let batch_size = batch_save_size.get();
            let mut blocks_in_appender = 0;
            let mut transactions_in_appender = 0;
            let append_duration_histogram = histogram!("indexer_duckdb_append_duration_seconds");
            let flush_duration_histogram = histogram!("indexer_duckdb_flush_duration_seconds");

            'main: while let Ok((block, receipts)) = rx.recv() {
                let append_start = Instant::now();

                let block_number = block.number();

                if let Err(err) = block_appender.append_row(params![
                    block_number,
                    block.hash().as_slice(),
                    DateTime::from_timestamp_secs(block.header.timestamp as i64)
                        .unwrap()
                        .naive_utc(),
                    block.header.parent_hash.as_slice(),
                    block.header.gas_used,
                    block.header.gas_limit,
                    block.header.transactions_root.as_slice(),
                    block.header.receipts_root.as_slice(),
                    block.header.state_root.as_slice(),
                    block.header.size.as_ref().map(|size| size.to_string())
                ]) {
                    error!("Failed to append block to appender: {}", err);
                    break;
                }

                let receipts_len = receipts.len();
                let mut block_transactions = block.transactions.into_transactions();

                for receipt in receipts {
                    let tx = block_transactions
                        .find(|tx| tx.info().hash.unwrap() == receipt.inner.transaction_hash);

                    let tx = if let Some(tx) = tx {
                        tx
                    } else {
                        error!(
                            "Transaction {:?} not found in block {}",
                            receipt.inner.transaction_hash, block_number
                        );
                        break 'main;
                    };

                    if let Err(err) = transaction_appender.append_row(params![
                        receipt.inner.transaction_hash.as_slice(),
                        receipt.inner.transaction_index.unwrap(),
                        receipt.inner.block_number.unwrap(),
                        receipt.inner.from.as_slice(),
                        receipt.inner.to.as_ref().map(|to| to.as_slice()),
                        receipt.inner.effective_gas_price as u64,
                        receipt.inner.gas_used,
                        receipt
                            .inner
                            .contract_address
                            .as_ref()
                            .map(|contract| contract.as_slice()),
                        receipt.inner.inner.receipt.tx_type() as u8,
                        receipt
                            .inner
                            .inner
                            .receipt
                            .as_receipt()
                            .status
                            .coerce_status(),
                        tx.inner.as_recovered().input().iter().as_slice(),
                        tx.nonce(),
                        tx.value().to_string()
                    ]) {
                        error!("Failed to append receipt to appender: {}", err);
                        break 'main;
                    }
                }

                append_duration_histogram.record(append_start.elapsed().as_secs_f64());

                blocks_in_appender += 1;
                transactions_in_appender += receipts_len;

                if blocks_in_appender >= batch_size {
                    let flush_start = Instant::now();
                    if let Err(err) = block_appender.flush() {
                        error!("Failed to flush block appender: {}", err);
                        break;
                    }
                    flush_duration_histogram.record(flush_start.elapsed().as_secs_f64());
                    last_saved_block_counter.absolute(block_number);
                    blocks_in_appender = 0;
                }

                if transactions_in_appender >= batch_size {
                    let flush_start = Instant::now();
                    if let Err(err) = transaction_appender.flush() {
                        error!("Failed to flush transaction appender: {}", err);
                        break;
                    }
                    flush_duration_histogram.record(flush_start.elapsed().as_secs_f64());
                    transactions_in_appender = 0;
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

    pub async fn save_block(
        &self,
        block: OpBlock,
        receipts: Vec<OpTransactionReceipt>,
    ) -> Result<(), SendError<(OpBlock, Vec<OpTransactionReceipt>)>> {
        self.tx.send_async((block, receipts)).await
    }
}
