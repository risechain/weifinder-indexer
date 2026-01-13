use std::{num::NonZeroUsize, sync::Arc, time::Instant};

use alloy::{consensus::Transaction, hex::ToHexExt};
use chrono::DateTime;
use duckdb::{OptionalExt, params};
use flume::Sender;
use metrics::{counter, histogram};
use op_alloy_rpc_types::OpTransactionReceipt;
use tokio::{sync::RwLock, task::JoinHandle};

use crate::indexer::{Checkpoint, IndexerProvider, types::OpBlock};

pub enum BlockSavePayload {
    NewBlock {
        block: OpBlock,
        receipts: Vec<OpTransactionReceipt>,
    },
    Reorg {
        new_block_number: u64,
        prev_checkpoint_tx: tokio::sync::oneshot::Sender<Checkpoint>,
    },
}

pub struct BlockSaver {
    pub tx: Sender<BlockSavePayload>,
    last_checkpoint: Arc<RwLock<Option<Checkpoint>>>,
    pub task_handle: JoinHandle<Result<(), crate::Error>>,
}

pub struct BlockSaverParams<'a> {
    pub batch_save_size: NonZeroUsize,
    pub catalog_db_url: &'a str,
    pub s3_endpoint: &'a str,
    pub s3_access_key_id: &'a str,
    pub s3_secret_access_key: &'a str,
    pub s3_bucket: &'a str,
    pub provider: IndexerProvider,
}

impl BlockSaver {
    pub fn run(
        BlockSaverParams {
            batch_save_size,
            catalog_db_url,
            s3_endpoint,
            s3_access_key_id,
            s3_secret_access_key,
            s3_bucket,
            provider,
        }: BlockSaverParams,
    ) -> Result<Self, crate::Error> {
        let data_conn = duckdb::Connection::open_in_memory()?;
        let is_remote = !s3_endpoint.starts_with("localhost");

        data_conn.execute_batch(&format!(
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

                    ATTACH 'ducklake:postgres:{catalog_db_url}' AS weifinder_data (
                        DATA_PATH 's3://{s3_bucket}/data/',
                        DATA_INLINING_ROW_LIMIT {}
                    );
                    USE weifinder_data;

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
                        block_number UINT64 NOT NULL,
                        index UINT64 NOT NULL,
                        hash BLOB NOT NULL,
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

                    CREATE TABLE IF NOT EXISTS logs (
                        index UINT64 NOT NULL,
                        transaction_hash BLOB NOT NULL,
                        address BLOB NOT NULL,
                        topic0 BLOB,
                        topic1 BLOB,
                        topic2 BLOB,
                        topic3 BLOB,
                        data BLOB NOT NULL
                    );
                "#,
            batch_save_size.get() - 1
        ))?;

        let last_checkpoint = data_conn
            .query_row(
                "SELECT number, hash FROM blocks ORDER BY number DESC LIMIT 1",
                [],
                |row| {
                    row.get("number").and_then(|number: u64| {
                        row.get("hash").map(|hash: Vec<u8>| Checkpoint {
                            block_number: number,
                            block_hash: hash.encode_hex_with_prefix(),
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

        let (tx, rx) = flume::bounded::<BlockSavePayload>(batch_save_size.get());

        let handle = tokio::task::spawn_blocking(move || {
            let mut blocks_in_appender = 0;
            let mut block_appender = data_conn.appender("blocks")?;
            let mut transaction_appender = data_conn.appender("transactions")?;
            let mut log_appender = data_conn.appender("logs")?;
            let batch_size = batch_save_size.get();

            let append_duration_histogram = histogram!("indexer_duckdb_append_duration_seconds");
            let flush_duration_histogram = histogram!("indexer_duckdb_flush_duration_seconds");

            while let Ok(payload) = rx.recv() {
                match payload {
                    BlockSavePayload::NewBlock { block, receipts } => {
                        let append_start = Instant::now();

                        let block_number = block.number();
                        let block_hash = block.hash();

                        block_appender.append_row(params![
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
                        ])?;

                        let mut block_transactions = block.transactions.into_transactions();

                        for receipt in receipts {
                            let tx = block_transactions
                                .find(|tx| {
                                    tx.info().hash.unwrap() == receipt.inner.transaction_hash
                                })
                                .ok_or(crate::Error::MissingTransaction {
                                    transaction_hash: receipt
                                        .inner
                                        .transaction_hash
                                        .encode_hex_with_prefix(),
                                    block_hash: block_hash.encode_hex_with_prefix(),
                                })?;

                            transaction_appender.append_row(params![
                                receipt.inner.block_number.unwrap(),
                                receipt.inner.transaction_index.unwrap(),
                                receipt.inner.transaction_hash.as_slice(),
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
                            ])?;

                            for log in receipt.inner.logs() {
                                log_appender.append_row(params![
                                    log.log_index.expect("block to exist"),
                                    log.transaction_hash.expect("block to exist").as_slice(),
                                    log.address().as_slice(),
                                    log.topic0().map(|topic| topic.as_slice()),
                                    log.topics().get(1).map(|topic| topic.as_slice()),
                                    log.topics().get(2).map(|topic| topic.as_slice()),
                                    log.topics().get(3).map(|topic| topic.as_slice()),
                                    log.data().data.iter().as_slice()
                                ])?;
                            }
                        }

                        append_duration_histogram.record(append_start.elapsed().as_secs_f64());

                        blocks_in_appender += 1;

                        let at_tip = {
                            let head = provider.current_head().borrow();
                            head.hash == block_hash || head.parent_hash == block_hash
                        };

                        if blocks_in_appender >= batch_size || at_tip {
                            let flush_start = Instant::now();

                            block_appender.flush()?;
                            transaction_appender.flush()?;
                            log_appender.flush()?;

                            flush_duration_histogram.record(flush_start.elapsed().as_secs_f64());
                            last_saved_block_counter.absolute(block_number);

                            if blocks_in_appender >= batch_size {
                                let txn = data_conn.unchecked_transaction()?;
                                txn.execute(
                                    "CALL ducklake_flush_inlined_data('weifinder_data', table_name => 'blocks')",
                                    [],
                                )?;
                                txn.execute(
                                    "CALL ducklake_flush_inlined_data('weifinder_data', table_name => 'transactions')",
                                    []
                                )?;
                                txn.execute(
                                    "CALL ducklake_flush_inlined_data('weifinder_data', table_name => 'logs')",
                                    [],
                                )?;
                                txn.commit()?;
                                blocks_in_appender = 0;
                            }
                        }
                    }
                    BlockSavePayload::Reorg {
                        new_block_number,
                        prev_checkpoint_tx,
                    } => {
                        let last_checkpoint = data_conn.query_row(
                            "SELECT number, hash FROM blocks WHERE number = $1",
                            [new_block_number - 1],
                            |row| {
                                row.get("number").and_then(|number: u64| {
                                    row.get("hash").map(|hash: Vec<u8>| Checkpoint {
                                        block_number: number,
                                        block_hash: hash.encode_hex_with_prefix(),
                                    })
                                })
                            },
                        )?;
                        prev_checkpoint_tx.send(last_checkpoint).ok();

                        block_appender.flush()?;
                        transaction_appender.flush()?;
                        log_appender.flush()?;
                        data_conn.execute(
                            r#"
                                DELETE FROM logs
                                WHERE transaction_hash IN (
                                    SELECT hash FROM transactions WHERE block_number >= $1
                                )
                            "#,
                            [new_block_number],
                        )?;
                        data_conn.execute(
                            r#"
                                DELETE FROM transactions
                                WHERE block_number >= $1
                            "#,
                            [new_block_number],
                        )?;
                        data_conn.execute(
                            r#"
                                DELETE FROM blocks
                                WHERE number >= $1
                            "#,
                            [new_block_number],
                        )?;
                    }
                }
            }

            Result::<(), crate::Error>::Ok(())
        });

        Ok(Self {
            tx,
            last_checkpoint: Arc::new(RwLock::new(last_checkpoint)),
            task_handle: handle,
        })
    }

    pub async fn last_saved_checkpoint(&self) -> Option<Checkpoint> {
        self.last_checkpoint.read().await.clone()
    }
}
