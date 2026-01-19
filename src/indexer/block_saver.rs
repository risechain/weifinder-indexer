use std::{num::NonZeroUsize, sync::Arc, time::Instant};

use alloy::{
    consensus::Transaction,
    hex::ToHexExt,
    rpc::types::trace::parity::{Action, CallType, TraceResultsWithTransactionHash},
};
use chrono::DateTime;
use diesel::{
    Connection, ExpressionMethods, OptionalExtension, QueryDsl, RunQueryDsl, SqliteConnection,
};
use diesel_migrations::{EmbeddedMigrations, MigrationHarness, embed_migrations};
use duckdb::params;
use flume::Sender;
use metrics::{counter, histogram};
use op_alloy_rpc_types::OpTransactionReceipt;
use tokio::{sync::RwLock, task::JoinHandle};

use crate::indexer::{Checkpoint, IndexerProvider, types::OpBlock};

#[allow(clippy::large_enum_variant)]
pub enum BlockSavePayload {
    NewBlock {
        block: OpBlock,
        receipts: Vec<OpTransactionReceipt>,
        traces: Vec<TraceResultsWithTransactionHash>,
    },
    Reorg {
        new_block_number: u64,
        prev_block_hash: String,
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
    pub checkpoint_db_path: &'a str,
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
            checkpoint_db_path,
        }: BlockSaverParams<'_>,
    ) -> Result<Self, crate::Error> {
        let mut data_conn = duckdb::Connection::open_in_memory()?;
        let is_remote = !s3_endpoint.starts_with("localhost");
        let batch_size = batch_save_size.get();

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
                        block_number UINT64 NOT NULL,
                        index UINT64 NOT NULL,
                        transaction_hash BLOB NOT NULL,
                        address BLOB NOT NULL,
                        topic0 BLOB,
                        topic1 BLOB,
                        topic2 BLOB,
                        topic3 BLOB,
                        data BLOB NOT NULL
                    );

                    CREATE TABLE IF NOT EXISTS inner_transactions (
                        block_number UINT64 NOT NULL,
                        parent_transaction_hash BLOB NOT NULL,
                        from_address BLOB NOT NULL,
                        to_address BLOB NOT NULL,
                        input BLOB NOT NULL,
                        value VARCHAR NOT NULL,
                        error BLOB,
                        trace_index VARCHAR NOT NULL
                    );
                "#,
            batch_size - 1
        ))?;

        let txn = data_conn.transaction()?;
        txn.execute("CALL ducklake_flush_inlined_data('weifinder_data')", [])?;
        txn.commit()?;

        let mut checkpoint_db = SqliteConnection::establish(checkpoint_db_path)?;
        checkpoint_db
            .run_pending_migrations(CHECKPOINT_MIGRATIONS)
            .map_err(|err| crate::Error::CheckpointDatabaseMigrationError { source: err })?;

        let last_checkpoint = {
            use crate::schema::checkpoints::dsl::*;

            checkpoints
                .select((block_number, block_hash))
                .first::<(i32, String)>(&mut checkpoint_db)
                .optional()?
                .map(|(number, hash)| Checkpoint {
                    block_hash: hash,
                    block_number: number as u64,
                })
        };

        let last_saved_block_counter = counter!("indexer_last_saved_block");

        last_saved_block_counter.absolute(
            last_checkpoint
                .as_ref()
                .map(|c| c.block_number)
                .unwrap_or(0),
        );

        let last_checkpoint = Arc::new(RwLock::new(last_checkpoint));
        let last_checkpoint_clone = last_checkpoint.clone();

        let chain_id = provider.chain_id();

        let (tx, rx) = flume::bounded::<BlockSavePayload>(batch_size);

        let handle = tokio::task::spawn_blocking(move || {
            let mut blocks_in_appender = 0;
            let mut block_appender = data_conn.appender("blocks")?;
            let mut transaction_appender = data_conn.appender("transactions")?;
            let mut log_appender = data_conn.appender("logs")?;
            let mut inner_transaction_appender = data_conn.appender("inner_transactions")?;

            let append_duration_histogram = histogram!("indexer_duckdb_append_duration_seconds");
            let flush_duration_histogram = histogram!("indexer_duckdb_flush_duration_seconds");

            while let Ok(payload) = rx.recv() {
                match payload {
                    BlockSavePayload::NewBlock {
                        block,
                        receipts,
                        traces,
                    } => {
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
                                block_number,
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
                                    block_number,
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

                        for TraceResultsWithTransactionHash {
                            full_trace,
                            transaction_hash,
                        } in traces
                        {
                            for trace in full_trace.trace {
                                if trace.trace_address.is_empty() {
                                    continue;
                                }
                                if let Action::Call(call) = trace.action
                                    && call.call_type == CallType::Call
                                {
                                    let trace_index = trace
                                        .trace_address
                                        .iter()
                                        .map(|i| i.to_string())
                                        .collect::<Vec<String>>()
                                        .join(",");
                                    inner_transaction_appender.append_row(params![
                                        block_number,
                                        transaction_hash.as_slice(),
                                        call.from.as_slice(),
                                        call.to.as_slice(),
                                        call.input.iter().as_slice(),
                                        call.value.to_string(),
                                        trace.error,
                                        trace_index
                                    ])?;
                                }
                            }
                        }

                        append_duration_histogram.record(append_start.elapsed().as_secs_f64());

                        blocks_in_appender += 1;

                        let streaming = {
                            let last_checkpoint_number = last_checkpoint_clone
                                .blocking_read()
                                .as_ref()
                                .map(|c| c.block_number);
                            let head = provider.current_head().borrow();

                            last_checkpoint_number.is_some_and(|checkpoint_number| {
                                checkpoint_number + batch_size as u64 > head.number
                            })
                        };

                        if blocks_in_appender >= batch_size || streaming {
                            let flush_start = Instant::now();

                            block_appender.flush()?;
                            transaction_appender.flush()?;
                            log_appender.flush()?;
                            inner_transaction_appender.flush()?;

                            {
                                use crate::schema::checkpoints::dsl;

                                let block_number_set = dsl::block_number.eq(block_number as i32);
                                let block_hash_set =
                                    dsl::block_hash.eq(block_hash.encode_hex_with_prefix());

                                diesel::insert_into(dsl::checkpoints)
                                    .values((
                                        dsl::chain_id.eq(chain_id as i32),
                                        block_number_set,
                                        block_hash_set.clone(),
                                    ))
                                    .on_conflict(dsl::chain_id)
                                    .do_update()
                                    .set((block_number_set, block_hash_set))
                                    .execute(&mut checkpoint_db)?;
                            }

                            flush_duration_histogram.record(flush_start.elapsed().as_secs_f64());
                            last_saved_block_counter.absolute(block_number);

                            *last_checkpoint_clone.blocking_write() = Some(Checkpoint {
                                block_number,
                                block_hash: block_hash.encode_hex_with_prefix(),
                            });

                            if blocks_in_appender >= batch_size {
                                let txn = data_conn.unchecked_transaction()?;
                                txn.execute(
                                    "CALL ducklake_flush_inlined_data('weifinder_data')",
                                    [],
                                )?;
                                txn.commit()?;
                                blocks_in_appender = 0;
                            }
                        }
                    }
                    BlockSavePayload::Reorg {
                        new_block_number,
                        prev_block_hash,
                    } => {
                        let prev_block_number = new_block_number - 1;

                        block_appender.flush()?;
                        transaction_appender.flush()?;
                        log_appender.flush()?;
                        inner_transaction_appender.flush()?;

                        data_conn.execute(
                            r#"
                                DELETE FROM logs
                                WHERE block_number >= $1
                            "#,
                            [new_block_number],
                        )?;
                        data_conn.execute(
                            r#"
                                DELETE FROM inner_transactions
                                WHERE block_number >= $1
                            "#,
                            [],
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

                        {
                            use crate::schema::checkpoints::dsl;

                            let block_number_set = dsl::block_number.eq(prev_block_number as i32);
                            let block_hash_set = dsl::block_hash.eq(&prev_block_hash);

                            diesel::insert_into(dsl::checkpoints)
                                .values((
                                    dsl::chain_id.eq(chain_id as i32),
                                    block_number_set,
                                    block_hash_set.clone(),
                                ))
                                .on_conflict(dsl::chain_id)
                                .do_update()
                                .set((block_number_set, block_hash_set))
                                .execute(&mut checkpoint_db)?;
                        }

                        *last_checkpoint_clone.blocking_write() = Some(Checkpoint {
                            block_number: prev_block_number,
                            block_hash: prev_block_hash,
                        });
                    }
                }
            }

            Result::<(), crate::Error>::Ok(())
        });

        Ok(Self {
            tx,
            last_checkpoint,
            task_handle: handle,
        })
    }

    pub async fn last_saved_checkpoint(&self) -> Option<Checkpoint> {
        self.last_checkpoint.read().await.clone()
    }
}

const CHECKPOINT_MIGRATIONS: EmbeddedMigrations = embed_migrations!();
