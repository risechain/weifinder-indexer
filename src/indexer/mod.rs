mod block_fetcher;
mod head_watcher;
mod provider;

pub use block_fetcher::*;
use duckdb::params;
pub use head_watcher::*;
pub use provider::*;
use tracing::debug;

use std::{collections::VecDeque, num::NonZeroU32};

use alloy::{
    hex::ToHexExt,
    providers::{Provider, ProviderBuilder, WsConnect},
    rpc::{client::ClientBuilder, types::Block},
    transports::layers::RetryBackoffLayer,
};
use diesel::{
    Connection, ExpressionMethods, OptionalExtension, QueryDsl, RunQueryDsl, SelectableHelper,
    SqliteConnection, connection::SimpleConnection, upsert::excluded,
};

use crate::{
    models::{Checkpoint, NewCheckpoint},
    settings::Settings,
};

pub struct ChainIndexer {
    chain_id: u64,
    db_conn: SqliteConnection,
    block_fetcher: BlockFetcher,
    data_conn: duckdb::Connection,
}

impl ChainIndexer {
    pub async fn run(settings: &Settings) -> Result<Self, crate::Error> {
        use crate::schema::checkpoints::dsl::*;

        let client = ClientBuilder::default()
            .layer(RetryBackoffLayer::new(
                10,
                1000,
                settings
                    .fetcher_max_rps
                    .checked_mul(NonZeroU32::new(20).unwrap())
                    .unwrap()
                    .get() as u64,
            ))
            .ws(WsConnect::new(&settings.rpc_url))
            .await?;
        let provider = IndexerProvider::new(
            ProviderBuilder::new()
                .disable_recommended_fillers()
                .connect_client(client),
        )
        .await?;

        let chain_id_ = provider.get_chain_id().await?;

        let mut db_conn = SqliteConnection::establish(&settings.db_url)?;
        db_conn.batch_execute("PRAGMA journal_mode = WAL;")?;

        let last_checkpoint = checkpoints
            .select(Checkpoint::as_select())
            .filter(chain_id.eq(chain_id_ as i32))
            .first(&mut db_conn)
            .optional()?;

        let data_conn = duckdb::Connection::open_in_memory()?;

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
                CREATE TABLE IF NOT EXISTS {} (
                number UINT64 NOT NULL,
                hash VARCHAR NOT NULL,
                timestamp UINT64 NOT NULL,
                parent_hash VARCHAR NOT NULL,
                gas_used UINT64 NOT NULL,
                gas_limit UINT64 NOT NULL,
                );
                "#,
                    format!("blocks_{}", chain_id_)
                ),
                [],
            )
            .unwrap();

        #[cfg(not(debug_assertions))]
        todo!("Implement data connection setup for release builds");

        let block_fetcher = BlockFetcher::fetch(
            provider.clone(),
            BlockFetcherParams {
                max_concurrency: settings.fetcher_max_concurrency,
                max_rps: settings.fetcher_max_rps,
                start_block: last_checkpoint
                    .as_ref()
                    .map(|c| c.block_number)
                    .unwrap_or(0) as u64,
            },
        )
        .await?;

        let mut res = Self {
            chain_id: chain_id_,
            db_conn,
            block_fetcher,
            data_conn,
        };

        res.process_blocks(last_checkpoint).await?;

        Ok(res)
    }

    async fn process_blocks(
        &mut self,
        mut last_checkpoint: Option<Checkpoint>,
    ) -> Result<(), crate::Error> {
        let rx = self.block_fetcher.receiver();
        let mut block_queue: VecDeque<Block> = VecDeque::new();
        let mut block_appender = self
            .data_conn
            .appender(&format!("blocks_{}", self.chain_id))?;
        let mut blocks_in_appender = 0;

        while let Ok((block_number, block_res)) = rx.recv_async().await {
            debug!("processing block #{block_number}: {:?}", block_res);

            let incoming_block = block_res
                .map_err(|err| crate::Error::BlockFetchError {
                    block_number,
                    source: err,
                })?
                .ok_or(crate::Error::MissingBlock(block_number))?;

            let is_data_store_reorged = last_checkpoint.as_ref().is_some_and(|last_checkpoint| {
                let last_block_number = last_checkpoint.block_number as u64;
                let incoming_block_hash = incoming_block.hash().encode_hex_with_prefix();

                if incoming_block.number() == last_block_number
                    && incoming_block_hash.ne(&last_checkpoint.block_hash)
                {
                    true
                } else if incoming_block.number() < last_block_number {
                    true
                } else {
                    false
                }
            });

            if is_data_store_reorged {
                todo!("Handle reorg in data store level")
            } else {
                let idx = block_queue.partition_point(|b| b.number() < incoming_block.number());
                block_queue.insert(idx, incoming_block);

                while let Some(next_block) = block_queue.front() {
                    let is_next_block = match &last_checkpoint {
                        Some(last_checkpoint) => {
                            next_block.number() == last_checkpoint.block_number as u64 + 1
                        }
                        None => next_block.number() == 0,
                    };

                    if !is_next_block {
                        break;
                    }

                    let is_reorged_block =
                        last_checkpoint.as_ref().is_some_and(|last_checkpoint| {
                            last_checkpoint.block_number as u64 == next_block.number()
                                || next_block
                                    .header
                                    .parent_hash
                                    .encode_hex_with_prefix()
                                    .ne(&last_checkpoint.block_hash)
                        });

                    let block = block_queue.pop_front().unwrap();

                    if is_reorged_block {
                        continue;
                    }

                    last_checkpoint = Some(Checkpoint {
                        block_number: block.number() as i32,
                        block_hash: block.hash().encode_hex_with_prefix(),
                    });

                    // todo: use insert with data inlining instead when at the tip of the chain
                    block_appender.append_row(params![
                        block.number(),
                        block.hash().encode_hex_with_prefix(),
                        block.header.timestamp,
                        block.header.parent_hash.encode_hex_with_prefix(),
                        block.header.gas_used,
                        block.header.gas_limit,
                    ])?;
                    blocks_in_appender += 1;
                    if blocks_in_appender >= 1000 {
                        block_appender.flush()?;
                        blocks_in_appender = 0;
                    }

                    use crate::schema::checkpoints::dsl::*;
                    let new_checkpoint = NewCheckpoint {
                        chain_id: self.chain_id as i32,
                        block_hash: block.hash().encode_hex_with_prefix(),
                        block_number: block.number() as i32,
                    };
                    diesel::insert_into(checkpoints)
                        .values(&new_checkpoint)
                        .on_conflict(chain_id)
                        .do_update()
                        .set((
                            block_hash.eq(excluded(block_hash)),
                            block_number.eq(excluded(block_number)),
                        ))
                        .execute(&mut self.db_conn)?;

                    debug!("appended block #{}: {:?}", block.number(), block);
                }
                // todo: flush the block appender based on number of rows
            }
        }

        Ok(())
    }
}
