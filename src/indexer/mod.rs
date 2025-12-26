mod block_fetcher;
mod head_watcher;
mod provider;

pub use block_fetcher::*;
pub use head_watcher::*;
pub use provider::*;

use std::{collections::VecDeque, num::NonZeroU32};

use alloy::{
    hex::ToHexExt,
    providers::{Provider, ProviderBuilder, WsConnect},
    rpc::{client::ClientBuilder, types::Block},
    transports::layers::RetryBackoffLayer,
};
use diesel::{
    Connection, ExpressionMethods, QueryDsl, RunQueryDsl, SelectableHelper, SqliteConnection,
    connection::SimpleConnection,
};

use crate::{models::Checkpoint, settings::Settings};

pub struct ChainIndexer {
    provider: IndexerProvider,
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
            .first(&mut db_conn)?;

        let data_conn = duckdb::Connection::open_in_memory()?;

        // todo: setup data store

        let block_fetcher = BlockFetcher::fetch(
            provider.clone(),
            BlockFetcherParams {
                max_concurrency: settings.fetcher_max_concurrency,
                max_rps: settings.fetcher_max_rps,
                start_block: last_checkpoint.block_number.unwrap_or(0) as u64,
            },
        )
        .await?;

        let mut res = Self {
            provider,
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
        mut last_checkpoint: Checkpoint,
    ) -> Result<(), crate::Error> {
        let rx = self.block_fetcher.receiver();

        let mut block_queue: VecDeque<Block> = VecDeque::new();

        while let Ok((block_number, block_res)) = rx.recv_async().await {
            let incoming_block = block_res
                .map_err(|err| crate::Error::BlockFetchError {
                    block_number,
                    source: err,
                })?
                .ok_or(crate::Error::MissingBlock(block_number))?;

            let is_data_store_reorged =
                match (last_checkpoint.block_number, &last_checkpoint.block_hash) {
                    (Some(last_block_number), Some(last_block_hash)) => {
                        let last_block_number = last_block_number as u64;
                        let incoming_block_hash = incoming_block.hash().encode_hex_with_prefix();

                        if incoming_block.number() == last_block_number
                            && incoming_block_hash.ne(last_block_hash)
                        {
                            true
                        } else if incoming_block.number() < last_block_number {
                            true
                        } else {
                            false
                        }
                    }
                    _ => false,
                };

            if is_data_store_reorged {
                todo!("Handle reorg in data store level")
            } else {
                let idx = block_queue.partition_point(|b| b.number() < incoming_block.number());
                block_queue.insert(idx, incoming_block);

                while let Some(next_block) = block_queue.front() {
                    let is_next_block = match last_checkpoint.block_number {
                        Some(last_block_number) => {
                            next_block.number() == last_block_number as u64 + 1
                        }
                        None => true,
                    };

                    if !is_next_block {
                        break;
                    }

                    let is_reorged_block = match &last_checkpoint.block_hash {
                        // todo: should properly check at common ancestor
                        Some(last_block_hash) => next_block
                            .header
                            .parent_hash
                            .encode_hex_with_prefix()
                            .ne(last_block_hash),
                        None => true,
                    };

                    let block = block_queue.pop_front().unwrap();

                    if is_reorged_block {
                        continue;
                    }

                    last_checkpoint.block_number = Some(block.number() as i32);
                    last_checkpoint.block_hash = Some(block.hash().encode_hex_with_prefix());

                    todo!("Save block to data store and update last_checkpoint")
                }
            }
        }

        Ok(())
    }
}
