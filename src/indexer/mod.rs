mod block_fetcher;
mod block_saver;
mod head_watcher;
mod provider;

pub use block_fetcher::*;
pub use block_saver::*;
pub use head_watcher::*;
pub use provider::*;

use std::{collections::VecDeque, num::NonZeroU32};

use alloy::{
    hex::ToHexExt,
    providers::{ProviderBuilder, WsConnect},
    rpc::{client::ClientBuilder, types::Block},
    transports::layers::RetryBackoffLayer,
};
use metrics::counter;

use crate::settings::Settings;

#[derive(Debug, Clone)]
pub struct Checkpoint {
    block_number: u64,
    block_hash: String,
}

pub struct ChainIndexer {
    block_fetcher: BlockFetcher,
    block_saver: BlockSaver,
}

impl ChainIndexer {
    pub async fn run(settings: &Settings) -> Result<Self, crate::Error> {
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
            .ws(WsConnect::new(&settings.rpc_ws))
            .await?;
        let provider = IndexerProvider::new(
            ProviderBuilder::new()
                .disable_recommended_fillers()
                .connect_client(client),
        )
        .await?;

        let block_saver = BlockSaver::run(BlockSaverParams {
            batch_save_size: settings.batch_save_size,
            catalog_db_url: &settings.catalog_db_url,
            s3_endpoint: &settings.s3_endpoint,
            s3_access_key_id: &settings.s3_access_key_id,
            s3_secret_access_key: &settings.s3_secret_access_key,
        })?;

        let last_checkpoint = block_saver.last_saved_checkpoint().await;

        let block_fetcher = BlockFetcher::fetch(
            provider,
            BlockFetcherParams {
                max_concurrency: settings.fetcher_max_concurrency,
                max_rps: settings.fetcher_max_rps,
                start_block: last_checkpoint.as_ref().map(|c| c.block_number + 1),
            },
        )
        .await?;

        let mut res = Self {
            block_saver,
            block_fetcher,
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

        let reorgs_detected_counter = counter!("indexer_reorgs_detected_total");

        while let Ok((block_number, block_res)) = rx.recv_async().await {
            let incoming_block = block_res
                .map_err(|err| crate::Error::BlockFetchError {
                    block_number,
                    source: err,
                })?
                .ok_or(crate::Error::MissingBlock(block_number))?;

            let incoming_block_number = incoming_block.number();
            let incoming_block_hash = incoming_block.hash().encode_hex_with_prefix();

            if last_checkpoint.as_ref().is_some_and(|last_checkpoint| {
                last_checkpoint.block_number == incoming_block_number
                    && last_checkpoint.block_hash == incoming_block_hash
            }) {
                continue;
            }

            let is_data_store_reorged = last_checkpoint.as_ref().is_some_and(|last_checkpoint| {
                incoming_block_number <= last_checkpoint.block_number
            });

            if is_data_store_reorged {
                // 1. flush appender
                // 2. delete reorged blocks from data store
                // 3. continue
                reorgs_detected_counter.increment(1);
                todo!("Handle reorg in data store level")
            } else {
                let idx = block_queue.partition_point(|b| b.number() < incoming_block.number());
                block_queue.insert(idx, incoming_block);

                while let Some(next_block) = block_queue.front() {
                    let next_block_number = next_block.number();
                    let next_block_hash = next_block.hash().encode_hex_with_prefix();

                    let is_next_block = match &last_checkpoint {
                        Some(last_checkpoint) => {
                            next_block_number == last_checkpoint.block_number as u64 + 1
                        }
                        None => next_block_number == 0,
                    };

                    if !is_next_block {
                        break;
                    }

                    let is_reorged_block =
                        last_checkpoint.as_ref().is_some_and(|last_checkpoint| {
                            last_checkpoint.block_number as u64 == next_block_number
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

                    if let Err(_) = self.block_saver.save_block(block).await {
                        break;
                    }

                    last_checkpoint = Some(Checkpoint {
                        block_number: next_block_number,
                        block_hash: next_block_hash,
                    });
                }
            }
        }

        Ok(())
    }
}
