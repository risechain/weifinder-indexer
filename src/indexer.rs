mod block_fetcher;
mod block_saver;
mod head_watcher;
mod provider;
mod types;

pub use block_fetcher::*;
pub use block_saver::*;
pub use head_watcher::*;
use op_alloy_rpc_types::OpTransactionReceipt;
pub use provider::*;
use tokio::task::JoinHandle;

use std::{collections::VecDeque, num::NonZeroU32};

use alloy::{
    eips::BlockNumberOrTag, hex::ToHexExt, providers::Provider,
    rpc::types::trace::parity::TraceResultsWithTransactionHash,
};
use metrics::counter;

use crate::{indexer::types::OpBlock, settings::Settings};

#[derive(Debug, Clone)]
pub struct Checkpoint {
    block_number: u64,
    block_hash: String,
}

pub struct ChainIndexer {
    block_fetcher: BlockFetcher,
    block_saver: BlockSaver,
    task_handle: JoinHandle<Result<(), crate::Error>>,
}

impl ChainIndexer {
    pub async fn run(settings: &Settings) -> Result<Self, crate::Error> {
        let provider = IndexerProvider::new(
            &settings.rpc_ws,
            settings
                .fetcher_max_blocks_per_second
                .saturating_mul(NonZeroU32::new(2).unwrap()),
        )
        .await?;

        let block_saver = BlockSaver::run(BlockSaverParams {
            batch_save_size: settings.batch_save_size,
            catalog_db_url: &settings.catalog_db_url,
            s3_endpoint: &settings.s3_endpoint,
            s3_access_key_id: &settings.s3_access_key_id,
            s3_secret_access_key: &settings.s3_secret_access_key,
            s3_bucket: &settings.s3_bucket,
            provider: provider.clone(),
            checkpoint_db_path: &settings.checkpoint_db_path,
        })?;

        let mut last_checkpoint = block_saver.last_saved_checkpoint().await;

        let block_fetcher = BlockFetcher::fetch(
            provider.clone(),
            settings.fetcher_max_blocks_per_second,
            last_checkpoint.as_ref().map(|c| c.block_number + 1),
        )
        .await?;

        let fetcher = block_fetcher.receiver().clone();
        let saver = block_saver.tx.clone();

        let handle = tokio::spawn(async move {
            let mut block_queue: VecDeque<(
                OpBlock,
                Vec<OpTransactionReceipt>,
                Vec<TraceResultsWithTransactionHash>,
            )> = VecDeque::new();

            let reorgs_detected_counter = counter!("indexer_reorgs_detected_total");

            while let Ok((block_number, block_res, receipts_res, traces_res)) =
                fetcher.recv_async().await
            {
                let incoming_block = block_res
                    .map_err(|err| crate::Error::BlockFetchError {
                        block_number,
                        source: err,
                    })?
                    .ok_or(crate::Error::MissingBlock(block_number))?;
                let receipts = receipts_res
                    .map_err(|err| crate::Error::BlockFetchError {
                        block_number,
                        source: err,
                    })?
                    .ok_or(crate::Error::MissingBlock(block_number))?;
                let traces = traces_res.map_err(|err| crate::Error::BlockFetchError {
                    block_number,
                    source: err,
                })?;

                let incoming_block_number = incoming_block.number();
                let incoming_block_hash = incoming_block.hash().encode_hex_with_prefix();

                if last_checkpoint.as_ref().is_some_and(|last_checkpoint| {
                    last_checkpoint.block_hash == incoming_block_hash
                }) {
                    continue;
                }

                let is_data_store_reorged =
                    last_checkpoint.as_ref().is_some_and(|last_checkpoint| {
                        incoming_block_number <= last_checkpoint.block_number
                    });

                if is_data_store_reorged {
                    reorgs_detected_counter.increment(1);
                    let prev_block = provider
                        .get_block_by_number(BlockNumberOrTag::Number(incoming_block_number - 1))
                        .await?
                        .expect("previous block to exist");
                    saver
                        .send_async(BlockSavePayload::Reorg {
                            new_block_number: incoming_block_number,
                            prev_block_hash: prev_block.hash().encode_hex_with_prefix(),
                        })
                        .await
                        .ok();
                    last_checkpoint = Some(Checkpoint {
                        block_number: incoming_block_number - 1,
                        block_hash: prev_block.hash().encode_hex_with_prefix(),
                    });
                }

                let idx =
                    block_queue.partition_point(|(b, _, _)| b.number() < incoming_block_number);
                block_queue.insert(idx, (incoming_block, receipts, traces));

                while let Some((next_block, _, _)) = block_queue.front() {
                    let next_block_number = next_block.number();
                    let next_block_hash = next_block.hash().encode_hex_with_prefix();

                    let is_next_block = match &last_checkpoint {
                        Some(last_checkpoint) => {
                            next_block_number == last_checkpoint.block_number + 1
                        }
                        None => next_block_number == 0,
                    };

                    if !is_next_block {
                        break;
                    }

                    let is_reorged_block =
                        last_checkpoint.as_ref().is_some_and(|last_checkpoint| {
                            last_checkpoint.block_number == next_block_number
                                || next_block
                                    .header
                                    .parent_hash
                                    .encode_hex_with_prefix()
                                    .ne(&last_checkpoint.block_hash)
                        });

                    let (block, receipts, traces) = block_queue.pop_front().unwrap();

                    if is_reorged_block {
                        continue;
                    }

                    saver
                        .send_async(BlockSavePayload::NewBlock {
                            block,
                            receipts,
                            traces,
                        })
                        .await
                        .ok();

                    last_checkpoint = Some(Checkpoint {
                        block_number: next_block_number,
                        block_hash: next_block_hash,
                    });
                }
            }

            Ok(())
        });

        Ok(Self {
            block_fetcher,
            block_saver,
            task_handle: handle,
        })
    }

    pub async fn wait_for_completion(self) -> Result<(), crate::Error> {
        tokio::select! {
            v = self.task_handle => {
                v?
            },
            v = self.block_saver.task_handle => {
                v?
            },
            v = self.block_fetcher.task_handle => {
                v?
            }
        }
    }
}
