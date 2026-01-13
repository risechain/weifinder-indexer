use std::{num::NonZeroU32, sync::Arc, time::Instant};

use alloy::{
    eips::{BlockId, BlockNumberOrTag},
    providers::{Provider, ext::TraceApi},
    rpc::types::trace::parity::TraceResultsWithTransactionHash,
    transports::{RpcError, TransportErrorKind},
};
use flume::Receiver;
use governor::{Quota, RateLimiter};
use metrics::{counter, histogram};
use op_alloy_rpc_types::OpTransactionReceipt;
use tokio::{sync::Semaphore, task::JoinHandle};

use crate::indexer::{provider::IndexerProvider, types::OpBlock};

pub type FallibleMaybeBlock = Result<Option<OpBlock>, RpcError<TransportErrorKind>>;
pub type FallibleMaybeReceipts =
    Result<Option<Vec<OpTransactionReceipt>>, RpcError<TransportErrorKind>>;
pub type FallibleTraces =
    Result<Vec<TraceResultsWithTransactionHash>, RpcError<TransportErrorKind>>;

pub struct BlockFetcher {
    rx: Receiver<(
        u64,
        FallibleMaybeBlock,
        FallibleMaybeReceipts,
        FallibleTraces,
    )>,
    pub task_handle: JoinHandle<Result<(), crate::Error>>,
}

impl BlockFetcher {
    pub async fn fetch(
        provider: IndexerProvider,
        max_blocks_per_second: NonZeroU32,
        start_block: Option<u64>,
    ) -> Result<Self, crate::error::Error> {
        let (tx, rx) = flume::bounded(1000);

        let handle = tokio::spawn(async move {
            let semaphore = Arc::new(Semaphore::new(max_blocks_per_second.get() as usize));
            let rate_limiter = RateLimiter::direct(Quota::per_second(max_blocks_per_second));

            let mut current_head = provider.current_head().clone();
            let mut fetching_block_number = start_block.unwrap_or(0);
            let last_fetched_block_counter = counter!("indexer_last_fetched_block");
            let block_fetch_duration_histogram = histogram!("indexer_block_fetch_duration_seconds");

            loop {
                let (current_head_number, is_new_head) = {
                    let head = current_head.borrow();
                    (head.number, head.has_changed())
                };

                let should_wait_next_block =
                    fetching_block_number >= current_head_number && !is_new_head;

                if should_wait_next_block {
                    current_head
                        .changed()
                        .await
                        .map_err(crate::Error::HeadWatcherClosed)?;
                    current_head.mark_changed();
                    continue;
                } else if fetching_block_number >= current_head_number {
                    current_head.mark_unchanged();
                }

                if fetching_block_number > current_head_number {
                    // this means there is a reorg
                    fetching_block_number = current_head_number;
                }

                let permit = semaphore
                    .clone()
                    .acquire_owned()
                    .await
                    .expect("semaphore to not be closed");
                rate_limiter.until_ready().await;

                let provider = provider.clone();
                let tx = tx.clone();
                let block_fetch_duration_histogram = block_fetch_duration_histogram.clone();

                tokio::spawn(async move {
                    let fetch_start = Instant::now();
                    let block_number = BlockNumberOrTag::Number(fetching_block_number);
                    let (block, receipts, traces) = tokio::join!(
                        provider.get_block_by_number(block_number).full(),
                        provider.get_block_receipts(BlockId::Number(block_number)),
                        provider
                            .trace_replay_block_transactions(BlockId::Number(block_number))
                            .trace()
                    );
                    block_fetch_duration_histogram.record(fetch_start.elapsed().as_secs_f64());

                    tx.send_async((fetching_block_number, block, receipts, traces))
                        .await
                        .unwrap();

                    drop(permit)
                });

                last_fetched_block_counter.absolute(fetching_block_number);

                fetching_block_number += 1;
            }
        });

        Ok(Self {
            rx,
            task_handle: handle,
        })
    }

    pub fn receiver(
        &self,
    ) -> &Receiver<(
        u64,
        FallibleMaybeBlock,
        FallibleMaybeReceipts,
        FallibleTraces,
    )> {
        &self.rx
    }
}
