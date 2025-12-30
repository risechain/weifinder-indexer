use std::{
    num::{NonZeroU32, NonZeroUsize},
    sync::Arc,
    time::Instant,
};

use alloy::{
    eips::BlockNumberOrTag,
    providers::Provider,
    rpc::types::Block,
    transports::{RpcError, TransportErrorKind},
};
use flume::Receiver;
use governor::{Quota, RateLimiter};
use metrics::{counter, histogram};
use tokio::sync::Semaphore;

use crate::indexer::provider::IndexerProvider;

pub struct BlockFetcherParams {
    pub max_concurrency: NonZeroUsize,
    pub max_rps: NonZeroU32,
    pub start_block: Option<u64>,
}

impl Default for BlockFetcherParams {
    fn default() -> Self {
        Self {
            max_concurrency: NonZeroUsize::new(100).unwrap(),
            max_rps: NonZeroU32::new(100).unwrap(),
            start_block: None,
        }
    }
}

pub struct BlockFetcher {
    rx: Receiver<(u64, Result<Option<Block>, RpcError<TransportErrorKind>>)>,
}

impl BlockFetcher {
    pub async fn fetch(
        provider: IndexerProvider,
        params: BlockFetcherParams,
    ) -> Result<Self, crate::error::Error> {
        let (tx, rx) = flume::bounded(1000);

        tokio::spawn(async move {
            let semaphore = Arc::new(Semaphore::new(params.max_concurrency.get() as usize));
            let rate_limiter = RateLimiter::direct(Quota::per_second(params.max_rps));

            let mut current_head = provider.current_head().clone();
            let mut fetching_block_number = params.start_block.unwrap_or(0);
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
                    if let Err(_) = current_head.changed().await {
                        break;
                    }
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
                    let block = provider
                        .get_block_by_number(BlockNumberOrTag::Number(fetching_block_number))
                        .await;
                    block_fetch_duration_histogram.record(fetch_start.elapsed().as_secs_f64());

                    tx.send_async((fetching_block_number, block)).await.unwrap();

                    drop(permit)
                });

                last_fetched_block_counter.absolute(fetching_block_number);

                fetching_block_number += 1;
            }
        });

        Ok(Self { rx })
    }

    pub fn receiver(
        &self,
    ) -> &Receiver<(u64, Result<Option<Block>, RpcError<TransportErrorKind>>)> {
        &self.rx
    }
}
