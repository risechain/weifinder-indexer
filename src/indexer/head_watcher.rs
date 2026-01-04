use alloy::{
    eips::BlockId,
    providers::{Provider, RootProvider},
    rpc::types::Header,
};
use metrics::counter;
use op_alloy_network::Optimism;
use tokio::sync::watch;
use tracing::error;

#[derive(Clone)]
pub struct ChainHeadWatcher {
    pub current_head: watch::Receiver<Header>,
}

impl ChainHeadWatcher {
    pub async fn watch(provider: RootProvider<Optimism>) -> Result<Self, crate::error::Error> {
        let current_head_counter = counter!("indexer_current_head_number");
        let current_head = provider
            .get_block(BlockId::latest())
            .await?
            .expect("latest block to exist")
            .header;
        current_head_counter.absolute(current_head.number);

        let mut sub = provider.subscribe_blocks().await?;

        let (tx, rx) = watch::channel(current_head);

        tokio::spawn(async move {
            while let Ok(header) = sub.recv().await {
                current_head_counter.absolute(header.number);
                tx.send(header).ok();
            }

            error!("Chain head watcher exited");
        });

        Ok(Self { current_head: rx })
    }
}
