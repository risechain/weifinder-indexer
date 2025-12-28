use alloy::{
    eips::BlockId,
    providers::{Provider, RootProvider},
    rpc::types::Header,
};
use tokio::sync::watch;
use tracing::error;

#[derive(Clone)]
pub struct ChainHeadWatcher {
    pub current_head: watch::Receiver<Header>,
}

impl ChainHeadWatcher {
    pub async fn watch(provider: RootProvider) -> Result<Self, crate::error::Error> {
        let current_head = provider
            .get_block(BlockId::latest())
            .await?
            .expect("latest block to exist")
            .header;

        let mut sub = provider.subscribe_blocks().await?;

        let (tx, rx) = watch::channel(current_head);

        tokio::spawn(async move {
            while let Ok(header) = sub.recv().await {
                tx.send(header).ok();
            }

            error!("Chain head watcher exited");
        });

        Ok(Self { current_head: rx })
    }
}
