use alloy::{
    providers::{Provider, RootProvider},
    rpc::types::Header,
};
use tokio::sync::watch::Receiver;

use crate::indexer::ChainHeadWatcher;

#[derive(Clone)]
pub struct IndexerProvider {
    provider: RootProvider,
    chain_head_watcher: ChainHeadWatcher,
    chain_id: u64,
}

impl IndexerProvider {
    pub async fn new(provider: RootProvider) -> Result<Self, crate::error::Error> {
        let chain_id = provider.get_chain_id().await?;
        let chain_head_watcher = ChainHeadWatcher::watch(provider.clone()).await?;

        Ok(Self {
            chain_id,
            provider,
            chain_head_watcher,
        })
    }

    pub fn current_head(&self) -> &Receiver<Header> {
        &self.chain_head_watcher.current_head
    }

    pub fn chain_id(&self) -> u64 {
        self.chain_id
    }
}

impl Provider for IndexerProvider {
    #[doc = " Returns the root provider."]
    fn root(&self) -> &RootProvider {
        &self.provider
    }
}
