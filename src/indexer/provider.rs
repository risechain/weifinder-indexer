use std::num::NonZeroU32;

use alloy::{
    providers::{Provider, ProviderBuilder, RootProvider, WsConnect},
    rpc::{client::ClientBuilder, types::Header},
    transports::layers::RetryBackoffLayer,
};
use op_alloy_network::Optimism;
use tokio::sync::watch::Receiver;

use crate::indexer::ChainHeadWatcher;

#[derive(Clone)]
pub struct IndexerProvider {
    provider: RootProvider<Optimism>,
    chain_head_watcher: ChainHeadWatcher,
    chain_id: u64,
}

impl IndexerProvider {
    pub async fn new(rpc_ws: &str, max_rps: NonZeroU32) -> Result<Self, crate::error::Error> {
        let client = ClientBuilder::default()
            .layer(RetryBackoffLayer::new(
                10,
                1000,
                max_rps
                    .checked_mul(NonZeroU32::new(20).unwrap())
                    .unwrap()
                    .get() as u64,
            ))
            .ws(WsConnect::new(rpc_ws))
            .await?;
        let provider = ProviderBuilder::new()
            .disable_recommended_fillers()
            .network::<Optimism>()
            .connect_client(client);

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

impl Provider<Optimism> for IndexerProvider {
    #[doc = " Returns the root provider."]
    fn root(&self) -> &RootProvider<Optimism> {
        &self.provider
    }
}
