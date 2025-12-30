use std::num::NonZeroU32;
use std::num::NonZeroUsize;

use tracing::info;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use weifinder_indexer::Settings;
use weifinder_indexer::indexer::ChainIndexer;
use weifinder_indexer::tui::TuiExporter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tui_logger::init_logger(log::LevelFilter::Trace)?;

    // Set up tracing with tui-logger layer
    tracing_subscriber::registry()
        .with(tui_logger::TuiTracingSubscriberLayer)
        .with(EnvFilter::from_default_env())
        .init();

    metrics::set_global_recorder(TuiExporter::new())?;

    let settings = Settings {
        fetcher_max_concurrency: NonZeroUsize::new(100).unwrap(),
        fetcher_max_rps: NonZeroU32::new(100).unwrap(),
        rpc_url: "wss://testnet.riselabs.xyz/ws".to_string(),
        batch_save_size: NonZeroUsize::new(1000).unwrap(),
    };

    info!("settings: {:?}", settings);

    ChainIndexer::run(&settings).await?;

    Ok(())
}
