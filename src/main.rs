use std::num::NonZeroU32;
use std::num::NonZeroUsize;

use tracing::info;
use weifinder_indexer::Settings;
use weifinder_indexer::indexer::ChainIndexer;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // let settings_path: PathBuf = env::args()
    //     .nth(1)
    //     .or(env::var("INDEXER_SETTINGS_PATH").ok())
    //     .map(|path_str| path_str.into())
    //     .or(Some(PathBuf::from("settings.toml")))
    //     .unwrap();
    // println!("Settings path: {}", settings_path.display());
    // todo: load settings from file or env

    tracing_subscriber::fmt::init();

    let settings = Settings {
        db_url: "data/indexer.db".to_string(),
        fetcher_max_concurrency: NonZeroUsize::new(100).unwrap(),
        fetcher_max_rps: NonZeroU32::new(100).unwrap(),
        rpc_url: "wss://testnet.riselabs.xyz/ws".to_string(),
    };

    info!("settings: {:?}", settings);

    ChainIndexer::run(&settings).await?;

    Ok(())
}
