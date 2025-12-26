use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::{env, num::NonZeroU32};

use weifinder_indexer::Settings;
use weifinder_indexer::indexer::ChainIndexer;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let settings_path: PathBuf = env::args()
        .nth(1)
        .or(env::var("INDEXER_SETTINGS_PATH").ok())
        .map(|path_str| path_str.into())
        .or(Some(PathBuf::from("settings.toml")))
        .unwrap();

    let settings = Settings {
        db_url: "file://data/weifinder.db".to_string(),
        fetcher_max_concurrency: NonZeroUsize::new(100).unwrap(),
        fetcher_max_rps: NonZeroU32::new(100).unwrap(),
        rpc_url: "https://mainnet.infura.io/v3/".to_string(),
        data_path: "data/data.ducklake".to_string(),
    };

    let indexer = ChainIndexer::run(&settings).await?;

    Ok(())
}
