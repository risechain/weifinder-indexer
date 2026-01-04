use std::path::PathBuf;

use clap::Parser;
use metrics_exporter_prometheus::PrometheusBuilder;
use tracing::info;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use weifinder_indexer::Settings;
use weifinder_indexer::indexer::ChainIndexer;
use weifinder_indexer::tui::{Tui, TuiExporter};

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct CliArgs {
    #[arg(short, long, default_value = "indexer.toml")]
    config: PathBuf,
    #[arg(long)]
    headless: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli_args = CliArgs::parse();

    let tui = if cli_args.headless {
        tracing_subscriber::fmt::init();
        PrometheusBuilder::new().install()?;
        None
    } else {
        tui_logger::init_logger(log::LevelFilter::Trace)?;
        tracing_subscriber::registry()
            .with(tui_logger::TuiTracingSubscriberLayer)
            .with(EnvFilter::from_default_env())
            .init();
        let tui = Tui::new();
        let tui_exporter = TuiExporter::from_stats(tui.stats.clone());
        metrics::set_global_recorder(tui_exporter)?;
        Some(tui)
    };

    let settings: Settings = config::Config::builder()
        .add_source(config::File::from(cli_args.config))
        .add_source(config::Environment::with_prefix("INDEXER"))
        .set_default("fetcher_max_blocks_per_second", 100)?
        .set_default("batch_save_size", 1000)?
        .build()?
        .try_deserialize()?;

    info!("Starting indexer with settings:\n{:?}", settings);

    let indexer = ChainIndexer::run(&settings).await?;

    if let Some(tui) = tui {
        tokio::select! {
            v = tui.task_handle => Ok(v??),
            v = indexer.wait_for_completion() => Ok(v?)
        }
    } else {
        Ok(indexer.wait_for_completion().await?)
    }
}
