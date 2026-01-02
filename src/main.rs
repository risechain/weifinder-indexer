use std::path::PathBuf;

use clap::Parser;
use metrics_exporter_prometheus::PrometheusBuilder;
use tracing::info;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use weifinder_indexer::Settings;
use weifinder_indexer::indexer::ChainIndexer;
use weifinder_indexer::tui::TuiExporter;

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

    if cli_args.headless {
        tracing_subscriber::fmt::init();
        PrometheusBuilder::new().install()?;
    } else {
        tui_logger::init_logger(log::LevelFilter::Trace)?;
        tracing_subscriber::registry()
            .with(tui_logger::TuiTracingSubscriberLayer)
            .with(EnvFilter::from_default_env())
            .init();
        metrics::set_global_recorder(TuiExporter::new())?;
    }

    let settings: Settings = config::Config::builder()
        .add_source(config::File::from(cli_args.config))
        .add_source(config::Environment::with_prefix("INDEXER"))
        .set_default("fetcher_max_concurrency", 100)?
        .set_default("fetcher_max_rps", 100)?
        .set_default("batch_save_size", 1000)?
        .build()?
        .try_deserialize()?;

    info!("Starting indexer with settings:\n{:?}", settings);

    ChainIndexer::run(&settings).await?;

    Ok(())
}
