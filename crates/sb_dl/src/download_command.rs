use clap::ArgMatches;
use sb_dl::{config::Config, Downloader};

pub async fn start(matches: &ArgMatches, config_path: &str) -> anyhow::Result<()> {
    let cfg = Config::load(config_path).await?;
    let downloader = Downloader::new(cfg.bigtable).await?;
    Ok(())
}