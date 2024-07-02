use clap::ArgMatches;
use sb_dl::config::Config;

pub async fn new_config(config_path: &str) -> anyhow::Result<()> {
    let cfg = Config::default();
    cfg.save(config_path).await
}

pub async fn set_credentials_path(matches: &ArgMatches, config_path: &str) -> anyhow::Result<()> {
    let mut cfg = Config::load(config_path).await?;
    let cred_path = matches.get_one::<String>("path").unwrap();

    cfg.bigtable_credentials_path(cred_path).await?;

    cfg.save(config_path).await
}
