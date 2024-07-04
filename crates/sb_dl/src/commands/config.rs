use sb_dl::config::Config;

pub async fn new_config(config_path: &str) -> anyhow::Result<()> {
    let cfg = Config::default();
    cfg.save(config_path).await
}
