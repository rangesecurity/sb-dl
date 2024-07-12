use sb_dl::config::Config;

pub async fn stream_geyser_blocks(
    matches: &clap::ArgMatches,
    config_path: &str
) -> anyhow::Result<()> {
    let cfg = Config::load(config_path).await?;
    let gc = g_dl::new_geyser_client(
        &cfg.geyser.endpoint,
        &cfg.geyser.token,
        cfg.geyser.max_decoding_size,
        cfg.geyser.max_encoding_size
    ).await?;

    g_dl::subscribe_blocks(gc).await
}