use sb_dl::{config::Config, services::transfer_flow_api::serve_api};

pub async fn transfer_flow_api(
    matches: &clap::ArgMatches,
    config_path: &str,
) -> anyhow::Result<()> {
    let cfg = Config::load(config_path).await?;
    let listen_url = matches.get_one::<String>("listen-url").unwrap();

    serve_api(listen_url, &cfg.db_url).await
}
