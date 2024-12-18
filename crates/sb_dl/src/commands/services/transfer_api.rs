use anyhow::anyhow;
use sb_dl::{config::Config, services::transfer_flow_api::serve_api};

use crate::cli::ServicesCommands;

pub async fn transfer_flow_api(
    cmd: ServicesCommands,
    config_path: &str,
) -> anyhow::Result<()> {
    let ServicesCommands::TransferFlowApi { listen_url } = cmd else {
        return Err(anyhow!("invalid command"));
    };
    let cfg = Config::load(config_path).await?;

    serve_api(&listen_url).await
}
