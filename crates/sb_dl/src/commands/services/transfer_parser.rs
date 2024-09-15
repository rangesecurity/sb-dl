use sb_dl::{config::Config, services::{transfer_flow_api::serve_api, transfer_parser::TransferParser}};
use tokio::signal::unix::{signal, SignalKind};

use crate::commands::handle_exit;

pub async fn transfer_parser(
    matches: &clap::ArgMatches,
    config_path: &str,
) -> anyhow::Result<()> {
    let cfg = Config::load(config_path).await?;

    let tx_parser = TransferParser::new(
        &cfg.db_url,
        &cfg.elasticsearch_url
    ).await?;

    let sig_quit = signal(SignalKind::quit())?;
    let sig_int = signal(SignalKind::interrupt())?;
    let sig_term = signal(SignalKind::terminate())?;
    let (exit_tx, exit_rx) = tokio::sync::oneshot::channel::<()>();
    let (_finished_tx, finished_rx) = tokio::sync::oneshot::channel();

    tokio::task::spawn(async move {
        let _ = handle_exit(sig_quit, sig_int, sig_term, finished_rx).await;
        let _ = exit_tx.send(());
    });
    tx_parser.start(exit_rx).await;
    Ok(())
}
