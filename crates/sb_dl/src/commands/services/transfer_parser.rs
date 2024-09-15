use db::{client::{BlockFilter, Client}, migrations::run_migrations, models::{BlockTableChoice, Blocks}};
use sb_dl::{config::Config, services::{transfer_flow_api::serve_api, transfer_parser::TransferParser}};
use tokio::signal::unix::{signal, SignalKind};

use crate::commands::handle_exit;

pub async fn transfer_parser(
    matches: &clap::ArgMatches,
    config_path: &str,
) -> anyhow::Result<()> {
    let blocks_table = BlockTableChoice::try_from(*matches.get_one::<u8>("block-table-choice").unwrap()).unwrap();
    let start = *matches.get_one::<i64>("start").unwrap();
    let end = *matches.get_one::<i64>("end").unwrap();
    let cfg = Config::load(config_path).await?;
    let (blocks_tx, blocks_rx) = tokio::sync::mpsc::channel::<Blocks>(1_000);
    let tx_parser = TransferParser::new(
        blocks_rx,
        &cfg.elasticsearch_url
    ).await?;
    
    let mut conn = db::new_connection(&cfg.db_url)?;
    run_migrations(&mut conn);

    let sig_quit = signal(SignalKind::quit())?;
    let sig_int = signal(SignalKind::interrupt())?;
    let sig_term = signal(SignalKind::terminate())?;
    let (exit_tx, exit_rx) = tokio::sync::oneshot::channel::<()>();
    let (_finished_tx, finished_rx) = tokio::sync::oneshot::channel();

    tokio::task::spawn(async move {
        tx_parser.start(exit_rx).await;
    });

    let client = Client{};
    for block in start..=end {
        match client.select_block(
            &mut conn,
            BlockFilter::Number(block),
            blocks_table
        ) {
            Ok(mut blocks) => if blocks.is_empty() {
                log::debug!("failed to find block({block})");
                continue;
            } else {
                if let Err(err) = blocks_tx.send(std::mem::take(&mut blocks[0])).await {
                    log::error!("failed to send block({block}) {err:#?}");
                }
            }
            Err(err) => {
                log::debug!("failed to query db {err:#?}");
            }
        }
    }
    let _ = handle_exit(sig_quit, sig_int, sig_term, finished_rx).await;
    let _ =  exit_tx.send(());
    Ok(())
}
