use anyhow::Context;
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
    let tx_parser = TransferParser::new(
        &cfg.elasticsearch.url,
        cfg.elasticsearch.storage_version
    ).await?;
    
    let mut conn = db::new_connection(&cfg.db_url)?;
    run_migrations(&mut conn);

    let client = Client{};
    log::info!("fetching blocks");
    for block in start..=end {
        match client.select_block(
            &mut conn,
            BlockFilter::Number(block),
            blocks_table
        ) {
            Ok(mut blocks) => if blocks.is_empty() {
                log::warn!("failed to find block({block})");
                continue;
            } else {
                let block_number = blocks[0].number;
                tx_parser.start(vec![std::mem::take(&mut blocks[0])]).await.with_context(|| "indexing failed")?;
                log::info!("indexed block({block_number})");
            }
            Err(err) => {
                log::debug!("failed to query db {err:#?}");
            }
        }
    }
    Ok(())
}
