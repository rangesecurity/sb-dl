use std::collections::HashSet;

use clap::ArgMatches;
use db::migrations::run_migrations;
use sb_dl::{config::Config, Downloader};

pub async fn start(matches: &ArgMatches, config_path: &str) -> anyhow::Result<()> {
    let cfg = Config::load(config_path).await?;
    let start = matches.get_one::<u64>("start").cloned();
    let limit = matches.get_one::<u64>("limit").cloned();
    let no_minimization = matches.get_flag("no-minimization");
    let downloader = Downloader::new(cfg.bigtable).await?;

    // load all currently indexed block number to avoid re-downloading already indexed block data
    let mut already_indexed: HashSet<u64> = {
        let mut conn = db::new_connection(&cfg.db_url)?;

        // perform db migrations
        run_migrations(&mut conn);

        let client = db::client::Client {};
        client
            .indexed_blocks(&mut conn)
            .unwrap_or_default()
            .into_iter()
            .map(|block| block as u64)
            .collect()
    };
    log::info!("starting block_indexing. disable_minimization={no_minimization}");
    let blocks = downloader
        .start(&mut already_indexed, start, limit, no_minimization)
        .await?;

    let mut conn = db::new_connection(&cfg.db_url)?;
    let client = db::client::Client {};
    for (slot, block) in blocks {
        match serde_json::to_value(block) {
            Ok(block) => {
                if let Err(err) = client.insert_block(&mut conn, slot as i64, block) {
                    log::error!("failed to persist block({slot}) {err:#?}");
                }
            }
            Err(err) => {
                log::error!("failed to serialize block({slot}) {err:#?}");
            }
        }
    }
    Ok(())
}
