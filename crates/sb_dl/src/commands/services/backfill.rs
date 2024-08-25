use std::time::Duration;

use anyhow::{anyhow, Context};
use db::{client::{BlockFilter, Client}, migrations::run_migrations};
use sb_dl::{config::Config, services::backfill::Backfiller, types::BlockInfo};

use super::downloaders::block_persistence_loop;

pub async fn backfill(
    matches: &clap::ArgMatches,
    config_path: &str
) -> anyhow::Result<()> {
    let failed_blocks_dir = matches.get_one::<String>("failed-blocks").unwrap().clone();
    let starting_number = matches.get_one::<i64>("starting-number").unwrap();

    let threads = *matches.get_one::<usize>("threads").unwrap();
    let cfg = Config::load(config_path).await?;
    let conn_pool = db::new_connection_pool(&cfg.db_url, threads as u32 * 2)?;


    {
        let mut conn = conn_pool.get()?;
        run_migrations(&mut conn);
    }

    let (blocks_tx, blocks_rx) = tokio::sync::mpsc::channel::<BlockInfo>(1000);


    {

        let conn_pool = conn_pool.clone();
        // start the background persistence task
        tokio::task::spawn(
            async move { block_persistence_loop(conn_pool, failed_blocks_dir, blocks_rx, threads).await },
        );
    }

    let mut conn = conn_pool.get()?;

    let backfiller = Backfiller::new(&cfg.rpc_url);
    let client = Client{};
    let gap_end = client.find_gap_end(&mut conn, *starting_number)?;

    // start trying to repair gaps at the block immediately preceeding the current missing block
    for missing_block in *starting_number-1..gap_end {
        // get block info for the previous block which isn't missing
        let blocks = client.select_block(&mut conn, BlockFilter::Number(missing_block - 1))?;
        if blocks.is_empty() {
            continue;
        }
        log::info!("guessing_slot(block={}, slot={:?})", blocks[0].number, blocks[0].slot);
        let mut possible_slot = blocks[0].slot.with_context(|| "missing slot")? + 1;
        loop {
            if let Ok(block) = backfiller.get_block(possible_slot as u64, false).await {
                log::info!("found missing block({possible_slot})");
                let Some(block_height) = block.block_height else {
                    log::warn!("missing block height");
                    break;
                };
                if let Err(err) = blocks_tx.send(BlockInfo {
                    block_height: block_height,
                    slot: Some(possible_slot as u64),
                    block,
                }).await {
                    log::error!("failed to send block {err:#?}");
                }
                break;
            } else {
                log::warn!("invalid slot({possible_slot}), trying next number...");
                possible_slot += 1;
            }
        }
        tokio::time::sleep(Duration::from_secs(10)).await;
    }






    Ok(())
}