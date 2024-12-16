use std::time::Duration;
use chrono::prelude::*;
use anyhow::{anyhow, Context};
use db::{client::{BlockFilter, Client}, migrations::run_migrations, new_connection};
use sb_dl::{config::Config, services::backfill::Backfiller, types::BlockInfo};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::clock::DEFAULT_SLOTS_PER_EPOCH;

use crate::cli::ServicesCommands;

use super::downloaders::block_persistence_loop;

const SLOTS_PER_SIX_HOURS: u64 = ((DEFAULT_SLOTS_PER_EPOCH / 2) / 24) *  6;

pub async fn find_gaps(
    cmd: ServicesCommands,
    config_path: &str
) -> anyhow::Result<()> {
    let ServicesCommands::FindGaps {limit} = cmd else {
        return Err(anyhow!("invalid command"));
    };
    let cfg = Config::load(config_path).await?;
    let rpc = RpcClient::new(cfg.rpc_url.clone());

    let current_slot = rpc.get_slot().await?;
    
    let mut conn = new_connection(&cfg.db_url)?;
    run_migrations(&mut conn);
    
    let client = Client{};
    
    let gaps = client.find_gaps(&mut conn, (current_slot - SLOTS_PER_SIX_HOURS) as i64, current_slot as i64, Some(limit))?;
    
    log::info!("found gaps {gaps:#?}");
    Ok(())
}

pub async fn repair_gaps(
    cmd: ServicesCommands,
    config_path: &str
) -> anyhow::Result<()> {
    let ServicesCommands::RepairGaps { limit, failed_blocks_dir, threads } = cmd else {
        return Err(anyhow!("invalid command"));
    };

    let cfg = Config::load(config_path).await?;

    let current_slot = {
        let rpc = RpcClient::new(cfg.rpc_url.clone());
        rpc.get_slot().await?
    };
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
            async move { block_persistence_loop(conn_pool, failed_blocks_dir, blocks_rx, threads as usize).await },
        );
    }

    let mut conn = conn_pool.get()?;

    let backfiller = Backfiller::new(&cfg.rpc_url);
    let client = Client{};
    let gaps = client.find_gaps(&mut conn, (current_slot - SLOTS_PER_SIX_HOURS) as i64, current_slot as i64, Some(limit))?;
    log::info!("found {} gaps", gaps.len());

    // start trying to repair gaps at the block immediately preceeding the current missing block
    for missing_block in gaps {
        // get block info for the previous block which isn't missing
        let blocks = client.select_block(&mut conn, BlockFilter::Number(missing_block - 1))?;
        if blocks.is_empty() {
            continue;
        }
        log::info!("guessing_slot(block={}, slot={:?})", blocks[0].number, blocks[0].slot);
        let mut possible_slot = blocks[0].slot + 1;
        loop {
            if let Ok(block) = backfiller.get_block(possible_slot as u64, false).await {
                log::info!("found missing block({possible_slot})");
                let Some(block_height) = block.block_height else {
                    log::warn!("missing block height");
                    break;
                };
                let time = if let Some(block_time) = block.block_time {
                    DateTime::from_timestamp(block_time, 0)
                } else {
                    None
                };
                if let Err(err) = blocks_tx.send(BlockInfo {
                    block_height: block_height,
                    slot: possible_slot as u64,
                    time,
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
        
    }
    
    Ok(())
}