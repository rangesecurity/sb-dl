use std::{collections::HashSet, sync::Arc};

use anyhow::{anyhow, Context};
use clap::ArgMatches;
use db::{client::{BlockFilter, Client}, migrations::run_migrations, models::BlockTableChoice};
use futures::StreamExt;
use sb_dl::config::Config;
use solana_client::{nonblocking::rpc_client::RpcClient, rpc_config::RpcTransactionConfig};
use solana_transaction_status::{EncodedTransaction, UiConfirmedBlock, UiTransactionEncoding};
use tokio::task::JoinSet;

pub async fn fill_missing_slots_no_tx(
    matches: &ArgMatches,
    config_path: &str
) -> anyhow::Result<()> {
    let blocks_table = BlockTableChoice::try_from(*matches.get_one::<u8>("block-table-choice").unwrap()).unwrap();
    let limit = matches.get_one::<i64>("limit").unwrap();
    let cfg = Config::load(config_path).await?;
    let rpc = Arc::new(RpcClient::new(cfg.rpc_url.clone()));
    let pool = db::new_connection_pool(&cfg.db_url, 10)?;
    let mut conn = db::new_connection(&cfg.db_url)?;
    {

        // perform db migrations
        run_migrations(&mut conn);
    }

    let client = db::client::Client {};
    let blocks = client.slot_is_null(&mut conn, *limit, &[])?;
    if blocks.is_empty() {
        return Ok(());
    }
    for block in blocks {
        let ui_block: UiConfirmedBlock = serde_json::from_value(block.data)?;
        let Some(txs) = ui_block.transactions else {
            log::warn!("transactions are None");
            continue;
        };
        if !txs.is_empty() {
            log::warn!("found null slot with txs");
            continue;
        }
        let next_block: UiConfirmedBlock = match client.select_block(&mut conn, BlockFilter::Number(block.number+1), blocks_table) {
            Ok(mut blocks) => if blocks.is_empty() {
                log::warn!("failed to find next block(current={})", block.number);
                continue;
            } else {
                serde_json::from_value(std::mem::take(&mut blocks[0].data))?
            }
            Err(err) => {
                log::error!("failed to query db {err:#?}");
                continue;
            }
        };
        log::info!("found missing_slot(slot={}, block={}, next_block={})", next_block.parent_slot, block.number, block.number+1);
        // confirm no block with the missing slot exists
        if let Ok(blocks) = client.select_block(&mut conn, BlockFilter::Slot(next_block.parent_slot as i64), blocks_table) {
            if !blocks.is_empty() {
                log::warn!("slot calculation isnt working");
            }
        }
        //if let Err(err) = client.update_slot(&mut conn, block.number, next_block.parent_slot as i64) {
        //    log::error!("failed to udpate slot(block={}, slot={})", block.number, next_block.parent_slot);
        //}
    }
    Ok(())
}

pub async fn fill_missing_slots(matches: &ArgMatches, config_path: &str) -> anyhow::Result<()> {    
    let blocks_table = BlockTableChoice::try_from(*matches.get_one::<u8>("block-table-choice").unwrap()).unwrap();

    let limit = matches.get_one::<i64>("limit").unwrap();
    let cfg = Config::load(config_path).await?;
    let rpc = Arc::new(RpcClient::new(cfg.rpc_url.clone()));
    let pool = db::new_connection_pool(&cfg.db_url, 10)?;
    let mut conn = db::new_connection(&cfg.db_url)?;
    {

        // perform db migrations
        run_migrations(&mut conn);
    }

    let client = db::client::Client {};
    let no_transactions = Arc::new(tokio::sync::RwLock::new(HashSet::<i64>::default()));
    loop {

        let blocks = client.slot_is_null(&mut conn, *limit, &no_transactions.read().await.iter().map(|v| *v).collect::<Vec<_>>())?;
        if blocks.is_empty() {
            // no more blocks to repair
            break;
        }
        let mut join_set = JoinSet::new();
        let block_chunks = blocks.chunks(10);
        for block_chunk in block_chunks.into_iter() {
            {
                match pool.get() {
                    Ok(mut pool_conn) => {
                        let rpc = rpc.clone();
                        let mut block_chunk = block_chunk.to_vec();
                        let no_transactions = no_transactions.clone();
                        join_set.spawn(async move {
                            for block in block_chunk.iter_mut() {
                                let block_data: UiConfirmedBlock = if let Ok(block) = serde_json::from_value(std::mem::take(&mut block.data)) {
                                    block
                                } else {
                                    continue;
                                };
                        
                                let (slot, sample_tx_hash) = match get_slot_for_block(&block_data, &rpc).await {
                                    Ok(Some(slot)) => slot,
                                    Ok(None) => {
                                        log::warn!("failed to find slot for block({})", block.number);
                                        let mut no_txs = no_transactions.write().await;
                                        no_txs.insert(block.number);
                                        continue;
                                    }
                                    Err(err) => {
                                        log::error!("failed to find slot for block({}) {err:#?}", block.number);
                                        let mut no_txs = no_transactions.write().await;
                                        no_txs.insert(block.number);
                                        continue;
                                    }
                                };
                                let new_block_number = if let Some(block_height) = block_data.block_height {
                                    block_height
                                } else {
                                    log::warn!(
                                        "found missing block_height(slot={slot}, block.number={})",
                                        block.number
                                    );
                                    continue;
                                };
                                log::info!(
                                    "block(slot={slot}, new_block_number={new_block_number} block.height={:?}, block.number={}, parent_slot={}, block_hash={}, sample_tx_hash={sample_tx_hash})",
                                    block_data.block_height,
                                    block.number,
                                    block_data.parent_slot,
                                    block_data.blockhash,
                                );
                                if let Err(err) = client.update_block_slot(
                                    &mut pool_conn,
                                    block.id,
                                    new_block_number as i64,
                                    slot as i64,
                                    blocks_table
                                ) {
                                    log::error!("failed to update_block_slot(old_block_number={}, new_block_number={new_block_number}, slot={slot}) {err:#?}", block.number);
                                }
                            }
                        });
                    }
                    Err(err) => {
                        log::error!("failed to get conn {err:#?}");
                    }
                }
            }
        }
        while let Some(_) = join_set.join_next().await {

        }
  

    }


    Ok(())
}

pub async fn repair_invalid_slots(matches: &ArgMatches, config_path: &str) -> anyhow::Result<()> {    
    let blocks_table = BlockTableChoice::try_from(*matches.get_one::<u8>("block-table-choice").unwrap()).unwrap();

    let cfg = Config::load(config_path).await?;
    let rpc = RpcClient::new(cfg.rpc_url.clone());
    let mut conn = db::new_connection(&cfg.db_url)?;

    // perform db migrations
    run_migrations(&mut conn);

    let client = db::client::Client {};
    // numbero f the block to repair, initially set to the very first block available
    let mut block_number = client
        .select_block(&mut conn, BlockFilter::FirstBlock, blocks_table)
        .unwrap()[0]
        .number;

    loop {
        log::info!("checking block({block_number}");
        let mut block = client.select_block(&mut conn, BlockFilter::Number(block_number), blocks_table)?;
        let mut block = if block.is_empty() {
            block_number -= 1;
            continue;
        } else if block.len() > 1 {
            return Err(anyhow!("found too many blocks({block_number})"));
        } else {
            std::mem::take(&mut block[0])
        };
        let block_data: UiConfirmedBlock = serde_json::from_value(std::mem::take(&mut block.data))?;

        let (slot, sample_tx_hash) = match get_slot_for_block(&block_data, &rpc).await {
            Ok(Some(slot)) => slot,
            Ok(None) => {
                log::warn!("failed to find slot for block({})", block.number);
                continue;
            }
            Err(err) => {
                log::error!("failed to find slot for block({}) {err:#?}", block.number);
                continue;
            }
        };

        if let Some(stored_slot) = block.slot {
            if stored_slot == slot as i64 {
                continue;
            }
        }

        client.update_slot(&mut conn, block.number, slot as i64, blocks_table)?;
        log::info!(
            "repaired block(number={}, slot={slot}, tx={sample_tx_hash})",
            block.number
        );

        // increment block_number to repair the next available block
        block_number += 1;
    }
}


pub async fn find_gap_end(
    matches: &clap::ArgMatches,
    config_path: &str
) -> anyhow::Result<()> {    
    let blocks_table = BlockTableChoice::try_from(*matches.get_one::<u8>("block-table-choice").unwrap()).unwrap();

    let starting_number = matches.get_one::<i64>("starting-number").unwrap();
    let cfg = Config::load(config_path).await?;
    let mut conn = db::new_connection(&cfg.db_url)?;
    let client = Client{};
    let gap_end = client.find_gap_end(&mut conn, *starting_number, blocks_table)?;
    log::info!("found_gap(start={starting_number}, end={gap_end})");
    Ok(())
}

/// returns the slot for the given block, along with the tx hash used to determine this
async fn get_slot_for_block(
    block: &UiConfirmedBlock,
    rpc: &RpcClient,
) -> anyhow::Result<Option<(u64, String)>> {
    let txs = block.transactions.as_ref().with_context(|| "no transactions")?;
    if txs.is_empty() {
        return Err(anyhow!("found no transactions"));
    }
    let sample_tx = &txs[0];
    let sample_tx_hash =  if let EncodedTransaction::Json(tx) = &sample_tx.transaction {
        if tx.signatures.is_empty() {
            return Err(anyhow!("found no tx hash"))
        } else {
            tx.signatures[0].clone()
        }
    } else {
        return Err(anyhow!("unsupported transaction type"))
    };
    // extract slot information
    let slot = if !sample_tx_hash.is_empty() {
        match rpc
            .get_transaction_with_config(
                &sample_tx_hash.parse()?,
                RpcTransactionConfig {
                    encoding: Some(UiTransactionEncoding::JsonParsed),
                    max_supported_transaction_version: Some(1),
                    ..Default::default()
                },
            )
            .await
        {
            Ok(tx) => tx.slot,
            Err(err) => {
                log::error!("failed to get tx({}) {err:#?}", sample_tx_hash);
                return Ok(None);
            }
        }
    } else {
        log::warn!("sample tx hash has no signature");
        return Ok(None);
    };
    return Ok(Some((slot, sample_tx_hash)));
}