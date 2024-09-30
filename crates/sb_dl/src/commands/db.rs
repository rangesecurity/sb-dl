use std::{collections::{HashMap, HashSet}, sync::{atomic::AtomicU64, Arc}};

use anyhow::{anyhow, Context};
use clap::ArgMatches;
use db::{client::{BlockFilter, Client}, migrations::run_migrations, models::{BlockTableChoice, Blocks}};
use futures::stream::{self, StreamExt};
use sb_dl::config::Config;
use solana_client::{nonblocking::rpc_client::RpcClient, rpc_config::RpcTransactionConfig};
use solana_transaction_status::{EncodedTransaction, UiConfirmedBlock, UiTransactionEncoding};
use tokio::{fs::File, io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter}, task::JoinSet};

/// given a range, find blocks that are missing
pub async fn find_missing_blocks(
    matches: &ArgMatches,
    config_path: &str
) -> anyhow::Result<()> {
    let start = *matches.get_one::<i64>("start").unwrap() as u64;
    let end = *matches.get_one::<i64>("end").unwrap() as u64;
    let output = matches.get_one::<String>("output").unwrap();
    let cfg = Config::load(config_path).await?;
    // load all currently indexed block number to avoid re-downloading already indexed block data
    let mut indexed: HashSet<u64> = {
        let mut conn = db::new_connection(&cfg.db_url)?;

        // perform db migrations
        run_migrations(&mut conn);

        let client = db::client::Client {};
        //let mut blocks_1_indexed = client
        //    .indexed_slots(&mut conn, BlockTableChoice::Blocks)
        //    .unwrap_or_default()
        //    .into_iter()
        //    .filter_map(|block| Some(block? as u64))
        //    .collect::<Vec<_>>();
        //let mut blocks_2_indexed = client
        //    .indexed_slots(&mut conn, BlockTableChoice::Blocks2)
        //    .unwrap_or_default()
        //    .into_iter()
        //    .filter_map(|block| Some(block? as u64))
        //    .collect::<Vec<_>>();
        let mut blocks_1_indexed = client.indexed_blocks(&mut conn, BlockTableChoice::Blocks)?;
        let mut blocks_2_indexed = client.indexed_blocks(&mut conn, BlockTableChoice::Blocks2)?;
        blocks_1_indexed.append(&mut blocks_2_indexed);
        blocks_1_indexed.into_iter().map(|block| block as u64).collect()
    };
    {
        let mut conn = db::new_connection(&cfg.remotedb_url)?;
        // merge indexed blocks from remotedb

        let client = db::client::Client {};
        //let mut blocks_1_indexed = client
        //    .indexed_slots(&mut conn, BlockTableChoice::Blocks)
        //    .unwrap_or_default()
        //    .into_iter()
        //    .filter_map(|block| Some(block? as u64))
        //    .collect::<Vec<_>>();
        //let mut blocks_2_indexed = client
        //    .indexed_slots(&mut conn, BlockTableChoice::Blocks2)
        //    .unwrap_or_default()
        //    .into_iter()
        //    .filter_map(|block| Some(block? as u64))
        //    .collect::<Vec<_>>();
        //blocks_1_indexed.append(&mut blocks_2_indexed);

        let mut blocks_1_indexed = client.indexed_blocks(&mut conn, BlockTableChoice::Blocks)?;
        let mut blocks_2_indexed = client.indexed_blocks(&mut conn, BlockTableChoice::Blocks2)?;
        blocks_1_indexed.append(&mut blocks_2_indexed);
        for block in blocks_1_indexed.into_iter() {
            indexed.insert(block as u64);
        }
    }
    let mut missing_blocks = vec![];

    for block in start..=end {
        if !indexed.contains(&block) {
            missing_blocks.push(block);
        }
    }

    log::info!("found {} missing blocks in range(start={start}, end={end})", missing_blocks.len());

    let mut fh = BufWriter::new(File::create(output).await?);
    for missing_block in missing_blocks {
        fh.write_all(format!("{missing_block}\n").as_bytes()).await?;
    }
    fh.flush().await.with_context(|| "failed to flush file")
}

pub async fn get_missing_slots_in_range(
    matches: &ArgMatches,
    config_path: &str
) -> anyhow::Result<()> {
    let start = matches.get_one::<u64>("start").unwrap();
    let end = matches.get_one::<u64>("end").unwrap();
    let cfg = Config::load(config_path).await?;

    let mut missing_slots = HashSet::new();


    // load all currently indexed block number to avoid re-downloading already indexed block data
    let mut indexed_slots: HashSet<u64> = {
        let mut conn = db::new_connection(&cfg.db_url)?;

        // perform db migrations
        run_migrations(&mut conn);

        let client = db::client::Client {};
        let mut blocks_1_indexed = client
            .indexed_slots(&mut conn, BlockTableChoice::Blocks)
            .unwrap_or_default()
            .into_iter()
            .filter_map(|block| Some(block? as u64))
            .collect::<Vec<_>>();
        let mut blocks_2_indexed = client
            .indexed_slots(&mut conn, BlockTableChoice::Blocks2)
            .unwrap_or_default()
            .into_iter()
            .filter_map(|block| Some(block? as u64))
            .collect::<Vec<_>>();
        blocks_1_indexed.append(&mut blocks_2_indexed);
        blocks_1_indexed.into_iter().map(|block| block as u64).collect::<HashSet<_>>()
    };
    {
        let mut conn = db::new_connection(&cfg.remotedb_url)?;
        // merge indexed blocks from remotedb

        let client = db::client::Client {};
        let mut blocks_1_indexed = client
            .indexed_slots(&mut conn, BlockTableChoice::Blocks)
            .unwrap_or_default()
            .into_iter()
            .filter_map(|block| Some(block? as u64))
            .collect::<Vec<_>>();
        let mut blocks_2_indexed = client
            .indexed_slots(&mut conn, BlockTableChoice::Blocks2)
            .unwrap_or_default()
            .into_iter()
            .filter_map(|block| Some(block? as u64))
            .collect::<Vec<_>>();
        blocks_1_indexed.append(&mut blocks_2_indexed);

        for block in blocks_1_indexed.into_iter() {
            indexed_slots.insert(block as u64);
        }
    }
    for slot in *start..=*end {
        if !indexed_slots.contains(&slot) {
            missing_slots.insert(slot);
        }
    }

    let mut fh = File::create("slots_to_fetch.txt").await?;
    for slot in missing_slots {
        fh.write_all(format!("{slot}\n").as_bytes()).await?;
    }
    fh.flush().await.with_context(|| "failed to flush file")
}

pub async fn guess_slot_numbers(
    matches: &ArgMatches,
    config_path: &str
) -> anyhow::Result<()> {
    let input = matches.get_one::<String>("input").unwrap();
    let limit = matches.get_one::<usize>("limit").unwrap();
    let threads = matches.get_one::<usize>("threads").unwrap();

    let cfg = Config::load(config_path).await?;
    let pool = db::new_connection_pool(&cfg.db_url, *threads as u32)?;
    let remote_pool = db::new_connection_pool(&cfg.remotedb_url, *threads as u32)?;

    let client = db::client::Client {};


    let mut db_block_1_indexed: HashSet<u64> = HashSet::default();
    let mut db_block_2_indexed: HashSet<u64> = HashSet::default();

    {    
        let mut conn = db::new_connection(&cfg.db_url)?;

        client.indexed_blocks(&mut conn, BlockTableChoice::Blocks)?.into_iter().for_each(|block| {
            db_block_1_indexed.insert(block as u64);
        });
        client.indexed_blocks(&mut conn, BlockTableChoice::Blocks2)?.into_iter().for_each(|block| {
            db_block_2_indexed.insert(block as u64);
        });
    }
    let mut remote_db_block_1_indexed: HashSet<u64> = HashSet::default();
    let mut remote_db_block_2_indexed: HashSet<u64> = HashSet::default();
    {
        let mut remote_conn = db::new_connection(&cfg.remotedb_url)?;

        client.indexed_blocks(&mut remote_conn, BlockTableChoice::Blocks)?.into_iter().for_each(|block| {
            remote_db_block_1_indexed.insert(block as u64);
        });
        client.indexed_blocks(&mut remote_conn, BlockTableChoice::Blocks2)?.into_iter().for_each(|block| {
            remote_db_block_2_indexed.insert(block as u64);
        });
    }
    let blocks_to_fetch = {
        let mut slots_to_fetch = vec![];
        {
            let file = File::open(input).await?;
            let reader = BufReader::new(file);
            let mut lines = reader.lines();

            // Read the file line by line
            while let Some(line) = lines.next_line().await? {
                // Parse each line as a u64
                match line.trim().parse::<u64>() {
                    Ok(number) => {
                        if db_block_1_indexed.contains(&number) || db_block_2_indexed.contains(&number) || remote_db_block_1_indexed.contains(&number) || remote_db_block_2_indexed.contains(&number) {
                            continue;
                        } else {
                            slots_to_fetch.push(number);
                        }
                    } ,
                    Err(_) => log::warn!("Warning: Skipping invalid line: {}", line),
                }
            }
        }
        slots_to_fetch
    };
    let db_block_1_indexed = Arc::new(db_block_1_indexed);
    let db_block_2_indexed = Arc::new(db_block_2_indexed);
    let remote_db_block_1_indexed = Arc::new(remote_db_block_1_indexed);
    let remote_db_block_2_indexed = Arc::new(remote_db_block_2_indexed);

    let total_blocks = blocks_to_fetch.len().min(*limit);

    let block_to_slot = stream::iter(blocks_to_fetch).enumerate().take(*limit).map(|(idx, block_to_fetch)| {
        if idx as u64 % 500 == 0 {
            log::info!("fetching block {idx}/{total_blocks}");
        }
        let db_block_1_indexed = db_block_1_indexed.clone();
        let db_block_2_indexed = db_block_2_indexed.clone();
        let remote_db_block_1_indexed = remote_db_block_1_indexed.clone();
        let remote_db_block_2_indexed = remote_db_block_2_indexed.clone();
        let pool = pool.clone();
        let remote_pool = remote_pool.clone();
        async move {
            let mut next_block = if db_block_1_indexed.contains(&(block_to_fetch + 1)) {
                match pool.get() {
                    Ok(mut conn) => {
                        client.select_block(
                            &mut conn,
                            BlockFilter::Number((block_to_fetch + 1) as i64),
                            BlockTableChoice::Blocks
                        ).unwrap_or_default()
                    }
                    Err(err) => {
                        log::error!("failed to get connection");
                        return (0, 0)
                    }
                }

            } else if db_block_2_indexed.contains(&(block_to_fetch + 1)) {
                match pool.get() {
                    Ok(mut conn) => {
                        client.select_block(
                            &mut conn,
                            BlockFilter::Number((block_to_fetch + 1) as i64),
                            BlockTableChoice::Blocks2
                        ).unwrap_or_default()
                    }
                    Err(err) => {
                        log::error!("failed to get connection");
                        return (0, 0)
                    }
                }

            } else if remote_db_block_1_indexed.contains(&(block_to_fetch + 1)) {
                match remote_pool.get() {
                    Ok(mut conn) => {
                        client.select_block(
                            &mut conn,
                            BlockFilter::Number((block_to_fetch + 1) as i64),
                            BlockTableChoice::Blocks
                        ).unwrap_or_default()
                    }
                    Err(err) => {
                        log::error!("failed to get connection");
                        return (0, 0)
                    }
                }

            } else if remote_db_block_2_indexed.contains(&(block_to_fetch + 1)) {
                match remote_pool.get() {
                    Ok(mut conn) => {
                        client.select_block(
                            &mut conn,
                            BlockFilter::Number((block_to_fetch + 1) as i64),
                            BlockTableChoice::Blocks2
                        ).unwrap_or_default()
                    }
                    Err(err) => {
                        log::error!("failed to get connection");
                        return (0, 0)
                    }
                }

            } else {
                log::debug!("failed to find block {block_to_fetch}");
                return (0, 0)
            };
            if next_block.is_empty() {
                log::debug!("failed to find block {block_to_fetch}");
                return (0, 0)
            }
            let next_block = std::mem::take(&mut next_block[0]);
            let block: UiConfirmedBlock = serde_json::from_value(next_block.data).unwrap();
            (block_to_fetch as u64, block.parent_slot as u64)
        }
    })
    .buffer_unordered(*threads - 1)
    .collect::<HashMap<_, _>>()
    .await;
    let mut fh = BufWriter::new(File::create("slots_to_fetch.txt").await?);
    for (_, slot) in block_to_slot {
        if slot != 0 {
            fh.write_all(format!("{slot}\n").as_bytes()).await?;
        }

    }
    fh.flush().await.with_context(|| "failed to flush file")
}

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


