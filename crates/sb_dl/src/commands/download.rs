use std::collections::HashSet;

use clap::ArgMatches;
use db::migrations::run_migrations;
use sb_dl::{
    config::{self, Config}, geyser::{new_geyser_client, subscribe_blocks}, Downloader
};
use serde_json::Value;
use solana_transaction_status::UiConfirmedBlock;
use tokio::signal::unix::{signal, SignalKind};

pub async fn start(matches: &ArgMatches, config_path: &str) -> anyhow::Result<()> {
    let cfg = Config::load(config_path).await?;
    let start = matches.get_one::<u64>("start").cloned();
    let limit = matches.get_one::<u64>("limit").cloned();
    let no_minimization = matches.get_flag("no-minimization");
    let failed_blocks_dir = matches.get_one::<String>("failed-blocks").unwrap().clone();

    // create failed blocks directory, ignoring error (its already created)
    let _ = tokio::fs::create_dir(&failed_blocks_dir).await;

    // read all failed blocks to append to the already_indexed hash set
    //
    // we do this so we can avoid re-downloading the blocks which are stored locally
    let failed_blocks = get_failed_blocks(&failed_blocks_dir).await.unwrap();

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

    // mark failed blocks as already indexed to avoid redownloading
    already_indexed.extend(failed_blocks.iter());

    let downloader = Downloader::new(cfg.bigtable).await?;

    // receives downloaded blocks, which allows us to persist downloaded data while we download and parse other data
    let (blocks_tx, mut blocks_rx) =
        tokio::sync::mpsc::channel::<(u64, UiConfirmedBlock)>(limit.unwrap_or(1_000) as usize);

    let mut sig_quit = signal(SignalKind::quit())?;
    let mut sig_int = signal(SignalKind::interrupt())?;
    let mut sig_term = signal(SignalKind::terminate())?;

    // if we fail to connect to postgres, we should terminate the thread
    let mut conn = db::new_connection(&cfg.db_url)?;

    // start the background persistence task
    tokio::task::spawn(async move {
        let client = db::client::Client {};

        while let Some((slot, block)) = blocks_rx.recv().await {
            match serde_json::to_value(block) {
                Ok(mut block) => {
                    if client
                        .insert_block(&mut conn, slot as i64, block.clone())
                        .is_err()
                    {
                        // block failed to be inserted into postgres
                        // so sanitize json and persist the block on disk
                        sanitize_value(&mut block);
                        match serde_json::to_string(&block) {
                            Ok(block_str) => {
                                if let Err(err) = tokio::fs::write(
                                    format!("{failed_blocks_dir}/block_{slot}.json"),
                                    block_str,
                                )
                                .await
                                {
                                    log::error!("failed to store failed block({slot}) {err:#?}");
                                } else {
                                    log::warn!("block({slot}) failed to persist, saved to {failed_blocks_dir}/block_{slot}.json");
                                }
                            }
                            Err(err) => {
                                log::error!("failed to json serialize block({slot}) {err:#?}");
                            }
                        }
                    } else {
                        log::info!("persisted block({slot})");
                    }
                }
                Err(err) => {
                    log::error!("failed to serialize block({slot}) {err:#?}");
                }
            }
        }
    });

    let (finished_tx, finished_rx) = tokio::sync::oneshot::channel();

    tokio::task::spawn(async move {
        log::info!("starting block_indexing. disable_minimization={no_minimization}");

        if let Err(err) = downloader
            .start(blocks_tx, already_indexed, start, limit, no_minimization)
            .await
        {
            log::error!("downloader failed {err:#?}");
        }

        log::info!("finished downloading blocks");
        let _ = finished_tx.send(());
    });

    // handle exit routines
    tokio::select! {
        _ = sig_quit.recv() => {
            log::warn!("goodbye..");
            return Ok(());
        }
        _ = sig_int.recv() => {
            log::warn!("goodbye..");
            return Ok(());
        }
        _ = sig_term.recv() => {
            log::warn!("goodbye..");
            return Ok(());
        }
        _ = finished_rx => {
            return Ok(());
        }
    }
}

pub async fn stream_geyser_blocks(matches: &ArgMatches, config_path: &str) -> anyhow::Result<()> {
    let cfg = Config::load(config_path).await?;
    let failed_blocks_dir = matches.get_one::<String>("failed-blocks").unwrap().clone();

    // create failed blocks directory, ignoring error (its already created)
    let _ = tokio::fs::create_dir(&failed_blocks_dir).await;

    let no_minimization = matches.get_flag("no-minimization");

    {
        let mut conn = db::new_connection(&cfg.db_url)?;

        // perform db migrations
        run_migrations(&mut conn);
    }
    
    let gc = new_geyser_client(
        &cfg.geyser.endpoint,
        &cfg.geyser.token,
        cfg.geyser.max_decoding_size,
        cfg.geyser.max_encoding_size
    ).await?;

    // receives downloaded blocks, which allows us to persist downloaded data while we download and parse other data
    let (blocks_tx, mut blocks_rx) =
        tokio::sync::mpsc::channel::<(u64, UiConfirmedBlock)>(1000);

    let mut sig_quit = signal(SignalKind::quit())?;
    let mut sig_int = signal(SignalKind::interrupt())?;
    let mut sig_term = signal(SignalKind::terminate())?;

    // if we fail to connect to postgres, we should terminate the thread
    let mut conn = db::new_connection(&cfg.db_url)?;

    // start the background persistence task
    tokio::task::spawn(async move {
        let client = db::client::Client {};

        while let Some((slot, block)) = blocks_rx.recv().await {
            match serde_json::to_value(block) {
                Ok(mut block) => {
                    if client
                        .insert_block(&mut conn, slot as i64, block.clone())
                        .is_err()
                    {
                        // block failed to be inserted into postgres
                        // so sanitize json and persist the block on disk
                        sanitize_value(&mut block);
                        match serde_json::to_string(&block) {
                            Ok(block_str) => {
                                if let Err(err) = tokio::fs::write(
                                    format!("{failed_blocks_dir}/block_{slot}.json"),
                                    block_str,
                                )
                                .await
                                {
                                    log::error!("failed to store failed block({slot}) {err:#?}");
                                } else {
                                    log::warn!("block({slot}) failed to persist, saved to {failed_blocks_dir}/block_{slot}.json");
                                }
                            }
                            Err(err) => {
                                log::error!("failed to json serialize block({slot}) {err:#?}");
                            }
                        }
                    } else {
                        log::info!("persisted block({slot})");
                    }
                }
                Err(err) => {
                    log::error!("failed to serialize block({slot}) {err:#?}");
                }
            }
        }
    });

    let (finished_tx, finished_rx) = tokio::sync::oneshot::channel();

    tokio::task::spawn(async move {
        log::info!("starting geyser stream. disable_minimization={no_minimization}");
        if let Err(err) = subscribe_blocks(
            gc,
            blocks_tx,
            no_minimization
        ).await {
            log::error!("geyser stream failed {err:#?}");
        }
        log::info!("geyser stream finished");
        let _ = finished_tx.send(());
    });

    // handle exit routines
    tokio::select! {
        _ = sig_quit.recv() => {
            log::warn!("goodbye..");
            return Ok(());
        }
        _ = sig_int.recv() => {
            log::warn!("goodbye..");
            return Ok(());
        }
        _ = sig_term.recv() => {
            log::warn!("goodbye..");
            return Ok(());
        }
        _ = finished_rx => {
            return Ok(());
        }
    }
}


pub async fn import_failed_blocks(matches: &ArgMatches, config_path: &str) -> anyhow::Result<()> {
    let cfg = Config::load(config_path).await?;
    let failed_blocks_dir = matches.get_one::<String>("failed-blocks").unwrap().clone();
    let failed_blocks = load_failed_blocks(&failed_blocks_dir).await?;

    // if we fail to connect to postgres, we should terminate the thread
    let mut conn = db::new_connection(&cfg.db_url)?;

    let client = db::client::Client {};

    for (slot, block) in failed_blocks {
        if let Err(err) = client.insert_block(&mut conn, slot as i64, block) {
            log::error!("failed to insert block({slot}) {err:#?}");
        }
    }

    Ok(())
}

// sanitizes utf8 encoding issues which prevent converting serde_json::Value to a string
fn sanitize_value(value: &mut Value) {
    match value {
        Value::String(s) => {
            // Check if the string contains valid UTF-8
            if let Err(_) = std::str::from_utf8(s.as_bytes()) {
                // Replace invalid UTF-8 with a placeholder
                *s = String::from_utf8_lossy(s.as_bytes()).into_owned();
            }
        }
        Value::Array(arr) => {
            for v in arr {
                sanitize_value(v);
            }
        }
        Value::Object(map) => {
            for (_, v) in map.iter_mut() {
                sanitize_value(v);
            }
        }
        _ => {}
    }
}

// reads all files from the failed_blocks directory, and retrieves the block numbers
async fn get_failed_blocks(dir: &str) -> anyhow::Result<HashSet<u64>> {
    use regex::Regex;
    use std::collections::HashSet;
    use std::path::Path;
    let dir_path = Path::new(dir);
    let re = Regex::new(r"block_(\d+)\.json").unwrap();
    let mut hash_set = HashSet::new();

    let entries = tokio::fs::read_dir(dir_path).await?;
    tokio::pin!(entries);

    while let Some(entry) = entries.next_entry().await? {
        if let Some(file_name) = entry.file_name().to_str() {
            if let Some(captures) = re.captures(file_name) {
                if let Some(matched) = captures.get(1) {
                    if let Ok(number) = matched.as_str().parse::<u64>() {
                        hash_set.insert(number);
                    }
                }
            }
        }
    }
    Ok(hash_set)
}

async fn load_failed_blocks(dir: &str) -> anyhow::Result<Vec<(u64, serde_json::Value)>> {
    use regex::Regex;

    let mut blocks = vec![];

    let re = Regex::new(r"block_(\d+)\.json").unwrap();
    let entries = tokio::fs::read_dir(dir).await?;
    tokio::pin!(entries);

    while let Some(entry) = entries.next_entry().await? {
        if let Some(file_name) = entry.file_name().to_str() {
            if let Some(captures) = re.captures(file_name) {
                if let Some(matched) = captures.get(1) {
                    if let Ok(slot) = matched.as_str().parse::<u64>() {
                        let block = tokio::fs::read_to_string(entry.path()).await?;
                        let block: serde_json::Value = serde_json::from_str(&block)?;
                        blocks.push((slot, block));
                    }
                }
            }
        }
    }
    Ok(blocks)
}
