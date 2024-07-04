use std::collections::HashSet;

use clap::ArgMatches;
use db::migrations::run_migrations;
use sb_dl::{config::Config, Downloader};
use solana_transaction_status::UiConfirmedBlock;
use tokio::signal::unix::{signal, SignalKind};

pub async fn start(matches: &ArgMatches, config_path: &str) -> anyhow::Result<()> {
    let cfg = Config::load(config_path).await?;
    let start = matches.get_one::<u64>("start").cloned();
    let limit = matches.get_one::<u64>("limit").cloned();
    let no_minimization = matches.get_flag("no-minimization");
    let downloader = Downloader::new(cfg.bigtable).await?;

    // load all currently indexed block number to avoid re-downloading already indexed block data
    let already_indexed: HashSet<u64> = {
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
                Ok(block) => {
                    if let Err(err) = client.insert_block(&mut conn, slot as i64, block) {
                        log::error!("failed to persist block({slot}) {err:#?}");
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
    tokio::task::spawn(async move {
        log::info!("starting block_indexing. disable_minimization={no_minimization}");

        if let Err(err) = downloader
            .start(blocks_tx, already_indexed, start, limit, no_minimization)
            .await
        {
            log::error!("downloader failed {err:#?}");
        }
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
    }
}
