use {
    super::utils::{get_failed_blocks, load_failed_blocks, sanitize_for_postgres, sanitize_value},
    anyhow::anyhow,
    clap::ArgMatches,
    db::migrations::run_migrations,
    diesel::PgConnection,
    sb_dl::{
        backfill::Backfiller, bigtable::Downloader, config::Config, geyser::{new_geyser_client, subscribe_blocks}
    },
    solana_transaction_status::UiConfirmedBlock,
    std::collections::HashSet,
    tokio::signal::unix::{signal, SignalKind},
};

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
    let (blocks_tx, blocks_rx) =
        tokio::sync::mpsc::channel::<(u64, UiConfirmedBlock)>(limit.unwrap_or(1_000) as usize);

    let mut sig_quit = signal(SignalKind::quit())?;
    let mut sig_int = signal(SignalKind::interrupt())?;
    let mut sig_term = signal(SignalKind::terminate())?;

    // if we fail to connect to postgres, we should terminate the thread
    let conn = db::new_connection(&cfg.db_url)?;

    // start the background persistence task
    tokio::task::spawn(
        async move { block_persistence_loop(conn, failed_blocks_dir, blocks_rx).await },
    );

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
        cfg.geyser.max_encoding_size,
    )
    .await?;

    // receives downloaded blocks, which allows us to persist downloaded data while we download and parse other data
    let (blocks_tx, blocks_rx) = tokio::sync::mpsc::channel::<(u64, UiConfirmedBlock)>(1000);

    let mut sig_quit = signal(SignalKind::quit())?;
    let mut sig_int = signal(SignalKind::interrupt())?;
    let mut sig_term = signal(SignalKind::terminate())?;

    // if we fail to connect to postgres, we should terminate the thread
    let conn = db::new_connection(&cfg.db_url)?;

    // start the background persistence task
    tokio::task::spawn(
        async move { block_persistence_loop(conn, failed_blocks_dir, blocks_rx).await },
    );

    // optional value containing error message encountered during program execution
    let (finished_tx, finished_rx) = tokio::sync::oneshot::channel::<Option<String>>();

    tokio::task::spawn(async move {
        log::info!("starting geyser stream. disable_minimization={no_minimization}");
        if let Err(err) = subscribe_blocks(gc, blocks_tx, no_minimization).await {
            let _ = finished_tx.send(Some(format!("geyser stream failed {err:#?}")));
        } else {
            log::info!("geyser stream finished");
            let _ = finished_tx.send(None);
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
        msg = finished_rx => {
            match msg {
                // geyser stream encountered error
                Ok(Some(msg)) => return Err(anyhow!(msg)),
                // geyser stream finished without error
                Ok(None) => return Ok(()),
                // underlying channel had an error
                Err(err) => return Err(anyhow!(err))
            }
        }
    }
}

pub async fn recent_backfill(matches: &ArgMatches, config_path: &str) -> anyhow::Result<()> {
    let cfg = Config::load(config_path).await?;
    let failed_blocks_dir = matches.get_one::<String>("failed-blocks").unwrap().clone();

    // create failed blocks directory, ignoring error (its already created)
    let _ = tokio::fs::create_dir(&failed_blocks_dir).await;

    let no_minimization = matches.get_flag("no-minimization");

    // receives downloaded blocks, which allows us to persist downloaded data while we download and parse other data
    let (blocks_tx, blocks_rx) = tokio::sync::mpsc::channel::<(u64, UiConfirmedBlock)>(1000);

    let mut sig_quit = signal(SignalKind::quit())?;
    let mut sig_int = signal(SignalKind::interrupt())?;
    let mut sig_term = signal(SignalKind::terminate())?;

    // if we fail to connect to postgres, we should terminate the thread
    let conn = db::new_connection(&cfg.db_url)?;

    // start the background persistence task
    tokio::task::spawn(
        async move { block_persistence_loop(conn, failed_blocks_dir, blocks_rx).await },
    );

    let backfiller = Backfiller::new(&cfg.rpc_url);

    let (finished_tx, finished_rx) = tokio::sync::oneshot::channel();

    tokio::task::spawn(async move {
        log::info!("starting backfiller. disable_minimization={no_minimization}");
        if let Err(err) = backfiller.start(
            blocks_tx,
            no_minimization
        ).await {
            log::error!("backfiller failed {err:#?}");
        }
        log::info!("backfiller finished");
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

    let (blocks_tx, mut blocks_rx) = tokio::sync::mpsc::channel::<(u64, serde_json::Value)>(1000);

    // if we fail to connect to postgres, we should terminate the thread
    let mut conn = db::new_connection(&cfg.db_url)?;

    let (finished_tx, finished_rx) = tokio::sync::oneshot::channel();
    {
        let failed_blocks_dir = failed_blocks_dir.clone();
        tokio::task::spawn(async move {
            let client = db::client::Client {};
    
            while let Some((slot, mut block)) = blocks_rx.recv().await {
                sanitize_for_postgres(&mut block);
                if let Err(err) = client.insert_block(&mut conn, slot as i64, block) {
                    log::error!("failed to insert block({slot}) {err:#?}");
                } else {
                    log::info!("inserted block({slot})");
                    if let Err(err) = tokio::fs::remove_file(format!("{failed_blocks_dir}/block_{slot}.json")).await {
                        log::error!("failed to remove persisted block({slot}) {err:#?}");
                    }
                }
            }
    
            let _ = finished_tx.send(());
        });
    }


    load_failed_blocks(&failed_blocks_dir, blocks_tx).await?;

    let _ = finished_rx.await;

    Ok(())
}

async fn block_persistence_loop(
    mut conn: PgConnection,
    failed_blocks_dir: String,
    mut blocks_rx: tokio::sync::mpsc::Receiver<(u64, UiConfirmedBlock)>,
) {
    let client = db::client::Client {};

    while let Some((slot, block)) = blocks_rx.recv().await {
        match serde_json::to_value(block) {
            Ok(mut block) => {
                if let Err(err) = client.insert_block(&mut conn, slot as i64, block.clone()) {
                    log::error!("block({slot}) persistence failed {err:#?}");
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
}
