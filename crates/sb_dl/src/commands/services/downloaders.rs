use {
    super::super::utils::{
        get_failed_blocks, load_failed_blocks, sanitize_for_postgres, sanitize_value,
    }, crate::commands::handle_exit, anyhow::{anyhow, Context}, clap::ArgMatches, db::{migrations::run_migrations, models::{BlockTableChoice, NewBlock, NewBlock2}}, diesel::{
        prelude::*,
        r2d2::{ConnectionManager, Pool, PooledConnection},
        PgConnection,
    }, sb_dl::{
        config::Config,
        services::{
            backfill::Backfiller,
            bigtable::Downloader,
            geyser::{new_geyser_client, subscribe_blocks},
        },
        types::BlockInfo,
    }, solana_client::{nonblocking::rpc_client::RpcClient, rpc_config::RpcTransactionConfig}, solana_transaction_status::{UiConfirmedBlock, UiTransactionEncoding}, std::{collections::HashSet, sync::Arc}, tokio::{
        signal::unix::{signal, Signal, SignalKind},
        sync::Semaphore,
    }
};
pub async fn get_transaction(
    matches: &clap::ArgMatches,
    config_path: &str
) -> anyhow::Result<()> {
    let tx_hash = matches.get_one::<String>("tx-hash").unwrap();
    let cfg = Config::load(config_path).await?;
    let rpc = RpcClient::new(cfg.rpc_url.clone());
    let tx = rpc.get_transaction_with_config(
        &tx_hash.parse().unwrap(),
        RpcTransactionConfig {
            encoding: Some(UiTransactionEncoding::JsonParsed),
            max_supported_transaction_version: Some(1),
            ..Default::default()
        }
    ).await?;
    log::info!("tx(hash={}, slot={})", tx_hash, tx.slot);
    Ok(())
}
/// Starts the geyser stream block downloader
pub async fn geyser_stream(matches: &ArgMatches, config_path: &str) -> anyhow::Result<()> {
    let blocks_table = BlockTableChoice::try_from(*matches.get_one::<u8>("block-table-choice").unwrap()).unwrap();

    let cfg = Config::load(config_path).await?;
    let failed_blocks_dir = matches.get_one::<String>("failed-blocks").unwrap().clone();
    let threads = *matches.get_one::<usize>("threads").unwrap();
    
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
    let (blocks_tx, blocks_rx) = tokio::sync::mpsc::channel::<BlockInfo>(1000);

    let sig_quit = signal(SignalKind::quit())?;
    let sig_int = signal(SignalKind::interrupt())?;
    let sig_term = signal(SignalKind::terminate())?;

    let pool = db::new_connection_pool(&cfg.db_url, threads as u32 *2)?;

    // start the background persistence task
    tokio::task::spawn(
        async move { block_persistence_loop(pool, failed_blocks_dir, blocks_rx, threads, blocks_table).await },
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

    handle_exit(sig_quit, sig_int, sig_term, finished_rx).await
}

pub async fn backfiller(matches: &ArgMatches, config_path: &str) -> anyhow::Result<()> {
    let blocks_table = BlockTableChoice::try_from(*matches.get_one::<u8>("block-table-choice").unwrap()).unwrap();

    let cfg = Config::load(config_path).await?;
    let failed_blocks_dir = matches.get_one::<String>("failed-blocks").unwrap().clone();
    let threads = *matches.get_one::<usize>("threads").unwrap();

    // create failed blocks directory, ignoring error (its already created)
    let _ = tokio::fs::create_dir(&failed_blocks_dir).await;

    let no_minimization = matches.get_flag("no-minimization");

    // receives downloaded blocks, which allows us to persist downloaded data while we download and parse other data
    let (blocks_tx, blocks_rx) = tokio::sync::mpsc::channel::<BlockInfo>(1000);

    let sig_quit = signal(SignalKind::quit())?;
    let sig_int = signal(SignalKind::interrupt())?;
    let sig_term = signal(SignalKind::terminate())?;

    {
        // if we fail to connect to postgres, we should terminate the thread
        let mut conn = db::new_connection(&cfg.db_url)?;

        run_migrations(&mut conn);
    }
    let pool = db::new_connection_pool(&cfg.db_url, threads as u32 *2)?;

    // start the background persistence task
    tokio::task::spawn(
        async move { block_persistence_loop(pool, failed_blocks_dir, blocks_rx, threads, blocks_table).await },
    );

    let backfiller = Backfiller::new(&cfg.rpc_url);

    let (finished_tx, finished_rx) = tokio::sync::oneshot::channel();

    tokio::task::spawn(async move {
        log::info!("starting backfiller. disable_minimization={no_minimization}");
        if let Err(err) = backfiller
            .automatic_backfill(blocks_tx, no_minimization)
            .await
        {
            let _ = finished_tx.send(Some(format!("backfiller failed {err:#?}")));
        } else {
            log::info!("backfiller finished");
            let _ = finished_tx.send(None);
        }
    });

    handle_exit(sig_quit, sig_int, sig_term, finished_rx).await
}

pub async fn import_failed_blocks(matches: &ArgMatches, config_path: &str) -> anyhow::Result<()> {
    let blocks_table = BlockTableChoice::try_from(*matches.get_one::<u8>("block-table-choice").unwrap()).unwrap();

    let cfg = Config::load(config_path).await?;
    let failed_blocks_dir = matches.get_one::<String>("failed-blocks").unwrap().clone();

    let (blocks_tx, mut blocks_rx) = tokio::sync::mpsc::channel::<(u64, serde_json::Value)>(1000);

    // if we fail to connect to postgres, we should terminate the thread
    let mut conn = db::new_connection(&cfg.db_url)?;

    run_migrations(&mut conn);

    let (finished_tx, finished_rx) = tokio::sync::oneshot::channel();
    {
        let failed_blocks_dir = failed_blocks_dir.clone();
        tokio::task::spawn(async move {
            let client = db::client::Client {};
            while let Some((slot_number, mut block)) = blocks_rx.recv().await {
                sanitize_for_postgres(&mut block);

                // this is a bit clunky, however in order to get the correct slot number
                // we first need to deserialize the block into a UiConfirmedBlock object
                // calculate the slot number, then serialize the block again

                let block: UiConfirmedBlock = match serde_json::from_value(block) {
                    Ok(block) => block,
                    Err(err) => {
                        log::error!("failed to deserialize block({slot_number}) {err:#?}");
                        continue;
                    }
                };
                let block_height = if let Some(height) = block.block_height {
                    height
                } else {
                    log::warn!("missing height for block(slot={slot_number})");
                    continue;
                };
                let block = match serde_json::to_value(block) {
                    Ok(block) => block,
                    Err(err) => {
                        log::error!("failed to serialize block{slot_number}) {err:#?}");
                        continue;
                    }
                };
                let err = match blocks_table {
                    BlockTableChoice::Blocks => client.insert_block(
                        &mut conn,NewBlock {
                            number: block_height as i64,
                            slot: Some(slot_number as i64),
                            data: block,
                        }
                    ),
                    BlockTableChoice::Blocks2 => client.insert_block(
                        &mut conn,NewBlock2 {
                            number: block_height as i64,
                            slot: Some(slot_number as i64),
                            data: block,
                        }
                    )
                };
                if let Err(err) = err {
                    log::error!("failed to insert block({slot_number}) {err:#?}");
                } else {
                    log::info!("inserted block({slot_number})");
                    if let Err(err) = tokio::fs::remove_file(format!(
                        "{failed_blocks_dir}/block_{slot_number}.json"
                    ))
                    .await
                    {
                        log::error!("failed to remove persisted block({slot_number}) {err:#?}");
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

// shared logic responsible for persisting blocks to the database
pub async fn block_persistence_loop(
    pool: Pool<ConnectionManager<PgConnection>>,
    failed_blocks_dir: String,
    mut blocks_rx: tokio::sync::mpsc::Receiver<BlockInfo>,
    threads: usize,
    block_table_choice: BlockTableChoice
) {
    let semaphore = Arc::new(Semaphore::new(threads));

    let client = db::client::Client {};

    while let Some(block_info) = blocks_rx.recv().await {
        match semaphore.clone().acquire_owned().await {
            Ok(permit) => {
                match pool.get() {
                    Ok(mut conn) => {
                        let failed_blocks_dir = failed_blocks_dir.clone();
                        tokio::task::spawn(async move {
                            process_block(block_info, &mut conn, failed_blocks_dir, client, block_table_choice).await;
                            drop(permit);
                        });
                    }
                    Err(err) => {
                        log::error!("failed to get pool connection {err:#?}");
                    }
                }
            }
            Err(err) => {
                log::error!("failed to acquire permit {err:#?}");
                return;
            }
        }
    }
}

async fn process_block(block_info: BlockInfo, conn: &mut PgConnection, failed_blocks_dir: String, client: db::client::Client, block_table_choice: BlockTableChoice) {
    // we cant rely on parentSlot + 1, as some slots may be skipped
    let slot = if let Some(slot) = block_info.slot {
        slot
    } else {
        log::warn!("slot is None for block(height={})", block_info.block_height);
        return;
    };

    // uncomment to display logs which can be used to verify the above statement
    //let sample_tx = block.transactions.clone().and_then(|vec| vec.into_iter().next());
    //let sample_tx_hash = if let Some(tx) = sample_tx {
    //    if let EncodedTransaction::Json(tx)  = &tx.transaction {
    //        tx.signatures.clone()
    //    } else {
    //        vec![]
    //    }
    //} else {
    //    vec![]
    //};
    //log::info!(
    //    "block(slot={slot}, height={block_number}, parent_slot={}, block_hash={}, sample_tx_hash={:?})",
    //    block.parent_slot, block.blockhash, sample_tx_hash
    //);
    match serde_json::to_value(block_info.block) {
        Ok(mut block) => {
            // sanitize the values first
            // escape invalid unicode points
            sanitize_value(&mut block);
            // replace escaped unicode points with empty string
            sanitize_for_postgres(&mut block);
            let err: Result<(), anyhow::Error> = match block_table_choice {
                BlockTableChoice::Blocks => client.insert_block(
                    conn,NewBlock {
                        number: block_info.block_height as i64,
                        slot: Some(slot as i64),
                        data: block.clone(),
                    }
                ),
                BlockTableChoice::Blocks2 => client.insert_block(
                    conn,NewBlock2 {
                        number: block_info.block_height as i64,
                        slot: Some(slot as i64),
                        data: block.clone(),
                    }
                )
            };
            if let Err(err) = err {
                // block persistence failed despite sanitization persist the data locally
                log::warn!("block({slot}) persistence failed {err:#?}");
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
                        log::error!("failed to serialize block({slot}) {err:#?}");
                    }
                }
            } else {
                log::info!("persisted block({slot})");
                drop(block);
            }
        }
        Err(err) => {
            log::error!("failed to serialize block({slot}) {err:#?}");
        }
    }
}
