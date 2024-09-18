use {
    super::{super::utils::get_failed_blocks, downloaders::block_persistence_loop},
    crate::commands::handle_exit,
    clap::ArgMatches,
    db::{migrations::run_migrations, models::BlockTableChoice},
    sb_dl::{config::Config, services::bigtable::Downloader, types::BlockInfo},
    std::{collections::HashSet, sync::Arc},
    tokio::{
        fs::File,
        io::{AsyncBufReadExt, BufReader},
        signal::unix::{signal, SignalKind},
    },
};
/// Starts the big table historical block downloader
pub async fn bigtable_downloader(matches: &ArgMatches, config_path: &str) -> anyhow::Result<()> {
    let blocks_table =
        BlockTableChoice::try_from(*matches.get_one::<u8>("block-table-choice").unwrap()).unwrap();

    let cfg = Config::load(config_path).await?;
    let start = matches.get_one::<u64>("start").cloned();
    let limit = matches.get_one::<u64>("limit").cloned();
    let no_minimization = matches.get_flag("no-minimization");
    let failed_blocks_dir = matches.get_one::<String>("failed-blocks").unwrap().clone();
    let threads = *matches.get_one::<usize>("threads").unwrap();

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
        blocks_1_indexed.into_iter().collect()
    };
    {
        let mut conn = db::new_connection(&cfg.remotedb_url)?;

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
        for block in blocks_1_indexed {
            already_indexed.insert(block);
        }
        for block in blocks_2_indexed {
            already_indexed.insert(block);
        }
    }

    // mark failed blocks as already indexed to avoid redownloading
    already_indexed.extend(failed_blocks.iter());

    let downloader = Arc::new(Downloader::new(cfg.bigtable).await?);

    // receives downloaded blocks, which allows us to persist downloaded data while we download and parse other data
    let (blocks_tx, blocks_rx) = tokio::sync::mpsc::channel::<BlockInfo>(10_000 as usize);

    let sig_quit = signal(SignalKind::quit())?;
    let sig_int = signal(SignalKind::interrupt())?;
    let sig_term = signal(SignalKind::terminate())?;

    let pool = db::new_connection_pool(&cfg.db_url, threads as u32 * 2)?;

    // start the background persistence task
    tokio::task::spawn(async move {
        block_persistence_loop(pool, failed_blocks_dir, blocks_rx, threads, blocks_table).await
    });

    let (finished_tx, finished_rx) = tokio::sync::oneshot::channel();
    let (stop_downloader_tx, stop_downloader_rx) = tokio::sync::oneshot::channel();
    tokio::task::spawn(async move {
        log::info!("starting block_indexing. disable_minimization={no_minimization}");

        if let Err(err) = downloader
            .start(
                blocks_tx,
                already_indexed,
                start,
                limit,
                no_minimization,
                threads,
                stop_downloader_rx,
            )
            .await
        {
            let _ = finished_tx.send(Some(format!("downloader failed {err:#?}")));
        } else {
            log::info!("finished downloading blocks");
            let _ = finished_tx.send(None);
        }
    });

    let err = handle_exit(sig_quit, sig_int, sig_term, finished_rx).await;
    // stop the downloader task
    let _ = stop_downloader_tx.send(());
    return err;
}

pub async fn manual_bigtable_downloader(
    matches: &ArgMatches,
    config_path: &str,
) -> anyhow::Result<()> {
    let blocks_table =
        BlockTableChoice::try_from(*matches.get_one::<u8>("block-table-choice").unwrap()).unwrap();

    let cfg = Config::load(config_path).await?;
    let no_minimization = matches.get_flag("no-minimization");
    let failed_blocks_dir = matches.get_one::<String>("failed-blocks").unwrap().clone();
    let threads = *matches.get_one::<usize>("threads").unwrap();
    let input = matches.get_one::<String>("input").unwrap();
    let full_range = matches.get_flag("full-range");
    // load all currently indexed block number to avoid re-downloading already indexed block data
    let mut already_indexed: HashSet<u64> = {
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
        blocks_1_indexed.into_iter().collect()
    };

    {
        let mut conn = db::new_connection(&cfg.remotedb_url)?;

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
        for block in blocks_1_indexed {
            already_indexed.insert(block);
        }
        for block in blocks_2_indexed {
            already_indexed.insert(block);
        }
    }

    let slots_to_fetch = {
        let mut slots_to_fetch = vec![];
        {
            let file = File::open(input).await?;
            let reader = BufReader::new(file);
            let mut lines = reader.lines();

            // Read the file line by line
            while let Some(line) = lines.next_line().await? {
                // Parse each line as a u64
                match line.trim().parse::<u64>() {
                    Ok(number) => slots_to_fetch.push(number),
                    Err(_) => log::warn!("Warning: Skipping invalid line: {}", line),
                }
            }
        }
        if full_range {
            let start = slots_to_fetch[0];
            let end = slots_to_fetch[slots_to_fetch.len()-1];
            (start..=end).collect::<Vec<_>>()
        } else {
            slots_to_fetch
        }

    };

    // receives downloaded blocks, which allows us to persist downloaded data while we download and parse other data
    let (blocks_tx, blocks_rx) = tokio::sync::mpsc::channel::<BlockInfo>(10_000 as usize);

    let sig_quit = signal(SignalKind::quit())?;
    let sig_int = signal(SignalKind::interrupt())?;
    let sig_term = signal(SignalKind::terminate())?;

    let downloader = Arc::new(Downloader::new(cfg.bigtable).await?);

    let pool = db::new_connection_pool(&cfg.db_url, threads as u32 * 2)?;

    // start the background persistence task
    tokio::task::spawn(async move {
        block_persistence_loop(pool, failed_blocks_dir, blocks_rx, threads, blocks_table).await
    });
    let (stop_downloader_tx, stop_downloader_rx) = tokio::sync::oneshot::channel();
    let (finished_tx, finished_rx) = tokio::sync::oneshot::channel();

    tokio::task::spawn(async move {
        log::info!("fetching blocks");
        if let Err(err) = downloader
            .fetch_blocks(
                blocks_tx,
                already_indexed,
                no_minimization,
                threads,
                slots_to_fetch,
                stop_downloader_rx,
            )
            .await
        {
            log::error!("failed to fetch blocks {err:#?}");
            let _ = finished_tx.send(Some(format!("downloader failed {err:#?}")));
        } else {
            log::info!("downloader finished");
            let _ = finished_tx.send(None);
        }
    });

    let err = handle_exit(sig_quit, sig_int, sig_term, finished_rx).await;
    // stop the downloader task
    let _ = stop_downloader_tx.send(());
    return err;
}
