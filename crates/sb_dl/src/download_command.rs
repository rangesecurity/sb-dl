use std::collections::HashSet;

use clap::ArgMatches;
use sb_dl::{config::Config, types::SerializableTransactionWithStatusMeta, Downloader};

pub async fn start(matches: &ArgMatches, config_path: &str) -> anyhow::Result<()> {
    let cfg = Config::load(config_path).await?;
    let start = matches.get_one::<u64>("start").cloned();
    let limit = matches.get_one::<u64>("limit").cloned();
    let downloader = Downloader::new(cfg.bigtable).await?;

    let mut already_indexed: HashSet<u64> = {
        let mut conn = db::new_connection(&cfg.db_url)?;
        let client = db::client::Client{};
        client.indexed_blocks(&mut conn).unwrap_or_default().into_iter().map(|block| block as u64).collect()
    };

    let blocks = downloader.start(&mut already_indexed, start, limit).await?;

    println!("indexed {} blocks", blocks.len());
    for (slot, block) in blocks {
        println!("==== slot {slot} ====");
        println!("txs {}", block.transactions.len());
        for tx in &block.transactions {
            match tx {
                SerializableTransactionWithStatusMeta::MissingMetadata(tx) => {
                    println!("missing_metadata {tx:#?}");
                }
                SerializableTransactionWithStatusMeta::Complete(tx) => {
                    println!("complete {tx:#?}");
                    
                }
            }
        }
    }

    // todo: store in postgres

    Ok(())
}