use clap::ArgMatches;
use db::migrations::run_migrations;
use sb_dl::config::Config;
use solana_client::{nonblocking::rpc_client::RpcClient, rpc_config::RpcTransactionConfig};
use solana_transaction_status::{EncodedTransaction, UiConfirmedBlock, UiTransactionEncoding};

pub async fn fill_missing_slots(matches: &ArgMatches, config_path: &str) -> anyhow::Result<()> {
    let limit = matches.get_one::<i64>("limit").unwrap();
    let cfg = Config::load(config_path).await?;
    let rpc = RpcClient::new(cfg.rpc_url.clone());
    let mut conn = db::new_connection(&cfg.db_url)?;

    // perform db migrations
    run_migrations(&mut conn);

    let client = db::client::Client {};

    let mut blocks = client.partial_blocks(&mut conn, *limit)?;
    for block in blocks.iter_mut() {
        let block_data: UiConfirmedBlock = serde_json::from_value(block.data.clone())?;
        let sample_tx = block_data
            .transactions
            .clone()
            .and_then(|vec| vec.into_iter().next());
        let sample_tx_hash = if let Some(tx) = sample_tx {
            if let EncodedTransaction::Json(tx) = &tx.transaction {
                tx.signatures.clone()
            } else {
                vec![]
            }
        } else {
            vec![]
        };
        // extract slot information
        let slot = if !sample_tx_hash.is_empty() {
            match rpc.get_transaction_with_config(&sample_tx_hash[0].parse()?, RpcTransactionConfig {
                encoding: Some(UiTransactionEncoding::JsonParsed),
                max_supported_transaction_version: Some(1),
                ..Default::default()
            }).await {
                Ok(tx) => {
                    tx.slot
                }
                Err(err) => {
                    log::error!("failed to get tx({}) {err:#?}", sample_tx_hash[0]);
                    continue;
                }
            }
        } else {
            log::warn!("sample tx hash has no signature");
            continue;
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
            "block(slot={slot}, new_block_number={new_block_number} block.height={:?}, block.number={}, parent_slot={}, block_hash={}, sample_tx_hash={:?})",
            block_data.block_height,
            block.number,
            block_data.parent_slot,
            block_data.blockhash,
            sample_tx_hash
        );
        client.update_block_slot(
            &mut conn,
            block.number,
            new_block_number as i64,
            slot as i64,
        )?;
    }

    Ok(())
}
