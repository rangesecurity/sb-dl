use anyhow::anyhow;
use clap::ArgMatches;
use db::{client::BlockFilter, migrations::run_migrations};
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
        client.update_block_slot(
            &mut conn,
            block.number,
            new_block_number as i64,
            slot as i64,
        )?;
    }

    Ok(())
}

pub async fn repair_invalid_slots(config_path: &str) -> anyhow::Result<()> {
    let cfg = Config::load(config_path).await?;
    let rpc = RpcClient::new(cfg.rpc_url.clone());
    let mut conn = db::new_connection(&cfg.db_url)?;

    // perform db migrations
    run_migrations(&mut conn);

    let client = db::client::Client {};
    // numbero f the block to repair, initially set to the very first block available
    let mut block_number = client
        .select_block(&mut conn, BlockFilter::FirstBlock)
        .unwrap()[0]
        .number;

    loop {
        let mut block = client.select_block(&mut conn, BlockFilter::Number(block_number))?;
        let mut block = if block.is_empty() {
            return Err(anyhow!("found no matching block({block_number})"));
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
                // if the slot in the database entry is equal to the slot from a tx in the block
                // we no longer need to repair any data since all additional blocks
                // were indexed after correct slot calculation was added
                log::warn!(
                    "stored_slot({stored_slot}) == calculated_slot({slot}) no more repairs needed"
                );
                break;
            }
        }

        client.update_slot(&mut conn, block.number, slot as i64)?;
        log::info!(
            "repaired block(number={}, slot={slot}, tx={sample_tx_hash})",
            block.number
        );

        // increment block_number to repair the next available block
        block_number += 1;
    }
    Ok(())
}

/// returns the slot for the given block, along with the tx hash used to determine this
async fn get_slot_for_block(
    block: &UiConfirmedBlock,
    rpc: &RpcClient,
) -> anyhow::Result<Option<(u64, String)>> {
    let sample_tx = block
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
        match rpc
            .get_transaction_with_config(
                &sample_tx_hash[0].parse()?,
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
                log::error!("failed to get tx({}) {err:#?}", sample_tx_hash[0]);
                return Ok(None);
            }
        }
    } else {
        log::warn!("sample tx hash has no signature");
        return Ok(None);
    };
    return Ok(Some((slot, sample_tx_hash[0].clone())));
}
