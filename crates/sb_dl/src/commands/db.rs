use anyhow::{anyhow, Context};
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
    loop {
        let mut blocks = client.slot_is_null(&mut conn, *limit)?;
        if blocks.is_empty() {
            // no more blocks to repair
            break;
        }
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
                continue;
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
