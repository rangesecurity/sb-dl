use std::{collections::HashSet, sync::Arc};

use anyhow::{anyhow, Context};
use clap::ArgMatches;
use db::{client::{BlockFilter, Client}, migrations::run_migrations};
use futures::StreamExt;
use sb_dl::config::Config;
use solana_client::{nonblocking::rpc_client::RpcClient, rpc_config::RpcTransactionConfig};
use solana_transaction_status::{EncodedTransaction, UiConfirmedBlock, UiTransactionEncoding};
use tokio::task::JoinSet;

pub async fn find_gap_end(
    starting_number: i64,
    config_path: &str
) -> anyhow::Result<()> {    
    let cfg = Config::load(config_path).await?;
    let mut conn = db::new_connection(&cfg.db_url)?;
    let client = Client{};
    let gap_end = client.find_gap_end(&mut conn, starting_number)?;
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