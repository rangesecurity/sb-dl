use std::time::Duration;

use anyhow::Context;
use solana_client::{nonblocking::rpc_client::RpcClient, rpc_config::RpcBlockConfig};
use solana_sdk::commitment_config::CommitmentConfig;
use solana_transaction_status::{TransactionDetails, UiConfirmedBlock, UiTransactionEncoding};

use crate::utils::{filter_vote_transactions, process_block};

pub struct Backfiller {
    rpc: RpcClient,
}

impl Backfiller {
    pub fn new(endpoint: &str) -> Self {
        Self {
            rpc: RpcClient::new(endpoint.to_string()),
        }
    }
    pub async fn start(
        &self,
        blocks_tx: tokio::sync::mpsc::Sender<(u64, UiConfirmedBlock)>,
        no_minimization: bool,
    ) -> anyhow::Result<()> {
        loop {
            let current_height = self
                .rpc
                .get_block_height()
                .await
                .with_context(|| "failed to get block height")?;
            // backfill 300 most recent blocks, over estimating blocks per second by 2x
            for block_height in (current_height - 300..current_height) {
                match self
                    .rpc
                    .get_block_with_config(
                        block_height,
                        RpcBlockConfig {
                            encoding: Some(UiTransactionEncoding::JsonParsed),
                            transaction_details: Some(TransactionDetails::Full),
                            rewards: Some(false),
                            commitment: Some(CommitmentConfig::finalized()),
                            max_supported_transaction_version: Some(1),
                        },
                    )
                    .await
                {
                    Ok(mut block) => {
                        if no_minimization == false {
                            block = filter_vote_transactions(block);
                        }
                        if let Err(err) = blocks_tx.send((block_height, block)).await {
                            log::error!("failed to notify block {err:#?}");
                        }
                    }
                    Err(err) => {
                        log::error!("failed to retrieve block({block_height}) {err:#?}");
                    }
                }
            }
            tokio::time::sleep(Duration::from_secs(60)).await;
        }
    }
}