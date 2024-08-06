use {
    crate::utils::filter_vote_transactions,
    anyhow::Context,
    solana_client::{nonblocking::rpc_client::RpcClient, rpc_config::RpcBlockConfig},
    solana_sdk::commitment_config::CommitmentConfig,
    solana_transaction_status::{TransactionDetails, UiConfirmedBlock, UiTransactionEncoding},
    std::time::Duration,
};

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
            for slot_height in current_height - 300..current_height {
                match self
                    .rpc
                    .get_block_with_config(
                        // this is actually the slot
                        slot_height,
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
                        let block_height = if let Some(block_height) = block.block_height {
                            block_height
                        } else {
                            log::warn!("block height is None for block({slot_height}");
                            continue;
                        };
                        if let Err(err) = blocks_tx.send((block_height, block)).await {
                            log::error!("failed to notify block {err:#?}");
                        }
                    }
                    Err(err) => {
                        log::error!("failed to retrieve block({slot_height}) {err:#?}");
                    }
                }
            }
            tokio::time::sleep(Duration::from_secs(60)).await;
        }
    }
}
