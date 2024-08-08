use {
    crate::{types::BlockInfo, utils::filter_vote_transactions},
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
        blocks_tx: tokio::sync::mpsc::Sender<BlockInfo>,
        no_minimization: bool,
    ) -> anyhow::Result<()> {
        loop {
            let current_slot = self
                .rpc
                .get_slot_with_commitment(CommitmentConfig::finalized())
                .await
                .with_context(|| "failed to get slot height")?;
            // backfill 300 most recent blocks, over estimating blocks per second by 2x
            for slot_height in current_slot - 300..current_slot {
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
                        if let Err(err) = blocks_tx
                            .send(BlockInfo {
                                slot: Some(slot_height),
                                block,
                                block_height,
                            })
                            .await
                        {
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
