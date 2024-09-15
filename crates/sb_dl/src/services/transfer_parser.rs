use {
    super::transfer_flow_api::OrderedTransfersResponse, crate::transfer_flow::create_ordered_transfer_for_block, anyhow::{anyhow, Result}, chrono::prelude::*, db::models::Blocks, elasticsearch::{http::{request::JsonBody, transport::Transport}, BulkParts, Elasticsearch}, solana_transaction_status::UiConfirmedBlock, tokio::sync::oneshot::Receiver
};

/// service to support real-time parsing of transfers 
pub struct TransferParser {
    blocks_notification: tokio::sync::mpsc::Receiver<Blocks>,
    elastic: Elasticsearch,
}

impl TransferParser {
    pub async fn new(
        blocks_notification: tokio::sync::mpsc::Receiver<Blocks>,
        elasticsearch_url: &str,
    ) -> Result<Self> {
        Ok(Self {
            blocks_notification,
            elastic: Elasticsearch::new(Transport::single_node(elasticsearch_url)?)
        })
    }
    pub async fn start(mut self, mut exit: Receiver<()>) {
        loop {
            tokio::select!{ 
                _ = &mut exit => {
                    log::warn!("received exit");
                    return;
                }
                block = self.blocks_notification.recv() => {
                    if let Some(block) = block {
                        let elastic = self.elastic.clone();
                        tokio::task::spawn(Self::process_message(elastic, block));
                    } else {
                        return;
                    }
                }
            }
        }
    }
    async fn process_message(
        elastic: Elasticsearch,
        block: Blocks,
    ) {
        let slot = block.slot;
        let transfers = match Self::decode_transfers(block) {
            Ok(transfers) => transfers,
            Err(err) => {
                log::error!("failed to decode transfers {err:#?}");
                return;
            }
        };
        let mut body: Vec<JsonBody<_>> = Vec::with_capacity(transfers.transfers.len());
        for tx in transfers.transfers {
            match serde_json::to_value(tx.transfers) {
                Ok(transfers) => {
                    body.push(serde_json::json!({"index": {"_id": tx.tx_hash}}).into());
                    body.push(serde_json::json!({
                        "id": tx.tx_hash,
                        "user": "REPLACEME_user",
                        "post_date": Utc::now(),
                        "message": transfers,
                    }).into());
                }
                Err(err) => {
                    log::error!("failed to serialize transfers({}) {err:#?}", tx.tx_hash);

                }
            }
        }
        match elastic
        .bulk(BulkParts::Index("REPLACEME_index"))
        .body(body)
        .send().await {
            Ok(response) => {
                // todo: how to handle response?
                match response.json::<serde_json::Value>().await {
                    Ok(response_body) => {
                        if !response_body["errors"].as_bool().unwrap_or_default() {
                            log::error!("failed to index transfers");
                        }
                    }
                    Err(err) => {
                        log::error!("failed to decode response {err:#?}");
                    }
                }
            }
            Err(err) => {
                log::error!("failed to push transfers(block={slot:?}) {err:#?}");
            }
        }
    }
    fn decode_transfers(mut block: Blocks) -> anyhow::Result<OrderedTransfersResponse> {
        let slot = block.slot;
        let block: UiConfirmedBlock = match serde_json::from_value(
            std::mem::take(&mut block.data)
        ) {
            Ok(block) => block,
            Err(err) => {
                return Err(anyhow!("failed to deserialize block {err:#?}"));
            }
        };
        let time = if let Some(block_time) = block.block_time {
            DateTime::from_timestamp(block_time, 0)
        } else {
            None
        };
        let block_height = block.block_height;
        match create_ordered_transfer_for_block(block) {
            Ok(ordered_transfers) => {
                return Ok(OrderedTransfersResponse {
                    transfers: ordered_transfers,
                    slot,
                    time,
                });
            }
            Err(err) => {
                return Err(anyhow!("failed to create ordered transfers for block({:?}) {err:#?}", block_height));
            }
        }
    }
}

