use {
    super::transfer_flow_api::OrderedTransfersResponse, crate::transfer_flow::{create_ordered_transfer_for_block, types::Transfer}, anyhow::{anyhow, Result}, chrono::prelude::{*}, chrono::prelude::{*}, db::models::Blocks, elasticsearch::{http::{request::JsonBody, transport::Transport}, BulkParts, Elasticsearch}, serde::{Deserialize, Serialize}, solana_transaction_status::UiConfirmedBlock, tokio::sync::oneshot::Receiver
};

/// service to support real-time parsing of transfers 
pub struct TransferParser {
    elastic: Elasticsearch,
    storage_version: u64,
}

impl TransferParser {
    pub async fn new(
        elasticsearch_url: &str,
        storage_version: u64,
    ) -> Result<Self> {
        Ok(Self {
            elastic: Elasticsearch::new(Transport::single_node(elasticsearch_url)?),
            storage_version,
        })
    }
    pub async fn start(&self, blocks: Vec<Blocks>) -> Result<()> {
        for block in blocks.into_iter() {
            let block_number = block.number;
            if let Err(err) = self.process_block(block).await {
                return Err(anyhow!("failed to process block({block_number}) {err:#?}"));
            }
        }
        Ok(())
    }
    async fn process_block(
        &self,
        block: Blocks,
    ) -> Result<()> {
        let block_number=  block.number;
        let transfers = match Self::decode_transfers(block) {
            Ok(transfers) => transfers,
            Err(err) => return Err(anyhow!("failed to decode transfers {err:#?}")),
        };
        if transfers.transfers.is_empty() {
            log::debug!("no transfers for block({block_number})");
            return Ok(());
        }
        //log::info!("found {} transfers", transfers.transfers.len());
        let mut counter = 0_u64;
        let mut body: Vec<JsonBody<_>> = Vec::with_capacity(transfers.transfers.len());
        for tx in transfers.transfers.iter() {
            let transfer_schemas = tx.transfers.iter().filter_map(|transfer| {
                if transfer.mint.is_empty() {
                    log::warn!("found empty mint {transfer:#?}");
                    return None;
                }
                let str_id = format_id(block_number, counter);
                let transfer_schema = (str_id, Schema::new(
                    &transfer,
                    transfers.time.unwrap_or_default(),
                    block_number,
                    counter,
                    tx.tx_hash.clone(),
                    self.storage_version,
                ));
                counter += 1;
                Some(transfer_schema)
            }).collect::<Vec<_>>();
            for (transfer_id, transfer_schema) in transfer_schemas {
                body.push(serde_json::json!({"index": {"_id": transfer_id}}).into());
                body.push(serde_json::json!(transfer_schema).into());
            }
        }
        //log::info!("counter {counter}");
        match self.elastic
        .bulk(BulkParts::Index("payments"))
        .routing(&format_route_key(transfers.time.unwrap_or_default()))
        .body(body)
        .send().await {
            Ok(response) => {
                // todo: how to handle response?
                match response.json::<serde_json::Value>().await {
                    Ok(response_body) => {
                        if response_body["errors"].as_bool().unwrap_or_default() {
                            return Err(anyhow!("failed to index transfers {response_body:#?}"))
                        }
                        if response_body["error"].as_bool().unwrap_or_default() {
                            return Err(anyhow!("failed to index transfers {response_body:#?}"));
                        }
                    }
                    Err(err) => return Err(anyhow!("failed to decode elastic response {err:#?}")),
                }
            }
            Err(err) => return Err(anyhow!("failed to index transfers {err:#?}")),
        }
        Ok(())
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

#[derive(Serialize, Deserialize, Debug)]
pub struct Schema {
    pub id: String,
    // set to 2, incrementing this can be used to delete previously indexed payment data
    pub version: u64,
    pub category: String,
    pub sender_address_id: String,
    pub sender_network: String,
    pub sender_height: String,
    pub sender_tx_hash: String,
    pub sender_entity: Option<String>,
    pub receiver_address_id: String,
    pub receiver_network: String,
    pub receiver_height: String,
    pub receiver_tx_hash: String,
    pub receiver_entity: Option<String>,
    pub time: DateTime<Utc>,
    #[serde(rename = "type")]
    pub type_: String,
    // the token mint
    pub denom: String,
    pub amount: String,
    // can be set to None for now
    pub usd: Option<u64>,

}

pub fn format_id(
    block_height: i64,
    counter: u64,
) -> String {
    format!("solana-{block_height}-{counter}")
}

// formats key used for specifying which elasticsearch node should index the payments data
pub fn format_route_key(
    time: DateTime<Utc>
) -> String {
    let date = time.format("%Y-%m-%d").to_string();
    format!("solana-{date}")
}

impl Schema {
    pub fn new(
        tx: &Transfer,
        block_time: DateTime<Utc>,
        block_height: i64,
        counter: u64,
        tx_hash: String,
        storage_version: u64,
    ) -> Self {
        let id = format_id(block_height, counter);
        Self {
            id,
            version: storage_version,
            category: "solana".to_string(),
            sender_address_id: format!("solana:{}", tx.sender),
            sender_network: "solana".to_string(),
            sender_height: block_height.to_string(),
            sender_tx_hash: tx_hash.clone(),
            sender_entity: None,
            receiver_address_id: format!("solana:{}", tx.recipient),
            receiver_network: "solana".to_string(),
            receiver_height: block_height.to_string(),
            receiver_tx_hash: tx_hash.clone(),
            receiver_entity: None,
            time: block_time,
            type_: "Transfer".to_string(),
            denom: tx.mint.clone(),
            amount: tx.amount.clone(),
            usd: None,
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    #[test]
    fn test_format_route_key() {
        let date = DateTime::from_timestamp(1726784321, 0).unwrap();
        let route_key = format_route_key(date);
        assert_eq!(route_key, "solana-2024-09-19");
    }
}