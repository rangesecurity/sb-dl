use {
    super::transfer_flow_api::OrderedTransfersResponse, crate::transfer_flow::create_ordered_transfer_for_block, anyhow::{anyhow, Result}, chrono::DateTime, db::{blocks_stream::BlocksStreamClient, client::{BlockFilter, Client}, models::BlockTableChoice, new_connection_pool, AsyncMessage}, diesel::{r2d2::{ConnectionManager, Pool}, PgConnection}, elasticsearch::{http::transport::Transport, Elasticsearch}, futures::{channel::mpsc::UnboundedReceiver, StreamExt}, solana_transaction_status::UiConfirmedBlock, std::str::FromStr, tokio::sync::oneshot::Receiver, uuid::Uuid
};

/// service to support real-time parsing of transfers 
pub struct TransferParser {
    client: BlocksStreamClient,
    blocks_notification: UnboundedReceiver<AsyncMessage>,
    connection_pool: Pool<ConnectionManager<PgConnection>>,
    elastic: Elasticsearch,
}

impl TransferParser {
    pub async fn new(
        db_url: &str,
        elasticsearch_url: &str,
    ) -> Result<Self> {
        let (client, blocks_notification) = db::blocks_stream::BlocksStreamClient::new(db_url).await?;
        Ok(Self {
            client,
            blocks_notification,
            connection_pool: new_connection_pool(db_url, 10)?,
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
                message = self.blocks_notification.next() => {
                    if let Some(message) = message {
                        match self.connection_pool.get() {
                            Ok(mut conn) => {
                                let elastic = self.elastic.clone();
                                tokio::task::spawn(async move {
                                    Self::process_message(elastic, &mut conn, message);
                                });
                            }
                            Err(err) => {
                                log::error!("failed to retrieve postgres connection {err:#?}");
                            }
                        }
                    } else {
                        return;
                    }
                }
            }
        }
    }
    fn process_message(
        elastic: Elasticsearch,
        conn: &mut PgConnection,
        message: AsyncMessage
    ) {
        let transfers = match Self::decode_transfers(conn, message) {
            Ok(transfers) => transfers,
            Err(err) => {
                log::error!("failed to decode transfers {err:#?}");
                return;
            }
        };
        // push transfers to elasticsearch
        log::warn!("TODO");
    }
    fn decode_transfers(
        conn: &mut PgConnection,
        message: AsyncMessage
    ) -> anyhow::Result<OrderedTransfersResponse> {
        const CLIENT: Client = Client{};
        let AsyncMessage::Notification(msg) = message  else {
            return Err(anyhow!("failed to parse message {message:#?}"));
        };
        let (block, slot): (UiConfirmedBlock, Option<i64>) = {
            let channel = msg.channel();
            let payload = match Uuid::from_str(msg.payload()) {
                Ok(id) => id,
                Err(err) => {
                    return Err(anyhow!("failed to parse payload {err:#?}"));
                }
            };
            let filter = BlockFilter::Id(payload);
            let table_choice = if channel.eq("block_changes") {
                BlockTableChoice::Blocks
            } else if channel.eq("block2_changes") {
                BlockTableChoice::Blocks2
            } else {
                return Err(anyhow!("received message on unsupported channel {channel}"));
            };
            match CLIENT.select_block(
                conn,
                filter,
                table_choice
            ) {
                Ok(mut blocks) => if blocks.is_empty() {
                    return Err(anyhow!("failed to find block matching id {payload}"));
                } else {
                    match serde_json::from_value(
                        std::mem::take(&mut blocks[0].data)
                    ) {
                        Ok(block) => (block, blocks[0].slot),
                        Err(err) => {
                            return Err(anyhow!("failed to deserialize block {err:#?}"));
                        }
                    }
                }
                Err(err) => {
                    return Err(anyhow!("failed to query db {err:#?}"));
                }
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

