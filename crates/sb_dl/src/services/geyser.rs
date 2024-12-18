use {
    crate::{types::BlockInfo, utils::process_block},
    anyhow::{anyhow, Context, Result},
    futures::{sink::SinkExt, stream::StreamExt},
    solana_transaction_status::UiConfirmedBlock,
    std::{collections::HashMap, time::Duration},
    yellowstone_grpc_client::{GeyserGrpcClient, Interceptor},
    yellowstone_grpc_proto::{
        convert_from::create_block,
        geyser::{
            subscribe_update::UpdateOneof, CommitmentLevel, SubscribeRequest,
            SubscribeRequestFilterBlocks, SubscribeRequestPing,
        },
    },
    tonic::transport::channel::ClientTlsConfig,
    chrono::prelude::*,
};

pub async fn new_geyser_client(
    endpoint: &str,
    token: &str,
    max_decoding_size: usize,
    max_encoding_size: usize,
) -> Result<GeyserGrpcClient<impl Interceptor>> {
    let client = GeyserGrpcClient::build_from_shared(endpoint.to_string())?
        .x_token(token.to_string().into())?
        .tls_config(ClientTlsConfig::new().with_native_roots())?
        .connect_timeout(Duration::from_secs(10))
        .timeout(Duration::from_secs(10))
        .keep_alive_while_idle(true)
        .http2_keep_alive_interval(Duration::from_secs(10))
        .keep_alive_timeout(Duration::from_secs(20))
        .max_decoding_message_size(max_decoding_size)
        .max_encoding_message_size(max_encoding_size)
        .tcp_nodelay(true)
        .connect()
        .await?;
    Ok(client)
}

pub async fn subscribe_blocks(
    mut client: GeyserGrpcClient<impl Interceptor>,
    blocks_tx: tokio::sync::mpsc::Sender<BlockInfo>,
    no_minimization: bool,
) -> Result<()> {
    let mut blocks: HashMap<String, SubscribeRequestFilterBlocks> = Default::default();
    blocks.insert(
        "client".to_owned(),
        SubscribeRequestFilterBlocks {
            account_include: vec![],
            include_transactions: Some(true),
            include_accounts: Some(false),
            include_entries: Some(false),
        },
    );
    let (mut subscribe_tx, mut stream) = client
        .subscribe_with_request(Some(SubscribeRequest {
            accounts: Default::default(),
            slots: Default::default(),
            transactions: Default::default(),
            transactions_status: Default::default(),
            blocks,
            blocks_meta: Default::default(),
            entry: Default::default(),
            commitment: Some(CommitmentLevel::Finalized).map(|x| x as i32),
            accounts_data_slice: vec![],
            ping: None,
        }))
        .await
        .with_context(|| "failedt to subscribe")?;
    while let Some(message) = stream.next().await {
        match message {
            Ok(msg) => match msg.update_oneof {
                Some(UpdateOneof::Ping(_)) => {
                    log::info!("processing ping");
                    if let Err(err) = subscribe_tx
                        .send(SubscribeRequest {
                            ping: Some(SubscribeRequestPing { id: 1 }),
                            ..Default::default()
                        })
                        .await
                    {
                        log::error!("failed to send ping {err:#?}");
                    }
                }
                Some(UpdateOneof::Block(block)) => {
                    let slot = block.slot;
                    match create_block(block) {
                        Ok(block) => match process_block(block, no_minimization) {
                            Ok(block) => {
                                if let Some(block_height) = block.block_height {
                                    log::info!("got_block(slot={}, height={})", slot, block_height);
                                    let time = if let Some(block_time) = block.block_time {
                                        DateTime::from_timestamp(block_time, 0)
                                    } else {
                                        None
                                    };
                                    if let Err(err) = blocks_tx
                                        .send(BlockInfo {
                                            slot,
                                            block,
                                            time,
                                            block_height,
                                        })
                                        .await
                                    {
                                        log::error!("failed to notify new block {err:#?}");
                                    }
                                } else {
                                    log::warn!("missing block height");
                                }
                            }
                            Err(err) => {
                                log::error!("failed to process block {err:#?}");
                            }
                        },
                        Err(err) => {
                            log::error!("failed to convert block {err:#?}")
                        }
                    }
                }
                Some(UpdateOneof::Pong(_)) => {}
                Some(msg_one_of) => {
                    log::warn!("unsupported message received {msg_one_of:#?}");
                }
                None => {}
            },
            Err(err) => return Err(anyhow!("failed to receive next message {err:#?}")),
        }
    }
    Ok(())
}
