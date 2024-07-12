use {
    anyhow::{Context, Result}, futures::{future::TryFutureExt, sink::SinkExt, stream::StreamExt}, std::{any::Any, collections::HashMap, time::Duration}, yellowstone_grpc_client::{GeyserGrpcClient, Interceptor}, yellowstone_grpc_proto::geyser::{subscribe_update::UpdateOneof, CommitmentLevel, SubscribeRequest, SubscribeRequestFilterBlocks, SubscribeRequestPing}
};

pub async fn new_geyser_client(
    endpoint: &str,
    token: &str,
    max_decoding_size: usize,
    max_encoding_size: usize,
)  -> Result<GeyserGrpcClient<impl Interceptor>> {
    let client = GeyserGrpcClient::build_from_shared(
        endpoint.to_string()
    )?.x_token(token.to_string().into())?.connect_timeout(
        Duration::from_secs(10)
    ).timeout(Duration::from_secs(10))
    .max_decoding_message_size(max_decoding_size)
    .max_encoding_message_size(max_encoding_size)
    .connect().await?;
    Ok(client)
}

pub async fn subscribe_blocks(
    mut client: GeyserGrpcClient<impl Interceptor>,
) -> Result<()> {
    let mut blocks: HashMap<String, SubscribeRequestFilterBlocks> = Default::default();
    blocks.insert(
        "client".to_owned(),
        SubscribeRequestFilterBlocks {
            account_include: vec![],
            include_transactions: Some(true),
            include_accounts: Some(true),
            include_entries: Some(true),
        },
    );
    let (mut subscribe_tx, mut stream) = client.subscribe_with_request(Some(SubscribeRequest {
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
    })).await.with_context(|| "failedt to subscribe")?;
    while let Some(message) = stream.next().await {
        match message {
            Ok(msg) => match msg.update_oneof {
                Some(UpdateOneof::Ping(_)) => {
                    log::info!("processing ping");
                    if let Err(err) = subscribe_tx.send(SubscribeRequest {
                        ping: Some(SubscribeRequestPing { id: 1}),
                        ..Default::default()
                    }).await {
                        log::error!("failed to send ping {err:#?}");
                    }
                }
                Some(UpdateOneof::Block(block)) => {
                    log::info!("received block {:?}", block.block_height);
                }
                Some(msg_one_of) => {
                    log::warn!("unsupported message received {:?}", msg_one_of.type_id());
                }
                None => {}

            }
            Err(err) => {
                log::error!("failed to receive next message {err:#?}");
            }
        }
    }
    Ok(())
}