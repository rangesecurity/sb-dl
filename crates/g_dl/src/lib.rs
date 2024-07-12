use {
    anyhow::{Context, Result},
    futures::{future::TryFutureExt, sink::SinkExt, stream::StreamExt},
    solana_account_decoder::parse_token::{UiTokenAccount, UiTokenAmount},
    solana_program::{instruction::CompiledInstruction, pubkey::Pubkey},
    solana_sdk::transaction_context::TransactionReturnData,
    solana_transaction_status::{
        ConfirmedBlock, InnerInstruction, InnerInstructions, TransactionStatusMeta,
        TransactionTokenBalance, TransactionWithStatusMeta, UiTransactionReturnData,
        VersionedTransactionWithStatusMeta,
    },
    std::{any::Any, collections::HashMap, time::Duration},
    yellowstone_grpc_client::{GeyserGrpcClient, Interceptor},
    yellowstone_grpc_proto::geyser::{
        subscribe_update::UpdateOneof, CommitmentLevel, SubscribeRequest,
        SubscribeRequestFilterBlocks, SubscribeRequestPing, SubscribeUpdateBlock,
    },
};

pub async fn new_geyser_client(
    endpoint: &str,
    token: &str,
    max_decoding_size: usize,
    max_encoding_size: usize,
) -> Result<GeyserGrpcClient<impl Interceptor>> {
    let client = GeyserGrpcClient::build_from_shared(endpoint.to_string())?
        .x_token(token.to_string().into())?
        .connect_timeout(Duration::from_secs(10))
        .timeout(Duration::from_secs(10))
        .max_decoding_message_size(max_decoding_size)
        .max_encoding_message_size(max_encoding_size)
        .connect()
        .await?;
    Ok(client)
}

pub async fn subscribe_blocks(mut client: GeyserGrpcClient<impl Interceptor>) -> Result<()> {
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
                    log::info!(
                        "received block(height={:?}, num_txs={})",
                        block.block_height,
                        block.executed_transaction_count
                    );
                }
                Some(msg_one_of) => {
                    log::warn!("unsupported message received {:?}", msg_one_of.type_id());
                }
                None => {}
            },
            Err(err) => {
                log::error!("failed to receive next message {err:#?}");
            }
        }
    }
    Ok(())
}

fn block_to_confirmed_block(block: SubscribeUpdateBlock) -> anyhow::Result<()> {
    let block_time = block.block_time.with_context(|| "missing block time")?;
    let block_height = block.block_height.with_context(|| "missing block height")?;
    let mut confirmed_block = ConfirmedBlock {
        previous_blockhash: "".to_string(),
        blockhash: "".to_string(),
        parent_slot: 0,
        transactions: vec![],
        rewards: vec![],
        block_time: None,
        block_height: None,
    };
    confirmed_block.previous_blockhash = block.parent_blockhash;
    confirmed_block.blockhash = block.blockhash;
    confirmed_block.parent_slot = block.parent_slot;
    // set transactions
    confirmed_block.transactions = block
        .transactions
        .into_iter()
        .filter_map(|tx| {
            if tx.is_vote {
                return None;
            };
            let tx_info = tx.transaction?;
            let tx_meta = tx.meta?;
            let tx_err = if let Ok(tx_err) =
                yellowstone_grpc_proto::convert_from::create_tx_error(tx_meta.err.as_ref())
            {
                if let Some(err) = tx_err {
                    Err(err)
                } else {
                    Ok(())
                }
            } else {
                Ok(())
            };
            let inner_instructions = if tx_meta.inner_instructions_none {
                None
            } else {
                Some(
                    tx_meta
                        .inner_instructions
                        .into_iter()
                        .map(|ix| InnerInstructions {
                            index: ix.index as u8,
                            instructions: ix
                                .instructions
                                .into_iter()
                                .map(|ix| InnerInstruction {
                                    instruction: CompiledInstruction {
                                        program_id_index: ix.program_id_index as u8,
                                        accounts: ix.accounts,
                                        data: ix.data,
                                    },
                                    stack_height: ix.stack_height,
                                })
                                .collect(),
                        })
                        .collect(),
                )
            };
            let log_messages = if tx_meta.log_messages_none {
                None
            } else {
                Some(tx_meta.log_messages)
            };
            let pre_token_balances = if tx_meta.pre_token_balances.len() > 0 {
                Some(
                    tx_meta
                        .pre_token_balances
                        .into_iter()
                        .filter_map(|balance| {
                            let token_amount = if let Some(amount) = balance.ui_token_amount {
                                UiTokenAmount {
                                    ui_amount: Some(amount.ui_amount),
                                    decimals: amount.decimals as u8,
                                    amount: amount.amount,
                                    ui_amount_string: amount.ui_amount_string,
                                }
                            } else {
                                return None;
                            };
                            Some(TransactionTokenBalance {
                                account_index: balance.account_index as u8,
                                mint: balance.mint,
                                ui_token_amount: token_amount,
                                owner: balance.owner,
                                program_id: balance.program_id,
                            })
                        })
                        .collect(),
                )
            } else {
                None
            };
            let post_token_balances = if tx_meta.post_token_balances.len() > 0 {
                Some(
                    tx_meta
                        .post_token_balances
                        .into_iter()
                        .filter_map(|balance| {
                            let token_amount = if let Some(amount) = balance.ui_token_amount {
                                UiTokenAmount {
                                    ui_amount: Some(amount.ui_amount),
                                    decimals: amount.decimals as u8,
                                    amount: amount.amount,
                                    ui_amount_string: amount.ui_amount_string,
                                }
                            } else {
                                return None;
                            };
                            Some(TransactionTokenBalance {
                                account_index: balance.account_index as u8,
                                mint: balance.mint,
                                ui_token_amount: token_amount,
                                owner: balance.owner,
                                program_id: balance.program_id,
                            })
                        })
                        .collect(),
                )
            } else {
                None
            };
            let return_data = if let Some(return_data) = tx_meta.return_data {
                Some(TransactionReturnData {
                    program_id: Pubkey::new(&return_data.program_id),
                    data: return_data.data,
                })
            } else {
                None
            };
            Some(TransactionWithStatusMeta::Complete(
                VersionedTransactionWithStatusMeta {
                    meta: TransactionStatusMeta {
                        status: tx_err,
                        fee: tx_meta.fee,
                        pre_balances: tx_meta.pre_balances,
                        post_balances: tx_meta.post_balances,
                        inner_instructions: inner_instructions,
                        log_messages: log_messages,
                        pre_token_balances: pre_token_balances,
                        post_token_balances: post_token_balances,
                        rewards: None,
                        loaded_addresses: Default::default(),
                        return_data: return_data,
                        compute_units_consumed: tx_meta.compute_units_consumed,
                    },
                    transaction: Default::default(),
                },
            ))
        })
        .collect();
    confirmed_block.block_time = Some(block_time.timestamp);
    confirmed_block.block_height = Some(block_height.block_height);
    Ok(())
}
