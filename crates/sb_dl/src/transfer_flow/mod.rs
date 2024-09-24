//!/ instructions which can transfer funds:
//!/ 11111111111111111111111111111111::transfer
//!/ 11111111111111111111111111111111:createAccount
//!/ 11111111111111111111111111111111:createAccountWithSeed (todo)
//!/ 11111111111111111111111111111111::transferWithSeed (todo)
//!/ 11111111111111111111111111111111::withdrawNonceAccount (todo)
//!/
//!/ TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA::transfer
//!/ TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA::mintTo
//!/ TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA::burn
//!/ TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA::transferChecked
//!/ TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA::mintToChecked
//!/ TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA::burnChecked
//!/ TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA::closeAccount (todo)
//!/  ^--- todo: this causes lamports to be sent back to the destination
//!/  ^--- todo: we need tof igure out a way to calculate this
//!
//! TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb::transfer
//!/ TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb::mintTo
//!/ TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb::burn
//!/ TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb::transferChecked
//!/ TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb::mintToChecked
//!/ TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb::burnChecked
//!/ TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb::closeAccount (todo)
//!/  ^--- todo: this causes lamports to be sent back to the destination
//!/  ^--- todo: we need tof igure out a way to calculate this

pub mod transfer_graph;
pub mod types;

use {
    crate::parsable_instructions::{self, token::TokenInstructions, DecodedInstruction},
    anyhow::{anyhow, Context, Result},
    solana_transaction_status::{
        option_serializer::OptionSerializer, parse_accounts::ParsedAccount, EncodedTransaction,
        EncodedTransactionWithStatusMeta, UiConfirmedBlock, UiInnerInstructions, UiInstruction,
        UiMessage, UiParsedInstruction, UiTransactionStatusMeta, UiTransactionTokenBalance,
    },
    std::collections::HashMap,
    types::{OrderedTransfers, TokenOwnerInfo, Transfer, TransferFlow},
};

// Create ordered transfers for an entire block
pub fn create_ordered_transfer_for_block(block: UiConfirmedBlock) -> Result<Vec<OrderedTransfers>> {
    let ordered_transfers = block
        .transactions
        .with_context(|| "no txs found")?
        .into_iter()
        .filter_map(|tx| {
            let tx_hash = if let EncodedTransaction::Json(ui_tx) = &tx.transaction {
                if ui_tx.signatures.is_empty() {
                    log::warn!("found no signatures");
                    return None;
                }
                &ui_tx.signatures[0]
            } else {
                log::warn!("unsupportd tx type");
                return None;
            };
            let transfer_flow = prepare_transfer_flow_for_tx(&tx)?;
            match create_ordered_transfers(tx_hash, transfer_flow) {
                Ok(ordered_transfers) => Some(ordered_transfers),
                Err(err) => {
                    log::debug!("failed to create ordered_transfers(tx={tx_hash}) {err:#?}");
                    return None;
                }
            }
        })
        .collect();
    Ok(ordered_transfers)
}

/// Creates ordered transfers for a single tx
pub fn create_ordered_transfer_for_tx(
    block: UiConfirmedBlock,
    tx_hash: &str,
) -> Result<OrderedTransfers> {
    let tx = block
        .transactions
        .with_context(|| "no txs found")?
        .iter()
        .find(|tx| {
            if let EncodedTransaction::Json(ui_tx) = &tx.transaction {
                if ui_tx.signatures[0].eq(tx_hash) {
                    return true;
                }
            }
            false
        })
        .cloned()
        .with_context(|| "failed to find matching tx")?;
    let transfer_flow =
        prepare_transfer_flow_for_tx(&tx).with_context(|| "failed to prepare transfer flow")?;
    create_ordered_transfers(tx_hash, transfer_flow)
}

// TODO: handle token extensions
pub fn prepare_transfer_flow_for_tx_hash(
    block: UiConfirmedBlock,
    tx_hash: &str,
) -> Result<TransferFlow> {
    let tx = block
        .transactions
        .with_context(|| "no txs found")?
        .iter()
        .find(|tx| {
            if let EncodedTransaction::Json(ui_tx) = &tx.transaction {
                if ui_tx.signatures[0].eq(tx_hash) {
                    return true;
                }
            }
            false
        })
        .cloned()
        .with_context(|| "failed to find matching tx")?;
    prepare_transfer_flow_for_tx(&tx).with_context(|| "failed to prepare transfer flow")
}

fn prepare_transfer_flow_for_tx(tx: &EncodedTransactionWithStatusMeta) -> Option<TransferFlow> {
    let tx_meta = tx.meta.as_ref()?;
    // skip parsing failed transactions
    if tx_meta.err.is_some() {
        return None;
    }
    // pre_balances[0] is equal to account_keys[0]
    let _pre_balances = tx_meta.pre_balances.clone();
    let _post_balances = tx_meta.post_balances.clone();
    
    // pre_token_balances[X].account_index = 1 is equal to account_keys[1]
    let pre_token_balances = if let OptionSerializer::Some(bals) = &tx_meta.pre_token_balances {
        bals.clone()
    } else {
        vec![]
    };
    let post_token_balances = if let OptionSerializer::Some(bals) = &tx_meta.post_token_balances {
        bals.clone()
    } else {
        vec![]
    };

    let token_owner_infos_by_index =
        prepare_token_owner_infos(&pre_token_balances, &post_token_balances);

    let inner_instructions = get_inner_instructions(&tx_meta);

    let (account_keys, outer_instructions) = get_account_keys_and_outer_instructions(&tx).ok()?;

    let mut token_mints_by_account =
        get_token_mints_by_owner(&token_owner_infos_by_index, &account_keys);

        extract_token_mints_from_account_init_instructions(
            &inner_instructions,
            &outer_instructions,
            &mut token_mints_by_account
        );

    let inner_instructions_by_index =
        get_inner_instructions_by_index(&token_mints_by_account, &inner_instructions).ok()?;

    let outer_instructions_by_index =
        get_outer_instructions_by_index(&outer_instructions, &token_mints_by_account).ok()?;

    Some(get_ordered_transfers(
        outer_instructions_by_index,
        inner_instructions_by_index,
    ))
}

fn prepare_token_owner_infos(
    pre_token_balances: &[UiTransactionTokenBalance],
    post_token_balances: &[UiTransactionTokenBalance],
) -> HashMap<u8, TokenOwnerInfo> {
    // we need to track address that own token accounts in order to get mint information
    // for non checked transfers by checking the account index
    let mut infos: HashMap<u8, TokenOwnerInfo> = Default::default();
    // chaining both interators is likely unnecessary since the mint, owner, and index
    // information should be the same
    pre_token_balances
        .iter()
        .chain(post_token_balances.iter())
        .for_each(|balance| {
            infos.insert(
                balance.account_index,
                TokenOwnerInfo {
                    mint: balance.mint.clone(),
                    // for older txs this can be an empty string
                    // https://github.com/solana-labs/solana/pull/22146
                    owner: Into::<Option<String>>::into(balance.owner.clone()).unwrap_or_default(),
                    // the account index numbering follows array element number
                    // so account_index 3, would be account_keys[3],
                    // this is different than instruction numbering where
                    // instruction_index 3 would be instructions[2]
                    account_index: balance.account_index,
                },
            );
        });

    return infos;
}

fn get_inner_instructions(tx_meta: &UiTransactionStatusMeta) -> Vec<UiInnerInstructions> {
    // get the inner instructions

    if let OptionSerializer::Some(inner_ixs) = &tx_meta.inner_instructions {
        inner_ixs.clone()
    } else {
        vec![]
    }
}

fn get_inner_instructions_by_index(
    token_mints_by_account: &HashMap<String, String>,
    inner_instructions: &[UiInnerInstructions],
) -> Result<HashMap<u8, Vec<DecodedInstruction>>> {
    // map inner instruction index => decoded instruction
    let mut inner_instruction_by_index: HashMap<u8, Vec<DecodedInstruction>> = Default::default();
    // iterate over inner instructions, searching for transfers
    for ix in inner_instructions {
        let mut decoded_instructions: Vec<DecodedInstruction> = vec![];

        // attempt to decode all possible inner instructions
        for inner_ix in &ix.instructions {
            let UiInstruction::Parsed(ui_ix) = inner_ix else {
                continue;
            };

            let UiParsedInstruction::Parsed(parsed_ix) = ui_ix else {
                continue;
            };

            let mut decoded_ix = match parsable_instructions::decode_instruction(&parsed_ix) {
                Ok(Some(decoded)) => decoded,
                Ok(None) => continue, // unrecognized instruction
                Err(err) => return Err(anyhow!("failed to decode instruction {err:#?}")),
            };

            if let DecodedInstruction::TokenInstruction(TokenInstructions::Transfer(ix)) =
                &mut decoded_ix
            {
                if let Some(token_mint) = token_mints_by_account.get(&ix.source) {
                    ix.mint = Some(token_mint.clone());
                }
                if ix.mint.is_none() {
                    if let Some(token_mint) = token_mints_by_account.get(&ix.destination) {
                        ix.mint = Some(token_mint.clone());
                    }
                }
            }

            decoded_instructions.push(decoded_ix);
        }

        if !decoded_instructions.is_empty() {
            // technically this really shouldn't be possible, as we will not encounter this index more than once
            // but to avoid possible edge cases lets check anyways
            if let Some(indexed_ixs) = inner_instruction_by_index.get_mut(&ix.index) {
                indexed_ixs.append(&mut decoded_instructions);
            } else {
                inner_instruction_by_index.insert(ix.index, decoded_instructions);
            }
        }
    }
    Ok(inner_instruction_by_index)
}

fn get_outer_instructions_by_index(
    outer_instructions: &[UiInstruction],
    token_mints_by_account: &HashMap<String, String>,
) -> Result<HashMap<u8, DecodedInstruction>> {
    // map outer instruction index => decoded instructions
    let mut outer_instruction_by_index: HashMap<u8, DecodedInstruction> = Default::default();
    for (idx, ix) in outer_instructions.iter().enumerate() {
        // token2022, spl-token, and system program will always be parsed programs will always be parsed
        let UiInstruction::Parsed(ui_ix) = ix else {
            continue;
        };

        let UiParsedInstruction::Parsed(parsed_ix) = ui_ix else {
            continue;
        };

        let mut decoded_ix = match parsable_instructions::decode_instruction(&parsed_ix) {
            Ok(Some(decoded)) => decoded,
            Ok(None) => continue, // unrecognized instruction
            Err(err) => return Err(anyhow!("failed to decode instruction {err:#?}")),
        };

        if let DecodedInstruction::TokenInstruction(TokenInstructions::Transfer(ix)) =
            &mut decoded_ix
        {
            if let Some(token_mint) = token_mints_by_account.get(&ix.source) {
                ix.mint = Some(token_mint.clone());
            }
            if ix.mint.is_none() {
                if let Some(token_mint) = token_mints_by_account.get(&ix.destination) {
                    ix.mint = Some(token_mint.clone());
                }
            }
        }

        // instruction numbering in existing explorers always starts at 1, however
        // when using `enumerate` which counts the position of the element in a vec
        // we need to offset idx by 1
        outer_instruction_by_index.insert((idx + 1) as u8, decoded_ix);
    }
    Ok(outer_instruction_by_index)
}

fn get_account_keys_and_outer_instructions(
    tx: &EncodedTransactionWithStatusMeta,
) -> Result<(Vec<ParsedAccount>, Vec<UiInstruction>)> {
    // get the account keys involved in the tx, as well as the outer instructions

    if let EncodedTransaction::Json(tx) = &tx.transaction {
        match &tx.message {
            UiMessage::Parsed(parsed_msg) => Ok((
                parsed_msg.account_keys.clone(),
                parsed_msg.instructions.clone(),
            )),
            UiMessage::Raw(_) => return Err(anyhow!("unsupported")),
        }
    } else {
        return Err(anyhow!("unsupported tx type"));
    }
}

fn get_token_mints_by_owner(
    token_owner_infos_by_index: &HashMap<u8, TokenOwnerInfo>,
    account_keys: &[ParsedAccount],
) -> HashMap<String, String> {
    // match account_keys => token_mint
    let mut token_mints_by_account: HashMap<String, String> = Default::default();
    for (idx, account) in account_keys.iter().enumerate() {
        if let Some(token_info) = token_owner_infos_by_index.get(&(idx as u8)) {
            token_mints_by_account.insert(account.pubkey.clone(), token_info.mint.clone());
        }
    }
    token_mints_by_account
}

fn get_ordered_transfers(
    outer_instruction_by_index: HashMap<u8, DecodedInstruction>,
    inner_instruction_by_index: HashMap<u8, Vec<DecodedInstruction>>,
) -> HashMap<
    u8,
    (
        Option<DecodedInstruction>,
        HashMap<u8, Vec<DecodedInstruction>>,
    ),
> {
    // the structure of this is as follows
    // map[k1] = (transfer_1, map[k2] => transfers_2)
    // where transfers_1 is a transfer initiated by outer instruction k1
    // where transfers_2 are any transfers initiated by inner instruction k2
    //
    // if transfer_1 is None, it means the outer k1 instruction triggered no transfer
    let mut ordered_transfers: HashMap<
        u8,
        (
            Option<DecodedInstruction>,
            HashMap<u8, Vec<DecodedInstruction>>,
        ),
    > = Default::default();

    // first prepare the outer instructions
    for (idx, ix) in outer_instruction_by_index {
        if let Some((_, _)) = ordered_transfers.get_mut(&idx) {
            // pretty sure this condition will be impossible, should we panic?
            log::warn!("invalid condition detected!");
        } else {
            ordered_transfers.insert(idx, (Some(ix), Default::default()));
        }
    }

    // now prepare the innser instructions
    for (idx, ixs) in inner_instruction_by_index {
        if let Some((_, inner_transfers)) = ordered_transfers.get_mut(&idx) {
            if let Some(_) = inner_transfers.get_mut(&idx) {
                // this case shouldn't happen, should we panic?
                log::warn!("invalid condition detected")
            } else {
                inner_transfers.insert(idx, ixs);
            }
        } else {
            // in this case the outer instruction which triggered this inner instruction didnt transfer any tokens
            // so we need to create the inital key in ordered_transfers
            let mut inner_ordered_transfers: HashMap<u8, Vec<DecodedInstruction>> =
                Default::default();
            inner_ordered_transfers.insert(idx, ixs);
            ordered_transfers.insert(idx, (None, inner_ordered_transfers));
        }
    }
    ordered_transfers
}

fn create_ordered_transfers(
    tx_hash: &str,
    transfer_flow: TransferFlow,
) -> anyhow::Result<OrderedTransfers> {
    let mut ordered_transfers: Vec<Transfer> = vec![];
    let mut keys = transfer_flow.keys().map(|key| *key).collect::<Vec<_>>();
    keys.sort();
    for key in keys {
        let (outer_transfer, inner_transfers) = transfer_flow
            .get(&key)
            .with_context(|| "should not be None")?;
        if let Some(transfer) = outer_transfer {
            let transfer: Option<Transfer> = From::from(transfer.clone());
            if let Some(transfer) = transfer {
                ordered_transfers.push(transfer);
            }
            
        }
        if !inner_transfers.contains_key(&key) {
            // no inner transfers
            continue;
        }
        let inner_transfers = inner_transfers
            .get(&key)
            .with_context(|| format!("should not be None for key {key}"))?;
        for inner_transfer in inner_transfers {
            let transfer: Option<Transfer> = From::from(inner_transfer.clone());
            if let Some(transfer) = transfer {
                ordered_transfers.push(transfer);
            }
        }
    }
    if ordered_transfers.is_empty() {
        return Err(anyhow!("found no transfers"));
    }
    Ok(OrderedTransfers {
        transfers: ordered_transfers,
        tx_hash: tx_hash.to_string(),
    })
}

/// extracts token mints from initializeAccount, initializeAccount2, initializeAccount3 instructions
fn extract_token_mints_from_account_init_instructions(
    inner_instructions: &[UiInnerInstructions],
    outer_instructions: &[UiInstruction],
    token_mints_by_account: &mut HashMap<String, String>, 
) {
    for inner_ix in inner_instructions {
        for ix in &inner_ix.instructions {
            // token2022, spl-token, and system program will always be parsed programs will always be parsed
            let UiInstruction::Parsed(ui_ix) = ix else {
                continue;
            };

            let UiParsedInstruction::Parsed(parsed_ix) = ui_ix else {
                continue;
            };

            let mut decoded_ix = match parsable_instructions::decode_instruction(&parsed_ix) {
                Ok(Some(decoded)) => decoded,
                Ok(None) => continue, // unrecognized instruction
                Err(err) => continue,
            };
            let DecodedInstruction::TokenInstruction(token_ix) = decoded_ix else {
                continue;
            };
            match token_ix {
                TokenInstructions::InitializeAccount(init_account) => {
                    token_mints_by_account.insert(init_account.account.clone(), init_account.mint.clone());
                }
                TokenInstructions::InitializeAccount3(init_account) => {
                    token_mints_by_account.insert(init_account.account.clone(), init_account.mint.clone());

                },
                _ => continue,
            }
        }
    }
    for ix in outer_instructions {

            // token2022, spl-token, and system program will always be parsed programs will always be parsed
            let UiInstruction::Parsed(ui_ix) = ix else {
                continue;
            };

            let UiParsedInstruction::Parsed(parsed_ix) = ui_ix else {
                continue;
            };

        let mut decoded_ix = match parsable_instructions::decode_instruction(&parsed_ix) {
            Ok(Some(decoded)) => decoded,
            Ok(None) => continue, // unrecognized instruction
            Err(err) => continue,
        };
        let DecodedInstruction::TokenInstruction(token_ix) = decoded_ix else {
            continue;
        };
        match token_ix {
            TokenInstructions::InitializeAccount(init_account) => {
                token_mints_by_account.insert(init_account.account.clone(), init_account.mint.clone());
            }
            TokenInstructions::InitializeAccount3(init_account) => {
                token_mints_by_account.insert(init_account.account.clone(), init_account.mint.clone());

            },
            _ => continue,
        }
    }
}