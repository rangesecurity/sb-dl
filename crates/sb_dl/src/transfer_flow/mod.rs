use {
    crate::parsable_instructions::{self, token::TokenInstructions, DecodedInstruction},
    anyhow::{anyhow, Context, Result},
    serde::{Deserialize, Serialize},
    solana_transaction_status::{
        option_serializer::OptionSerializer, parse_accounts::ParsedAccount, EncodedTransaction,
        EncodedTransactionWithStatusMeta, UiConfirmedBlock, UiInnerInstructions, UiInstruction,
        UiMessage, UiParsedInstruction, UiTransactionStatusMeta, UiTransactionTokenBalance,
    },
    std::collections::HashMap,
};

#[derive(Clone, Serialize, Deserialize)]
pub struct Transfer {
    /// the sending token account
    pub sender: String,
    /// the recipient token account
    pub recipient: String,
    pub amount: u64,
}

pub struct TokenOwnerInfo {
    pub mint: String,
    pub owner: String,
    pub account_index: u8,
}

/// The data we need to be able to reconstruct the flow of funds
pub struct TransferData {
    pub pre_token_balances: Vec<UiTransactionTokenBalance>,
    pub post_token_balances: Vec<UiTransactionTokenBalance>,
    pub token_owner_infos_by_index: HashMap<u8, TokenOwnerInfo>,
}

// instructions which can transfer funds:
// 11111111111111111111111111111111::transfer
// 11111111111111111111111111111111:createAccount
// 11111111111111111111111111111111:createAccountWithSeed (todo)
// 11111111111111111111111111111111::transferWithSeed (todo)
// 11111111111111111111111111111111::withdrawNonceAccount (todo)
//
// TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA::transfer
// TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA::mintTo
// TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA::burn
// TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA::transferChecked
// TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA::mintToChecked
// TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA::burnChecked
// TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA::closeAccount (todo)
//  ^--- todo: this causes lamports to be sent back to the destination
//  ^--- todo: we need tof igure out a way to calculate this

// TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb::transfer
// TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb::mintTo
// TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb::burn
// TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb::transferChecked
// TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb::mintToChecked
// TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb::burnChecked
// TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb::closeAccount (todo)
//  ^--- todo: this causes lamports to be sent back to the destination
//  ^--- todo: we need tof igure out a way to calculate this

// TODO: handle token extensions
pub fn prepare_transfer_flow_for_tx(
    block: UiConfirmedBlock,
    tx_hash: &str,
) -> Result<
    HashMap<
        u8,
        (
            Option<DecodedInstruction>,
            HashMap<u8, Vec<DecodedInstruction>>,
        ),
    >,
> {
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
    let tx_meta = tx.meta.clone().with_context(|| "meta is none")?;
    // pre_balances[0] is equal to account_keys[0]
    let pre_balances = tx_meta.pre_balances.clone();
    let post_balances = tx_meta.post_balances.clone();

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

    let (account_keys, outer_instructions) = get_account_keys_and_outer_instructions(&tx)?;

    let mut token_mints_by_account =
        get_token_mints_by_account(&token_owner_infos_by_index, &account_keys);

    let mut inner_instructions_by_index =
        get_inner_instructions_by_index(&token_mints_by_account, &inner_instructions)?;

    let mut outer_instructions_by_index =
        get_outer_instructions_by_index(&outer_instructions, &token_mints_by_account)?;

    Ok(get_ordered_transfers(
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

fn get_token_mints_by_account(
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
