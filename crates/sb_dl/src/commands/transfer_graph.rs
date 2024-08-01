use std::collections::HashMap;

use anyhow::{anyhow, Context};
use db::{client::BlockFilter, new_connection};
use sb_dl::{config::Config, parsable_instructions::{self, DecodedInstruction}};
use solana_transaction_status::{option_serializer::OptionSerializer, EncodedTransaction, UiConfirmedBlock, UiInnerInstructions, UiInstruction, UiMessage, UiParsedInstruction};

pub async fn create_transfer_graph(
    matches: &clap::ArgMatches,
    config_path: &str
) -> anyhow::Result<()> {
    let cfg: Config = Config::load(config_path).await?;
    // slot to pull tx from
    let slot_number = matches.get_one::<i64>("slot-number").unwrap();
    // tx to generate graph for
    let tx_hash = matches.get_one::<String>("tx-hash").unwrap();
    let mut db_conn = new_connection(&cfg.db_url)?;
    let client = db::client::Client{};
    let mut block = client.select_block(&mut db_conn, BlockFilter::Slot(*slot_number))?;
    let block = if block.is_empty() {
        return Err(anyhow!("no block found"))
    } else {
        std::mem::take(&mut block[0])
    };

    let block: UiConfirmedBlock = serde_json::from_value(block.data)?;
    let tx = block.transactions.with_context(|| "no txs found")?.into_iter().find(|tx| {
        if let EncodedTransaction::Json(ui_tx) = &tx.transaction {
            if ui_tx.signatures[0].eq(tx_hash) {
                return true
            } 
        }
        false
    }).with_context(|| "failed to find matching tx")?;
    let tx_meta = tx.meta.with_context(|| "meta is none")?;

    // todo: validate pre+post balances

    // pre_balances[0] is equal to account_keys[0]
    let pre_balances = tx_meta.pre_balances.clone();
    let post_balances = tx_meta.post_balances.clone();
    let pre_token_balances = if let OptionSerializer::Some(bals) = tx_meta.pre_token_balances {
        bals
    } else {
        vec![]
    };
    let post_token_balances = if let OptionSerializer::Some(bals) = tx_meta.post_token_balances {
        bals
    } else {
        vec![]
    };
    let mut inner_instructions = if let OptionSerializer::Some(inner_ixs) = tx_meta.inner_instructions {
        inner_ixs
    } else {
        vec![]
    };
    let (account_keys, outer_instructions) = if let EncodedTransaction::Json(tx) = tx.transaction {
        match tx.message {
            UiMessage::Parsed(parsed_msg) => {
                (parsed_msg.account_keys, parsed_msg.instructions)
            }
            UiMessage::Raw(_) => {
                return Err(anyhow!("unsupported"))
            }
        }
    } else {
        return Err(anyhow!("unsupported tx type"))
    };

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

    // map inner instruction index => decoded instruction
    let mut inner_instruction_by_index: HashMap<u8, Vec<DecodedInstruction>> = Default::default();

    // iterate over inner instructions, searching for transfers
    for ix in inner_instructions {
        let mut decoded_instructions: Vec<DecodedInstruction> = vec![];

        // attempt to decode all possible inner instructions
        for inner_ix in ix.instructions {
            let UiInstruction::Parsed(ui_ix) = inner_ix else {
                continue;
            };
            let UiParsedInstruction::Parsed(parsed_ix) = ui_ix else {
                continue;
            };
            let decoded_ix = match parsable_instructions::decode_instruction(&parsed_ix) {
                Ok(Some(decoded)) => decoded,
                Ok(None) => continue, // unrecognized instruction
                Err(err) => return Err(anyhow!("failed to decode instruction {err:#?}"))
            };
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
        let decoded_ix = match parsable_instructions::decode_instruction(&parsed_ix) {
            Ok(Some(decoded)) => decoded,
            Ok(None) => continue, // unrecognized instruction
            Err(err) => return Err(anyhow!("failed to decode instruction {err:#?}"))
        };
        // instruction numbering in existing explorers always starts at 1, however
        // when using `enumerate` which counts the position of the element in a vec
        // we need to offset idx by 1
        outer_instruction_by_index.insert((idx+1) as u8, decoded_ix);
    }

    // the structure of this is as follows
    // map[k1] = (transfer_1, map[k2] => transfers_2)
    // where transfers_1 is a transfer initiated by outer instruction k1
    // where transfers_2 are any transfers initiated by inner instruction k2
    //
    // if transfer_1 is None, it means the outer k1 instruction triggered no transfer
    let mut ordered_transfers: HashMap<u8, (Option<DecodedInstruction>, HashMap<u8, Vec<DecodedInstruction>>)> = Default::default();

    // first prepare the outer instructions
    for (idx, ix) in outer_instruction_by_index {
        if let Some((_, _)) = ordered_transfers.get_mut(&idx) {
            // pretty sure this condition will be impossible, should we panic?
            log::warn!("invalid condition detected!");
        } else {
            ordered_transfers.insert(idx, (Some(ix), Default::default()));
        }
    }

    for (idx, ixs) in inner_instruction_by_index {
        if let Some((outer_transfer, inner_transfers)) = ordered_transfers.get_mut(&idx) {
            if let Some(inner_transfers) = inner_transfers.get_mut(&idx) {
                // this case shouldn't happen, should we panic?
                log::warn!("invalid condition detected")
            } else {
                inner_transfers.insert(idx, ixs);
            }
        } else {
            // in this case the outer instruction which triggered this inner instruction didnt transfer any tokens
            // so we need to create the inital key in ordered_transfers
            let mut inner_ordered_transfers: HashMap<u8, Vec<DecodedInstruction>> = Default::default();
            inner_ordered_transfers.insert(idx, ixs);
            ordered_transfers.insert(idx, (None, inner_ordered_transfers));
        }
    }

    log::info!("{ordered_transfers:#?}");


    Ok(())
}