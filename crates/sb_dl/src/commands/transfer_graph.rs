use std::collections::HashMap;

use anyhow::{anyhow, Context};
use db::{client::BlockFilter, new_connection};
use sb_dl::{parsable_instructions, config::Config};
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
    // TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA::transfer
    // TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA::mintTo
    // TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA::burn
    // TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA::transferChecked
    // TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA::mintToChecked
    // TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA::burnChecked

    // TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb::transfer
    // TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb::mintTo
    // TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb::burn
    // TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb::transferChecked
    // TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb::mintToChecked
    // TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb::burnChecked

    // TODO: handle token extensions

    let expected_programs = vec![
        "11111111111111111111111111111111".to_string(), 
        "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA".to_string(), 
        "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb".to_string()
    ];

    // map outer instruction index => inner instructions
    let mut inner_instruction_by_index: HashMap<u8, Vec<UiInstruction>> = Default::default();

    for ix in inner_instructions {
        if let Some(indexed_ixs) = inner_instruction_by_index.get_mut(&ix.index) {
            indexed_ixs.append(&mut ix.instructions.clone());
        } else {
            inner_instruction_by_index.insert(ix.index, ix.instructions);
        }
    }

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
    }

    Ok(())
}