use std::collections::HashMap;

use anyhow::{anyhow, Context};
use db::{client::BlockFilter, new_connection};
use sb_dl::{
    config::Config,
    parsable_instructions::{self, token::TokenInstructions, DecodedInstruction}, transfer_flow::prepare_transfer_flow_for_tx,
};
use solana_transaction_status::{
    option_serializer::OptionSerializer, EncodedTransaction, UiConfirmedBlock, UiInnerInstructions,
    UiInstruction, UiMessage, UiParsedInstruction, UiTransactionTokenBalance,
};

#[derive(Clone)]
pub struct TokenOwnerInfo {
    pub mint: String,
    pub owner: String,
    pub account_index: u8,
}

pub async fn create_transfer_graph(
    matches: &clap::ArgMatches,
    config_path: &str,
) -> anyhow::Result<()> {
    let cfg: Config = Config::load(config_path).await?;
    // slot to pull tx from
    let slot_number = matches.get_one::<i64>("slot-number").unwrap();
    // tx to generate graph for
    let tx_hash = matches.get_one::<String>("tx-hash").unwrap();
    let mut db_conn = new_connection(&cfg.db_url)?;
    let client = db::client::Client {};
    let mut block = client.select_block(&mut db_conn, BlockFilter::Slot(*slot_number))?;
    let block = if block.is_empty() {
        return Err(anyhow!("no block found"));
    } else {
        std::mem::take(&mut block[0])
    };

    let block: UiConfirmedBlock = serde_json::from_value(block.data)?;

    let ordered_transfers = prepare_transfer_flow_for_tx(block, tx_hash)?;
    log::info!("{ordered_transfers:#?}");
    return Ok(());
}
