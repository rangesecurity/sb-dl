use {
    anyhow::anyhow,
    db::{client::BlockFilter, new_connection},
    sb_dl::{
        config::Config,
        transfer_flow::{
            create_ordered_transfer_for_block, prepare_transfer_flow_for_tx_hash,
            transfer_graph::prepare_transfer_graph,
        },
    },
    solana_transaction_status::UiConfirmedBlock,
};

#[derive(Clone)]
pub struct TokenOwnerInfo {
    pub mint: String,
    pub owner: String,
    pub account_index: u8,
}

pub async fn create_transfer_graph_for_tx(
    matches: &clap::ArgMatches,
    config_path: &str,
) -> anyhow::Result<()> {
    log::warn!("assumes querying blocks table");
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

    let ordered_transfers = prepare_transfer_flow_for_tx_hash(block, tx_hash)?;
    log::info!("{ordered_transfers:#?}");
    prepare_transfer_graph(ordered_transfers)?;
    //log::info!("{ordered_transfers:#?}");
    return Ok(());
}

pub async fn create_ordered_transfers_for_entire_block(
    matches: &clap::ArgMatches,
    config_path: &str,
) -> anyhow::Result<()> {
    log::warn!("assumes querying blocks table");
    let cfg: Config = Config::load(config_path).await?;
    // slot to pull tx from
    let slot_number = matches.get_one::<i64>("slot-number").unwrap();
    // tx to generate graph for
    let mut db_conn = new_connection(&cfg.db_url)?;
    let client = db::client::Client {};
    let mut block = client.select_block(&mut db_conn, BlockFilter::Slot(*slot_number))?;
    let block = if block.is_empty() {
        return Err(anyhow!("no block found"));
    } else {
        std::mem::take(&mut block[0])
    };

    let block: UiConfirmedBlock = serde_json::from_value(block.data)?;
    let ordered_transfers = create_ordered_transfer_for_block(block)?;
    log::info!(
        "founds {} transfers for block({slot_number})",
        ordered_transfers.len()
    );
    return Ok(());
}
