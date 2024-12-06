use db::models::NewBlock;
use solana_transaction_status::UiConfirmedBlock;
use chrono::prelude::*;

#[derive(Clone)]
pub struct BlockInfo {
    pub block_height: u64,
    pub slot: u64,
    pub time: Option<DateTime<Utc>>,
    pub block: UiConfirmedBlock,
}