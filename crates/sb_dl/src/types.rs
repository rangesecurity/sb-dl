use solana_transaction_status::UiConfirmedBlock;

#[derive(Clone)]
pub struct BlockInfo {
    pub block_height: u64,
    pub slot: Option<u64>,
    pub block: UiConfirmedBlock,
}
