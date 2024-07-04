//! wrapper types which implement serde::Serialize and serde::Deserialize
use serde::{Deserialize, Serialize};
use solana_sdk::{
    clock::{Slot, UnixTimestamp},
    transaction::{Transaction, VersionedTransaction},
};

use solana_transaction_status::{
    ConfirmedBlock, Rewards, TransactionStatusMeta, TransactionWithStatusMeta,
    UiTransactionStatusMeta,
};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum SerializableTransactionWithStatusMeta {
    MissingMetadata(Transaction),
    Complete(SerializableVersionedTransactionWithStatusMeta),
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SerializableVersionedTransactionWithStatusMeta {
    pub transaction: VersionedTransaction,
    pub meta: UiTransactionStatusMeta,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SerializableConfirmedBlock {
    pub previous_blockhash: String,
    pub blockhash: String,
    pub parent_slot: Slot,
    pub transactions: Vec<SerializableTransactionWithStatusMeta>,
    /// we use an option here to allow ignoring reward data
    ///
    /// this is useful if you want to minimize storage consumption by
    /// excluded all consensus related data
    pub rewards: Option<Rewards>,
    pub block_time: Option<UnixTimestamp>,
    pub block_height: Option<u64>,
}

impl From<ConfirmedBlock> for SerializableConfirmedBlock {
    fn from(value: ConfirmedBlock) -> Self {
        Self {
            previous_blockhash: value.previous_blockhash,
            blockhash: value.blockhash,
            parent_slot: value.parent_slot,
            transactions: value.transactions.into_iter().map(From::from).collect(),
            rewards: Some(value.rewards),
            block_time: value.block_time,
            block_height: value.block_height,
        }
    }
}

impl From<TransactionWithStatusMeta> for SerializableTransactionWithStatusMeta {
    fn from(value: TransactionWithStatusMeta) -> Self {
        match value {
            TransactionWithStatusMeta::Complete(tx) => {
                Self::Complete(SerializableVersionedTransactionWithStatusMeta {
                    transaction: tx.transaction,
                    meta: Into::into(tx.meta),
                })
            }
            TransactionWithStatusMeta::MissingMetadata(tx) => Self::MissingMetadata(tx),
        }
    }
}
