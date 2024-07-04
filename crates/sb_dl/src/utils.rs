use anyhow::Context;
use solana_sdk::message::VersionedMessage;
use solana_transaction_status::{
    BlockEncodingOptions, ConfirmedBlock, TransactionDetails, TransactionWithStatusMeta,
    UiConfirmedBlock, UiTransactionEncoding,
};

/// Performs the following
///
/// > Space minimization
///    > filter vote transactions
///    > Exclude rewards
///
/// > Encodeds with UiTransactionEncoding for easier parsing
pub fn minimize_and_encode_block(block: ConfirmedBlock) -> anyhow::Result<UiConfirmedBlock> {
    let block = filter_vote_transactions(block);
    block
        .encode_with_options(
            UiTransactionEncoding::JsonParsed,
            BlockEncodingOptions {
                transaction_details: TransactionDetails::Full,
                show_rewards: false,
                // current tx version is 0, but set to 1 for future compatibility
                // if at some point tx version goes to 2, we would need to update this code
                max_supported_transaction_version: Some(1),
            },
        )
        .with_context(|| "failed to encode block")
}

/// To save space on indexing, exclude all vote/consensus related transactions
pub fn filter_vote_transactions(mut block: ConfirmedBlock) -> ConfirmedBlock {
    block.transactions = block
        .transactions
        .into_iter()
        .filter(|tx| {
            match tx {
                TransactionWithStatusMeta::MissingMetadata(tx) => {
                    let msg = tx.message();
                    if msg.instructions.len() == 1 && msg.account_keys.len() > 0 {
                        let pid_index = msg.instructions[0].program_id_index;
                        if msg.account_keys.len() - 1 > pid_index as usize {
                            log::warn!("found unparsable instruction");
                            return false;
                        }
                        if msg.account_keys[pid_index as usize] == solana_sdk::vote::program::id() {
                            false
                        } else {
                            true
                        }
                    } else {
                        // not a vote transaction
                        true
                    }
                }
                TransactionWithStatusMeta::Complete(tx) => {
                    let msg = &tx.transaction.message;
                    match msg {
                        VersionedMessage::Legacy(legacy_msg) => {
                            if legacy_msg.instructions.len() == 1
                                && legacy_msg.account_keys.len() > 0
                            {
                                let pid_index = legacy_msg.instructions[0].program_id_index;
                                if legacy_msg.account_keys.len() - 1 > pid_index as usize {
                                    log::warn!("found unparsable instruction");
                                    return false;
                                }
                                if legacy_msg.account_keys[pid_index as usize]
                                    == solana_sdk::vote::program::id()
                                {
                                    false
                                } else {
                                    true
                                }
                            } else {
                                true
                            }
                        }
                        VersionedMessage::V0(v0_msg) => {
                            if v0_msg.instructions.len() == 1 && v0_msg.account_keys.len() > 0 {
                                let pid_index = v0_msg.instructions[0].program_id_index;
                                if v0_msg.account_keys.len() - 1 > pid_index as usize {
                                    log::warn!("found unparsable instruction");
                                    return false;
                                }
                                if v0_msg.account_keys[pid_index as usize]
                                    == solana_sdk::vote::program::id()
                                {
                                    false
                                } else {
                                    true
                                }
                            } else {
                                true
                            }
                        }
                    }
                }
            }
        })
        .collect();
    block
}

#[cfg(test)]
mod test {
    use bigtable_rs::bigtable::RowCell;
    use solana_storage_bigtable::{
        bigtable::{deserialize_protobuf_or_bincode_cell_data, CellData},
        StoredConfirmedBlock,
    };
    use solana_storage_proto::convert::generated;

    use crate::solana_bigtable::slot_to_key;

    use super::*;
    #[derive(serde::Serialize, serde::Deserialize)]
    pub struct SerializedBlock {
        pub slot: u64,
        pub cells: Vec<RowCellSerializable>,
    }

    #[derive(serde::Serialize, serde::Deserialize)]
    pub struct RowCellSerializable {
        pub family_name: String,
        pub qualifier: Vec<u8>,
        pub value: Vec<u8>,
        pub timestamp_micros: i64,
        pub labels: Vec<String>,
    }

    impl From<RowCell> for RowCellSerializable {
        fn from(value: RowCell) -> Self {
            Self {
                family_name: value.family_name,
                qualifier: value.qualifier,
                value: value.value,
                timestamp_micros: value.timestamp_micros,
                labels: value.labels,
            }
        }
    }
    #[tokio::test]
    async fn test_minimize_and_filter() {
        let block_data = tokio::fs::read_to_string("../../testdata/block_275131467.json")
            .await
            .unwrap();
        let block: SerializedBlock = serde_json::from_str(&block_data).unwrap();
        let key = slot_to_key(block.slot);
        let cell_name = String::from_utf8(block.cells[0].qualifier.clone()).unwrap();
        let cell_data = deserialize_protobuf_or_bincode_cell_data::<
            StoredConfirmedBlock,
            generated::ConfirmedBlock,
        >(&[(cell_name, block.cells[0].value.clone())], "blocks", key)
        .unwrap();

        let mut c_block: ConfirmedBlock = match cell_data {
            CellData::Bincode(block) => block.into(),
            CellData::Protobuf(block) => block.try_into().unwrap(),
        };
        assert_eq!(c_block.transactions.len(), 1109);
        let ui_block = minimize_and_encode_block(c_block).unwrap();
        assert_eq!(ui_block.transactions.unwrap().len(), 405);
    }
}
