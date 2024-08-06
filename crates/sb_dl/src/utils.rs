use {
    anyhow::Context,
    solana_sdk::pubkey::Pubkey,
    solana_transaction_status::{
        BlockEncodingOptions, ConfirmedBlock, EncodedTransaction, TransactionDetails,
        UiConfirmedBlock, UiInstruction, UiMessage, UiParsedInstruction, UiTransactionEncoding,
    },
    std::str::FromStr,
    tracing_subscriber::{filter::LevelFilter, prelude::*, EnvFilter, Layer},
};

/// Performs the following
///
/// > Space minimization (optional)
///    > filter vote transactions
///    > Exclude rewards
///
/// > Encodeds with UiTransactionEncoding for easier parsing
pub fn process_block(
    block: ConfirmedBlock,
    no_minimization: bool,
) -> anyhow::Result<UiConfirmedBlock> {
    let mut block = block
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
        .with_context(|| "failed to encode block")?;
    if no_minimization == false {
        block = filter_vote_transactions(block);
    }
    Ok(block)
}

/// To save space on indexing, exclude all vote/consensus related transactions
pub fn filter_vote_transactions(mut block: UiConfirmedBlock) -> UiConfirmedBlock {
    if let Some(txs) = block.transactions {
        block.transactions = Some(
            txs.into_iter()
                .filter(|tx| match &tx.transaction {
                    EncodedTransaction::Json(tx) => {
                        match &tx.message {
                            UiMessage::Parsed(msg) => {
                                if msg.instructions.len() == 1 && msg.account_keys.len() > 0 {
                                    match &msg.instructions[0] {
                                        UiInstruction::Compiled(ix) => {
                                            let pid_index = ix.program_id_index;
                                            if let Ok(pid) = Pubkey::from_str(
                                                &msg.account_keys[pid_index as usize].pubkey,
                                            ) {
                                                if pid == solana_sdk::vote::program::id() {
                                                    return false;
                                                }
                                            }
                                        }
                                        UiInstruction::Parsed(ix) => match ix {
                                            UiParsedInstruction::PartiallyDecoded(ix) => {
                                                if let Ok(pid) = Pubkey::from_str(&ix.program_id) {
                                                    if pid == solana_sdk::vote::program::id() {
                                                        return false;
                                                    }
                                                }
                                            }
                                            UiParsedInstruction::Parsed(ix) => {
                                                if let Ok(pid) = Pubkey::from_str(&ix.program_id) {
                                                    if pid == solana_sdk::vote::program::id() {
                                                        return false;
                                                    }
                                                }
                                            }
                                        },
                                    }
                                }
                            }
                            UiMessage::Raw(msg) => {
                                if msg.instructions.len() == 1 && msg.account_keys.len() > 0 {
                                    let pid_index = msg.instructions[0].program_id_index;
                                    if let Ok(pid) =
                                        Pubkey::from_str(&msg.account_keys[pid_index as usize])
                                    {
                                        if pid == solana_sdk::vote::program::id() {
                                            return false;
                                        }
                                    }
                                }
                            }
                        }
                        true
                    }
                    _ => {
                        log::error!("found incorrectly encoded tx");
                        false
                    }
                })
                .collect(),
        );
    }
    block
}

/// initializes logging capabilities but adds a variety of customization, including file+line which sourced the log,
/// a tokio-console used for monitoring async tasks, as well as log-level filtration
pub fn init_log(level: &str, file: &str) {
    let mut layers = Vec::with_capacity(2);
    let level_filter = LevelFilter::from_level(tracing::Level::from_str(level).unwrap());
    let filter = EnvFilter::from_default_env().add_directive(level_filter.into());

    layers.push(
        tracing_subscriber::fmt::layer()
            .with_level(true)
            .with_line_number(true)
            .with_file(true)
            .with_filter(filter)
            .boxed(),
    );
    if file != "" {
        let log_file = std::fs::File::options()
            .create(true)
            .append(true)
            .open(file)
            .unwrap();
        layers.push(
            tracing_subscriber::fmt::layer()
                .json()
                .with_writer(log_file)
                .with_filter(EnvFilter::from_default_env().add_directive(level_filter.into()))
                .boxed(),
        );
    }
    if let Err(err) = tracing_subscriber::registry().with(layers).try_init() {
        log::warn!("global subscriber already registered {err:#?}");
    }
}

#[cfg(test)]
mod test {
    use bigtable_rs::bigtable::RowCell;
    use solana_storage_bigtable::{
        bigtable::{deserialize_protobuf_or_bincode_cell_data, CellData},
        slot_to_key, StoredConfirmedBlock,
    };
    use solana_storage_proto::convert::generated;

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

        let c_block: ConfirmedBlock = match cell_data {
            CellData::Bincode(block) => block.into(),
            CellData::Protobuf(block) => block.try_into().unwrap(),
        };
        assert_eq!(c_block.transactions.len(), 1109);
        let ui_block = process_block(c_block.clone(), true).unwrap();
        assert_eq!(ui_block.transactions.unwrap().len(), 1109);
        let ui_block = process_block(c_block.clone(), false).unwrap();
        assert_eq!(ui_block.transactions.unwrap().len(), 405);
    }
}
