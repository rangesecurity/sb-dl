pub mod config;
pub mod logger;
pub mod solana_bigtable;
pub mod types;
pub mod utils;

use {
    anyhow::{anyhow, Context},
    bigtable_rs::{
        bigtable::{BigTableConnection, RowCell},
        google::bigtable::v2::{row_filter::Filter, ReadRowsRequest, RowFilter, RowSet},
    },
    config::BigTableConfig,
    solana_bigtable::{key_to_slot, slot_to_blocks_key, slot_to_key},
    solana_sdk::{clock::Slot, message::VersionedMessage},
    solana_storage_bigtable::{
        bigtable::{deserialize_protobuf_or_bincode_cell_data, CellData},
        StoredConfirmedBlock,
    },
    solana_storage_proto::convert::generated,
    solana_transaction_status::{ConfirmedBlock, TransactionWithStatusMeta, UiConfirmedBlock},
    std::collections::HashSet,
    types::SerializableConfirmedBlock,
    utils::minimize_and_encode_block,
};

#[derive(Clone)]
pub struct Downloader {
    conn: BigTableConnection,
}

impl Downloader {
    pub async fn new(cfg: BigTableConfig) -> anyhow::Result<Self> {
        std::env::set_var("GOOGLE_APPLICATION_CREDENTIALS", cfg.credentials_file);
        let bigtable_conn = BigTableConnection::new(
            &cfg.project_id,
            &cfg.instance_name,
            true,
            cfg.channel_size,
            Some(cfg.timeout),
        )
        .await
        .with_context(|| "failed to initialize bigtable connection")?;
        Ok(Self {
            conn: bigtable_conn,
        })
    }
    /// Starts the bigtable downloader
    ///
    /// # Parameters
    ///
    /// `already_indexed`: the slots for which we have already downloaded blocks
    /// `start`: optional slot to start downloading from, if None starts at slot 0
    /// `limit`: max number of slots to index, if None use latest slot as bound
    pub async fn start(
        &self,
        already_indexed: &mut HashSet<u64>,
        start: Option<u64>,
        limit: Option<u64>,
    ) -> anyhow::Result<Vec<(solana_program::clock::Slot, UiConfirmedBlock)>> {
        let start = match start {
            Some(start) => start,
            None => 0,
        };
        let limit = match limit {
            Some(limit) => limit,
            None => u64::MAX - start, // TODO: use latest slot
        };
        let slots_to_fetch = (start..start + limit)
            .into_iter()
            .filter_map(|slot| {
                if already_indexed.contains(&slot) {
                    None
                } else {
                    Some(solana_program::clock::Slot::from(slot))
                }
            })
            .collect::<Vec<solana_program::clock::Slot>>();
        let mut slots: Vec<(solana_program::clock::Slot, UiConfirmedBlock)> = vec![];
        for slot in slots_to_fetch {
            match self.get_confirmed_block(slot).await {
                Ok(block) => {
                    match minimize_and_encode_block(block) {
                        Ok(block) => {
                            already_indexed.insert(slot);
                            slots.push((slot, block))
                        }
                        Err(err) => {
                            log::error!("failed to minimize and encode block({slot}) {err:#?}");
                        }
                    }
                    already_indexed.insert(slot);
                }
                Err(err) => {
                    log::error!("failed to fetch block({slot}) {err:#?}");
                }
            }
        }
        Ok(slots)
    }

    pub async fn get_confirmed_block(&self, slot: Slot) -> anyhow::Result<ConfirmedBlock> {
        let mut client = self.conn.client();
        let response = client
            .read_rows(ReadRowsRequest {
                table_name: client.get_full_table_name("blocks"),
                app_profile_id: "default".to_string(),
                rows_limit: 1,
                rows: Some(RowSet {
                    row_keys: vec![slot_to_blocks_key(slot).into()],
                    row_ranges: vec![],
                }),
                filter: Some(RowFilter {
                    // Only return the latest version of each cell
                    filter: Some(Filter::CellsPerColumnLimitFilter(1)),
                }),
                request_stats_view: 0,
                reversed: false,
            })
            .await
            .with_context(|| format!("failed to get block for slot({slot})"))?;

        // todo: is this needed? need more testing to confirm
        if response.len() != 1 {
            return Err(anyhow!(
                "mismatched response length. got {}, want {}",
                response.len(),
                1
            ));
        }

        // parse the key from the response
        let key = String::from_utf8(response[0].0.clone())
            .with_context(|| format!("failed to parse key for slot({slot})"))?;

        // do we need to do this? it seems excessive
        /*match key_to_slot(&key).with_context(|| format!("failed to parse slot({slot}) response")) {
            Some(keyed_slot) => {
                if keyed_slot != slot {
                    return Err(anyhow!("keyed_slot({keyed_slot}) != slot({slot}"))
                }
                keyed_slot
            }
            None => return Err(anyhow!("failed to parse key to slot"))
        }*/

        if response[0].1.len() != 1 {
            return Err(anyhow!(
                "mismatched cell count for slot({slot}). got {} want {}",
                response[0].1.len(),
                1
            ));
        }

        let cell_name = String::from_utf8(response[0].1[0].qualifier.clone())
            .with_context(|| format!("failed to parse cell_name for slot({slot})"))?;

        let cell_data = deserialize_protobuf_or_bincode_cell_data::<
            StoredConfirmedBlock,
            generated::ConfirmedBlock,
        >(
            &[(cell_name, response[0].1[0].value.clone())],
            "blocks",
            key,
        )
        .with_context(|| "failed to decode cell data for slot({slot})")?;

        let confirmed_block: ConfirmedBlock = match cell_data {
            CellData::Bincode(block) => block.into(),
            CellData::Protobuf(block) => match block.try_into() {
                Ok(block) => block,
                Err(err) => {
                    return Err(anyhow!(
                        "failed to parse cell_data for slot({slot}) {err:#?}"
                    ))
                }
            },
        };
        Ok(confirmed_block)
    }
}
