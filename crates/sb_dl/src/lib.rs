pub mod config;
pub mod utils;

use {
    anyhow::{anyhow, Context},
    bigtable_rs::{
        bigtable::{read_rows::decode_read_rows_response, BigTableConnection},
        google::bigtable::v2::{row_filter::Filter, ReadRowsRequest, RowFilter, RowSet},
    },
    config::BigTableConfig,
    solana_sdk::clock::Slot,
    solana_storage_bigtable::{
        bigtable::{deserialize_protobuf_or_bincode_cell_data, CellData},
        key_to_slot, slot_to_blocks_key, StoredConfirmedBlock,
    },
    solana_storage_proto::convert::generated,
    solana_transaction_status::{ConfirmedBlock, UiConfirmedBlock},
    std::collections::HashSet,
    utils::process_block,
};

#[derive(Clone)]
pub struct Downloader {
    conn: BigTableConnection,
    max_decoding_size: usize,
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
            max_decoding_size: cfg.max_decoding_size,
        })
    }
    /// Starts the bigtable downloader
    ///
    /// # Parameters
    ///
    /// `blocks_tx`: channel which downloaded blocks are sent too
    /// `already_indexed`: the slots for which we have already downloaded blocks
    /// `start`: optional slot to start downloading from, if None starts at slot 0
    /// `limit`: max number of slots to index, if None use latest slot as bound
    pub async fn start(
        &self,
        blocks_tx: tokio::sync::mpsc::Sender<(u64, UiConfirmedBlock)>,
        already_indexed: HashSet<u64>,
        start: Option<u64>,
        limit: Option<u64>,
        no_minimization: bool,
    ) -> anyhow::Result<()> {
        let start = match start {
            Some(start) => start,
            None => 0,
        };

        let limit = match limit {
            Some(limit) => limit,
            // TODO: use latest slot, while not exactly critical is a "nice to have" feature
            // although this requires using a solana rpc
            None => u64::MAX - start,
        };

        // get the list of slots to fetch, excluding any previously indexed slots from the specified range
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

        for slot in slots_to_fetch {
            // retrieve confirmed block from bigtable
            match self.get_confirmed_block(slot).await {
                Ok(block) => {
                    if let Some(block) = block {
                        // post process the block to handle encoding and space minimization
                        match process_block(block, no_minimization) {
                            Ok(block) => {
                                if let Err(err) = blocks_tx.send((slot, block)).await {
                                    log::error!("failed to send block({slot}) {err:#?}");
                                }
                            }
                            Err(err) => {
                                log::error!("failed to minimize and encode block({slot}) {err:#?}");
                            }
                        }
                    }
                }
                Err(err) => {
                    log::error!("failed to fetch block({slot}) {err:#?}");
                }
            }
        }

        Ok(())
    }

    pub async fn get_confirmed_block(&self, slot: Slot) -> anyhow::Result<Option<ConfirmedBlock>> {
        let mut client = self.conn.client();

        let mut big_client = client
            .get_client()
            .clone()
            .max_decoding_message_size(self.max_decoding_size);

        let mut response = decode_read_rows_response(
            &None,
            big_client
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
                .with_context(|| format!("failed to get block for slot({slot})"))?
                .into_inner(),
        )
        .await
        .with_context(|| "failed to decode response")?;
    
        // ensure we got a single response, as we requested 1 slot
        if response.len() != 1 {
            // block does not exist and cant be found
            return Ok(None);
        }

        // ensure the cell contains some data
        if response[0].1.len() != 1 {
            return Err(anyhow!(
                "mismatched cell count for slot({slot}). got {} want {}",
                response[0].1.len(),
                1
            ));
        }

        // parse the key from the response
        let key = String::from_utf8(std::mem::take(&mut response[0].0))
            .with_context(|| format!("failed to parse key for slot({slot})"))?;

        // verify that the response is for the slot we requested, probably a bit excessive
        match key_to_slot(&key) {
            Some(keyed_slot) => {
                if keyed_slot != slot {
                    return Err(anyhow!("keyed_slot({keyed_slot}) != slot({slot}"));
                }
            }
            None => return Err(anyhow!("failed to parse key to slot({slot})")),
        }

        if response[0].1[0].qualifier.is_empty() {
            return Err(anyhow!("empty qualifier for slot({slot})"));
        }

        let cell_name = String::from_utf8(std::mem::take(&mut response[0].1[0].qualifier))
            .with_context(|| format!("failed to parse cell_name for slot({slot})"))?;

        if response[0].1[0].value.is_empty() {
            return Err(anyhow!("empty value for slot({slot})"));
        }

        let cell_data = deserialize_protobuf_or_bincode_cell_data::<
            StoredConfirmedBlock,
            generated::ConfirmedBlock,
        >(
            &[(cell_name, std::mem::take(&mut response[0].1[0].value))],
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
        Ok(Some(confirmed_block))
    }
}
