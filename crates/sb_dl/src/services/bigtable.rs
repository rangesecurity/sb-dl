use {
    crate::{config::BigTableConfig, types::BlockInfo, utils::process_block}, anyhow::{anyhow, Context}, bigtable_rs::{
        bigtable::{read_rows::decode_read_rows_response, BigTable, BigTableConnection},
        google::bigtable::v2::{row_filter::Filter, ReadRowsRequest, RowFilter, RowSet},
    }, futures::stream::{self, StreamExt}, solana_sdk::clock::Slot, solana_storage_bigtable::{
        bigtable::{deserialize_protobuf_or_bincode_cell_data, CellData},
        key_to_slot, slot_to_blocks_key, StoredConfirmedBlock,
    }, solana_storage_proto::convert::generated, solana_transaction_status::{ConfirmedBlock, UiConfirmedBlock}, std::{collections::HashSet, sync::{atomic::{AtomicBool, Ordering}, Arc}}, tokio::task::JoinSet,
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
        self: &Arc<Self>,
        blocks_tx: tokio::sync::mpsc::Sender<BlockInfo>,
        already_indexed: HashSet<u64>,
        start: Option<u64>,
        limit: Option<u64>,
        no_minimization: bool,
        threads: usize,
        exit_ch: tokio::sync::oneshot::Receiver<()>
    ) -> anyhow::Result<()> {

        // handle exit signals to prevent the concurrent streams from downloading bigtable blocks
        // and not being able to persist the data, this does not allow for a graceful exit
        // but prevents bigtable bandwidth consumption

        let exit = Arc::new(AtomicBool::new(false));
        {
            let exit = exit.clone();
            tokio::task::spawn(async move {
                let _ = exit_ch.await;
                exit.store(true, Ordering::SeqCst);
            });
    }
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
        log::info!("preparing slots to index");
        
        // get the list of slots to fetch, excluding any previously indexed slots from the specified range
        let slots_to_fetch = (start..start + limit)
            .into_iter()
            .filter(|slot| !already_indexed.contains(slot))
            .collect::<Vec<solana_program::clock::Slot>>();
        
        log::info!("starting downloader");

        // instantiate the client which will be cloned between threads
        let client = self.conn.client();
        stream::iter(slots_to_fetch).map(|slot| {
            let blocks_tx = blocks_tx.clone();
            let client = client.clone();
            let max_decoding_size = self.max_decoding_size;
            let exit = exit.clone();
            async move {
                if !exit.load(Ordering::SeqCst) {
                    match Self::get_confirmed_block(client, max_decoding_size, slot).await {
                        Ok(block) => {
                            if let Some(block) = block {
                                let block_height = if let Some(block_height) = block.block_height {
                                    block_height
                                } else {
                                    log::warn!("block({slot}) height is none");
                                    return;
                                };
                                // post process the block to handle encoding and space minimization
                                match process_block(block, no_minimization) {
                                    Ok(block) => {
                                        if let Err(err) = blocks_tx
                                            .send(BlockInfo {
                                                block_height,
                                                slot: Some(slot),
                                                block,
                                            })
                                            .await
                                        {
                                            log::error!("failed to send block({slot}) {err:#?}");
                                        } else {
                                            log::debug!("processed block({slot})");
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

            }
        })
        .buffer_unordered(threads)
        .collect::<Vec<_>>()
        .await;
        Ok(())
    }
    /// Downloads multiple blocks at once, returning a vector of vec![(block_slot, block_data)]
    pub async fn get_confirmed_blocks(
        &self,
        slots: &[Slot]
    ) -> anyhow::Result<Vec<(u64, ConfirmedBlock)>> {
        let mut client = self.conn.client();

        let mut big_client = client
            .get_client()
            .clone()
            .max_decoding_message_size(self.max_decoding_size);

        let response = decode_read_rows_response(
            &None,
            big_client
                .read_rows(ReadRowsRequest {
                    table_name: client.get_full_table_name("blocks"),
                    app_profile_id: "default".to_string(),
                    rows_limit: slots.len() as i64,
                    rows: Some(RowSet {
                        row_keys: slots.into_iter().map(|slot| slot_to_blocks_key(*slot).into()).collect(),
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
                .with_context(|| format!("failed to get blocks"))?
                .into_inner(),
        )
        .await
        .with_context(|| "failed to decode response")?;
        Ok(response.into_iter().filter_map(|mut response| {
            if response.1.len() != 1 {
                log::error!("cell contains no data");
                return None;
            }
            let key = match String::from_utf8(response.0) {
                Ok(key) => key,
                Err(err) => {
                    log::error!("failed to parse key {err:#?}");
                    return None;
                }
            };
            if response.1[0].qualifier.is_empty() {
                log::warn!("empty qualifier(key={key})");
                return None;
            }
            let cell_name = match String::from_utf8(
                std::mem::take(&mut response.1[0].qualifier)
            ) {
                Ok(cell_name) => cell_name,
                Err(err) => {
                    log::error!("failed to parse qualifier(key={key}) {err:#?}");
                    return None
                }
            };
            if response.1[0].value.is_empty() {
                log::error!("empty value(key={key})");
                return None;
            }
            let slot = match key_to_slot(&key) {
                Some(slot) => slot,
                None => {
                    log::error!("failed to parse key_to_slot(key={key})");
                    return None;
                }
            };

            let cell_data = match deserialize_protobuf_or_bincode_cell_data::<
                StoredConfirmedBlock,
                generated::ConfirmedBlock,
            >(
                &[(cell_name, std::mem::take(&mut response.1[0].value))],
                "blocks",
                key,
            ) {
                Ok(cell_data) => cell_data,
                Err(err) => {
                    log::error!("failed to deserialize(slot={slot}) {err:#?}");
                    return None;
                }
            };
            let confirmed_block: ConfirmedBlock = match cell_data {
                CellData::Bincode(block) => block.into(),
                CellData::Protobuf(block) => match block.try_into() {
                    Ok(block) => block,
                    Err(err) => {
                        log::error!("failed to parse cell_data(slot={slot}) {err:#?}");
                        return None;
                    }
                },
            };
            Some((slot, confirmed_block))
        }).collect::<Vec<_>>())

     
    }
    pub async fn get_confirmed_block(
        mut client: BigTable,
        max_decoding_size: usize,
        slot: Slot
    ) -> anyhow::Result<Option<ConfirmedBlock>> {
        let mut big_client = client
            .get_client()
            .clone()
            .max_decoding_message_size(max_decoding_size);

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
