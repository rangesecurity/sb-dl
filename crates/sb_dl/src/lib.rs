pub mod config;
pub mod types;
pub mod logger;
pub mod solana_bigtable;

use {
    anyhow::{anyhow, Context}, bigtable_rs::{bigtable::{BigTableConnection, RowCell}, google::bigtable::v2::{row_filter::Filter, ReadRowsRequest, RowFilter, RowSet}}, config::BigTableConfig, solana_bigtable::{key_to_slot, slot_to_blocks_key, slot_to_key}, solana_sdk::clock::Slot, solana_storage_bigtable::{bigtable::{deserialize_protobuf_or_bincode_cell_data, CellData}, StoredConfirmedBlock}, solana_storage_proto::convert::generated, solana_transaction_status::ConfirmedBlock, std::collections::HashSet, types::SerializableConfirmedBlock
};

#[derive(Clone)]
pub struct Downloader {
    conn: BigTableConnection,
}

impl Downloader {
    pub async fn new(
        cfg: BigTableConfig,
    ) -> anyhow::Result<Self> {
        std::env::set_var("GOOGLE_APPLICATION_CREDENTIALS", cfg.credentials_file);
        let bigtable_conn = BigTableConnection::new(
            &cfg.project_id,
            &cfg.instance_name,
            true,
            cfg.channel_size,
            Some(cfg.timeout)
        ).await.with_context(|| "failed to initialize bigtable connection")?;
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
    ) -> anyhow::Result<Vec<(solana_program::clock::Slot, SerializableConfirmedBlock)>> {
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
        let mut slots: Vec<(solana_program::clock::Slot, SerializableConfirmedBlock)> = vec![];
        for slot in slots_to_fetch {
            self.get_confirmed_block(slot).await?;
            //slots.push((slot, From::from(block)))
        }
        /*let slots = self
            .ls
            .get_confirmed_blocks_with_data(&slots_to_fetch)
            .await
            .with_context(|| "failed to get slots")?
            .map(|(slot, block)| (slot, From::from(block)))
            .collect::<Vec<_>>();*.

        slots.iter().for_each(|(slot, _)| {
            already_indexed.insert(*slot);
        });*/
        Ok(slots)
    }
    async fn get_confirmed_block(
        &self,
        slot: Slot,
    ) -> anyhow::Result<ConfirmedBlock> {
        let mut client = self.conn.client();
        let data = tokio::fs::read_to_string("data.json").await?;
        let block: SerializedBlock = serde_json::from_str(&data).unwrap();
        let key = slot_to_key(block.slot);
        let cell_name = String::from_utf8(block.cells[0].qualifier.clone()).unwrap();
        let cell_data = deserialize_protobuf_or_bincode_cell_data::<StoredConfirmedBlock, generated::ConfirmedBlock>(
            &[(cell_name, block.cells[0].value.clone())],
            "blocks",
            key,
        ).unwrap();
        println!("decoded cell data");
        let c_block: ConfirmedBlock = match cell_data {
            CellData::Bincode(block) => {
                block.into()
            }
            CellData::Protobuf(block) => {
                block.try_into().unwrap()
            }
        };
        println!("txns {}", c_block.transactions.len());
        println!("decoded block");
        /*let response = client.read_rows(ReadRowsRequest {
            table_name: client.get_full_table_name("blocks"),
            app_profile_id: "default".to_string(),
            rows_limit: 1,
            rows: Some(RowSet {
                row_keys: vec![slot_to_blocks_key(slot).into()],
                row_ranges: vec![]
            }),
            filter: Some(RowFilter {
                    // Only return the latest version of each cell
                    filter: Some(Filter::CellsPerColumnLimitFilter(1)),
            }),
            request_stats_view: 0,
            reversed: false,
        }).await.with_context(|| "Failed to get confirmed block")?;
        println!("response_count {}", response.len());
        for res in response {
            let slot = match String::from_utf8(res.0) {
                Ok(name) => {
                    if let Some(slot) = key_to_slot(&name) {
                        println!("slot {slot}");
                        slot
                    } else {
                        println!("failed to convert key to slot");
                        continue;
                    }
                },
                Err(err) => {
                    println!("failed to decode {err:#?}");
                    continue;
                }
            };
            let to_save = SerializedBlock {
                slot,
                cells: res.1.into_iter().map(|r| From::from(r)).collect(),
            };
            tokio::fs::write("data.json", serde_json::to_string(&to_save).unwrap()).await.unwrap();
        }
        */Err(anyhow!("TODO"))
    }
}


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
            labels: value.labels
        }
    }
}