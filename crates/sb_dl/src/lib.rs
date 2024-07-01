pub mod config;
pub mod types;

use {
    anyhow::Context,
    solana_storage_bigtable::{LedgerStorage, LedgerStorageConfig},
    std::collections::HashSet,
    types::SerializableConfirmedBlock,
};

#[derive(Clone)]
pub struct Downloader {
    ls: LedgerStorage,
}

impl Downloader {
    pub async fn new(ledger_config: LedgerStorageConfig) -> anyhow::Result<Self> {
        let ls = LedgerStorage::new_with_config(ledger_config)
            .await
            .with_context(|| "failed to initialize LedgerStorage")?;
        Ok(Self { ls })
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
        let slots_to_fetch = (start..start+limit)
            .into_iter()
            .filter_map(|slot| {
                if already_indexed.contains(&slot) {
                    None
                } else {
                    Some(solana_program::clock::Slot::from(slot))
                }
            })
            .collect::<Vec<solana_program::clock::Slot>>();
        let slots = self
            .ls
            .get_confirmed_blocks_with_data(&slots_to_fetch)
            .await
            .with_context(|| "failed to get slots")?
            .map(|(slot, block)| (slot, From::from(block)))
            .collect::<Vec<_>>();

        slots.iter().for_each(|(slot, _)| {
            already_indexed.insert(*slot);
        });
        Ok(slots)
    }
}
