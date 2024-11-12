use std::str::FromStr;

use db::{migrations::run_migrations, new_connection};
use sb_dl::{config::Config, services::{idl_indexer::IdlIndexer, squads_indexer::SquadsIndexer}};
use solana_sdk::pubkey::Pubkey;

pub async fn index_multisigs(config_path: &str) -> anyhow::Result<()> {
    let cfg = Config::load(config_path).await?;
    let indexer = SquadsIndexer::new(cfg.rpc_url.clone());
    let v3_msigs = indexer.fetch_multisigs_v3().await?;
    log::info!("found {} v3 multisigs", v3_msigs.len());
    drop(v3_msigs);
    let v4_msigs = indexer.fetch_multisigs_v4().await?;
    log::info!("found {} v4 multisigs", v4_msigs.len());
    Ok(())
}
