use std::{str::FromStr, time::Duration};
use chrono::prelude::*;
use db::{client::Client, migrations::run_migrations, new_connection};
use diesel::Connection;
use sb_dl::{config::Config, programs::squads::{v3::MultisigV3, v4::{MultisigV4, Permission as PermissionV4}}, services::{idl_indexer::IdlIndexer, squads_indexer::SquadsIndexer}};
use solana_sdk::pubkey::Pubkey;

pub async fn index_multisigs(matches: &clap::ArgMatches, config_path: &str) -> anyhow::Result<()> {
    let frequency = Duration::from_secs(*matches.get_one::<u64>("frequency").unwrap());

    let cfg = Config::load(config_path).await?;
    let indexer = SquadsIndexer::new(cfg.rpc_url.clone());
    let mut conn = new_connection(&cfg.db_url)?;
    
    run_migrations(&mut conn);

    let mut ticker = tokio::time::interval(frequency);
    
    let client = Client{};

    loop {
        ticker.tick().await;
        let v4_msigs = indexer.fetch_multisigs_v4().await?;
        log::info!("found {} v4 multisig accounts", v4_msigs.len());
        let start = Utc::now();
        if let Err(err) = conn.transaction::<_, anyhow::Error, _>(|conn| {
            for (account, msig_info) in v4_msigs.into_iter() {
                let vault = MultisigV4::derive_vault_pda(&account, 0).0;
                if let Err(err) = client.insert_or_update_squads(
                    conn,
                    &account.to_string(),
                    &[vault.to_string()],
                    &msig_info.members
                    .into_iter()
                    .filter_map(|member| {
                        if !member.permissions.has(
                        PermissionV4::Vote
                        ) {
                            None
                        } else {
                            Some(member.key.to_string())
                        }
                    })
                    .collect::<Vec<_>>(),
                    msig_info.threshold as i32,
                    4
                ) {
                    log::error!("failed to record multisig(v4=true, account={account}) {err:#?}");
                }
            }
            Ok(())
        }) {
            log::error!("failed to insert v4 multisigs {err:#?}");
        }
        log::info!("took {} seconds to insert v4 records", Utc::now().signed_duration_since(start).num_seconds());

        let v3_msigs = indexer.fetch_multisigs_v3().await?;
        log::info!("found {} v3 multisig accounts", v3_msigs.len());
        let start = Utc::now();
        if let Err(err) = conn.transaction::<_, anyhow::Error,_>(|conn| {
            for (account, msig_info) in v3_msigs.into_iter() {
                // vault index 0 is reserved for internal usage only
                let vaults = (1..=msig_info.authority_index).into_iter().map(|idx| {
                    MultisigV3::derive_vault_pda(&account, idx as u32).0.to_string()
                }).collect::<Vec<_>>();
                if let Err(err) = client.insert_or_update_squads(
                    conn,
                    &account.to_string(),
                    &vaults,
                    &msig_info.keys.into_iter().map(|key| key.to_string()).collect::<Vec<_>>(),
                    msig_info.threshold as i32,
                    3
                ) {
                    log::error!("failed to record multisig(v3=true, account={account}) {err:#?}");
                }
            }
            Ok(())
        }) {
            log::error!("failed to insert v3 multisigs {err:#?}");
        }
        log::info!("took {} seconds to insert v3 records", Utc::now().signed_duration_since(start).num_seconds());
    }
    Ok(())
}
