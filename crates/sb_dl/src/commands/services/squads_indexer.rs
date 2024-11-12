use std::{str::FromStr, time::Duration};

use db::{client::Client, migrations::run_migrations, new_connection};
use sb_dl::{config::Config, programs::squads::v4::derive_vault_pda, services::{idl_indexer::IdlIndexer, squads_indexer::SquadsIndexer}};
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
        for (account, msig_info) in v4_msigs.into_iter() {
            let vault = derive_vault_pda(&account, 0).0;
            if let Err(err) = client.insert_or_update_squads(
                &mut conn,
                &account.to_string(),
                &[vault.to_string()],
                &msig_info.members.into_iter().map(|member| member.key.to_string()).collect::<Vec<_>>(),
                msig_info.threshold as i32,
                4
            ) {
                log::error!("failed to record multisig(v4=true, account={}) {err:#?}", account);
            }
        }
    }
    Ok(())
}
