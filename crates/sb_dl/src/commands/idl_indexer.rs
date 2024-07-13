use std::str::FromStr;

use db::{migrations::run_migrations, new_connection};
use sb_dl::{config::Config, idl::IdlIndexer};
use solana_sdk::pubkey::Pubkey;

pub async fn index_idls(
    config_path: &str
) -> anyhow::Result<()> {
    let cfg = Config::load(config_path).await?;
    let idl_indexer = IdlIndexer::new(&cfg.rpc_url).await?;
    let program_ids = {
        let mut conn = new_connection(&cfg.db_url)?;
        run_migrations(&mut conn);
        db::client::Client{}.indexed_program_ids(&mut conn)?.into_iter().filter_map(|id| Pubkey::from_str(&id).ok()).collect::<Vec<_>>()
    };
    let idls = idl_indexer.get_idl_accounts(&program_ids).await?;
    let mut conn = new_connection(&cfg.db_url)?;
    let client = db::client::Client{};
    for idl in idls {
        if let Err(err) = client.insert_or_update_idl(
            &mut conn,
            idl.program_id.to_string(),
            0,
            None,
            idl.idl,
        ) {
            log::error!("failed to insert idl(pid={}) {err:#?}", idl.program_id);
        }
    }
    Ok(())
}