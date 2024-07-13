use db::{migrations::run_migrations, new_connection};
use sb_dl::{config::Config, idl::IdlIndexer, programs::ProgramIndexer};

pub async fn index_programs(
    config_path: &str
) -> anyhow::Result<()> {
    let cfg = Config::load(config_path).await?;
    let p_indexer = ProgramIndexer::new(&cfg.rpc_url).await?;
    {
        let mut conn = new_connection(&cfg.db_url)?;
        run_migrations(&mut conn);
    }
    let programs = p_indexer.get_programs().await?;
    let mut conn = new_connection(&cfg.db_url)?;
    let client = db::client::Client{};
    for program in programs {
        if let Err(err) = client.insert_or_update_program(
            &mut conn,
            program.program_id.to_string(),
            program.deployed_slot as i64,
            program.executable_account.to_string(),
            program.program_data
        ) {
            log::error!("failed to insert program {err:#?}");
        }
    }
    Ok(())
}