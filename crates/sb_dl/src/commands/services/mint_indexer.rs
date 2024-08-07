use {
    anyhow::{Context, Result}, db::{client::Client, migrations::run_migrations, new_connection}, sb_dl::{config::Config, services::mint_indexer::MintIndexer}
};

pub async fn index_spl_token_mints(
    config_path: &str
) -> Result<()> {
    let cfg = Config::load(config_path).await?;
    let indexer = MintIndexer::new(&cfg.rpc_url).await?;
    let token_mints = indexer.get_spl_token_mints().await.with_context(|| "faield to get token mints")?;
    let mut conn = new_connection(&cfg.db_url)?;
    run_migrations(&mut conn);
    let client = Client{};
    for (mint, mint_info) in token_mints {
        if let Err(err) = client.insert_token_mint(
            &mut conn,
            mint.to_string(),
            None,
            None,
            mint_info.decimals as f32,
            false
        ) {
            log::error!("failed to record token_mint({mint}, 2022=false) {err:#?}");
        }
    }
    Ok(())
}

pub async fn index_spl_token2022_mints(
    config_path: &str
) -> Result<()> {
    let cfg = Config::load(config_path).await?;
    let indexer = MintIndexer::new(&cfg.rpc_url).await?;
    let token_mints = indexer.get_token2022_mints().await.with_context(|| "faield to get token mints")?;
    let mut conn = new_connection(&cfg.db_url)?;
    run_migrations(&mut conn);
    let client = Client{};
    for (mint, mint_info) in token_mints {
        if let Err(err) = client.insert_token_mint(
            &mut conn,
            mint.to_string(),
            None,
            None,
            mint_info.decimals as f32,
            true
        ) {
            log::error!("failed to record token_mint({mint}, 2022=true) {err:#?}");
        }
    }


    Ok(())
}