use {
    anyhow::{Context, Result}, sb_dl::{config::Config, services::mint_indexer::MintIndexer},
};

pub async fn index_spl_token_mints(
    config_path: &str
) -> Result<()> {
    let cfg = Config::load(config_path).await?;
    let indexer = MintIndexer::new(&cfg.rpc_url).await?;
    indexer.get_spl_token_mints().await.with_context(|| "faield to get token mints")?;

    Ok(())
}

pub async fn index_spl_token2022_mints(
    config_path: &str
) -> Result<()> {
    let cfg = Config::load(config_path).await?;
    let indexer = MintIndexer::new(&cfg.rpc_url).await?;
    indexer.get_token2022_mints().await.with_context(|| "faield to get token mints")?;
    
    Ok(())
}