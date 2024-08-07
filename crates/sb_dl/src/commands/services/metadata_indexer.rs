use {
    anyhow::{Context, Result}, db::{client::{Client, TokenMintFilter}, migrations::run_migrations, new_connection}, sb_dl::{config::Config, services::{metadata_indexer::MetadataIndexer, mint_indexer::MintIndexer}}, solana_sdk::pubkey::Pubkey, std::str::FromStr
};

pub async fn index_metadata_accounts(
    config_path: &str
) -> Result<()> {
    let cfg = Config::load(config_path).await?;
    let mut conn = new_connection(&cfg.db_url)?;
    run_migrations(&mut conn);
    let client = Client{};

    let token_mints = client.select_token_mint(&mut conn, TokenMintFilter::All)?;
    let token_mints = token_mints.into_iter().filter_map(|token| Pubkey::from_str(&token.mint).ok()).collect::<Vec<_>>();
    let total_mints = token_mints.len();
    let indexer = MetadataIndexer::new(&cfg.rpc_url).await?;

    let token_metadatas = indexer.get_metadata_accounts(token_mints).await?;

    log::info!("found {}/{total_mints} metadata accounts", token_metadatas.len());
    Ok(())
}