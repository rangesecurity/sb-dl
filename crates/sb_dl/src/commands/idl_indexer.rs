use sb_dl::{config::Config, idl::IdlIndexer};

pub async fn index_idls(
    config_path: &str
) -> anyhow::Result<()> {
    let cfg = Config::load(config_path).await?;
    let idl_indexer = IdlIndexer::new(&cfg.rpc_url).await?;
    idl_indexer.get_idl_accounts().await?;
    Ok(())
}