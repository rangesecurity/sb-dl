use {
    anyhow::{Context, Result}, db::{client::Client, migrations::run_migrations, new_connection}, sb_dl::{config::Config, services::mint_indexer::MintIndexer},
    solana_account_decoder::UiAccountEncoding,
    solana_client::{nonblocking::rpc_client::RpcClient, rpc_config::{RpcAccountInfoConfig, RpcTransactionConfig}},
    solana_program::program_pack::Pack,
    solana_transaction_status::{EncodedTransaction, UiConfirmedBlock, UiTransactionEncoding},

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

pub async fn manual_mint_import(
    matches: &clap::ArgMatches,
    config_path: &str
) -> anyhow::Result<()> {
    let mint = matches.get_one::<String>("mint").unwrap();
    let mint_name = matches.get_one::<String>("mint-name").unwrap();
    let mint_symbol = matches.get_one::<String>("mint-symbol").unwrap();

    let is_token_2022 = matches.get_flag("is-token-2022");
    let cfg = Config::load(config_path).await?;
    let rpc = RpcClient::new(cfg.rpc_url.clone());
    let mut conn = db::new_connection(&cfg.db_url)?;

    run_migrations(&mut conn);

    let token_mint_data = rpc.get_account_with_config(
        &mint.parse()?,
        RpcAccountInfoConfig {
            encoding: Some(UiAccountEncoding::Base64),
            ..Default::default()
        }
    ).await?.value.with_context(|| "account is None")?.data;

    let decimals = if is_token_2022 {
        spl_token_2022::state::Mint::unpack(&token_mint_data)?.decimals
    }  else {
        spl_token::state::Mint::unpack(&token_mint_data)?.decimals
    };

    let client = Client{};

    client.insert_token_mint(
        &mut conn,
        mint.clone(),
        Some(mint_name.clone()),
        Some(mint_symbol.clone()),
        decimals as f32,
        is_token_2022
    )?;

    Ok(())
    
}
