use {
    anyhow::Context,
    solana_account_decoder::{
        parse_bpf_loader::{parse_bpf_upgradeable_loader, BpfUpgradeableLoaderAccountType},
        UiAccountEncoding,
    },
    solana_client::{
        nonblocking::rpc_client::RpcClient,
        rpc_config::{RpcAccountInfoConfig, RpcProgramAccountsConfig}, rpc_filter::RpcFilterType,
    },
    solana_sdk::{bpf_loader_upgradeable, pubkey::Pubkey},
    std::time::Duration,
};

#[derive(Clone)]
pub struct ProgramInfo {
    pub program_id: Pubkey,
    pub executable_account: Pubkey,
    pub program_data: Vec<u8>,
    pub deployed_slot: u64,
}
pub struct MintIndexer {
    rpc: RpcClient,
}

impl MintIndexer {
    pub async fn new(endpoint: &str) -> anyhow::Result<Self> {
        let rpc = RpcClient::new_with_timeout(endpoint.to_string(), Duration::from_secs(600));
        Ok(Self { rpc })
    }
    pub async fn get_spl_token_mints(&self) -> anyhow::Result<()> {
        let mint_accounts = self.rpc.get_program_accounts_with_config(
            &spl_token::id(),
            RpcProgramAccountsConfig {
                account_config: RpcAccountInfoConfig {
                    encoding: Some(UiAccountEncoding::Base64),
                    ..Default::default()
                },
                filters: Some(vec![
                    RpcFilterType::DataSize(84), // size of the token mint account
                ]),
                ..Default::default()
            }
        ).await.with_context(|| "failed to get spl-token mints")?;
        log::info!("found {} token mints", mint_accounts.len());
        Ok(())
    }
    pub async fn get_token2022_mints(&self) -> anyhow::Result<()> {
        let mint_accounts = self.rpc.get_program_accounts_with_config(
            &spl_token_2022::id(),
            RpcProgramAccountsConfig {
                account_config: RpcAccountInfoConfig {
                    encoding: Some(UiAccountEncoding::Base64),
                    ..Default::default()
                },
                filters: Some(vec![
                    RpcFilterType::DataSize(82), // size of the token mint account
                ]),
                ..Default::default()
            }
        ).await.with_context(|| "failed to get spl-token mints")?;
        log::info!("found {} token mints", mint_accounts.len());
        Ok(())
    }
}
