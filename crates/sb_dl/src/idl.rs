use std::time::Duration;

use anyhow::Context;
use solana_account_decoder::UiAccountEncoding;
use solana_client::{nonblocking::rpc_client::RpcClient, rpc_config::{RpcAccountInfoConfig, RpcProgramAccountsConfig}};
use solana_sdk::pubkey::Pubkey;

const IDL_SEED: &str = "anchor:idl";

pub struct IdlIndexer {
    rpc: RpcClient
}

impl IdlIndexer {
    pub async fn new(
        endpoint: &str
    ) -> anyhow::Result<Self> {
        let rpc = RpcClient::new_with_timeout(endpoint.to_string(), Duration::from_secs(600));
        Ok(Self {rpc})
    }
    /// returns all possible accounts used to store idl's for deployed programs
    pub async fn get_idl_accounts(&self) -> anyhow::Result<()> {
        let program_accounts = self.rpc.get_program_accounts_with_config(
            &"BPFLoaderUpgradeab1e11111111111111111111111".parse::<Pubkey>()?,
            RpcProgramAccountsConfig {
                account_config: RpcAccountInfoConfig {
                    encoding: Some(UiAccountEncoding::Base64),
                    ..Default::default()
                },
                ..Default::default()
            }
        ).await.with_context(|| "failed to get program accounts")?;
        let program_idls = program_accounts.into_iter().map(|(program, _)| {

            // Generate the PDA (Program Derived Address)
            let (idl_address, _) = Pubkey::find_program_address(
                &[IDL_SEED.as_bytes()],
                &program
            );
            (program, idl_address)
        }).collect::<Vec<_>>();
        log::info!("found {} total programs", program_idls.len());
        Ok(())
    }
}