use std::time::Duration;

use anyhow::Context;
use solana_account_decoder::{
    parse_bpf_loader::{parse_bpf_upgradeable_loader, BpfUpgradeableLoaderAccountType},
    UiAccountEncoding,
};
use solana_client::{
    nonblocking::rpc_client::RpcClient,
    rpc_config::{RpcAccountInfoConfig, RpcProgramAccountsConfig},
};
use solana_sdk::{
    address_lookup_table::state::ProgramState,
    bpf_loader_upgradeable::{self, UpgradeableLoaderState},
    pubkey::Pubkey,
};

const IDL_SEED: &str = "anchor:idl";
#[derive(Clone)]
pub struct ProgramInfo {
    pub program_id: Pubkey,
    pub executable_account: Pubkey,
    pub program_data: Vec<u8>,
    pub deployed_slot: u64,
}
pub struct ProgramIndexer {
    rpc: RpcClient,
}

impl ProgramIndexer {
    pub async fn new(endpoint: &str) -> anyhow::Result<Self> {
        let rpc = RpcClient::new_with_timeout(endpoint.to_string(), Duration::from_secs(600));
        Ok(Self { rpc })
    }
    /// returns all possible accounts used to store idl's for deployed programs
    pub async fn get_programs(&self) -> anyhow::Result<Vec<ProgramInfo>> {
        let program_accounts = self
            .rpc
            .get_program_accounts_with_config(
                &"BPFLoaderUpgradeab1e11111111111111111111111".parse::<Pubkey>()?,
                RpcProgramAccountsConfig {
                    account_config: RpcAccountInfoConfig {
                        encoding: Some(UiAccountEncoding::Base64Zstd),
                        ..Default::default()
                    },
                    ..Default::default()
                },
            )
            .await
            .with_context(|| "failed to get program accounts")?;

        let mut program_accounts = program_accounts
            .into_iter()
            .map(|(pid, _)| ProgramInfo {
                program_id: pid,
                executable_account: get_program_data_account(pid),
                deployed_slot: 0,
                program_data: vec![],
            })
            .collect::<Vec<_>>();

        for program_account_chunks in program_accounts.chunks_mut(100) {
            match self
                .rpc
                .get_multiple_accounts_with_config(
                    &program_account_chunks
                        .iter()
                        .map(|info| info.executable_account)
                        .collect::<Vec<_>>(),
                    RpcAccountInfoConfig {
                        encoding: Some(UiAccountEncoding::Base64Zstd),
                        ..Default::default()
                    },
                )
                .await
            {
                Ok(accounts) => {
                    for (idx, acct) in accounts.value.into_iter().enumerate() {
                        if let Some(acct) = acct {
                            if let Ok(info) = parse_bpf_upgradeable_loader(&acct.data) {
                                if let BpfUpgradeableLoaderAccountType::ProgramData(data) = info {
                                    if let Some(decoded_data) = data.data.decode() {
                                        program_account_chunks[idx].deployed_slot = data.slot;
                                        program_account_chunks[idx].program_data = decoded_data;
                                    }
                                }
                            }
                        }
                    }
                }
                Err(err) => {
                    log::error!("failed to load program executable accounts {err:#?}");
                }
            }
        }

        Ok(program_accounts)
    }
}

pub fn get_program_data_account(program_id: Pubkey) -> Pubkey {
    Pubkey::find_program_address(&[&program_id.to_bytes()], &bpf_loader_upgradeable::id()).0
}
#[cfg(test)]
mod test {
    use super::*;
    #[tokio::test]
    async fn test_idl() {
        assert_eq!(
            "4Ec7ZxZS6Sbdg5UGSLHbAnM7GQHp2eFd4KYWRexAipQT",
            get_program_data_account(
                "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4"
                    .parse()
                    .unwrap()
            )
            .to_string()
        );
    }
}
