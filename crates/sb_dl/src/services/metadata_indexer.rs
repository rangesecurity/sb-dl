use {
    anyhow::Context,
    borsh::BorshDeserialize,
    mpl_token_metadata::accounts::Metadata,
    solana_account_decoder::{
        parse_bpf_loader::{parse_bpf_upgradeable_loader, BpfUpgradeableLoaderAccountType},
        UiAccountEncoding,
    },
    solana_client::{
        nonblocking::rpc_client::RpcClient,
        rpc_config::{RpcAccountInfoConfig, RpcProgramAccountsConfig},
        rpc_filter::RpcFilterType,
    },
    solana_sdk::{bpf_loader_upgradeable, program_pack::Pack, pubkey::Pubkey},
    spl_token::state::Mint,
    spl_token_2022::state::Mint as Mint2022,
    std::time::Duration,
};

#[derive(Clone, Copy)]
pub struct MetadataInfo {
    pub metadata_account: Pubkey,
    pub mint: Pubkey,
}

pub struct MetadataIndexer {
    rpc: RpcClient,
}

impl MetadataIndexer {
    pub async fn new(endpoint: &str) -> anyhow::Result<Self> {
        let rpc = RpcClient::new_with_timeout(endpoint.to_string(), Duration::from_secs(600));
        Ok(Self { rpc })
    }
    pub async fn get_metadata_accounts(
        &self,
        mints: Vec<Pubkey>,
    ) -> anyhow::Result<Vec<(Pubkey, Metadata)>> {
        let metadata_accounts = derive_metadata_accounts(mints);
        let metadata_account_chunks = metadata_accounts.chunks(100);

        // vec![(mint, metadata)]
        let mut metadatas: Vec<(Pubkey, Metadata)> = Vec::with_capacity(metadata_accounts.len());

        for chunk in metadata_account_chunks {
            match self
                .rpc
                .get_multiple_accounts_with_config(
                    &chunk
                        .iter()
                        .map(|info| info.metadata_account)
                        .collect::<Vec<_>>(),
                    RpcAccountInfoConfig {
                        encoding: Some(UiAccountEncoding::Base64),
                        ..Default::default()
                    },
                )
                .await
            {
                Ok(accounts) => {
                    for (idx, account) in accounts.value.into_iter().enumerate() {
                        if let Some(account) = account {
                            match Metadata::from_bytes(&account.data) {
                                Ok(metadata) => metadatas.push((chunk[idx].mint, metadata)),
                                Err(err) => {
                                    log::error!(
                                        "failed to deserialize metadata(account={}) {err:#?}",
                                        chunk[idx].mint
                                    )
                                }
                            }
                        }
                    }
                }
                Err(err) => {
                    log::error!("failed to get multiple accounts {err:#?}");
                }
            }
        }
        Ok(metadatas)
    }
}

pub fn derive_metadata_accounts(mints: Vec<Pubkey>) -> Vec<MetadataInfo> {
    mints
        .into_iter()
        .map(|mint| {
            let (metadata_account, _) = Metadata::find_pda(&mint);
            MetadataInfo {
                mint,
                metadata_account,
            }
        })
        .collect()
}
