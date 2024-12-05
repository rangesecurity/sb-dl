use {crate::programs::{self, squads::{v3::MultisigV3, v4::MultisigV4}}, anyhow::{Context, Result}, borsh::BorshDeserialize, solana_account_decoder::UiAccountEncoding, solana_client::{nonblocking::rpc_client::RpcClient, rpc_config::{RpcAccountInfoConfig, RpcProgramAccountsConfig}, rpc_filter::{Memcmp, MemcmpEncodedBytes, RpcFilterType}}, solana_sdk::pubkey::Pubkey, std::{sync::Arc, time::Duration}};

pub enum Multisig {
    V4{
        account: Pubkey,
        multisig: MultisigV4
    },
    V3{
        account: Pubkey,
        multisig: MultisigV3
    }
}

pub struct SquadsIndexer {
    rpc: Arc<RpcClient>
}

impl SquadsIndexer {
    pub fn new(url: String) -> Self {
        Self {
            rpc: Arc::new(RpcClient::new_with_timeout(url, Duration::from_secs(600)))
        }
    }
    pub async fn fetch_multisigs_v4(&self) -> Result<Vec<(Pubkey, MultisigV4)>> {
        Ok(self.rpc.get_program_accounts_with_config(
            &programs::squads::v4::ID,
            RpcProgramAccountsConfig {
                filters: Some(vec![
                    RpcFilterType::Memcmp(
                        Memcmp::new(
                            0,
                            MemcmpEncodedBytes::Bytes(
                                programs::squads::v4::DISCRIMINATOR.to_vec()
                            )
                        )
                    )
                ]),
                account_config: RpcAccountInfoConfig {
                    encoding: Some(UiAccountEncoding::Base64),
                    ..Default::default()
                },
                ..Default::default()
            }
        ).await
        .with_context(|| "failed to load v4 accounts")?
        .into_iter()
        .filter_map(|(account_key, account)| {
            Some((account_key, programs::squads::v4::MultisigV4::deserialize(&mut &account.data[..]).ok()?))
        }).collect::<Vec<_>>())
    }
    pub async fn fetch_multisigs_v3(&self) -> Result<Vec<(Pubkey, MultisigV3)>> {
        Ok(self.rpc.get_program_accounts_with_config(
            &programs::squads::v3::ID,
            RpcProgramAccountsConfig {
                filters: Some(vec![
                    RpcFilterType::Memcmp(
                        Memcmp::new(
                            0,
                            MemcmpEncodedBytes::Bytes(
                                programs::squads::v3::DISCRIMINATOR.to_vec()
                            )
                        )
                    )
                ]),
                account_config: RpcAccountInfoConfig {
                    encoding: Some(UiAccountEncoding::Base64),
                    ..Default::default()
                },
                ..Default::default()
            }
        ).await
        .with_context(|| "failed to load v4 accounts")?
        .into_iter()
        .filter_map(|(account_key, account)| {
            Some((account_key, programs::squads::v3::MultisigV3::deserialize(&mut &account.data[..]).ok()?))
        }).collect::<Vec<_>>())
    }
}