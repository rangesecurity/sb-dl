use std::{io::Read, time::Duration};

use anyhow::Context;
use flate2::read::GzDecoder;
use flate2::read::ZlibDecoder;
use flate2::write::{GzEncoder, ZlibEncoder};
use flate2::Compression;
use serde_json::json;
use solana_account_decoder::UiAccountEncoding;
use solana_client::{
    nonblocking::rpc_client::RpcClient,
    rpc_config::{RpcAccountInfoConfig, RpcProgramAccountsConfig},
};
use solana_sdk::pubkey::Pubkey;
const IDL_SEED: &str = "anchor:idl";

#[derive(borsh::BorshDeserialize, borsh::BorshSerialize)]
pub struct IdlAccount {
    // Address that can modify the IDL.
    pub authority: Pubkey,
    // Length of compressed idl bytes.
    pub data_len: u32,
    // Followed by compressed idl bytes.
}

pub struct ProgramIdl {
    pub program_id: Pubkey,
    pub idl: serde_json::Value,
}

pub struct IdlIndexer {
    rpc: RpcClient,
}

impl IdlIndexer {
    pub async fn new(endpoint: &str) -> anyhow::Result<Self> {
        let rpc = RpcClient::new_with_timeout(endpoint.to_string(), Duration::from_secs(600));
        Ok(Self { rpc })
    }
    /// returns all possible accounts used to store idl's for deployed programs
    pub async fn get_idl_accounts(&self, programs: &[Pubkey]) -> anyhow::Result<Vec<ProgramIdl>> {
        let program_idls = programs
            .into_iter()
            .filter_map(|(program)| Some((program, IdlAccount::address(&program).ok()?)))
            .collect::<Vec<_>>();

        let mut idls = Vec::with_capacity(program_idls.len());
        for program_idl_chunk in program_idls.chunks(100) {
            let idl_accounts = self
                .rpc
                .get_multiple_accounts_with_config(
                    &program_idl_chunk
                        .iter()
                        .map(|(_, idl)| *idl)
                        .collect::<Vec<_>>(),
                    RpcAccountInfoConfig {
                        encoding: Some(UiAccountEncoding::Base64Zstd),
                        ..Default::default()
                    },
                )
                .await
                .with_context(|| "failed to fetch multiple accounts")?
                .value
                .into_iter()
                .enumerate()
                .filter_map(|(idx, acct)| Some((idx, acct?)))
                .collect::<Vec<_>>();

            for (idx, account) in idl_accounts {
                if account.data.is_empty() {
                    continue;
                }
                match borsh::BorshDeserialize::deserialize(&mut &account.data[8..]) {
                    Ok(idl_account) => {
                        let idl_account: IdlAccount = idl_account;
                        if idl_account.data_len == 0 {
                            continue;
                        }
                        let compressed_len: usize = idl_account.data_len.try_into().unwrap();
                        let compressed_bytes = &account.data[44..44 + compressed_len];
                        let mut z = ZlibDecoder::new(compressed_bytes);
                        let mut s = Vec::new();
                        if let Err(err) = z.read_to_end(&mut s) {
                            log::error!(
                                "deflate stream read failed for pid({}) idl({}) {err:#?}",
                                program_idl_chunk[idx].0,
                                program_idl_chunk[idx].1
                            );
                            continue;
                        }
                        match serde_json::from_slice::<serde_json::Value>(&s[..]) {
                            Ok(mut idl_json) => {
                                // handle idl conversion
                                if let Some(obj) = idl_json.as_object_mut() {
                                    // Insert or get the `metadata` object
                                    let metadata =
                                        obj.entry("metadata").or_insert_with(|| json!({}));

                                    // Ensure `metadata` is an object
                                    if let Some(metadata_obj) = metadata.as_object_mut() {
                                        if !metadata_obj.contains_key("address") {
                                            // Insert the `address` field into `metadata`
                                            metadata_obj.insert(
                                                "address".to_string(),
                                                json!(program_idl_chunk[idx].0.to_string()),
                                            );
                                        }
                                    }
                                }
                                if let Ok(idl_json) = serde_json::to_vec(&idl_json) {
                                    match anchor_lang_idl::convert::convert_idl(&idl_json) {
                                        Ok(idl) => match serde_json::to_value(&idl) {
                                            Ok(idl_json) => {
                                                idls.push(ProgramIdl {
                                                    program_id: *program_idl_chunk[idx].0,
                                                    idl: idl_json,
                                                });
                                            }
                                            Err(err) => {
                                                log::error!("failed to convert idl {err:#?}");
                                            }
                                        },
                                        Err(err) => {
                                            log::error!("failed to convert idl {err:#?}");
                                        }
                                    }
                                }
                            }
                            Err(err) => {
                                log::error!(
                                    "failed to deserialize json idl(pid={},idl={}) {err:#?}",
                                    program_idl_chunk[idx].0,
                                    program_idl_chunk[idx].1
                                );
                            }
                        }
                    }
                    Err(err) => {
                        log::error!("failed to deserialize idl account {err:#?}")
                    }
                };
            }
        }
        Ok(idls)
    }
}

impl IdlAccount {
    pub fn address(program_id: &Pubkey) -> anyhow::Result<Pubkey> {
        let program_signer = Pubkey::find_program_address(&[], program_id).0;
        Pubkey::create_with_seed(&program_signer, IdlAccount::seed(), program_id)
            .with_context(|| "failed to get idl account")
    }
    pub fn seed() -> &'static str {
        "anchor:idl"
    }
}

#[cfg(test)]
mod test {
    use super::*;
    #[test]
    fn test_idl() {
        assert_eq!(
            "C88XWfp26heEmDkmfSzeXP7Fd7GQJ2j9dDTUsyiZbUTa",
            IdlAccount::address(
                &"JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4"
                    .parse()
                    .unwrap()
            )
            .unwrap()
            .to_string()
        );
    }
}
