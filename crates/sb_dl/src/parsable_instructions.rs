use {
    anyhow::{anyhow, Context},
    lazy_static::lazy_static,
    serde::{Deserialize, Serialize},
    solana_account_decoder::parse_token::UiTokenAmount,
    solana_transaction_status::parse_instruction::ParsedInstruction,
    system::SystemInstructions,
    token::TokenInstructions,
};

#[derive(serde::Serialize, serde::Deserialize)]
pub struct PartiallyDecodedInstruction {
    pub info: serde_json::Value,
    #[serde(alias = "type")]
    pub type_: String,
}

#[derive(Clone, Debug)]
pub enum DecodedInstruction {
    SystemInstruction(SystemInstructions),
    TokenInstruction(TokenInstructions),
}

/// # Returns
///
/// Ok(None) if decoding succeeded, but this is an unsupported / unrecognized instruction
/// Ok(Some) if decoding succeeded, and the instruction is supported
/// Err      if an error was encountered
/// Some(Ok)  if decoding succeeded, and the instruction is recognized
/// Some(Err) if decoding failed
/// None      if decoding succeeded but this is an unsupported program
pub fn decode_instruction(
    parsed_instruction: &ParsedInstruction,
) -> anyhow::Result<Option<DecodedInstruction>> {
    // perform initial decoding, which will give us access to the instruction name, and parsed instruction data
    let partially_decoded: PartiallyDecodedInstruction =
        match PartiallyDecodedInstruction::deserialize(&parsed_instruction.parsed) {
            Ok(partially_decoded) => partially_decoded,
            Err(err) => return Err(anyhow!("failed to partially decode instruction {err:#?}")),
        };
    if parsed_instruction
        .program_id
        .eq("11111111111111111111111111111111")
    {
        match system::decode_system_instruction(partially_decoded)
            .with_context(|| "failed to decode system instruction")?
        {
            Some(decoded) => return Ok(Some(DecodedInstruction::SystemInstruction(decoded))),
            None => return Ok(None),
        }
    } else if parsed_instruction
        .program_id
        .eq("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA")
        || parsed_instruction
            .program_id
            .eq("TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb")
    {
        match token::decode_token_instruction(partially_decoded)
            .with_context(|| "failed to decode token instruction")?
        {
            Some(decoded) => return Ok(Some(DecodedInstruction::TokenInstruction(decoded))),
            None => return Ok(None),
        }
    } else {
        Ok(None)
    }
}

pub mod system {

    use super::*;

    lazy_static! {
        static ref TRANSFER: String = "transfer".to_string();
        static ref CREATE_ACCOUNT: String = "createAccount".to_string();
        static ref CREATE_ACCOUNT_WITH_SEED: String = "createAccountWithSeed".to_string();
        static ref TRANSFER_WITH_SEED: String = "transferWithSeed".to_string();
        static ref WITHDRAW_NONCE_ACCOUNT: String = "withdrawNonceAccount".to_string();
    }

    #[derive(Clone, Debug)]
    pub enum SystemInstructions {
        Transfer(Transfer),
        CreateAccount(CreateAccount),
        CreateAccountWithSeed(CreateAccountWithSeed),
        TransferWithSeed(TransferWithSeed),
        WithdrawNonceAccount(WithdrawNonceAccount),
    }

    #[derive(Serialize, Deserialize, Clone, Debug)]
    pub struct Transfer {
        pub source: String,
        pub destination: String,
        pub lamports: u64,
    }

    #[derive(Serialize, Deserialize, Clone, Debug)]
    pub struct TransferWithSeed {
        pub source: String,
        #[serde(alias = "sourceBase")]
        pub source_base: String,
        pub destination: String,
        pub lamports: String,
        #[serde(alias = "sourceSeed")]
        pub source_seed: String,
        #[serde(alias = "sourceOwner")]
        pub source_owner: String,
    }

    #[derive(Serialize, Deserialize, Clone, Debug)]
    pub struct WithdrawNonceAccount {
        #[serde(alias = "nonceAccount")]
        pub nonce_account: String,
        pub destination: String,
        #[serde(alias = "recentBlockhashesSysvar")]
        pub recent_blockhashes_sysvar: String,
        #[serde(alias = "rentSysvar")]
        pub rent_sysvar: String,
        #[serde(alias = "nonceAuthority")]
        pub nonce_authority: String,
        pub lamports: String,
    }

    #[derive(Serialize, Deserialize, Clone, Debug)]
    pub struct CreateAccount {
        pub source: String,
        #[serde(alias = "newAccount")]
        pub new_account: String,
        pub lamports: u64,
        pub space: u64,
        pub owner: String,
    }
    #[derive(Serialize, Deserialize, Clone, Debug)]
    pub struct CreateAccountWithSeed {
        pub source: String,
        #[serde(alias = "newAccount")]
        pub new_account: String,
        pub base: String,
        pub seed: String,
        pub lamports: u64,
        pub space: u64,
        pub owner: String,
    }

    /// # Returns
    ///
    /// Err is decoding failed
    /// Ok(None) if this isnt a system instruction we are interested in decoding
    /// Ok(Some) if this is a system instruction we are interested in decoding
    pub fn decode_system_instruction(
        partially_decoded: PartiallyDecodedInstruction,
    ) -> anyhow::Result<Option<SystemInstructions>> {
        let ix_type = &partially_decoded.type_;
        if TRANSFER.eq(ix_type) {
            Ok(Some(SystemInstructions::Transfer(serde_json::from_value(
                partially_decoded.info,
            )?)))
        } else if CREATE_ACCOUNT.eq(ix_type) {
            Ok(Some(SystemInstructions::CreateAccount(
                serde_json::from_value(partially_decoded.info)?,
            )))
        } else if CREATE_ACCOUNT_WITH_SEED.eq(ix_type) {
            Ok(Some(SystemInstructions::CreateAccountWithSeed(
                serde_json::from_value(partially_decoded.info)?,
            )))
        } else if TRANSFER_WITH_SEED.eq(ix_type) {
            Ok(Some(SystemInstructions::TransferWithSeed(
                serde_json::from_value(partially_decoded.info)?,
            )))
        } else if WITHDRAW_NONCE_ACCOUNT.eq(ix_type) {
            Ok(Some(SystemInstructions::WithdrawNonceAccount(
                serde_json::from_value(partially_decoded.info)?,
            )))
        } else {
            return Ok(None);
        }
    }
}

// can be used for both spl-token and token2022
pub mod token {

    use super::*;

    lazy_static! {
        static ref TRANSFER: String = "transfer".to_string();
        static ref MINT_TO: String = "mintTo".to_string();
        static ref BURN: String = "burn".to_string();
        static ref TRANSFER_CHECKED: String = "transferChecked".to_string();
        static ref MINT_TO_CHECKED: String = "mintToChecked".to_string();
        static ref BURN_CHECKED: String = "burnChecked".to_string();
        static ref CLOSE_ACCOUNT: String = "closeAccount".to_string();
        static ref INITIALIZE_ACCOUNT: String = "initializeAccount".to_string();
        static ref INITIALIZE_ACCOUNT_2: String = "initializeAccount2".to_string();
        static ref INITIALIZE_ACCOUNT_3: String = "initializeAccoun3".to_string();
    }

    #[derive(Clone, Debug)]
    pub enum TokenInstructions {
        Transfer(Transfer),
        MintTo(MintTo),
        Burn(Burn),
        TransferChecked(TransferChecked),
        MintToChecked(MintToChecked),
        BurnChecked(BurnChecked),
        // used for initializeAccount or initializeAccount2
        InitializeAccount(InitializeAccount),
        InitializeAccount3(InitializeAccount3)
        // temporarily omit instruction until account closure is fully supported
        //CloseAccount(CloseAccount),
    }

    #[derive(Serialize, Deserialize, Clone, Debug)]
    pub struct Transfer {
        pub source: String,
        pub destination: String,
        pub amount: String,
        // this information is missing from the instruction
        // however we can add it after decoding
        pub mint: Option<String>,
        pub authority: Option<String>,
    }
    #[derive(Serialize, Deserialize, Clone, Debug)]
    pub struct MintTo {
        pub mint: String,
        pub account: String,
        pub amount: u64,
        #[serde(alias = "mintAuthority")]
        pub mint_authority: String,
    }
    #[derive(Serialize, Deserialize, Clone, Debug)]
    pub struct Burn {
        pub account: String,
        pub mint: String,
        pub amount: u64,
        pub authority: String,
    }
    #[derive(Serialize, Deserialize, Clone, Debug)]
    pub struct TransferChecked {
        pub source: String,
        pub mint: String,
        pub destination: String,
        pub authority: Option<String>,
        #[serde(alias = "tokenAmount")]
        pub token_amount: UiTokenAmount,
    }
    #[derive(Serialize, Deserialize, Clone, Debug)]
    pub struct MintToChecked {
        pub mint: String,
        pub account: String,
        #[serde(alias = "tokenAmount")]
        pub token_amount: UiTokenAmount,
    }
    #[derive(Serialize, Deserialize, Clone, Debug)]
    pub struct BurnChecked {
        pub account: String,
        pub mint: String,
        #[serde(alias = "tokenAmount")]
        pub token_amount: UiTokenAmount,
    }

    #[derive(Serialize, Deserialize, Clone, Debug)]
    pub struct CloseAccount {
        pub owner: String,
        pub account: String,
        pub destination: String,
        /// this is the SOL held by the token account at the time it was closed
        /// in the case of wSOL, it will include rent cost + wsol held, and is set after
        /// all instructions have been parsed
        ///
        /// todo: not yet sure how to account for this
        pub amount: Option<String>,
    }

    /// same structure is used for "initializeAccount2"
    #[derive(Serialize, Deserialize, Clone, Debug)]
    pub struct InitializeAccount {
        pub account: String,
        pub mint: String,
        pub owner: String,
        #[serde(alias = "rentSysvar")]
        pub rent_sysvar: String,
    }
    #[derive(Serialize, Deserialize, Clone, Debug)]
    pub struct InitializeAccount3 {
        pub account: String,
        pub mint: String,
        pub owner: String,
    }


    /// # Returns
    ///
    /// Err is decoding failed
    /// Ok(None) if this isnt a token instruction we are interested in decoding
    /// Ok(Some) if this is a token instruction we are interested in decoding
    pub fn decode_token_instruction(
        partially_decoded: PartiallyDecodedInstruction,
    ) -> anyhow::Result<Option<TokenInstructions>> {
        let ix_type = &partially_decoded.type_;
        if TRANSFER.eq(ix_type) {
            Ok(Some(TokenInstructions::Transfer(serde_json::from_value(
                partially_decoded.info,
            )?)))
        } else if MINT_TO.eq(ix_type) {
            Ok(Some(TokenInstructions::MintTo(serde_json::from_value(
                partially_decoded.info,
            )?)))
        } else if BURN.eq(ix_type) {
            Ok(Some(TokenInstructions::Burn(serde_json::from_value(
                partially_decoded.info,
            )?)))
        } else if TRANSFER_CHECKED.eq(ix_type) {
            Ok(Some(TokenInstructions::TransferChecked(
                serde_json::from_value(partially_decoded.info)?,
            )))
        } else if MINT_TO_CHECKED.eq(ix_type) {
            Ok(Some(TokenInstructions::MintToChecked(
                serde_json::from_value(partially_decoded.info)?,
            )))
        } else if BURN_CHECKED.eq(ix_type) {
            Ok(Some(TokenInstructions::BurnChecked(
                serde_json::from_value(partially_decoded.info)?,
            )))
        } else if INITIALIZE_ACCOUNT.eq(ix_type) || INITIALIZE_ACCOUNT_2.eq(ix_type) {
            Ok(Some(TokenInstructions::InitializeAccount(
                serde_json::from_value(partially_decoded.info)?,
            )))
        } else if INITIALIZE_ACCOUNT_3.eq(ix_type) {
            Ok(Some(TokenInstructions::InitializeAccount3(
                serde_json::from_value(partially_decoded.info)?,
            )))
        } /*else if CLOSE_ACCOUNT.eq(ix_type) {
            Ok(Some(TokenInstructions::CloseAccount(
                serde_json::from_value(partially_decoded.info)?,
            )))
        } */else {
            return Ok(None);
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    #[test]
    fn test_decode_token_transfer() {
        let ix: ParsedInstruction = serde_json::from_value(serde_json::json!({
            "parsed": {
                "info": {
                    "amount": "8141292030",
                    "source": "7GWrFUVjTv7fZ9s1L5asqCfrMTWqhjA5otdgW7Wkd1n9",
                    "authority": "BbaLXTZg7xEkff2TdShu6FHfcDVuywdmqKu77f13hfRt",
                    "destination": "5ruvMsmvCk6Uahrtc475LjBBKRkez1sw7ctTmM6MoNWD"
                },
                "type": "transfer"
            },
            "program": "spl-token",
            "programId": "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
            "stackHeight": 3
        }))
        .unwrap();
        let decoded_ix = decode_instruction(&ix).unwrap().unwrap();
        assert!(matches!(
            decoded_ix,
            DecodedInstruction::TokenInstruction(TokenInstructions::Transfer(..))
        ));
    }
    #[test]
    fn test_decode_token_transfer_checked() {
        let ix: ParsedInstruction = serde_json::from_value(serde_json::json!({
            "parsed": {
                "info": {
                    "mint": "So11111111111111111111111111111111111111112",
                    "source": "EmwPSZuqkZRUoHdWChu6omcNmv8BoUavkv2qLyUhzYi4",
                    "authority": "8FbVeDtxTUKLJB9rqxDFM9eKh3aYfmEn2EN1Q2hSKy4S",
                    "destination": "DSJjnhv1AcTbQ9GxKvsMe4pAEvJWEkQmKyXwBKe2nX5B",
                    "tokenAmount": {
                        "amount": "4140632274",
                        "decimals": 9,
                        "uiAmount": 4.140632274,
                        "uiAmountString": "4.140632274"
                    }
                },
                "type": "transferChecked"
            },
            "program": "spl-token",
            "programId": "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
            "stackHeight": 3
        }))
        .unwrap();
        let decoded_ix = decode_instruction(&ix).unwrap().unwrap();
        assert!(matches!(
            decoded_ix,
            DecodedInstruction::TokenInstruction(TokenInstructions::TransferChecked(..))
        ));
    }
    #[test]
    fn test_decode_system_create_account() {
        let ix: ParsedInstruction = serde_json::from_value(serde_json::json!({
            "parsed": {
                "info": {
                    "owner": "3tZPEagumHvtgBhivFJCmhV9AyhBHGW9VgdsK52i4gwP",
                    "space": 32,
                    "source": "BgU4TACDKnBYJzAGTSPQgunhp5BYD7vx9ZW95NEzWTbk",
                    "lamports": 1113600,
                    "newAccount": "2Rf7adcTkVwKk3AJLRmnkMqP2VN6JxiKrdYDPHctVbF2"
                },
                "type": "createAccount"
            },
            "program": "system",
            "programId": "11111111111111111111111111111111",
            "stackHeight": 2
        }))
        .unwrap();
        let decoded_ix = decode_instruction(&ix).unwrap().unwrap();
        assert!(matches!(
            decoded_ix,
            DecodedInstruction::SystemInstruction(SystemInstructions::CreateAccount(..))
        ));
    }
    #[test]
    fn test_decode_system_transfer() {
        let ix: ParsedInstruction = serde_json::from_value(serde_json::json!({
            "parsed": {
                "info": {
                    "source": "MVDv8FHLovYWapDcz2JemwDoWhGPWoVQmygeNnLorXK",
                    "lamports": 1,
                    "destination": "4ECMsSTxTZ4UqrLBJpMqWG6G4XdX1XXfXdjHdhfva8gJ"
                },
                "type": "transfer"
            },
            "program": "system",
            "programId": "11111111111111111111111111111111",
            "stackHeight": 3
        }))
        .unwrap();
        let decoded_ix = decode_instruction(&ix).unwrap().unwrap();
        assert!(matches!(
            decoded_ix,
            DecodedInstruction::SystemInstruction(SystemInstructions::Transfer(..))
        ));
    }
    #[test]
    fn test_decode_invalid_program() {
        let ix: ParsedInstruction = serde_json::from_value(serde_json::json!({
            "parsed": {
                "info": {
                    "source": "MVDv8FHLovYWapDcz2JemwDoWhGPWoVQmygeNnLorXK",
                    "lamports": 1,
                    "destination": "4ECMsSTxTZ4UqrLBJpMqWG6G4XdX1XXfXdjHdhfva8gJ"
                },
                "type": "transfer"
            },
            "program": "system",
            "programId": "GFXsSL5sSaDfNFQUYsHekbWBW1TsFdjDYzACh62tEHxn",
            "stackHeight": 3
        }))
        .unwrap();
        assert!(decode_instruction(&ix).unwrap().is_none());
    }
    #[test]
    fn test_decode_invalid_json() {
        let ix = ParsedInstruction {
            program: "foobar".to_string(),
            program_id: "foobarbaz".to_string(),
            parsed: serde_json::json!({"a": "b"}),
            stack_height: None,
        };
        assert!(decode_instruction(&ix).is_err());
    }
}
