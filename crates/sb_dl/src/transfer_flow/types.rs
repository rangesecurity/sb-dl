use std::collections::HashMap;

use lazy_static::lazy_static;
use serde::{Serialize, Deserialize};

use crate::parsable_instructions::{system::SystemInstructions, token::TokenInstructions, DecodedInstruction};

lazy_static! {
    /// identify this as a sol transfer, not a wsol transfer
    static ref SOL_MINT: String = "So11111111111111111111111111111111111111111".to_string();
}



pub type TransferFlow = HashMap<
u8,
(
    Option<DecodedInstruction>,
    HashMap<u8, Vec<DecodedInstruction>>,
),
>;

#[derive(Clone, Serialize, Deserialize)]
pub struct OrderedTransfers {
    pub tx_hash: String,
    pub transfers: Vec<Transfer>,
}


#[derive(Clone, Serialize, Deserialize)]
pub struct TokenOwnerInfo {
    pub mint: String,
    pub owner: String,
    pub account_index: u8,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Transfer {
    /// the sending account
    pub sender: String,
    /// the recipient token account
    pub recipient: String,
    /// the token mint involved in the transfer
    pub mint: String,
    /// the amount that was transferred
    pub amount: String,
}


impl From<DecodedInstruction> for Transfer {
    fn from(value: DecodedInstruction) -> Self {
        match value {
            DecodedInstruction::SystemInstruction(ix) => match ix {
                SystemInstructions::Transfer(tx) => Transfer {
                    sender: tx.source,
                    recipient: tx.destination,
                    mint: SOL_MINT.clone(),
                    amount: tx.lamports.to_string()
                },
                SystemInstructions::CreateAccount(tx) => Transfer {
                    sender: tx.source,
                    recipient: tx.new_account,
                    mint: SOL_MINT.clone(),
                    amount: tx.lamports.to_string(),
                },
                SystemInstructions::CreateAccountWithSeed(tx) => Transfer {
                    sender: tx.source,
                    recipient: tx.new_account,
                    mint: SOL_MINT.clone(),
                    amount: tx.lamports.to_string(),
                },
                SystemInstructions::TransferWithSeed(tx) => Transfer {
                    sender: tx.source,
                    recipient: tx.destination,
                    mint: SOL_MINT.clone(),
                    amount: tx.lamports
                },
                SystemInstructions::WithdrawNonceAccount(tx) => Transfer {
                    sender: tx.nonce_account,
                    recipient: tx.destination,
                    // nonce accounts hold sol
                    mint: SOL_MINT.clone(),
                    amount: tx.lamports
                }
            }
            DecodedInstruction::TokenInstruction(ix) => match ix {
                TokenInstructions::Transfer(tx) => Transfer {
                    sender: tx.source,
                    recipient: tx.destination,
                    mint: tx.mint.unwrap_or_default(),
                    amount: tx.amount,
                },
                TokenInstructions::MintTo(tx) => Transfer {
                    // mints have no sender, so empty string
                    sender: "".to_string(),
                    recipient: tx.account,
                    mint: tx.mint,
                    amount: tx.amount.to_string()
                },
                TokenInstructions::Burn(tx) => Transfer {
                    sender: tx.account,
                    // burns have no recipient
                    recipient: "".to_string(),
                    mint: tx.mint,
                    amount: tx.amount.to_string(),
                },
                TokenInstructions::TransferChecked(tx) => Transfer {
                    sender: tx.source,
                    recipient: tx.destination,
                    mint: tx.mint,
                    amount: tx.token_amount.amount
                },
                TokenInstructions::MintToChecked(tx) => Transfer {
                    sender: "".to_string(),
                    recipient: tx.account,
                    mint: tx.mint,
                    amount: tx.token_amount.amount
                },
                TokenInstructions::BurnChecked(tx) => Transfer {
                    sender: tx.account,
                    recipient: "".to_string(),
                    mint: tx.mint,
                    amount: tx.token_amount.amount,
                },
                TokenInstructions::CloseAccount(tx) => Transfer {
                    // should we use owner here instead?
                    sender: tx.account,
                    recipient: tx.destination,
                    // account closure will refund the rent which is always going to be SOL
                    mint: SOL_MINT.clone(),
                    // todo: need to figure out the way to handle this when the token account is for wsol
                    // for non wsol accounts this will just be the rent
                    amount: tx.amount.unwrap_or_default()
                }
            }
        }
    }
}