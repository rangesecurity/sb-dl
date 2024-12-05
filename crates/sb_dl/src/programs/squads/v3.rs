use {
    anchor_lang::{AnchorDeserialize, AnchorSerialize}, solana_sdk::pubkey::Pubkey
};

pub const ID: Pubkey = solana_sdk::pubkey!("SMPLecH534NA9acpos4G6x7uf3LWbCAwZQE9e8ZekMu");
pub const DISCRIMINATOR: [u8; 8] = [70, 118, 9, 108, 254, 215, 31, 120];

#[derive(AnchorSerialize, AnchorDeserialize, Debug)]
pub struct MultisigV3 {
    __discriminator: [u8; 8],
    pub threshold: u16,                 // threshold for signatures to execute.

    pub authority_index: u16,           // luxury field to help track how many authorities are currently used.

    pub transaction_index: u32,         // look up and seed reference for transactions.

    pub ms_change_index: u32,           // the last executed/closed transaction
                                        // this is needed to deprecate any active transactions
                                        // if the multisig is changed, helps prevent gaming.
                                        // this will automatically be increased when the multisig
                                        // is changed, ie. change of members or threshold.

    pub bump: u8,                       // bump for the multisig seed.

    pub create_key: Pubkey,             // random key(or not) used to seed the multisig pda.
                                   
    pub allow_external_execute: bool,   // DEPRECATED - allow non-member keys to execute txs

    pub keys: Vec<Pubkey>,              // keys of the members/owners of the multisig.
}

impl MultisigV3 {
    pub fn derive_vault_pda(
        multisig_pda: &Pubkey,
        authority_index: u32,
    ) -> (Pubkey, u8) {
        Pubkey::find_program_address(
            &[
                b"squad",
                multisig_pda.as_ref(),
                &authority_index.to_le_bytes()[..],
                b"authority",
            ],
            &ID
        )
    }
}