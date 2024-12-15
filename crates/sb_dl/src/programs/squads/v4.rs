use std::cmp::max;
use solana_sdk::{pubkey::Pubkey, system_program};

pub const ID: Pubkey = solana_sdk::pubkey!("SQDS4ep65T869zMMBKyuUq6aD6EgTu8psMjkvj52pCf");
pub const DISCRIMINATOR: [u8; 8] = [224, 116, 121, 186, 68, 161, 79, 236];
pub const MAX_TIME_LOCK: u32 = 3 * 30 * 24 * 60 * 60; // 3 months


#[derive(borsh::BorshSerialize, borsh::BorshDeserialize, Debug)]
pub struct MultisigV4 {
    __discriminator: [u8; 8],
    /// Key that is used to seed the multisig PDA.
    pub create_key: Pubkey,
    /// The authority that can change the multisig config.
    /// This is a very important parameter as this authority can change the members and threshold.
    ///
    /// The convention is to set this to `Pubkey::default()`.
    /// In this case, the multisig becomes autonomous, so every config change goes through
    /// the normal process of voting by the members.
    ///
    /// However, if this parameter is set to any other key, all the config changes for this multisig
    /// will need to be signed by the `config_authority`. We call such a multisig a "controlled multisig".
    pub config_authority: Pubkey,
    /// Threshold for signatures.
    pub threshold: u16,
    /// How many seconds must pass between transaction voting settlement and execution.
    pub time_lock: u32,
    /// Last transaction index. 0 means no transactions have been created.
    pub transaction_index: u64,
    /// Last stale transaction index. All transactions up until this index are stale.
    /// This index is updated when multisig config (members/threshold/time_lock) changes.
    pub stale_transaction_index: u64,
    /// The address where the rent for the accounts related to executed, rejected, or cancelled
    /// transactions can be reclaimed. If set to `None`, the rent reclamation feature is turned off.
    pub rent_collector: Option<Pubkey>,
    /// Bump for the multisig PDA seed.
    pub bump: u8,
    /// Members of the multisig.
    pub members: Vec<Member>,
}

impl MultisigV4 {

    pub fn num_voters(members: &[Member]) -> usize {
        members
            .iter()
            .filter(|m| m.permissions.has(Permission::Vote))
            .count()
    }

    pub fn num_proposers(members: &[Member]) -> usize {
        members
            .iter()
            .filter(|m| m.permissions.has(Permission::Initiate))
            .count()
    }

    pub fn num_executors(members: &[Member]) -> usize {
        members
            .iter()
            .filter(|m| m.permissions.has(Permission::Execute))
            .count()
    }

    /// Makes the transactions created up until this moment stale.
    /// Should be called whenever any multisig parameter related to the voting consensus is changed.
    pub fn invalidate_prior_transactions(&mut self) {
        self.stale_transaction_index = self.transaction_index;
    }

    /// Returns `Some(index)` if `member_pubkey` is a member, with `index` into the `members` vec.
    /// `None` otherwise.
    pub fn is_member(&self, member_pubkey: Pubkey) -> Option<usize> {
        self.members
            .binary_search_by_key(&member_pubkey, |m| m.key)
            .ok()
    }

    pub fn member_has_permission(&self, member_pubkey: Pubkey, permission: Permission) -> bool {
        match self.is_member(member_pubkey) {
            Some(index) => self.members[index].permissions.has(permission),
            _ => false,
        }
    }

    /// How many "reject" votes are enough to make the transaction "Rejected".
    /// The cutoff must be such that it is impossible for the remaining voters to reach the approval threshold.
    /// For example: total voters = 7, threshold = 3, cutoff = 5.
    pub fn cutoff(&self) -> usize {
        Self::num_voters(&self.members)
            .checked_sub(usize::from(self.threshold))
            .unwrap()
            .checked_add(1)
            .unwrap()
    }

    /// Add `new_member` to the multisig `members` vec and sort the vec.
    pub fn add_member(&mut self, new_member: Member) {
        self.members.push(new_member);
        self.members.sort_by_key(|m| m.key);
    }


    pub fn derive_vault_pda(
        multisig_pda: &Pubkey,
        index: u8,
    ) -> (Pubkey, u8) {
        Pubkey::find_program_address(
            &[
                b"multisig",
                multisig_pda.as_ref(),
                b"vault",
                &[index]
            ],
            &ID
        )
    }
    
}

#[derive(borsh::BorshSerialize, borsh::BorshDeserialize, Eq, PartialEq, Clone, Debug)]
pub struct Member {
    pub key: Pubkey,
    pub permissions: Permissions,
}

#[derive(Clone, Copy, Debug)]
pub enum Permission {
    Initiate = 1 << 0,
    Vote = 1 << 1,
    Execute = 1 << 2,
}

/// Bitmask for permissions.
#[derive(
    borsh::BorshSerialize, borsh::BorshDeserialize, Eq, PartialEq, Clone, Copy, Default, Debug,
)]
pub struct Permissions {
    pub mask: u8,
}

impl Permissions {
    /// Currently unused.
    pub fn from_vec(permissions: &[Permission]) -> Self {
        let mut mask = 0;
        for permission in permissions {
            mask |= *permission as u8;
        }
        Self { mask }
    }

    pub fn has(&self, permission: Permission) -> bool {
        self.mask & (permission as u8) != 0
    }
}