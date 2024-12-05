use anyhow::{anyhow, Context};
use chrono::prelude::*;
use diesel::prelude::*;
use uuid::Uuid;

use crate::models::{Blocks, Idls, NewBlock, NewSquads, Programs, Squads};

#[derive(Clone, Copy)]
pub struct Client {}

#[derive(Clone, Copy)]
pub enum BlockFilter {
    /// filters for block based on slot number
    Slot(i64),
    /// filters for block based on block number
    Number(i64),
    /// returns the oldest block we have based on block number
    FirstBlock,
    /// return all blocks
    All,
}

#[derive(Clone)]
pub enum SquadsFilter<'a> {
    Account(&'a str),
    All,
}

impl Client {
    /// Returns the slot number of blocks which we have indexed
    pub fn indexed_blocks(self, conn: &mut PgConnection) -> anyhow::Result<Vec<i64>> {
        use super::schema::blocks::dsl::{self, blocks};
        let numbers: Vec<i64> = blocks
            .select(dsl::slot)
            .get_results(conn)
            .with_context(|| "failed to select block numbers")?;
        Ok(numbers)
    }
    pub fn indexed_program_ids(self, conn: &mut PgConnection) -> anyhow::Result<Vec<String>> {
        use crate::schema::programs::dsl::*;
        let ids = programs
            .select(id)
            .get_results(conn)
            .with_context(|| "failed to select program ids")?;
        Ok(ids)
    }
    /// Select a block matching against the block number or slot number
    pub fn select_block(
        self,
        conn: &mut PgConnection,
        filter: BlockFilter,
    ) -> anyhow::Result<Vec<Blocks>> {
        use crate::schema::blocks;
        match filter {
            BlockFilter::Number(blk_num) => Ok(blocks::dsl::blocks
                .filter(blocks::dsl::number.eq(blk_num))
                .select(Blocks::as_select())
                .get_results(conn)?),
            BlockFilter::Slot(slot_num) => Ok(blocks::dsl::blocks
                .filter(blocks::dsl::slot.eq(slot_num))
                .select(Blocks::as_select())
                .get_results(conn)?),
            BlockFilter::FirstBlock => Ok(blocks::dsl::blocks
                .order(blocks::dsl::number.asc())
                .limit(1)
                .select(Blocks::as_select())
                .get_results(conn)?),
            BlockFilter::All => Ok(blocks::dsl::blocks
                .select(Blocks::as_select())
                .get_results(conn)?),
        }
    }
    pub fn select_squads<'a>(
        self,
        conn: &mut PgConnection,
        filter: SquadsFilter<'a>,
    ) -> anyhow::Result<Vec<Squads>> {
        use crate::schema::squads::dsl::*;
        match filter {
            SquadsFilter::Account(acct) => Ok(squads
                .filter(account.eq(acct))
                .select(Squads::as_select())
                .load(conn)?),
            SquadsFilter::All => Ok(squads.select(Squads::as_select()).load(conn)?),
        }
    }
    pub fn insert_or_update_idl(
        self,
        conn: &mut PgConnection,
        program_id: String,
        b_height: i64,
        e_height: Option<i64>,
        program_idl: serde_json::Value,
    ) -> anyhow::Result<()> {
        use crate::schema::idls::dsl::*;
        conn.transaction::<_, anyhow::Error, _>(|conn| {
            match idls
                .filter(id.eq(&program_id))
                .filter(begin_height.eq(b_height))
                .limit(1)
                .select(Idls::as_select())
                .load(conn)
            {
                Ok(mut idl_infos) => {
                    if idl_infos.is_empty() {
                        // new idl
                        Idls {
                            id: program_id,
                            begin_height: b_height,
                            end_height: e_height,
                            idl: program_idl,
                        }
                        .insert_into(idls)
                        .execute(conn)?;
                    } else {
                        // updated idl
                        // todo: we need to set the end height of the old idl
                        let mut idl_info: Idls = std::mem::take(&mut idl_infos[0]);
                        idl_info.begin_height = b_height;
                        idl_info.end_height = e_height;
                        idl_info.idl = program_idl;
                        diesel::update(
                            idls.filter(id.eq(&program_id))
                                .filter(begin_height.eq(b_height)),
                        )
                        .set(idl_info)
                        .execute(conn)?;
                    }
                }
                Err(err) => return Err(anyhow!("failed to query db {err:#?}")),
            }
            Ok(())
        })?;
        Ok(())
    }
    pub fn insert_or_update_program(
        self,
        conn: &mut PgConnection,
        program_id: String,
        l_slot: i64,
        e_account: String,
        e_data: Vec<u8>,
    ) -> anyhow::Result<()> {
        use crate::schema::programs::dsl::*;
        conn.transaction::<_, anyhow::Error, _>(|conn| {
            match programs
                .filter(id.eq(&program_id))
                .filter(last_deployed_slot.eq(l_slot))
                .limit(1)
                .select(Programs::as_select())
                .load(conn)
            {
                Ok(p_infos) => {
                    if p_infos.is_empty() {
                        // new idl
                        Programs {
                            id: program_id,
                            last_deployed_slot: l_slot,
                            executable_account: e_account,
                            executable_data: e_data,
                        }
                        .insert_into(programs)
                        .execute(conn)?;
                    }
                    // program already exists
                }
                Err(err) => return Err(anyhow!("failed to query db {err:#?}")),
            }
            Ok(())
        })?;
        Ok(())
    }
    pub fn insert_or_update_squads(
        self,
        conn: &mut PgConnection,
        acct: &str,
        vault: &[String],
        member: &[String],
        num_voters: i64,
        thres: i64,
        version: i64,
    ) -> anyhow::Result<()> {
        use crate::schema::squads::dsl::*;
        NewSquads {
            account: acct,
            vaults: vault.to_vec(),
            members: member.to_vec(),
            threshold: thres,
            program_version: version,
            voting_members_count: num_voters,
        }
        .insert_into(squads)
        .on_conflict(account)
        .do_update()
        .set((
            vaults.eq(vault),
            members.eq(member),
            threshold.eq(thres),
            voting_members_count.eq(num_voters),
        ))
        .execute(conn)?;
        Ok(())
    }
    /// Given a starting block height, determine the next block for which we have data available.
    ///
    /// If starting_number == 10, and the return value is 20, this means we are missing data for blocks 10 -> 20
    pub fn find_gap_end(
        self,
        conn: &mut PgConnection,
        starting_number: i64,
    ) -> anyhow::Result<i64> {
        use crate::schema::blocks;
        let end_number;
        let mut next_number = starting_number + 1;
        loop {
            match blocks::dsl::blocks
                .filter(blocks::dsl::number.eq(&next_number))
                .select(Blocks::as_select())
                .limit(1)
                .load(conn)
            {
                Ok(block_infos) => {
                    if !block_infos.is_empty() {
                        end_number = next_number - 1;
                        break;
                    }
                }
                Err(err) => {
                    return Err(anyhow!("failed to check for block({next_number}) {err:#?}"))
                }
            }

            next_number += 1;
        }

        Ok(end_number)
    }

    pub fn insert_block(
        &self,
        conn: &mut PgConnection,
        n: i64,
        s: i64,
        t: Option<DateTime<Utc>>,
        d: &serde_json::Value,
    ) -> anyhow::Result<()> {
        use crate::schema::blocks::dsl::*;
        NewBlock {
            number: n,
            slot: s,
            time: t,
            processed: false,
            data: d,
        }
        .insert_into(blocks)
        .on_conflict_do_nothing()
        .execute(conn)?;

        Ok(())
    }
}
