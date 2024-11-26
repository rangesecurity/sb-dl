use anyhow::{anyhow, Context};
use diesel::prelude::*;
use uuid::Uuid;

use crate::models::{
    BlockTableChoice, Blocks, DbBlocks, DbBlocks2, Idls, NewBlock, NewBlockTrait, NewSquads, Programs, Squads
};

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
    pub fn indexed_blocks(
        self,
        conn: &mut PgConnection,
        block_table_choice: BlockTableChoice,
    ) -> anyhow::Result<Vec<Option<i64>>> {
        let numbers: Vec<Option<i64>> = match block_table_choice {
            BlockTableChoice::Blocks => {
                use super::schema::blocks::dsl::{self, blocks};
                blocks
                    .select(dsl::slot)
                    .get_results(conn)
                    .with_context(|| "failed to select block numbers")?
            }
            BlockTableChoice::Blocks2 => {
                use super::schema::blocks_2::dsl::{self, blocks_2};
                blocks_2
                    .select(dsl::slot)
                    .get_results(conn)
                    .with_context(|| "failed to select block numbers")?
            }
        };

        Ok(numbers)
    }
    /// Returns up to `limit` blocks which do not have the slot column set
    pub fn slot_is_null(
        self,
        conn: &mut PgConnection,
        limit: i64,
        excluded_blocks: &[i64],
    ) -> anyhow::Result<Vec<Blocks>> {
        use crate::schema::blocks::dsl::*;
        let mut query = blocks.into_boxed().filter(slot.is_null());

        for excluded_block in excluded_blocks {
            query = query.filter(number.ne(excluded_block));
        }

        Ok(query
            .limit(limit)
            .select(DbBlocks::as_select())
            .load(conn)
            .with_context(|| "failed to load blocks")?
            .into_iter()
            .map(|block| Blocks {
                id: block.id,
                number: block.number,
                data: block.data,
                slot: block.slot,
            })
            .collect())
    }
    /// Returns up to `limit` blocks which have slot and number as the same
    pub fn slot_equals_blocks(
        self,
        conn: &mut PgConnection,
        limit: i64,
    ) -> anyhow::Result<Vec<Blocks>> {
        use crate::schema::blocks::dsl::*;
        Ok(blocks
            .filter(number.nullable().eq(slot))
            .limit(limit)
            .select(DbBlocks::as_select())
            .load(conn)
            .with_context(|| "failed to load blocks")?
            .into_iter()
            .map(|block| Blocks {
                id: block.id,
                number: block.number,
                data: block.data,
                slot: block.slot,
            })
            .collect())
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
        block_table_choice: BlockTableChoice,
    ) -> anyhow::Result<Vec<Blocks>> {
        use crate::schema::{blocks, blocks_2};
        match filter {
            BlockFilter::Number(blk_num) => match block_table_choice {
                BlockTableChoice::Blocks => Ok(blocks::dsl::blocks
                    .filter(blocks::dsl::number.eq(blk_num))
                    .select(DbBlocks::as_select())
                    .load(conn)?
                    .into_iter()
                    .map(|block| Blocks {
                        id: block.id,
                        number: block.number,
                        data: block.data,
                        slot: block.slot,
                    })
                    .collect()),
                BlockTableChoice::Blocks2 => Ok(blocks_2::dsl::blocks_2
                    .filter(blocks_2::dsl::number.eq(blk_num))
                    .select(DbBlocks2::as_select())
                    .load(conn)?
                    .into_iter()
                    .map(|block| Blocks {
                        id: block.id,
                        number: block.number,
                        data: block.data,
                        slot: block.slot,
                    })
                    .collect()),
            },
            BlockFilter::Slot(slot_num) => match block_table_choice {
                BlockTableChoice::Blocks => Ok(blocks::dsl::blocks
                    .filter(blocks::dsl::slot.eq(slot_num))
                    .select(DbBlocks::as_select())
                    .load(conn)?
                    .into_iter()
                    .map(|block| Blocks {
                        id: block.id,
                        number: block.number,
                        data: block.data,
                        slot: block.slot,
                    })
                    .collect()),
                BlockTableChoice::Blocks2 => Ok(blocks_2::dsl::blocks_2
                    .filter(blocks_2::dsl::slot.eq(slot_num))
                    .select(DbBlocks2::as_select())
                    .load(conn)?
                    .into_iter()
                    .map(|block| Blocks {
                        id: block.id,
                        number: block.number,
                        data: block.data,
                        slot: block.slot,
                    })
                    .collect()),
            },
            BlockFilter::FirstBlock => match block_table_choice {
                BlockTableChoice::Blocks => Ok(blocks::dsl::blocks
                    .order(blocks::dsl::number.asc())
                    .limit(1)
                    .select(DbBlocks::as_select())
                    .load(conn)?
                    .into_iter()
                    .map(|block| Blocks {
                        id: block.id,
                        number: block.number,
                        data: block.data,
                        slot: block.slot,
                    })
                    .collect()),
                BlockTableChoice::Blocks2 => Ok(blocks_2::dsl::blocks_2
                    .order(blocks_2::dsl::number.asc())
                    .limit(1)
                    .select(DbBlocks2::as_select())
                    .load(conn)?
                    .into_iter()
                    .map(|block| Blocks {
                        id: block.id,
                        number: block.number,
                        data: block.data,
                        slot: block.slot,
                    })
                    .collect()),
            },
            BlockFilter::All => match block_table_choice {
                BlockTableChoice::Blocks => Ok(blocks::dsl::blocks
                    .select(DbBlocks::as_select())
                    .load(conn)?
                    .into_iter()
                    .map(|block| Blocks {
                        id: block.id,
                        number: block.number,
                        data: block.data,
                        slot: block.slot,
                    })
                    .collect()),
                BlockTableChoice::Blocks2 => Ok(blocks_2::dsl::blocks_2
                    .select(DbBlocks2::as_select())
                    .load(conn)?
                    .into_iter()
                    .map(|block| Blocks {
                        id: block.id,
                        number: block.number,
                        data: block.data,
                        slot: block.slot,
                    })
                    .collect()),
            },
        }
    }
    pub fn select_squads<'a>(
        self,
        conn: &mut PgConnection,
        filter: SquadsFilter<'a>
    ) -> anyhow::Result<Vec<Squads>> {
        use crate::schema::squads::dsl::*;
        match filter {
            SquadsFilter::Account(acct) => Ok(
                squads
                .filter(account.eq(acct))
                .select(Squads::as_select())
                .load(conn)?
            ),
            SquadsFilter::All => Ok(
                squads
                .select(Squads::as_select())
                .load(conn)?
            )
        }
    }
    /// Used to update blocks which have missing slot information
    pub fn update_block_slot(
        self,
        conn: &mut PgConnection,
        block_id: Uuid,
        new_block_number: i64,
        slot_number: i64,
        block_table_choice: BlockTableChoice,
    ) -> anyhow::Result<()> {
        match block_table_choice {
            BlockTableChoice::Blocks => {
                use crate::schema::blocks::dsl::{self, blocks};
                diesel::update(blocks.filter(dsl::id.eq(block_id)))
                    .set((dsl::slot.eq(slot_number), dsl::number.eq(new_block_number)))
                    .execute(conn)?;
            }
            BlockTableChoice::Blocks2 => {
                use crate::schema::blocks_2::dsl::{self, blocks_2};
                diesel::update(blocks_2.filter(dsl::id.eq(block_id)))
                    .set((dsl::slot.eq(slot_number), dsl::number.eq(new_block_number)))
                    .execute(conn)?;
            }
        }

        Ok(())
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
    pub fn update_slot(
        self,
        conn: &mut PgConnection,
        block_number: i64,
        slot_number: i64,
        block_table_choice: BlockTableChoice,
    ) -> anyhow::Result<()> {
        conn.transaction::<_, anyhow::Error, _>(|conn| {
            match block_table_choice {
                BlockTableChoice::Blocks => {
                    use crate::schema::blocks;
                    match blocks::dsl::blocks
                        .filter(blocks::dsl::number.eq(&block_number))
                        .select(DbBlocks::as_select())
                        .limit(1)
                        .load(conn)
                    {
                        Ok(mut block_infos) => {
                            if block_infos.is_empty() {
                                return Ok(());
                            } else {
                                let mut block = std::mem::take(&mut block_infos[0]);
                                block.slot = Some(slot_number);
                                diesel::update(
                                    blocks::dsl::blocks.filter(blocks::dsl::id.eq(block.id)),
                                )
                                .set(block)
                                .execute(conn)?;
                            }
                        }
                        Err(err) => {
                            return Err(anyhow!(
                                "failed to check for block({block_number}) {err:#?}"
                            ))
                        }
                    }
                }
                BlockTableChoice::Blocks2 => {
                    use crate::schema::blocks_2;
                    match blocks_2::dsl::blocks_2
                        .filter(blocks_2::dsl::number.eq(&block_number))
                        .select(DbBlocks2::as_select())
                        .limit(1)
                        .load(conn)
                    {
                        Ok(mut block_infos) => {
                            if block_infos.is_empty() {
                                return Ok(());
                            } else {
                                let mut block = std::mem::take(&mut block_infos[0]);
                                block.slot = Some(slot_number);
                                diesel::update(
                                    blocks_2::dsl::blocks_2.filter(blocks_2::dsl::id.eq(block.id)),
                                )
                                .set(block)
                                .execute(conn)?;
                            }
                        }
                        Err(err) => {
                            return Err(anyhow!(
                                "failed to check for block({block_number}) {err:#?}"
                            ))
                        }
                    }
                }
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
        num_voters: i32,
        thres: i32,
        version: i32,
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
            voting_members_count.eq(num_voters)
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
        block_table_choice: BlockTableChoice,
    ) -> anyhow::Result<i64> {
        use crate::schema::{blocks, blocks_2};
        let end_number;
        let mut next_number = starting_number + 1;
        loop {
            match block_table_choice {
                BlockTableChoice::Blocks => {
                    match blocks::dsl::blocks
                        .filter(blocks::dsl::number.eq(&next_number))
                        .select(DbBlocks::as_select())
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
                            return Err(anyhow!(
                                "failed to check for block({next_number}) {err:#?}"
                            ))
                        }
                    }
                }
                BlockTableChoice::Blocks2 => match blocks_2::dsl::blocks_2
                    .filter(blocks_2::dsl::number.eq(&next_number))
                    .select(DbBlocks2::as_select())
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
                },
            }

            next_number += 1;
        }

        Ok(end_number)
    }

    pub fn insert_block(
        &self,
        conn: &mut PgConnection,
        new_block: impl NewBlockTrait,
    ) -> anyhow::Result<()> {
        let nb = NewBlock {
            number: new_block.number(),
            data: new_block.data(),
            slot: new_block.slot(),
        };
        match new_block.table_choice() {
            BlockTableChoice::Blocks => {
                nb.insert_into(crate::schema::blocks::table)
                    .on_conflict_do_nothing()
                    .execute(conn)
                    .with_context(|| "failed to insert into blocks")?;
            }
            BlockTableChoice::Blocks2 => {
                nb.insert_into(crate::schema::blocks_2::table)
                    .on_conflict_do_nothing()
                    .execute(conn)
                    .with_context(|| "failed to insert into blocks")?;
            }
        }
        Ok(())
    }
}
