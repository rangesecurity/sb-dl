use anyhow::{anyhow, Context};
use diesel::prelude::*;

use crate::models::{Blocks, Idls, NewBlock, Programs};

#[derive(Clone, Copy)]
pub struct Client {}

impl Client {
    pub fn indexed_blocks(self, conn: &mut PgConnection) -> anyhow::Result<Vec<i64>> {
        use crate::schema::blocks::dsl::*;
        let numbers: Vec<i64> = blocks
            .select(number)
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
    pub fn insert_block(
        self,
        conn: &mut PgConnection,
        block_number: i64,
        slot_number: i64,
        block_data: serde_json::Value,
    ) -> anyhow::Result<()> {
        use crate::schema::blocks::dsl::*;
        conn.transaction::<_, anyhow::Error, _>(|conn| {
            match blocks
                .filter(number.eq(&block_number))
                .filter(slot.eq(&Some(slot_number)))
                .limit(1)
                .select(Blocks::as_select())
                .load(conn)
            {
                Ok(block_infos) => {
                    if block_infos.is_empty() {
                        NewBlock {
                            number: block_number,
                            data: block_data,
                            slot: Some(slot_number)
                        }
                        .insert_into(blocks)
                        .execute(conn)
                        .with_context(|| "failed to insert block")?;
                        Ok(())
                    } else {
                        // block already exists
                        return Ok(());
                    }
                }
                Err(err) => return Err(anyhow!("failed to check for pre-existing block {err:#?}")),
            }
        })?;
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
                Ok(mut p_infos) => {
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
}
