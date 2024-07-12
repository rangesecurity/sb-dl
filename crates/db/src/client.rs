use anyhow::{anyhow, Context};
use diesel::prelude::*;

use crate::models::{Idls, NewBlock};

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
    pub fn insert_block(
        self,
        conn: &mut PgConnection,
        block_number: i64,
        block_data: serde_json::Value,
    ) -> anyhow::Result<()> {
        use crate::schema::blocks::dsl::*;
        NewBlock {
            number: block_number,
            data: block_data,
        }
        .insert_into(blocks)
        .execute(conn)
        .with_context(|| "failed to insert block")?;
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
                        let mut idl_info: Idls = std::mem::take(&mut idl_infos[0]);
                        idl_info.begin_height = b_height;
                        idl_info.end_height = e_height;
                        idl_info.idl = program_idl;
                        diesel::update(idls.filter(id.eq(&program_id)))
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
}
