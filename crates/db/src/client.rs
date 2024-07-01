use anyhow::Context;
use diesel::{query_dsl::methods::SelectDsl, Insertable, PgConnection, RunQueryDsl};

use crate::models::NewBlock;

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
}
