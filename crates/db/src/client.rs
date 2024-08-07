use anyhow::{anyhow, Context};
use diesel::prelude::*;

use crate::models::{Blocks, Idls, NewBlock, NewTokenMint, Programs, TokenMints};

#[derive(Clone, Copy)]
pub struct Client {}

#[derive(Clone, Copy)]
pub enum BlockFilter {
    Slot(i64),
    Number(i64),
}

#[derive(Clone)]
pub enum TokenMintFilter {
    Mint(String),
    IsToken2022(bool),
    All,
}

impl Client {
    /// Returns the slot number of blocks which we have indexed
    pub fn indexed_blocks(self, conn: &mut PgConnection) -> anyhow::Result<Vec<Option<i64>>> {
        use crate::schema::blocks::dsl::*;
        let numbers: Vec<Option<i64>> = blocks
            .select(slot)
            .get_results(conn)
            .with_context(|| "failed to select block numbers")?;
        Ok(numbers)
    }
    /// Returns up to `limit` blocks which do not have the slot column set or all blocks
    /// which have slot and number as the same
    pub fn partial_blocks(
        self,
        conn: &mut PgConnection,
        limit: i64,
    ) -> anyhow::Result<Vec<Blocks>> {
        use crate::schema::blocks::dsl::*;
        Ok(blocks
            .filter(slot.is_null().or(number.nullable().eq(slot)))
            .limit(limit)
            .select(Blocks::as_select())
            .load(conn)
            .with_context(|| "failed to load blocks")?)
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
        use crate::schema::blocks::dsl::*;
        match filter {
            BlockFilter::Number(blk_num) => Ok(blocks
                .filter(number.eq(blk_num))
                .select(Blocks::as_select())
                .load(conn)?),
            BlockFilter::Slot(slot_num) => Ok(blocks
                .filter(slot.eq(Some(slot_num)))
                .select(Blocks::as_select())
                .load(conn)?),
        }
    }
    pub fn select_token_mint(
        self,
        conn: &mut PgConnection,
        filter: TokenMintFilter,
    ) -> anyhow::Result<Vec<TokenMints>> {
        use crate::schema::token_mints::dsl::*;
        match filter {
            TokenMintFilter::IsToken2022(is_2022) => Ok(token_mints
                .filter(token_2022.eq(is_2022))
                .select(TokenMints::as_select())
                .load(conn)?),
            TokenMintFilter::Mint(tkn_mint) => Ok(token_mints
                .filter(mint.eq(tkn_mint))
                .select(TokenMints::as_select())
                .load(conn)?),
            TokenMintFilter::All => Ok(token_mints.select(TokenMints::as_select()).load(conn)?),
        }
    }
    /// Inserts a new block
    pub fn insert_block(
        self,
        conn: &mut PgConnection,
        block_number: i64,
        slot_number: Option<i64>,
        block_data: serde_json::Value,
    ) -> anyhow::Result<()> {
        use crate::schema::blocks::dsl::*;
        conn.transaction::<_, anyhow::Error, _>(|conn| {
            match blocks
                .filter(number.eq(&block_number))
                .filter(slot.eq(&slot_number))
                .limit(1)
                .select(Blocks::as_select())
                .load(conn)
            {
                Ok(block_infos) => {
                    if block_infos.is_empty() {
                        NewBlock {
                            number: block_number,
                            data: block_data,
                            slot: slot_number,
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
    /// Used to update blocks which have missing slot information
    pub fn update_block_slot(
        self,
        conn: &mut PgConnection,
        old_block_number: i64,
        new_block_number: i64,
        slot_number: i64,
    ) -> anyhow::Result<()> {
        use crate::schema::blocks::dsl::*;
        conn.transaction::<_, anyhow::Error, _>(|conn| {
            match blocks
                .filter(
                    number
                        .eq(&old_block_number)
                        .and(slot.is_null())
                        .or(number.eq(slot_number).and(slot.eq(slot_number))),
                )
                .select(Blocks::as_select())
                .limit(1)
                .load(conn)
            {
                Ok(mut block_infos) => {
                    if block_infos.is_empty() {
                        return Err(anyhow!("block({old_block_number})"));
                    } else {
                        let mut block = std::mem::take(&mut block_infos[0]);
                        block.slot = Some(slot_number);
                        block.number = new_block_number;

                        diesel::update(blocks.filter(id.eq(block.id)))
                            .set(block)
                            .execute(conn)?;
                    }
                }
                Err(err) => {
                    return Err(anyhow!(
                        "failed to check for block({old_block_number}) {err:#?}"
                    ))
                }
            }
            Ok(())
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
    pub fn insert_token_mint(
        self,
        conn: &mut PgConnection,
        tkn_mint: String,
        tkn_name: Option<String>,
        tkn_symbol: Option<String>,
        tkn_decimals: f32,
        is_2022: bool,
    ) -> anyhow::Result<()> {
        use crate::schema::token_mints::dsl::*;
        conn.transaction::<_, anyhow::Error, _>(|conn| {
            match token_mints
                .filter(mint.eq(&tkn_mint))
                .limit(1)
                .select(TokenMints::as_select())
                .load(conn)
            {
                Ok(token_infos) => {
                    if token_infos.is_empty() {
                        NewTokenMint {
                            mint: tkn_mint,
                            name: tkn_name,
                            symbol: tkn_symbol,
                            decimals: tkn_decimals,
                            token_2022: is_2022,
                        }
                        .insert_into(token_mints)
                        .execute(conn)?;
                    } else {
                        // token already exists
                    }
                }
                Err(err) => return Err(anyhow!("failed to query db {err:#?}")),
            }
            Ok(())
        })?;
        Ok(())
    }
    /// This is only used to update token name and symbol which cant be retrieved via the mint account
    /// and needs to be retrieved via a secondary source (ie: token metadata program)
    ///
    /// Decimals cant be changed after the mint account is created
    pub fn update_token_mint(
        self,
        conn: &mut PgConnection,
        tkn_mint: String,
        tkn_name: Option<String>,
        tkn_symbol: Option<String>,
    ) -> anyhow::Result<()> {
        use crate::schema::token_mints::dsl::*;
        conn.transaction::<_, anyhow::Error, _>(|conn| {
            match token_mints
                .filter(mint.eq(&tkn_mint))
                .limit(1)
                .select(TokenMints::as_select())
                .load(conn)
            {
                Ok(mut token_infos) => {
                    if token_infos.is_empty() {
                        return Err(anyhow!("token not found"));
                    } else {
                        let mut token_info = std::mem::take(&mut token_infos[0]);
                        token_info.name = tkn_name;
                        token_info.symbol = tkn_symbol;

                        diesel::update(token_mints.filter(mint.eq(&tkn_mint)))
                            .set(token_info)
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
