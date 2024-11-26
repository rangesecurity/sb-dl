use {anyhow::anyhow, diesel::prelude::*, uuid::Uuid};

#[derive(Clone, Copy)]
#[repr(u8)]
pub enum BlockTableChoice{
    Blocks = 1,
    Blocks2 = 2,
}
impl TryFrom<u8> for BlockTableChoice {
    type Error = anyhow::Error;
    fn try_from(value: u8) -> Result<Self, Self::Error> {
        if value == 1 {
            return Ok(BlockTableChoice::Blocks)
        } else if value == 2 {
            return Ok(BlockTableChoice::Blocks2)
        } else {
            Err(anyhow!("invalid block table selection"))
        }
    }
}

#[derive(
    Queryable, AsChangeset, Identifiable, Debug, Clone, Selectable, Default, PartialEq, Eq,
)]
#[diesel(table_name = super::schema::blocks)]
pub struct DbBlocks {
    pub id: Uuid,
    pub number: i64,
    pub data: serde_json::Value,
    pub slot: Option<i64>,
}
#[derive(
    Queryable, AsChangeset, Identifiable, Debug, Clone, Selectable, Default, PartialEq, Eq,
)]
#[diesel(table_name = super::schema::blocks_2)]
pub struct DbBlocks2 {
    pub id: Uuid,
    pub number: i64,
    pub data: serde_json::Value,
    pub slot: Option<i64>,
}

/// A type which DbBlocks and DbBlocks2 can be converted into to return the same data type
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Blocks {
    pub id: Uuid,
    pub number: i64,
    pub data: serde_json::Value,
    pub slot: Option<i64>,
}

#[derive(Queryable, AsChangeset, Identifiable, Debug, Clone, Selectable, Default, Insertable)]
#[diesel(table_name = super::schema::idls)]
pub struct Idls {
    pub id: String,
    pub begin_height: i64,
    pub end_height: Option<i64>,
    pub idl: serde_json::Value,
}

#[derive(Queryable, AsChangeset, Identifiable, Debug, Clone, Selectable, Default, Insertable)]
#[diesel(table_name = super::schema::programs)]
pub struct Programs {
    pub id: String,
    pub last_deployed_slot: i64,
    pub executable_account: String,
    pub executable_data: Vec<u8>,
}

#[derive(Queryable, AsChangeset, Identifiable, Debug, Clone, Selectable, Default, Insertable)]
#[diesel(table_name = super::schema::squads)]
pub struct Squads {
    pub id: Uuid,
    pub account: String,
    pub vaults: Vec<Option<String>>,
    pub members: Vec<Option<String>>,
    pub threshold: i32,
    pub program_version: i32,
    pub voting_members_count: i32,
}

#[derive(Insertable)]
#[diesel(table_name = super::schema::blocks)]
#[diesel(table_name = super::schema::blocks_2)]
pub struct NewBlock{
    pub number: i64,
    pub data: serde_json::Value,
    pub slot: Option<i64>,
}

#[derive(Insertable)]
#[diesel(table_name = super::schema::idls)]
pub struct NewIdl {
    pub id: String,
    pub begin_height: i64,
    pub end_height: Option<i64>,
    pub idl: serde_json::Value,
}

#[derive(Insertable)]
#[diesel(table_name = super::schema::blocks_2)]
pub struct NewBlock2 {
    pub number: i64,
    pub data: serde_json::Value,
    pub slot: Option<i64>,
}

#[derive(Insertable)]
#[diesel(table_name = super::schema::squads)]
pub struct NewSquads<'a> {
    pub account: &'a str,
    pub vaults: Vec<String>,
    pub members: Vec<String>,
    pub threshold: i32,
    pub program_version: i32,
    pub voting_members_count: i32,
}

pub trait NewBlockTrait {
    fn number(&self) -> i64;
    fn data(&self) -> serde_json::Value;
    fn slot(&self) -> Option<i64>;
    fn table_choice(&self) -> BlockTableChoice;
}

impl NewBlockTrait for NewBlock {
    fn data(&self) -> serde_json::Value {
        self.data.clone()
    }
    fn number(&self) -> i64 {
        self.number
    }
    fn slot(&self) -> Option<i64> {
        self.slot
    }
    fn table_choice(&self) -> BlockTableChoice {
        BlockTableChoice::Blocks
    }
}
impl NewBlockTrait for NewBlock2 {
    fn data(&self) -> serde_json::Value {
        self.data.clone()
    }
    fn number(&self) -> i64 {
        self.number
    }
    fn slot(&self) -> Option<i64> {
        self.slot
    }
    fn table_choice(&self) -> BlockTableChoice {
        BlockTableChoice::Blocks2
    }
}