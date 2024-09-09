use {diesel::{prelude::*, query_builder::BoxedSqlQuery, sql_types::{BigInt, Jsonb, Nullable}}, uuid::Uuid};

#[derive(Clone, Copy)]
pub enum BlockTableChoice{
    Blocks,
    Blocks2,
}

#[derive(
    Queryable, AsChangeset, Identifiable, Debug, Clone, Selectable, Default, PartialEq, Eq,
)]
#[diesel(table_name = super::schema::blocks)]
pub struct Blocks {
    pub id: Uuid,
    pub number: i64,
    pub data: serde_json::Value,
    pub slot: Option<i64>,
}
#[derive(
    Queryable, AsChangeset, Identifiable, Debug, Clone, Selectable, Default, PartialEq, Eq,
)]
#[diesel(table_name = super::schema::blocks_2)]
pub struct Blocks2 {
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

#[derive(Insertable)]
#[diesel(table_name = super::schema::blocks)]
#[diesel(table_name = super::schema::blocks_2)]
pub struct NewBlock<'a> {
    pub number: i64,
    pub data: &'a serde_json::Value,
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
pub struct NewBlock2<'a> {
    pub number: i64,
    pub data: &'a serde_json::Value,
    pub slot: Option<i64>,
}

pub trait NewBlockTrait {
    fn number(&self) -> i64;
    fn data(&self) -> &serde_json::Value;
    fn slot(&self) -> Option<i64>;
    fn table_choice(&self) -> BlockTableChoice;
}

impl<'a> NewBlockTrait for NewBlock<'a> {
    fn data(&self) -> &'a serde_json::Value {
        &self.data
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
impl<'a> NewBlockTrait for NewBlock2<'a> {
    fn data(&self) -> &'a serde_json::Value {
        &self.data
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