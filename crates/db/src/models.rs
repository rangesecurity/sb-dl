use {anyhow::anyhow, chrono::{DateTime, Utc}, diesel::prelude::*, uuid::Uuid};


#[derive(
    Queryable, AsChangeset, Identifiable, Debug, Clone, Selectable, Default, PartialEq, Eq,
)]
#[diesel(table_name = super::schema::blocks)]
#[diesel(primary_key(number))]
pub struct Blocks {
    pub number: i64,
    pub slot: i64,
    pub time: Option<DateTime<Utc>>,
    pub processed: bool,
    pub data: serde_json::Value,
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
#[diesel(primary_key(account))]
pub struct Squads {
    pub account: String,
    pub vaults: Vec<Option<String>>,
    pub members: Vec<Option<String>>,
    pub threshold: i64,
    pub program_version: i64,
    pub voting_members_count: i64,
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
#[diesel(table_name = super::schema::blocks)]
pub struct NewBlock<'a> {
    pub number: i64,
    pub slot: i64,
    pub time: Option<DateTime<Utc>>,
    pub processed: bool,
    pub data: &'a serde_json::Value,
}

#[derive(Insertable)]
#[diesel(table_name = super::schema::squads)]
pub struct NewSquads<'a> {
    pub account: &'a str,
    pub vaults: Vec<String>,
    pub members: Vec<String>,
    pub threshold: i64,
    pub program_version: i64,
    pub voting_members_count: i64,
}
