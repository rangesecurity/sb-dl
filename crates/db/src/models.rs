use {diesel::prelude::*, uuid::Uuid};

#[derive(Queryable, AsChangeset, Identifiable, Debug, Clone)]
#[diesel(table_name = super::schema::blocks)]
pub struct Blocks {
    pub id: Uuid,
    pub number: i64,
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


#[derive(Insertable)]
#[diesel(table_name = super::schema::blocks)]
pub struct NewBlock {
    pub number: i64,
    pub data: serde_json::Value,
}


#[derive(Insertable)]
#[diesel(table_name = super::schema::idls)]
pub struct NewIdl {
    pub id: String,
    pub begin_height: i64,
    pub end_height: Option<i64>,
    pub idl: serde_json::Value,
}