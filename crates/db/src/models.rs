use {diesel::prelude::*, uuid::Uuid};

#[derive(Queryable, AsChangeset, Identifiable, Debug, Clone)]
#[diesel(table_name = super::schema::blocks)]
pub struct Blocks {
    pub id: Uuid,
    pub number: i64,
    pub data: serde_json::Value,
}

#[derive(Insertable)]
#[diesel(table_name = super::schema::blocks)]
pub struct NewBlock {
    pub number: i64,
    pub data: serde_json::Value,
}
