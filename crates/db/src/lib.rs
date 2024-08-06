use {
    anyhow::Result,
    deadpool_diesel::{Manager, Pool},
    diesel::prelude::*,
};
//use diesel::{Connection, PgConnection};
pub mod client;
pub mod migrations;
pub mod models;
pub mod schema;
#[cfg(any(test, feature = "testing"))]
pub mod test_utils;
#[cfg(test)]
pub mod tests;

/// establishes a single connection to postgres
pub fn new_connection(path: &str) -> anyhow::Result<PgConnection> {
    Ok(PgConnection::establish(path)?)
}

/// establishes a new connection pool manager to postgres
pub fn new_connection_pool(db_url: &str) -> Result<Pool<Manager<PgConnection>>> {
    let manager = deadpool_diesel::postgres::Manager::new(db_url, deadpool_diesel::Runtime::Tokio1);
    Ok(deadpool_diesel::postgres::Pool::builder(manager).build()?)
}
