use {
    anyhow::{Context, Result},
    diesel::{
        r2d2::{ConnectionManager, Pool},

        prelude::*,
    },
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
pub fn new_connection_pool(db_url: &str, max_size: u32) -> Result<Pool<ConnectionManager<PgConnection>>> {
    let manager = ConnectionManager::<PgConnection>::new(db_url);

    Pool::builder()
        .max_size(max_size)
        .test_on_check_out(true)
        .build(manager).with_context(|| "failed to build connection pool")
}