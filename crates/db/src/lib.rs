use {
    anyhow::{Context, Result},
    diesel::{
        r2d2::{ConnectionManager, Pool, PooledConnection},

        prelude::*,
    },
};
//use diesel::{Connection, PgConnection};
pub mod client;
pub mod migrations;
pub mod models;
pub mod schema;
pub mod blocks_stream;
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

/// establishes a connection to postgres using tokio_postgres, specifically
/// for leveraging the LISTEN capability of postgres
pub async fn new_tokio_postgres_connection(path: &str) -> anyhow::Result<tokio_postgres::Client> {
    let (client, connection) = tokio_postgres::connect(path, tokio_postgres::NoTls).await?;
    tokio::task::spawn(async move {
        if let Err(err) = connection.await {
            log::error!("failed to connect {err:#?}");
        }
    });
    Ok(client)
}