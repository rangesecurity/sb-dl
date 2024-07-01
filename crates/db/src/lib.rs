use diesel::prelude::*;

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
