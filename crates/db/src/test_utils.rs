//! provides a helper object for creating temporary test databases
//! taken from https://github.com/diesel-rs/diesel/issues/1549

use diesel::{sql_query, Connection, PgConnection, RunQueryDsl};
use rand::{thread_rng, Rng};

pub struct TestDb {
    default_db_url: String,
    name: String,
    delete_on_drop: bool,
}
impl TestDb {
    pub fn new() -> Self {
        let mut rng = thread_rng();
        let id = rng.gen_range(u64::MIN..u64::MAX);
        let name = format!("test_db_{}_{}", std::process::id(), id);
        let default_db_url = "postgres://postgres:password123@localhost";
        let mut conn = super::new_connection(default_db_url).unwrap();
        sql_query(format!("CREATE DATABASE {name};"))
            .execute(&mut conn)
            .unwrap();
        Self {
            default_db_url: default_db_url.to_string(),
            name,
            delete_on_drop: true,
        }
    }
    pub fn conn(&self) -> PgConnection {
        PgConnection::establish(self.default_db_url.as_str()).unwrap()
    }

    pub fn leak(&mut self) {
        self.delete_on_drop = false;
    }
    pub fn delete_all_tables(&self) {
        let mut conn = self.conn();
        let _ = diesel::delete(super::schema::blocks::dsl::blocks).execute(&mut conn);
        let _ = diesel::delete(super::schema::idls::dsl::idls).execute(&mut conn);
        let _ = diesel::delete(super::schema::squads::dsl::squads).execute(&mut conn);
        let _ = diesel::delete(super::schema::programs::dsl::programs).execute(&mut conn);
    }
    pub fn name(&self) -> String {
        self.name.clone()
    }
}
impl Drop for TestDb {
    fn drop(&mut self) {
        if !self.delete_on_drop {
            return;
        }
        self.delete_all_tables();
        let mut conn = PgConnection::establish(&self.default_db_url).unwrap();
        sql_query(format!(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '{}'",
            self.name
        ))
        .execute(&mut conn)
        .unwrap();
        sql_query(format!("DROP DATABASE {}", self.name))
            .execute(&mut conn)
            .unwrap();
    }
}

impl Default for TestDb {
    fn default() -> Self {
        Self::new()
    }
}
