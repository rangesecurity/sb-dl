use std::collections::HashSet;

use client::Client;

use crate::{migrations::run_migrations, test_utils::TestDb};

use super::*;
#[test]
fn test_blocks() {
    let test_db = TestDb::new();
    run_migrations(&mut test_db.conn());
    let mut db_conn = test_db.conn();
    let client = Client {};
    for i in 1..100 {
        client
            .insert_block(
                &mut db_conn,
                i,
                serde_json::json!({
                    "a": "b"
                }),
            )
            .unwrap();
    }
    let indexed_blocks: HashSet<i64> = client
        .indexed_blocks(&mut db_conn)
        .unwrap()
        .into_iter()
        .collect();
    let expected_set = (1..100).into_iter().collect::<HashSet<i64>>();
    assert_eq!(expected_set, indexed_blocks);
    drop(test_db);
}
