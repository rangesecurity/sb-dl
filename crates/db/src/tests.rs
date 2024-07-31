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
                Some(i+1),
                serde_json::json!({
                    "a": "b"
                }),
            )
            .unwrap();
    }
    for i in 200..300 {
        client
            .insert_block(
                &mut db_conn,
                i,
                None,
                serde_json::json!({
                    "a": "b"
                }),
            )
            .unwrap();
    }
    // check that setting slot by default worked
    let indexed_blocks: HashSet<i64> = client
        .indexed_blocks(&mut db_conn)
        .unwrap()
        .into_iter()
        .filter_map(|block| {
            Some(block?)
        })
        .collect();
    let mut expected_set = (2..101).into_iter().collect::<HashSet<i64>>();
    assert_eq!(expected_set, indexed_blocks);

    // update slot number for blocks which are missing it
    for i in 200..300 {
        client
            .update_block_slot(
                &mut db_conn,
                i,
                i+1,
            )
            .unwrap();
    }
    let indexed_blocks: HashSet<i64> = client
    .indexed_blocks(&mut db_conn)
    .unwrap()
    .into_iter()
    .filter_map(|block| {
        Some(block?)
    })
    .collect();
    expected_set.extend((201..301).into_iter().collect::<HashSet<i64>>());
    assert_eq!(expected_set, indexed_blocks);

    // check that manual slot update worked
    drop(test_db);
}
