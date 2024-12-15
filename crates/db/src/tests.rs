use std::collections::HashSet;

use client::{BlockFilter, Client, SquadsFilter};
use models::{NewBlock};

use crate::{migrations::run_migrations, test_utils::TestDb};

use super::*;
#[test]
fn test_blocks() {
    {
        let test_db = TestDb::new();
        test_db.delete_all_tables();
        drop(test_db);
    }
    let test_db = TestDb::new();
    run_migrations(&mut test_db.conn());
    let mut db_conn = test_db.conn();
    let client = Client {};
    for i in 1..100 {
        client
            .insert_block(
                &mut db_conn,
                i,
                i+1,
                None,
                &serde_json::json!({
                    "a": "b"
                })
            )
            .unwrap();
        let res = client.select_block(&mut db_conn, BlockFilter::Number(i)).unwrap();
        assert_eq!(res.len(), 1);
        let res = client.select_block(&mut db_conn, BlockFilter::Slot(i+1)).unwrap();
        assert_eq!(res.len(), 1);
    }

    for i in 200..300 {
        client
        .insert_block(
            &mut db_conn,
            i,
            i+1,
            None,
            &serde_json::json!({
                "a": "b"
            })
        )
        .unwrap();
        let res = client.select_block(&mut db_conn, BlockFilter::Number(i)).unwrap();
        assert_eq!(res.len(), 1);
        let res = client.select_block(&mut db_conn, BlockFilter::Slot(i+1)).unwrap();
        assert_eq!(res.len(), 1);
    }
    // check that setting slot by default worked
    let indexed_blocks: HashSet<i64> = client
        .indexed_blocks(&mut db_conn)
        .unwrap()
        .into_iter()
        .collect();
    let mut expected_set = (2..101).into_iter().collect::<HashSet<i64>>();
    assert_eq!(expected_set, indexed_blocks);
    // check that setting slot by default worked
    let indexed_blocks: HashSet<i64> = client
        .indexed_blocks(&mut db_conn)
        .unwrap()
        .into_iter()
        .collect();
    assert_eq!(expected_set, indexed_blocks);

    let indexed_blocks: HashSet<i64> = client
        .indexed_blocks(&mut db_conn)
        .unwrap()
        .into_iter()
        .collect();
    expected_set.extend((201..301).into_iter().collect::<HashSet<i64>>());
    assert_eq!(expected_set, indexed_blocks);
    let indexed_blocks: HashSet<i64> = client
        .indexed_blocks(&mut db_conn)
        .unwrap()
        .into_iter()
        .collect();
    assert_eq!(expected_set, indexed_blocks);

    // check that manual slot update worked
    drop(test_db);
}
#[test]
fn test_squads() {
    {
        let test_db = TestDb::new();
        test_db.delete_all_tables();
        drop(test_db);
    }
    let test_db = TestDb::new();
    let mut conn = test_db.conn();
    run_migrations(&mut conn);

    let client = Client{};

    client.insert_or_update_squads(
        &mut conn,
        "acct_1",
        &["vault_1".to_string()],
        &["member1".to_string()],
        1,
        1,
        4
    ).unwrap();

    let msig = client.select_squads(&mut conn, SquadsFilter::Account("acct_1")).unwrap();
    assert_eq!(msig.len(), 1);
    assert_eq!(msig[0].account, "acct_1".to_string());
    assert_eq!(msig[0].vaults, vec![Some("vault_1".to_string())]);
    assert_eq!(msig[0].members, vec![Some("member1".to_string())]);
    assert_eq!(msig[0].threshold, 1);
    assert_eq!(msig[0].program_version, 4);
    assert_eq!(msig[0].voting_members_count, 1);

    // test updates
    client.insert_or_update_squads(
        &mut conn,
        "acct_1",
        &["vault_1".to_string(), "vault_2".to_string()],
        &["member1".to_string()],
        1,
        1,
        3 // ensure the version can't be updated once its set
    ).unwrap();

    let msig = client.select_squads(&mut conn, SquadsFilter::Account("acct_1")).unwrap();
    assert_eq!(msig.len(), 1);
    assert_eq!(msig[0].account, "acct_1".to_string());
    assert_eq!(msig[0].vaults, vec![Some("vault_1".to_string()), Some("vault_2".to_string())]);
    assert_eq!(msig[0].members, vec![Some("member1".to_string())]);
    assert_eq!(msig[0].threshold, 1);
    assert_eq!(msig[0].program_version, 4);
    assert_eq!(msig[0].voting_members_count, 1);

    client.insert_or_update_squads(
        &mut conn,
        "acct_1",
        &["vault_1".to_string(), "vault_2".to_string()],
        &["member1".to_string(), "member2".to_string()],
        2,
        1,
        4
    ).unwrap();

    let msig = client.select_squads(&mut conn, SquadsFilter::Account("acct_1")).unwrap();
    assert_eq!(msig.len(), 1);
    assert_eq!(msig[0].account, "acct_1".to_string());
    assert_eq!(msig[0].vaults, vec![Some("vault_1".to_string()), Some("vault_2".to_string())]);
    assert_eq!(msig[0].members, vec![Some("member1".to_string()), Some("member2".to_string())]);
    assert_eq!(msig[0].threshold, 1);
    assert_eq!(msig[0].program_version, 4);
    assert_eq!(msig[0].voting_members_count, 2);

    client.insert_or_update_squads(
        &mut conn,
        "acct_1",
        &["vault_1".to_string(), "vault_2".to_string(), "vault_3".to_string()],
        &["member1".to_string(), "member2".to_string(), "member3".to_string()],
        3,
        2,
        4
    ).unwrap();

    let msig = client.select_squads(&mut conn, SquadsFilter::Account("acct_1")).unwrap();
    assert_eq!(msig.len(), 1);
    assert_eq!(msig[0].account, "acct_1".to_string());
    assert_eq!(msig[0].vaults, vec![Some("vault_1".to_string()), Some("vault_2".to_string()), Some("vault_3".to_string())]);
    assert_eq!(msig[0].members, vec![Some("member1".to_string()), Some("member2".to_string()), Some("member3".to_string())]);
    assert_eq!(msig[0].threshold, 2);
    assert_eq!(msig[0].voting_members_count, 3);
    assert_eq!(msig[0].program_version, 4);

    drop(test_db);
}