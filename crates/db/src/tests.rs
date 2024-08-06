use std::collections::HashSet;

use client::{Client, TokenMintFilter};

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
                Some(i + 1),
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
        .filter_map(|block| Some(block?))
        .collect();
    let mut expected_set = (2..101).into_iter().collect::<HashSet<i64>>();
    assert_eq!(expected_set, indexed_blocks);

    // update slot number for blocks which are missing it
    for i in 200..300 {
        client.update_block_slot(&mut db_conn, i, i, i + 1).unwrap();
    }
    let indexed_blocks: HashSet<i64> = client
        .indexed_blocks(&mut db_conn)
        .unwrap()
        .into_iter()
        .filter_map(|block| Some(block?))
        .collect();
    expected_set.extend((201..301).into_iter().collect::<HashSet<i64>>());
    assert_eq!(expected_set, indexed_blocks);

    // now test the edge case where block_number == slot number and slot number is not null
    for i in 1000..1500 {
        client
            .insert_block(
                &mut db_conn,
                i,
                Some(i),
                serde_json::json!({
                    "a": "b"
                }),
            )
            .unwrap();
        let block_1 = client
            .select_block(&mut db_conn, client::BlockFilter::Slot(i))
            .unwrap();
        let block_2 = client
            .select_block(&mut db_conn, client::BlockFilter::Number(i))
            .unwrap();
        assert_eq!(block_1, block_2);
        // now update the block number
        client
            .update_block_slot(&mut db_conn, i, i + 1000, i)
            .unwrap();
        let block_3 = client
            .select_block(&mut db_conn, client::BlockFilter::Slot(i))
            .unwrap();
        let block_4 = client
            .select_block(&mut db_conn, client::BlockFilter::Number(i + 1000))
            .unwrap();
        assert_eq!(block_3, block_4);
    }

    // test the edge case where block_number == slot_number and slot_number is null
    for i in 3000..3500 {
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
        let block_1 = client
            .select_block(&mut db_conn, client::BlockFilter::Slot(i))
            .unwrap();
        let block_2 = client
            .select_block(&mut db_conn, client::BlockFilter::Number(i))
            .unwrap();
        assert!(block_1.is_empty());
        assert_eq!(block_2[0].number, i);
        assert!(block_2[0].slot.is_none());
        // now update the block number
        client
            .update_block_slot(&mut db_conn, i, i + 1000, i)
            .unwrap();
        let block_3 = client
            .select_block(&mut db_conn, client::BlockFilter::Slot(i))
            .unwrap();
        let block_4 = client
            .select_block(&mut db_conn, client::BlockFilter::Number(i + 1000))
            .unwrap();
        assert_eq!(block_3, block_4);
    }
    // check that manual slot update worked
    drop(test_db);
}

#[test]
fn test_token_mints() {
    let test_db = TestDb::new();
    run_migrations(&mut test_db.conn());
    let mut db_conn = test_db.conn();
    let client = Client {};
    struct MintInfo {
        token_mint: String,
        token_name: Option<String>,
        token_symbol: Option<String>,
        token_decimals: f32,
        is_2022: bool,
    }
    let test_data = vec![
        MintInfo {
            token_mint: "mint_1".to_string(),
            token_name: Some("mintable_token_1".to_string()),
            token_symbol: Some("mntkn_1".to_string()),
            token_decimals: 9_f32,
            is_2022: false,
        },
        MintInfo {
            token_mint: "mint_2".to_string(),
            token_name: None,
            token_symbol: None,
            token_decimals: 6_f32,
            is_2022: true,
        },
        MintInfo {
            token_mint: "mint_3".to_string(),
            token_name: None,
            token_symbol: None,
            token_decimals: 3_f32,
            is_2022: false,
        },
    ];
    for td in test_data {
        client
            .insert_token_mint(
                &mut db_conn,
                td.token_mint,
                td.token_name,
                td.token_symbol,
                td.token_decimals,
                td.is_2022,
            )
            .unwrap();
    }
    let results = client
        .select_token_mint(&mut db_conn, TokenMintFilter::Mint("mint_1".to_string()))
        .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].mint, "mint_1");
    assert_eq!(results[0].name, Some("mintable_token_1".to_string()));
    assert_eq!(results[0].symbol, Some("mntkn_1".to_string()));
    assert_eq!(results[0].decimals, 9_f32);
    assert_eq!(results[0].token_2022, false);

    let results = client
        .select_token_mint(&mut db_conn, TokenMintFilter::Mint("mint_2".to_string()))
        .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].mint, "mint_2");
    assert_eq!(results[0].name, None);
    assert_eq!(results[0].symbol, None);
    assert_eq!(results[0].decimals, 6_f32);
    assert_eq!(results[0].token_2022, true);

    let results = client
        .select_token_mint(&mut db_conn, TokenMintFilter::Mint("mint_3".to_string()))
        .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].mint, "mint_3");
    assert_eq!(results[0].name, None);
    assert_eq!(results[0].symbol, None);
    assert_eq!(results[0].decimals, 3_f32);
    assert_eq!(results[0].token_2022, false);

    let results = client
        .select_token_mint(&mut db_conn, TokenMintFilter::IsToken2022(false))
        .unwrap();
    assert_eq!(results.len(), 2);
    let results = client
        .select_token_mint(&mut db_conn, TokenMintFilter::IsToken2022(true))
        .unwrap();
    assert_eq!(results.len(), 1);

    client
        .update_token_mint(
            &mut db_conn,
            "mint_2".to_string(),
            Some("mtn_2".to_string()),
            Some("m2".to_string()),
        )
        .unwrap();
    let results = client
        .select_token_mint(&mut db_conn, TokenMintFilter::Mint("mint_2".to_string()))
        .unwrap();
    assert_eq!(results[0].name, Some("mtn_2".to_string()));
    assert_eq!(results[0].symbol, Some("m2".to_string()));

    client
        .update_token_mint(
            &mut db_conn,
            "mint_3".to_string(),
            Some("mtn_3".to_string()),
            Some("m3".to_string()),
        )
        .unwrap();
    let results = client
        .select_token_mint(&mut db_conn, TokenMintFilter::Mint("mint_3".to_string()))
        .unwrap();
    assert_eq!(results[0].name, Some("mtn_3".to_string()));
    assert_eq!(results[0].symbol, Some("m3".to_string()));

    drop(test_db);
}
