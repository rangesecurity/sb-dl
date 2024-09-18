use std::collections::HashSet;

use client::{BlockFilter, Client};
use models::{BlockTableChoice, NewBlock, NewBlock2, NewBlockTrait};

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
                NewBlock {
                    number: i,
                    data: serde_json::json!({
                        "a": "b"
                    }),
                    slot: Some(i+1)
                },
            )
            .unwrap();
        let res = client.select_block(&mut db_conn, BlockFilter::Number(i), BlockTableChoice::Blocks).unwrap();
        assert_eq!(res.len(), 1);
        let res = client.select_block(&mut db_conn, BlockFilter::Number(i), BlockTableChoice::Blocks2).unwrap();
        assert_eq!(res.len(), 0);
        client
            .insert_block(
                &mut db_conn,
                NewBlock2 {
                    number: i,
                    data: serde_json::json!({
                        "a": "b"
                    }),
                    slot: Some(i+1)
                },
            )
            .unwrap();
        let res = client.select_block(&mut db_conn, BlockFilter::Number(i), BlockTableChoice::Blocks).unwrap();
        assert_eq!(res.len(), 1);
        let res = client.select_block(&mut db_conn, BlockFilter::Number(i), BlockTableChoice::Blocks2).unwrap();
        assert_eq!(res.len(), 1);
        let res = client.select_block(&mut db_conn, BlockFilter::Number(i), BlockTableChoice::Blocks).unwrap();
        assert_eq!(res.len(), 1);
    }

    for i in 200..300 {
        client
        .insert_block(
            &mut db_conn,
            NewBlock {
                number: i,
                data: serde_json::json!({
                    "a": "b"
                }),
                slot: None
            },
        )
        .unwrap();
        let res = client.select_block(&mut db_conn, BlockFilter::Number(i), BlockTableChoice::Blocks).unwrap();
        assert_eq!(res.len(), 1);
        let res = client.select_block(&mut db_conn, BlockFilter::Number(i), BlockTableChoice::Blocks2).unwrap();
        assert_eq!(res.len(), 0);
        client
        .insert_block(
            &mut db_conn,
            NewBlock2 {
                number: i,
                data: serde_json::json!({
                    "a": "b"
                }),
                slot: None
            },
        )
            .unwrap();
        let res = client.select_block(&mut db_conn, BlockFilter::Number(i), BlockTableChoice::Blocks).unwrap();
        assert_eq!(res.len(), 1);
        let res = client.select_block(&mut db_conn, BlockFilter::Number(i), BlockTableChoice::Blocks2).unwrap();
        assert_eq!(res.len(), 1);
    }
    // check that setting slot by default worked
    let indexed_slots: HashSet<i64> = client
        .indexed_slots(&mut db_conn, BlockTableChoice::Blocks)
        .unwrap()
        .into_iter()
        .filter_map(|block| Some(block?))
        .collect();
    let mut expected_set = (2..101).into_iter().collect::<HashSet<i64>>();
    assert_eq!(expected_set, indexed_slots);
    // check that setting slot by default worked
    let indexed_slots: HashSet<i64> = client
        .indexed_slots(&mut db_conn, BlockTableChoice::Blocks2)
        .unwrap()
        .into_iter()
        .filter_map(|block| Some(block?))
        .collect();
    assert_eq!(expected_set, indexed_slots);

    // update slot number for blocks which are missing it
    for i in 200..300 {
        let block = client.select_block(
            &mut db_conn, 
            BlockFilter::Number(i),
            BlockTableChoice::Blocks
        ).unwrap();
        client.update_block_slot(
            &mut db_conn,
            block[0].id,
            i,
            i + 1,
            BlockTableChoice::Blocks
        ).unwrap();
        let block2 = client.select_block(&mut db_conn, BlockFilter::Number(i), BlockTableChoice::Blocks).unwrap();
        assert_eq!(block2[0].number, block[0].number);
        assert_eq!(block2[0].slot, Some(i+1));
        assert_ne!(block2[0].slot, block[0].slot);
        assert_eq!(block2[0].data, block[0].data);
        // verify all data is the same as the above except that the slot is still none
        let block2 = client.select_block(&mut db_conn, BlockFilter::Number(i), BlockTableChoice::Blocks2).unwrap();
        assert_eq!(block2[0].number, block[0].number);
        assert!(block2[0].slot.is_none());
        assert_eq!(block2[0].data, block[0].data);
        let block = client.select_block(
            &mut db_conn, 
            BlockFilter::Number(i),
            BlockTableChoice::Blocks2
        ).unwrap();
        client.update_block_slot(
            &mut db_conn,
            block[0].id,
            i,
            i + 1,
            BlockTableChoice::Blocks2
        ).unwrap();
        let block2 = client.select_block(&mut db_conn, BlockFilter::Number(i), BlockTableChoice::Blocks2).unwrap();
        assert_eq!(block2[0].number, block[0].number);
        assert_eq!(block2[0].slot, Some(i+1));
        assert_ne!(block2[0].slot, block[0].slot);
        assert_eq!(block2[0].data, block[0].data);
    }
    let indexed_slots: HashSet<i64> = client
        .indexed_slots(&mut db_conn, BlockTableChoice::Blocks)
        .unwrap()
        .into_iter()
        .filter_map(|block| Some(block?))
        .collect();
    expected_set.extend((201..301).into_iter().collect::<HashSet<i64>>());
    assert_eq!(expected_set, indexed_slots);
    let indexed_slots: HashSet<i64> = client
        .indexed_slots(&mut db_conn, BlockTableChoice::Blocks2)
        .unwrap()
        .into_iter()
        .filter_map(|block| Some(block?))
        .collect();
    assert_eq!(expected_set, indexed_slots);

    // now test the edge case where block_number == slot number and slot number is not null
    for i in 1000..1500 {
        client
            .insert_block(
                &mut db_conn,
                NewBlock {
                    number: i,
                    slot: Some(i),
                    data: serde_json::json!({
                        "a": "b"
                    }),
                }
            )
            .unwrap();
        let block_1 = client
            .select_block(&mut db_conn, client::BlockFilter::Slot(i), BlockTableChoice::Blocks)
            .unwrap();
        let block_2 = client
            .select_block(&mut db_conn, client::BlockFilter::Number(i), BlockTableChoice::Blocks)
            .unwrap();
        assert_eq!(block_1, block_2);
        // now update the block number
        client
            .update_block_slot(&mut db_conn, block_1[0].id, i + 1000, i, BlockTableChoice::Blocks)
            .unwrap();
        let block_3 = client
            .select_block(&mut db_conn, client::BlockFilter::Slot(i), BlockTableChoice::Blocks)
            .unwrap();
        let block_4 = client
            .select_block(&mut db_conn, client::BlockFilter::Number(i + 1000), BlockTableChoice::Blocks)
            .unwrap();
        assert_eq!(block_3, block_4);
        assert_eq!(block_1[0].data, block_3[0].data);
    }
    // now test the edge case where block_number == slot number and slot number is not null
    for i in 1000..1500 {
        client
            .insert_block(
                &mut db_conn,
                NewBlock2 {
                    number: i,
                    slot: Some(i),
                    data: serde_json::json!({
                        "a": "b"
                    }),
                }
            )
            .unwrap();
        let block_1 = client
            .select_block(&mut db_conn, client::BlockFilter::Slot(i), BlockTableChoice::Blocks2)
            .unwrap();
        let block_2 = client
            .select_block(&mut db_conn, client::BlockFilter::Number(i), BlockTableChoice::Blocks2)
            .unwrap();
        assert_eq!(block_1, block_2);
        // now update the block number
        client
            .update_block_slot(&mut db_conn, block_1[0].id, i + 1000, i, BlockTableChoice::Blocks2)
            .unwrap();
        let block_3 = client
            .select_block(&mut db_conn, client::BlockFilter::Slot(i), BlockTableChoice::Blocks2)
            .unwrap();
        let block_4 = client
            .select_block(&mut db_conn, client::BlockFilter::Number(i + 1000), BlockTableChoice::Blocks2)
            .unwrap();
        assert_eq!(block_3, block_4);
        assert_eq!(block_1[0].data, block_3[0].data);
    }

    // test the edge case where block_number == slot_number and slot_number is null
    for i in 3000..3500 {
        client
            .insert_block(
                &mut db_conn,
                NewBlock {
                    number: i,
                    slot: None,
                    data: serde_json::json!({
                        "a": "b"
                    })
                },
            )
            .unwrap();
        let block_1 = client
            .select_block(&mut db_conn, client::BlockFilter::Slot(i),BlockTableChoice::Blocks)
            .unwrap();
        let block_2 = client
            .select_block(&mut db_conn, client::BlockFilter::Number(i),BlockTableChoice::Blocks)
            .unwrap();
        assert!(block_1.is_empty());
        assert_eq!(block_2[0].number, i);
        assert!(block_2[0].slot.is_none());
        // now update the block number
        client
            .update_block_slot(&mut db_conn, block_2[0].id, i + 1000, i, BlockTableChoice::Blocks)
            .unwrap();
        let block_3 = client
            .select_block(&mut db_conn, client::BlockFilter::Slot(i),BlockTableChoice::Blocks)
            .unwrap();
        let block_4 = client
            .select_block(&mut db_conn, client::BlockFilter::Number(i + 1000),BlockTableChoice::Blocks)
            .unwrap();
        assert_eq!(block_3, block_4);
    }
    // test the edge case where block_number == slot_number and slot_number is null
    for i in 3000..3500 {
        client
            .insert_block(
                &mut db_conn,
                NewBlock2 {
                    number: i,
                    slot: None,
                    data: serde_json::json!({
                        "a": "b"
                    })
                },
            )
            .unwrap();
        let block_1 = client
            .select_block(&mut db_conn, client::BlockFilter::Slot(i),BlockTableChoice::Blocks2)
            .unwrap();
        let block_2 = client
            .select_block(&mut db_conn, client::BlockFilter::Number(i),BlockTableChoice::Blocks2)
            .unwrap();
        assert!(block_1.is_empty());
        assert_eq!(block_2[0].number, i);
        assert!(block_2[0].slot.is_none());
        // now update the block number
        client
            .update_block_slot(&mut db_conn, block_2[0].id, i + 1000, i, BlockTableChoice::Blocks2)
            .unwrap();
        let block_3 = client
            .select_block(&mut db_conn, client::BlockFilter::Slot(i),BlockTableChoice::Blocks2)
            .unwrap();
        let block_4 = client
            .select_block(&mut db_conn, client::BlockFilter::Number(i + 1000),BlockTableChoice::Blocks2)
            .unwrap();
        assert_eq!(block_3, block_4);
    }
    // check that manual slot update worked
    drop(test_db);
}

#[test]
fn test_update_slot() {
    {
        let test_db = TestDb::new();
        test_db.delete_all_tables();
        drop(test_db);
    }
    let test_db = TestDb::new();
    run_migrations(&mut test_db.conn());
    let mut db_conn = test_db.conn();
    let client = Client {};
    for i in 100..300 {
        client
            .insert_block(
                &mut db_conn,
                NewBlock {
                    number: i,
                    slot: None,
                    data: serde_json::json!({
                        "a": i.to_string()
                    })
                },
            )
            .unwrap();
    }
    let got_blocks: HashSet<i64> = client.select_block(&mut db_conn, BlockFilter::All, BlockTableChoice::Blocks).unwrap().into_iter().map(|block| block.number).collect();
    assert_eq!(
        got_blocks,
        (100..300).collect::<HashSet<i64>>()
    );
    for i in 100..300 {
        client
            .insert_block(
                &mut db_conn,
                NewBlock2 {
                    number: i,
                    slot: None,
                    data: serde_json::json!({
                        "a": i.to_string()
                    })
                },
            )
            .unwrap();
    }
    let got_blocks: HashSet<i64> = client.select_block(&mut db_conn, BlockFilter::All, BlockTableChoice::Blocks2).unwrap().into_iter().map(|block| block.number).collect();
    assert_eq!(
        got_blocks,
        (100..300).collect::<HashSet<i64>>()
    );
    for i in 100..300 {
        client.update_slot(
            &mut db_conn,
            i,
            i+1000,
            BlockTableChoice::Blocks
        ).unwrap();
        let block = &client.select_block(&mut db_conn, BlockFilter::Slot(i+1000), BlockTableChoice::Blocks).unwrap()[0];
        assert_eq!(
            block.data,
            serde_json::json!({
                "a": i.to_string()
            })
        );
        assert_eq!(block.number, i);
        assert_eq!(block.slot, Some(i+1000));
    }
    let block = &client.select_block(&mut db_conn, BlockFilter::FirstBlock, BlockTableChoice::Blocks).unwrap()[0];
    assert_eq!(block.number, 100);
    for i in 100..300 {
        client.update_slot(
            &mut db_conn,
            i,
            i+1000,
            BlockTableChoice::Blocks2
        ).unwrap();
        let block = &client.select_block(&mut db_conn, BlockFilter::Slot(i+1000), BlockTableChoice::Blocks2).unwrap()[0];
        assert_eq!(
            block.data,
            serde_json::json!({
                "a": i.to_string()
            })
        );
        assert_eq!(block.number, i);
        assert_eq!(block.slot, Some(i+1000));
    }
    let block = &client.select_block(&mut db_conn, BlockFilter::FirstBlock, BlockTableChoice::Blocks2).unwrap()[0];
    assert_eq!(block.number, 100);

    drop(test_db);
}