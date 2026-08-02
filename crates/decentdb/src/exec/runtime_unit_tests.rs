//! Unit tests for exec module helpers to increase coverage.

#[cfg(test)]
mod tests {
    use super::super::*;
    use crate::record::value::Value;
    use std::collections::{BTreeMap, BTreeSet};
    use std::ops::Bound;
    use std::sync::Arc;

    fn assert_encoded_posting_shrink_count_exact(postings: &RuntimeEncodedPostings) {
        let actual = postings
            .values()
            .filter(|row_ids| row_ids.is_shrinkable())
            .count();
        assert_eq!(postings.shrinkable_posting_count(), actual);
    }

    #[test]
    fn map_get_ci_and_mut() {
        let mut map: BTreeMap<String, i32> = BTreeMap::new();
        map.insert("AbC".to_string(), 1);
        assert_eq!(map_get_ci(&map, "abc").copied(), Some(1));

        let mut map2: BTreeMap<String, i32> = BTreeMap::new();
        map2.insert("Xy".to_string(), 2);
        if let Some(v) = map_get_ci_mut(&mut map2, "xy") {
            *v = 3;
        }
        assert_eq!(map2.get("Xy"), Some(&3));
    }

    #[test]
    fn tabledata_row_index_and_by_id() {
        let mut td = TableData::default();
        td.push_row(StoredRow {
            row_id: 1,
            values: vec![],
        });
        td.push_row(StoredRow {
            row_id: 3,
            values: vec![],
        });
        assert_eq!(td.row_index_by_id(1), Some(0));
        assert_eq!(td.row_index_by_id(3), Some(1));
        assert_eq!(td.row_by_id(3).unwrap().row_id, 3);

        // binary_search path: non-zero offset
        let mut td2 = TableData::default();
        td2.push_row(StoredRow {
            row_id: 5,
            values: vec![],
        });
        td2.push_row(StoredRow {
            row_id: 10,
            values: vec![],
        });
        assert_eq!(td2.row_index_by_id(10), Some(1));
    }

    #[test]
    fn int64_identity_hasher_and_rowidset() {
        let mut h = Int64IdentityHasher::default();
        h.write_i64(42);
        assert_eq!(h.finish(), 42);

        let mut h2 = Int64IdentityHasher::default();
        h2.write(&[1, 2, 3, 4, 5, 6, 7, 8]);
        assert_ne!(h2.finish(), 0);

        assert_eq!(RuntimeRowIdSet::Empty.len(), 0);
        assert!(RuntimeRowIdSet::Empty.is_empty());
        assert_eq!(RuntimeRowIdSet::Single(10).len(), 1);
        let mut seen = vec![];
        RuntimeRowIdSet::Many(&[1, 2, 3]).for_each(|id| seen.push(id));
        assert_eq!(seen, vec![1, 2, 3]);
    }

    #[test]
    fn runtime_btree_keys_encoded_unique_operations() {
        let mut keys_map: BTreeMap<RuntimeEncodedKey, i64> = BTreeMap::new();
        let key = RuntimeEncodedKey::from_slice(&[1u8, 2, 3]);
        keys_map.insert(key.clone(), 7);
        let r = RuntimeBtreeKeys::UniqueEncoded(Arc::new(keys_map), BTreeSet::new());
        assert_eq!(
            r.row_ids_for_key(&RuntimeBtreeKey::Encoded(key.clone())),
            vec![7]
        );
        assert!(r.contains_any(&RuntimeBtreeKey::Encoded(key.clone())));

        // insert duplicate -> error
        let mut r2 = RuntimeBtreeKeys::UniqueEncoded(Arc::new(BTreeMap::new()), BTreeSet::new());
        r2.insert_row_id(RuntimeBtreeKey::Encoded(vec![9].into()), 1)
            .unwrap();
        assert!(r2
            .insert_row_id(RuntimeBtreeKey::Encoded(vec![9].into()), 2)
            .is_err());

        // type mismatch -> error
        let mut r3 = RuntimeBtreeKeys::UniqueEncoded(Arc::new(BTreeMap::new()), BTreeSet::new());
        assert!(r3.insert_row_id(RuntimeBtreeKey::Int64(1), 1).is_err());
    }

    #[test]
    fn remove_row_id_unique_and_nonunique_behaviour() {
        let mut keys_map: BTreeMap<RuntimeEncodedKey, i64> = BTreeMap::new();
        keys_map.insert(vec![1].into(), 99);
        let mut rt = RuntimeBtreeKeys::UniqueEncoded(Arc::new(keys_map), BTreeSet::new());
        // mismatch
        assert!(rt
            .remove_row_id(&RuntimeBtreeKey::Encoded(vec![1].into()), 1)
            .is_err());
        // correct remove
        rt.remove_row_id(&RuntimeBtreeKey::Encoded(vec![1].into()), 99)
            .unwrap();
        assert!(!rt.contains_any(&RuntimeBtreeKey::Encoded(vec![1].into())));

        let mut keys2: BTreeMap<RuntimeEncodedKey, RuntimeEncodedRowIds> = BTreeMap::new();
        keys2.insert(vec![2].into(), RuntimeEncodedRowIds::many(vec![1, 2]));
        let mut rt2 = RuntimeBtreeKeys::NonUniqueEncoded(
            Arc::new(RuntimeEncodedPostings::new(keys2)),
            BTreeSet::new(),
        );
        rt2.remove_row_id(&RuntimeBtreeKey::Encoded(vec![2].into()), 1)
            .unwrap();
        assert!(rt2.contains_any(&RuntimeBtreeKey::Encoded(vec![2].into())));
        rt2.remove_row_id(&RuntimeBtreeKey::Encoded(vec![2].into()), 2)
            .unwrap();
        assert!(!rt2.contains_any(&RuntimeBtreeKey::Encoded(vec![2].into())));
    }

    #[test]
    fn encoded_row_ids_preserve_vec_sized_owner_and_promotion_order() {
        let posting_size = std::mem::size_of::<RuntimeEncodedRowIds>();
        let vec_size = std::mem::size_of::<Vec<i64>>();
        assert_eq!(posting_size, vec_size);

        let mut posting = RuntimeEncodedRowIds::one(10);
        assert_eq!(posting.as_slice(), &[10]);
        #[cfg(target_pointer_width = "64")]
        assert!(posting.is_inline_singleton());
        #[cfg(target_pointer_width = "32")]
        assert!(!posting.is_inline_singleton());
        posting.push(11);
        assert_eq!(posting.as_slice(), &[10, 11]);
        assert!(!posting.is_inline_singleton());
    }

    #[test]
    fn nonunique_encoded_move_revival_and_cow_preserve_mappings() {
        let first_key = RuntimeBtreeKey::Encoded(vec![1; 32].into());
        let second_key = RuntimeBtreeKey::Encoded(vec![2].into());
        let revived_key = RuntimeBtreeKey::Encoded(vec![3; 40].into());
        assert!(matches!(&first_key, RuntimeBtreeKey::Encoded(key) if key.spilled()));
        assert!(matches!(&revived_key, RuntimeBtreeKey::Encoded(key) if key.spilled()));
        let mut runtime = RuntimeBtreeKeys::NonUniqueEncoded(
            Arc::new(RuntimeEncodedPostings::new(BTreeMap::new())),
            BTreeSet::new(),
        );

        runtime.insert_row_id(first_key.clone(), 10).unwrap();
        let snapshot = runtime.clone();
        runtime.insert_row_id(first_key.clone(), 11).unwrap();
        assert_eq!(snapshot.row_ids_for_key(&first_key), vec![10]);
        assert_eq!(runtime.row_ids_for_key(&first_key), vec![10, 11]);

        assert!(runtime
            .move_row_id(&first_key, second_key.clone(), 10)
            .unwrap());
        assert_eq!(runtime.row_ids_for_key(&first_key), vec![11]);
        assert_eq!(runtime.row_ids_for_key(&second_key), vec![10]);

        runtime.remove_row_id(&second_key, 10).unwrap();
        assert!(runtime.row_ids_for_key(&second_key).is_empty());
        runtime.insert_row_id(revived_key.clone(), 10).unwrap();
        assert!(runtime.row_ids_for_key(&second_key).is_empty());
        assert_eq!(runtime.row_ids_for_key(&revived_key), vec![10]);

        let RuntimeBtreeKeys::NonUniqueEncoded(entries, _) = &runtime else {
            panic!("expected encoded runtime index");
        };
        assert!(!entries.contains_key(&[2][..]));
        assert_eq!(
            entries
                .get(&[3; 40][..])
                .map(RuntimeEncodedRowIds::as_slice),
            Some(&[10][..])
        );
    }

    #[test]
    fn encoded_posting_shrink_demotes_a_single_remaining_row_id() {
        let mut row_ids = Vec::with_capacity(8);
        row_ids.push(42);
        let mut posting = RuntimeEncodedRowIds::many(row_ids);

        #[cfg(target_pointer_width = "64")]
        let expected_freed = 8 * std::mem::size_of::<i64>();
        #[cfg(target_pointer_width = "32")]
        let expected_freed = 7 * std::mem::size_of::<i64>();
        assert_eq!(posting.shrink_to_fit(), expected_freed);
        assert_eq!(posting.as_slice(), &[42]);
        #[cfg(target_pointer_width = "64")]
        assert!(posting.is_inline_singleton());
        #[cfg(target_pointer_width = "32")]
        assert!(!posting.is_inline_singleton());
    }

    #[test]
    fn encoded_posting_commit_shrink_skips_singletons_and_tracks_only_shrinkable_lists() {
        let singleton_entries = (0_u16..1_000)
            .map(|value| {
                (
                    value.to_be_bytes().to_vec().into(),
                    RuntimeEncodedRowIds::one(i64::from(value)),
                )
            })
            .collect();
        let mut singleton_postings = RuntimeEncodedPostings::new(singleton_entries);
        assert_eq!(singleton_postings.shrinkable_posting_count(), 0);
        assert_eq!(singleton_postings.shrink_to_fit(), 0);

        let key = RuntimeEncodedKey::from_slice(&[9]);
        singleton_postings.insert_row_id(key.clone(), 2_000);
        singleton_postings.insert_row_id(key, 2_001);
        assert_eq!(singleton_postings.shrinkable_posting_count(), 1);
        assert!(singleton_postings.shrink_to_fit() > 0);
        assert_encoded_posting_shrink_count_exact(&singleton_postings);
        assert_eq!(singleton_postings.shrink_to_fit(), 0);
    }

    #[test]
    fn encoded_posting_many_reduces_to_singleton_and_empty() {
        let mut row_ids = Vec::with_capacity(8);
        row_ids.extend([41, 42]);
        let mut posting = RuntimeEncodedRowIds::many(row_ids);

        posting.retain(|row_id| *row_id == 42);
        assert_eq!(posting.as_slice(), &[42]);
        assert!(posting.is_shrinkable());
        assert!(posting.shrink_to_fit() > 0);
        assert_eq!(posting.as_slice(), &[42]);

        posting.retain(|_| false);
        assert!(posting.is_empty());
        assert_eq!(posting.as_slice(), &[] as &[i64]);

        let mut row_ids = Vec::with_capacity(8);
        row_ids.extend([1, 2]);
        let mut emptied_many = RuntimeEncodedRowIds::many(row_ids);
        emptied_many.retain(|_| false);
        assert!(emptied_many.is_empty());
        assert!(emptied_many.shrink_to_fit() > 0);
    }

    #[test]
    fn encoded_posting_remove_everywhere_recomputes_exact_shrink_state() {
        let mut first = Vec::with_capacity(4);
        first.extend([1, 2]);
        let mut second = Vec::with_capacity(4);
        second.extend([1, 3]);
        let entries = BTreeMap::from([
            (
                RuntimeEncodedKey::from_slice(&[1]),
                RuntimeEncodedRowIds::many(first),
            ),
            (
                RuntimeEncodedKey::from_slice(&[2]),
                RuntimeEncodedRowIds::many(second),
            ),
            (
                RuntimeEncodedKey::from_slice(&[3]),
                RuntimeEncodedRowIds::one(4),
            ),
        ]);
        let mut postings = RuntimeEncodedPostings::new(entries);
        assert_eq!(postings.shrinkable_posting_count(), 2);

        postings.remove_row_id_everywhere(1);
        assert_eq!(
            postings.get(&[1][..]).map(|ids| ids.as_slice()),
            Some(&[2][..])
        );
        assert_eq!(
            postings.get(&[2][..]).map(|ids| ids.as_slice()),
            Some(&[3][..])
        );
        assert_eq!(postings.shrinkable_posting_count(), 2);

        postings.remove_row_id_everywhere(2);
        assert!(!postings.contains_key(&[1][..]));
        assert_eq!(postings.shrinkable_posting_count(), 1);
        postings.remove_row_id_everywhere(3);
        assert!(!postings.contains_key(&[2][..]));
        assert_eq!(postings.shrinkable_posting_count(), 0);
        assert_eq!(
            postings.get(&[3][..]).map(|ids| ids.as_slice()),
            Some(&[4][..])
        );
    }

    #[test]
    fn encoded_posting_repromotion_after_shrink_restores_dirty_state() {
        let key = RuntimeEncodedKey::from_slice(&[9]);
        let mut postings = RuntimeEncodedPostings::new(BTreeMap::new());
        postings.insert_row_id(key.clone(), 1);
        postings.insert_row_id(key.clone(), 2);
        assert_eq!(postings.shrinkable_posting_count(), 1);

        postings.remove_row_id_for_key(key.as_slice(), 2);
        assert_eq!(postings.shrinkable_posting_count(), 1);
        assert!(postings.shrink_to_fit() > 0);
        #[cfg(target_pointer_width = "64")]
        assert_eq!(postings.shrinkable_posting_count(), 0);
        assert_encoded_posting_shrink_count_exact(&postings);
        assert_eq!(
            postings.get(key.as_slice()).map(|ids| ids.as_slice()),
            Some(&[1][..])
        );

        postings.insert_row_id(key.clone(), 3);
        assert_eq!(
            postings.get(key.as_slice()).map(|ids| ids.as_slice()),
            Some(&[1, 3][..])
        );
        assert_eq!(postings.shrinkable_posting_count(), 1);
        assert!(postings.shrink_to_fit() > 0);
        assert_encoded_posting_shrink_count_exact(&postings);
    }

    #[test]
    fn encoded_posting_arc_cow_keeps_shrink_state_isolated() {
        let key = RuntimeBtreeKey::Encoded(RuntimeEncodedKey::from_slice(&[7]));
        let mut live = RuntimeBtreeKeys::NonUniqueEncoded(
            Arc::new(RuntimeEncodedPostings::new(BTreeMap::new())),
            BTreeSet::new(),
        );
        live.insert_row_id(key.clone(), 1).unwrap();
        live.insert_row_id(key.clone(), 2).unwrap();
        let snapshot = live.clone();

        assert_eq!(live.shrink_to_fit_if_unique(), 0);
        for keys in [&live, &snapshot] {
            let RuntimeBtreeKeys::NonUniqueEncoded(postings, _) = keys else {
                panic!("expected encoded postings");
            };
            assert_eq!(postings.shrinkable_posting_count(), 1);
        }

        live.insert_row_id(key.clone(), 3).unwrap();
        assert_eq!(snapshot.row_ids_for_key(&key), vec![1, 2]);
        assert_eq!(live.row_ids_for_key(&key), vec![1, 2, 3]);
        let RuntimeBtreeKeys::NonUniqueEncoded(snapshot_postings, _) = &snapshot else {
            panic!("expected encoded postings");
        };
        let RuntimeBtreeKeys::NonUniqueEncoded(live_postings, _) = &live else {
            panic!("expected encoded postings");
        };
        assert_eq!(snapshot_postings.shrinkable_posting_count(), 1);
        assert_eq!(live_postings.shrinkable_posting_count(), 1);

        drop(snapshot);
        assert!(live.shrink_to_fit_if_unique() > 0);
        let RuntimeBtreeKeys::NonUniqueEncoded(postings, _) = &live else {
            panic!("expected encoded postings");
        };
        assert_encoded_posting_shrink_count_exact(postings);
    }

    #[test]
    fn encoded_unique_and_nonunique_borrowed_ranges_cross_key_storage_boundary() {
        let short = encode_runtime_index_key(&Value::Text("a".into())).unwrap();
        let middle = encode_runtime_index_key(&Value::Text("middle".into())).unwrap();
        let long =
            encode_runtime_index_key(&Value::Text("zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz".into()))
                .unwrap();
        assert!(!short.spilled());
        assert!(long.spilled());
        let lower = short.as_slice().to_vec();
        let upper = long.as_slice().to_vec();

        let unique = BTreeMap::from([(short.clone(), 1), (middle.clone(), 2), (long.clone(), 3)]);
        assert_eq!(
            unique
                .range::<[u8], _>((
                    Bound::Included(lower.as_slice()),
                    Bound::Included(upper.as_slice()),
                ))
                .map(|(_, row_id)| *row_id)
                .collect::<Vec<_>>(),
            vec![1, 2, 3]
        );

        let nonunique = RuntimeEncodedPostings::new(BTreeMap::from([
            (short, RuntimeEncodedRowIds::one(1)),
            (middle, RuntimeEncodedRowIds::many(vec![2, 20])),
            (long, RuntimeEncodedRowIds::one(3)),
        ]));
        assert_eq!(
            nonunique
                .range::<[u8], _>((
                    Bound::Included(lower.as_slice()),
                    Bound::Included(upper.as_slice()),
                ))
                .flat_map(|(_, row_ids)| row_ids.iter().copied())
                .collect::<Vec<_>>(),
            vec![1, 2, 20, 3]
        );
    }

    #[test]
    fn unique_int64_row_ids_for_key_and_value() {
        let mut m: Int64Map<i64> = Int64Map::default();
        m.insert(9, 33);
        let rt = RuntimeBtreeKeys::UniqueInt64(Arc::new(m.into()), BTreeSet::new());
        assert_eq!(rt.row_ids_for_key(&RuntimeBtreeKey::Int64(9)), vec![33]);
        assert_eq!(rt.row_ids_for_value(&Value::Int64(9)).unwrap(), vec![33]);
    }

    #[test]
    fn unique_int64_keys_keep_contiguous_identity_mappings_dense() {
        let mut keys = UniqueInt64Keys::new();
        assert!(keys.is_dense_identity());
        assert_eq!(keys.len(), 0);

        assert_eq!(keys.insert(10, 10), None);
        assert_eq!(keys.insert(11, 11), None);
        assert_eq!(keys.insert(9, 9), None);
        assert_eq!(keys.insert(10, 10), Some(10));

        assert!(keys.is_dense_identity());
        assert_eq!(keys.len(), 3);
        assert_eq!(keys.get(&8), None);
        assert_eq!(keys.get(&9), Some(9));
        assert_eq!(keys.get(&11), Some(11));
        assert_eq!(
            keys.iter().collect::<Vec<_>>(),
            vec![(9, 9), (10, 10), (11, 11)]
        );
    }

    #[test]
    fn unique_int64_keys_fall_back_for_gaps_and_non_identity_mappings() {
        let mut keys = UniqueInt64Keys::new();
        keys.insert(1, 1);
        keys.insert(2, 2);

        assert_eq!(keys.insert(4, 4), None);
        assert!(!keys.is_dense_identity());
        assert_eq!(keys.get(&1), Some(1));
        assert_eq!(keys.get(&4), Some(4));

        assert_eq!(keys.insert(7, 70), None);
        assert_eq!(keys.get(&7), Some(70));
        assert_eq!(keys.insert(7, 71), Some(70));
        assert_eq!(keys.get(&7), Some(71));
    }

    #[test]
    fn unique_int64_runtime_delete_reinsert_and_key_change_preserve_semantics() {
        let mut runtime =
            RuntimeBtreeKeys::UniqueInt64(Arc::new(UniqueInt64Keys::new()), BTreeSet::new());
        runtime
            .insert_row_id(RuntimeBtreeKey::Int64(10), 10)
            .unwrap();
        runtime
            .insert_row_id(RuntimeBtreeKey::Int64(11), 11)
            .unwrap();

        runtime
            .remove_row_id(&RuntimeBtreeKey::Int64(10), 10)
            .unwrap();
        assert!(runtime
            .row_ids_for_key(&RuntimeBtreeKey::Int64(10))
            .is_empty());
        runtime
            .insert_row_id(RuntimeBtreeKey::Int64(10), 10)
            .unwrap();
        assert_eq!(
            runtime.row_ids_for_key(&RuntimeBtreeKey::Int64(10)),
            vec![10]
        );

        runtime
            .remove_row_id(&RuntimeBtreeKey::Int64(10), 10)
            .unwrap();
        runtime
            .insert_row_id(RuntimeBtreeKey::Int64(20), 10)
            .unwrap();
        assert!(runtime
            .row_ids_for_key(&RuntimeBtreeKey::Int64(10))
            .is_empty());
        assert_eq!(
            runtime.row_ids_for_key(&RuntimeBtreeKey::Int64(20)),
            vec![10]
        );
    }

    #[test]
    fn nonunique_int64_postings_compact_contiguous_rows_and_preserve_fallback_order() {
        let posting_size = std::mem::size_of::<RuntimeInt64RowIds>();
        let vec_size = std::mem::size_of::<Vec<i64>>();
        assert!(
            posting_size <= vec_size,
            "adaptive INT64 posting grew from {vec_size} to {posting_size} bytes"
        );

        let mut posting = RuntimeInt64RowIds::One(10);
        posting.push(11);
        posting.push(12);
        assert_eq!(
            posting,
            RuntimeInt64RowIds::Contiguous { start: 10, len: 3 }
        );
        assert_eq!(posting.iter().collect::<Vec<_>>(), vec![10, 11, 12]);

        posting.push(20);
        assert_eq!(posting, RuntimeInt64RowIds::Many(vec![10, 11, 12, 20]));
        posting.push(19);
        assert_eq!(posting.to_vec(), vec![10, 11, 12, 20, 19]);
    }

    #[test]
    fn nonunique_int64_keys_use_dense_domain_and_fall_back_conservatively() {
        let mut dense = NonUniqueInt64Keys::new();
        dense.insert_row_id(5, 100);
        dense.insert_row_id(5, 101);
        dense.insert_row_id(6, 102);
        assert!(dense.is_dense());
        assert_eq!(dense.len(), 2);
        assert_eq!(
            dense.get(&5).map(RuntimeInt64RowIds::to_vec),
            Some(vec![100, 101])
        );
        assert_eq!(
            dense.get(&6).map(RuntimeInt64RowIds::to_vec),
            Some(vec![102])
        );

        dense.insert_row_id(8, 103);
        assert!(
            !dense.is_dense(),
            "a key-domain gap must use sparse storage"
        );
        assert_eq!(
            dense.get(&8).map(RuntimeInt64RowIds::to_vec),
            Some(vec![103])
        );

        let mut out_of_order = NonUniqueInt64Keys::new();
        out_of_order.insert_row_id(10, 1);
        out_of_order.insert_row_id(11, 2);
        out_of_order.insert_row_id(10, 3);
        assert!(
            !out_of_order.is_dense(),
            "inserting into an earlier key must conservatively use sparse storage"
        );
        assert_eq!(
            out_of_order.get(&10).map(RuntimeInt64RowIds::to_vec),
            Some(vec![1, 3])
        );
    }

    #[test]
    fn nonunique_int64_delete_move_revival_and_cow_preserve_mappings() {
        let first_key = RuntimeBtreeKey::Int64(1);
        let second_key = RuntimeBtreeKey::Int64(2);
        let revived_key = RuntimeBtreeKey::Int64(3);
        let mut runtime =
            RuntimeBtreeKeys::NonUniqueInt64(Arc::new(NonUniqueInt64Keys::new()), BTreeSet::new());

        runtime.insert_row_id(first_key.clone(), 10).unwrap();
        runtime.insert_row_id(first_key.clone(), 11).unwrap();
        runtime.insert_row_id(second_key.clone(), 12).unwrap();
        let snapshot = runtime.clone();

        runtime.remove_row_id(&first_key, 11).unwrap();
        assert_eq!(runtime.row_ids_for_key(&first_key), vec![10]);
        assert_eq!(snapshot.row_ids_for_key(&first_key), vec![10, 11]);
        runtime.insert_row_id(first_key.clone(), 11).unwrap();
        assert_eq!(runtime.row_ids_for_key(&first_key), vec![10, 11]);

        assert!(runtime
            .move_row_id(&first_key, second_key.clone(), 11)
            .unwrap());
        assert_eq!(runtime.row_ids_for_key(&first_key), vec![10]);
        assert_eq!(runtime.row_ids_for_key(&second_key), vec![12, 11]);

        runtime.remove_row_id(&first_key, 10).unwrap();
        runtime.insert_row_id(revived_key.clone(), 10).unwrap();
        assert!(runtime.row_ids_for_key(&first_key).is_empty());
        assert_eq!(runtime.row_ids_for_key(&revived_key), vec![10]);
    }

    #[test]
    fn runtime_row_id_set_contiguous_iterates_without_materializing() {
        let row_ids = RuntimeRowIdSet::Contiguous { start: 40, len: 4 };
        assert_eq!(row_ids.len(), 4);
        let mut seen = Vec::new();
        row_ids.for_each(|row_id| seen.push(row_id));
        assert_eq!(seen, vec![40, 41, 42, 43]);

        let mut posting = RuntimeInt64RowIds::Many(vec![50, 51, 52]);
        let old_capacity = match &posting {
            RuntimeInt64RowIds::Many(row_ids) => row_ids.capacity(),
            _ => 0,
        };
        assert_eq!(
            posting.shrink_to_fit(),
            old_capacity * std::mem::size_of::<i64>()
        );
        assert_eq!(
            posting,
            RuntimeInt64RowIds::Contiguous { start: 50, len: 3 }
        );
    }
}
