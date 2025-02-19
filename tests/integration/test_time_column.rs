// Copyright © 2024 Pathway

#![allow(clippy::disallowed_methods)]

use super::operator_test_utils::run_test;

use differential_dataflow::operators::arrange::ArrangeByKey;

use pathway_engine::engine::dataflow::operators::time_column::{
    postpone_core, MaxTimestamp, TimeColumnBuffer, TimeColumnForget, TimeColumnFreeze, TimeKey,
};
use pathway_engine::engine::Timestamp;

trait FromTimeKey<T, K> {
    fn from_time_key(time: T, key: K) -> TimeKey<isize, T, K>;
}

impl<T, K> FromTimeKey<T, K> for TimeKey<isize, T, K> {
    fn from_time_key(time: T, key: K) -> TimeKey<isize, T, K> {
        TimeKey {
            instance: 0,
            time,
            key,
        }
    }
}

#[test]
fn test_core_basic() {
    let input = vec![
        vec![
            (
                (TimeKey::from_time_key(100, 1), (100, 11)),
                Timestamp::original_from(0),
                1,
            ),
            (
                (TimeKey::from_time_key(100, 2), (100, 22)),
                Timestamp::original_from(0),
                1,
            ),
        ],
        vec![
            (
                (TimeKey::from_time_key(200, 3), (200, 33)),
                Timestamp::original_from(1),
                1,
            ),
            (
                (TimeKey::from_time_key(200, 4), (200, 44)),
                Timestamp::original_from(1),
                1,
            ),
        ],
        vec![
            (
                (TimeKey::from_time_key(300, 5), (300, 55)),
                Timestamp::original_from(2),
                1,
            ),
            (
                (TimeKey::from_time_key(300, 6), (300, 66)),
                Timestamp::original_from(2),
                1,
            ),
        ],
    ];
    // while reading the last batch, the upper limit for release is set to 199 (as current time is *t-1),
    // hence no release of 200 threshold entries
    // also, the first batch is released only when we reach the third (update of time to 199 happens after we are done reading the second batch)
    let expected = vec![vec![
        ((1, (100, 11)), Timestamp::original_from(2), 1),
        ((2, (100, 22)), Timestamp::original_from(2), 1),
    ]];
    run_test(input, expected, |coll| {
        postpone_core(coll.arrange_by_key(), |(t, _d)| *t - 1, false, false).arrange_by_key()
    });
}

#[test]
fn test_core_basic_test_border_threshold() {
    let input = vec![
        vec![
            (
                (TimeKey::from_time_key(100, 1), (100, 11)),
                Timestamp::original_from(0),
                1,
            ),
            (
                (TimeKey::from_time_key(100, 2), (100, 22)),
                Timestamp::original_from(0),
                1,
            ),
        ],
        vec![
            (
                (TimeKey::from_time_key(200, 3), (200, 33)),
                Timestamp::original_from(1),
                1,
            ),
            (
                (TimeKey::from_time_key(200, 4), (200, 44)),
                Timestamp::original_from(1),
                1,
            ),
        ],
        vec![
            (
                (TimeKey::from_time_key(300, 5), (300, 55)),
                Timestamp::original_from(2),
                1,
            ),
            (
                (TimeKey::from_time_key(300, 6), (300, 66)),
                Timestamp::original_from(2),
                1,
            ),
        ],
    ];
    let expected = vec![
        vec![
            ((1, (100, 11)), Timestamp::original_from(1), 1),
            ((2, (100, 22)), Timestamp::original_from(1), 1),
        ],
        vec![
            ((3, (200, 33)), Timestamp::original_from(2), 1),
            ((4, (200, 44)), Timestamp::original_from(2), 1),
        ],
    ];
    run_test(input, expected, |coll| {
        postpone_core(coll.arrange_by_key(), |(t, _d)| *t, false, false).arrange_by_key()
    });
}

#[test]
fn test_core_basic_flush() {
    let input = vec![
        vec![
            (
                (TimeKey::from_time_key(100, 1), (100, 11)),
                Timestamp::original_from(0),
                1,
            ),
            (
                (TimeKey::from_time_key(100, 2), (100, 22)),
                Timestamp::original_from(0),
                1,
            ),
        ],
        vec![
            (
                (TimeKey::from_time_key(200, 3), (200, 33)),
                Timestamp::original_from(1),
                1,
            ),
            (
                (TimeKey::from_time_key(200, 4), (200, 44)),
                Timestamp::original_from(1),
                1,
            ),
        ],
        vec![
            (
                (TimeKey::from_time_key(300, 5), (300, 55)),
                Timestamp::original_from(2),
                1,
            ),
            (
                (TimeKey::from_time_key(300, 6), (300, 66)),
                Timestamp::original_from(2),
                1,
            ),
        ],
    ];
    let expected = vec![
        vec![
            ((1, (100, 11)), Timestamp::original_from(2), 1),
            ((2, (100, 22)), Timestamp::original_from(2), 1),
        ],
        vec![
            ((3, (200, 33)), Timestamp::get_max_timestamp(), 1),
            ((4, (200, 44)), Timestamp::get_max_timestamp(), 1),
            ((5, (300, 55)), Timestamp::get_max_timestamp(), 1),
            ((6, (300, 66)), Timestamp::get_max_timestamp(), 1),
        ],
    ];
    run_test(input, expected, |coll| {
        postpone_core(coll.arrange_by_key(), |(t, _d)| *t - 1, true, false).arrange_by_key()
    });
}

#[test]
fn test_core_late_forwarding() {
    let input = vec![
        vec![
            (
                (TimeKey::from_time_key(100, 1), (100, 11)),
                Timestamp::original_from(0),
                1,
            ),
            (
                (TimeKey::from_time_key(100, 2), (100, 22)),
                Timestamp::original_from(0),
                1,
            ),
        ],
        vec![
            (
                (TimeKey::from_time_key(200, 3), (200, 33)),
                Timestamp::original_from(1),
                1,
            ),
            (
                (TimeKey::from_time_key(200, 4), (200, 44)),
                Timestamp::original_from(1),
                1,
            ),
        ],
        vec![(
            (TimeKey::from_time_key(300, 5), (300, 55)),
            Timestamp::original_from(2),
            1,
        )],
        vec![
            (
                (TimeKey::from_time_key(400, 7), (400, 77)),
                Timestamp::original_from(3),
                1,
            ),
            (
                (TimeKey::from_time_key(100, 6), (100, 66)),
                Timestamp::original_from(3),
                1,
            ),
            (
                (TimeKey::from_time_key(100, 8), (100, 60)),
                Timestamp::original_from(3),
                1,
            ),
        ],
    ];
    let expected = vec![
        vec![
            ((1, (100, 11)), Timestamp::original_from(2), 1),
            ((2, (100, 22)), Timestamp::original_from(2), 1),
        ],
        vec![
            ((3, (200, 33)), Timestamp::original_from(3), 1),
            ((4, (200, 44)), Timestamp::original_from(3), 1),
            ((6, (100, 66)), Timestamp::original_from(3), 1),
            ((8, (100, 60)), Timestamp::original_from(3), 1),
        ],
    ];
    run_test(input, expected, |coll| {
        postpone_core(coll.arrange_by_key(), |(t, _d)| *t - 1, false, false).arrange_by_key()
    });
}

#[test]
fn test_core_emitted_immediately() {
    let input = vec![
        vec![
            (
                (TimeKey::from_time_key(100, 1), (100, 11)),
                Timestamp::original_from(0),
                1,
            ),
            (
                (TimeKey::from_time_key(100, 11), (100, 13)),
                Timestamp::original_from(0),
                1,
            ),
            (
                (TimeKey::from_time_key(102, 2), (102, 22)),
                Timestamp::original_from(0),
                1,
            ),
        ],
        vec![
            (
                (TimeKey::from_time_key(200, 3), (200, 33)),
                Timestamp::original_from(1),
                1,
            ),
            (
                (TimeKey::from_time_key(200, 13), (200, 35)),
                Timestamp::original_from(1),
                1,
            ),
            (
                (TimeKey::from_time_key(202, 4), (202, 44)),
                Timestamp::original_from(1),
                1,
            ),
        ],
        vec![(
            (TimeKey::from_time_key(300, 5), (300, 55)),
            Timestamp::original_from(2),
            1,
        )],
        vec![
            (
                (TimeKey::from_time_key(400, 7), (400, 77)),
                Timestamp::original_from(3),
                1,
            ),
            (
                (TimeKey::from_time_key(200, 6), (200, 66)),
                Timestamp::original_from(3),
                1,
            ),
            (
                (TimeKey::from_time_key(200, 8), (200, 60)),
                Timestamp::original_from(3),
                1,
            ),
            (
                (TimeKey::from_time_key(202, 9), (202, 67)),
                Timestamp::original_from(3),
                1,
            ),
        ],
    ];
    let expected = vec![
        vec![
            ((1, (100, 11)), Timestamp::original_from(0), 1),
            ((11, (100, 13)), Timestamp::original_from(0), 1),
        ],
        vec![
            ((2, (102, 22)), Timestamp::original_from(1), 1),
            ((3, (200, 33)), Timestamp::original_from(1), 1),
            ((13, (200, 35)), Timestamp::original_from(1), 1),
        ],
        vec![((4, (202, 44)), Timestamp::original_from(2), 1)],
        vec![
            ((5, (300, 55)), Timestamp::original_from(3), 1),
            ((6, (200, 66)), Timestamp::original_from(3), 1),
            ((8, (200, 60)), Timestamp::original_from(3), 1),
            ((9, (202, 67)), Timestamp::original_from(3), 1),
        ],
    ];
    run_test(input, expected, |coll| {
        postpone_core(coll.arrange_by_key(), |(t, _d)| *t - 1, false, true).arrange_by_key()
    });
}

#[test]
fn test_core_late_forwarding_ignore_retraction() {
    let input = vec![
        vec![
            (
                (TimeKey::from_time_key(100, 1), (100, 11)),
                Timestamp::original_from(0),
                1,
            ),
            (
                (TimeKey::from_time_key(100, 2), (100, 22)),
                Timestamp::original_from(0),
                1,
            ),
        ],
        vec![
            (
                (TimeKey::from_time_key(300, 3), (300, 33)),
                Timestamp::original_from(1),
                1,
            ),
            (
                (TimeKey::from_time_key(300, 4), (300, 44)),
                Timestamp::original_from(1),
                1,
            ),
        ],
        vec![
            (
                (TimeKey::from_time_key(400, 5), (400, 55)),
                Timestamp::original_from(3),
                1,
            ),
            (
                (TimeKey::from_time_key(100, 6), (100, 66)),
                Timestamp::original_from(3),
                1,
            ),
        ],
        vec![(
            (TimeKey::from_time_key(100, 7), (100, 77)),
            Timestamp::original_from(4),
            1,
        )],
        vec![(
            (TimeKey::from_time_key(100, 8), (100, 88)),
            Timestamp::retraction_from(4),
            1,
        )],
    ];
    let expected = vec![
        vec![
            ((1, (100, 11)), Timestamp::original_from(3), 1),
            ((2, (100, 22)), Timestamp::original_from(3), 1),
            ((6, (100, 66)), Timestamp::original_from(3), 1),
        ],
        vec![
            ((7, (100, 77)), Timestamp::original_from(4), 1),
            ((3, (300, 33)), Timestamp::original_from(4), 1),
            ((4, (300, 44)), Timestamp::original_from(4), 1),
        ],
    ];
    run_test(input, expected, |coll| {
        postpone_core(coll.arrange_by_key(), |(t, _d)| *t - 1, false, false).arrange_by_key()
    });
}

#[test]
fn test_core_aggregate_to_zero() {
    let input = vec![
        vec![
            (
                (TimeKey::from_time_key(200, 1), (200, 11)),
                Timestamp::original_from(0),
                1,
            ),
            (
                (TimeKey::from_time_key(200, 2), (200, 22)),
                Timestamp::original_from(0),
                1,
            ),
        ],
        vec![
            (
                (TimeKey::from_time_key(200, 3), (200, 33)),
                Timestamp::original_from(1),
                1,
            ),
            (
                (TimeKey::from_time_key(200, 1), (200, 11)),
                Timestamp::original_from(1),
                -1,
            ),
        ],
        vec![
            (
                (TimeKey::from_time_key(300, 5), (300, 55)),
                Timestamp::original_from(2),
                1,
            ),
            (
                (TimeKey::from_time_key(300, 6), (300, 66)),
                Timestamp::original_from(2),
                1,
            ),
        ],
        vec![(
            (TimeKey::from_time_key(300, 7), (700, 77)),
            Timestamp::original_from(3),
            1,
        )],
    ];
    let expected = vec![vec![
        ((2, (200, 22)), Timestamp::original_from(3), 1),
        ((3, (200, 33)), Timestamp::original_from(3), 1),
    ]];
    run_test(input, expected, |coll| {
        postpone_core(coll.arrange_by_key(), |(t, _d)| *t - 1, false, false).arrange_by_key()
    });
}

#[test]
fn test_core_aggregate_replace() {
    let input = vec![
        vec![
            (
                (TimeKey::from_time_key(200, 1), (200, 11)),
                Timestamp::original_from(0),
                1,
            ),
            (
                (TimeKey::from_time_key(200, 2), (200, 22)),
                Timestamp::original_from(0),
                1,
            ),
        ],
        vec![
            (
                (TimeKey::from_time_key(200, 3), (200, 33)),
                Timestamp::original_from(1),
                1,
            ),
            (
                (TimeKey::from_time_key(200, 1), (200, 11)),
                Timestamp::original_from(1),
                -1,
            ),
        ],
        vec![
            (
                (TimeKey::from_time_key(200, 1), (200, 55)),
                Timestamp::original_from(2),
                1,
            ),
            (
                (TimeKey::from_time_key(300, 6), (300, 66)),
                Timestamp::original_from(2),
                1,
            ),
        ],
        vec![(
            (TimeKey::from_time_key(300, 7), (700, 77)),
            Timestamp::original_from(3),
            1,
        )],
    ];
    let expected = vec![vec![
        ((1, (200, 55)), Timestamp::original_from(3), 1),
        ((2, (200, 22)), Timestamp::original_from(3), 1),
        ((3, (200, 33)), Timestamp::original_from(3), 1),
    ]];

    run_test(input, expected, |coll| {
        postpone_core(coll.arrange_by_key(), |(t, _d)| *t - 1, false, false).arrange_by_key()
    });
}

#[test]
fn test_wrapper_basic() {
    let input = vec![
        vec![
            ((1, (100, 11)), Timestamp::original_from(0), 1),
            ((2, (100, 22)), Timestamp::original_from(0), 1),
        ],
        vec![
            ((3, (200, 33)), Timestamp::original_from(1), 1),
            ((4, (200, 44)), Timestamp::original_from(1), 1),
        ],
        vec![
            ((5, (300, 55)), Timestamp::original_from(2), 1),
            ((6, (300, 66)), Timestamp::original_from(2), 1),
        ],
    ];
    let expected = vec![vec![
        ((1, (100, 11)), Timestamp::original_from(2), 1),
        ((2, (100, 22)), Timestamp::original_from(2), 1),
    ]];

    run_test(input, expected, |coll| {
        coll.postpone(
            coll.scope(),
            |(t, _d)| *t,
            |(t, _d)| *t - 1,
            |_| 0,
            false,
            false,
            Ok,
        )
        .unwrap()
        .arrange_by_key()
    });
}

#[test]
fn test_wrapper_basic_flush() {
    let input = vec![
        vec![
            ((1, (100, 11)), Timestamp::original_from(0), 1),
            ((2, (100, 22)), Timestamp::original_from(0), 1),
        ],
        vec![
            ((3, (200, 33)), Timestamp::original_from(1), 1),
            ((4, (200, 44)), Timestamp::original_from(1), 1),
        ],
        vec![
            ((5, (300, 55)), Timestamp::original_from(2), 1),
            ((6, (300, 66)), Timestamp::original_from(2), 1),
        ],
    ];
    let expected = vec![
        vec![
            ((1, (100, 11)), Timestamp::original_from(2), 1),
            ((2, (100, 22)), Timestamp::original_from(2), 1),
        ],
        vec![
            ((3, (200, 33)), Timestamp::get_max_timestamp(), 1),
            ((4, (200, 44)), Timestamp::get_max_timestamp(), 1),
            ((5, (300, 55)), Timestamp::get_max_timestamp(), 1),
            ((6, (300, 66)), Timestamp::get_max_timestamp(), 1),
        ],
    ];

    run_test(input, expected, |coll| {
        coll.postpone(
            coll.scope(),
            |(t, _d)| *t,
            |(t, _d)| *t - 1,
            |_| 0,
            true,
            false,
            Ok,
        )
        .unwrap()
        .arrange_by_key()
    });
}

#[test]
fn test_wrapper_split_batch_by_time() {
    let input = vec![vec![
        ((1, (100, 11)), Timestamp::original_from(0), 1),
        ((2, (100, 22)), Timestamp::original_from(0), 1),
        ((3, (200, 33)), Timestamp::original_from(2), 1),
        ((4, (200, 44)), Timestamp::original_from(2), 1),
        ((5, (300, 55)), Timestamp::original_from(4), 1),
        ((6, (300, 66)), Timestamp::original_from(4), 1),
        ((7, (400, 77)), Timestamp::original_from(6), 1),
    ]];
    let expected = vec![
        vec![
            ((1, (100, 11)), Timestamp::original_from(4), 1),
            ((2, (100, 22)), Timestamp::original_from(4), 1),
        ],
        vec![
            ((3, (200, 33)), Timestamp::original_from(6), 1),
            ((4, (200, 44)), Timestamp::original_from(6), 1),
        ],
    ];

    run_test(input, expected, |coll| {
        coll.postpone(
            coll.scope(),
            |(t, _d)| *t,
            |(t, _d)| *t - 1,
            |_| 0,
            false,
            false,
            Ok,
        )
        .unwrap()
        .arrange_by_key()
    });
}

#[test]
fn test_wrapper_late_forwarding() {
    let input = vec![
        vec![
            ((1, (100, 11)), Timestamp::original_from(0), 1),
            ((2, (100, 22)), Timestamp::original_from(0), 1),
        ],
        vec![
            ((3, (200, 33)), Timestamp::original_from(1), 1),
            ((4, (200, 44)), Timestamp::original_from(1), 1),
        ],
        vec![
            ((5, (300, 55)), Timestamp::original_from(2), 1),
            ((6, (100, 66)), Timestamp::original_from(2), 1),
        ],
        vec![
            ((7, (400, 77)), Timestamp::original_from(3), 1),
            ((8, (100, 88)), Timestamp::original_from(3), 1),
        ],
    ];
    let expected = vec![
        vec![
            ((1, (100, 11)), Timestamp::original_from(2), 1),
            ((2, (100, 22)), Timestamp::original_from(2), 1),
            ((6, (100, 66)), Timestamp::original_from(2), 1),
        ],
        vec![
            ((3, (200, 33)), Timestamp::original_from(3), 1),
            ((4, (200, 44)), Timestamp::original_from(3), 1),
            ((8, (100, 88)), Timestamp::original_from(3), 1),
        ],
    ];

    run_test(input, expected, |coll| {
        coll.postpone(
            coll.scope(),
            |(t, _d)| *t,
            |(t, _d)| *t - 1,
            |_| 0,
            false,
            false,
            Ok,
        )
        .unwrap()
        .arrange_by_key()
    });
}

#[test]
fn test_wrapper_aggregate_to_zero() {
    let input = vec![
        vec![
            ((1, (200, 11)), Timestamp::original_from(0), 1),
            ((2, (200, 22)), Timestamp::original_from(0), 1),
        ],
        vec![
            ((3, (200, 33)), Timestamp::original_from(1), 1),
            ((1, (200, 11)), Timestamp::original_from(1), -1),
        ],
        vec![
            ((5, (300, 55)), Timestamp::original_from(2), 1),
            ((6, (300, 66)), Timestamp::original_from(2), 1),
        ],
        vec![((7, (400, 77)), Timestamp::original_from(3), 1)],
    ];
    let expected = vec![vec![
        ((2, (200, 22)), Timestamp::original_from(3), 1),
        ((3, (200, 33)), Timestamp::original_from(3), 1),
    ]];

    run_test(input, expected, |coll| {
        coll.postpone(
            coll.scope(),
            |(t, _d)| *t,
            |(t, _d)| *t - 1,
            |_| 0,
            false,
            false,
            Ok,
        )
        .unwrap()
        .arrange_by_key()
    });
}

#[test]
fn test_wrapper_aggregate_replace() {
    let input = vec![
        vec![
            ((1, (200, 11)), Timestamp::original_from(0), 1),
            ((2, (200, 22)), Timestamp::original_from(0), 1),
        ],
        vec![
            ((3, (200, 33)), Timestamp::original_from(1), 1),
            ((1, (200, 11)), Timestamp::original_from(1), -1),
        ],
        vec![
            ((1, (200, 55)), Timestamp::original_from(2), 1),
            ((6, (300, 66)), Timestamp::original_from(2), 1),
        ],
        vec![((7, (400, 77)), Timestamp::original_from(3), 1)],
    ];
    let expected = vec![vec![
        ((1, (200, 55)), Timestamp::original_from(3), 1),
        ((2, (200, 22)), Timestamp::original_from(3), 1),
        ((3, (200, 33)), Timestamp::original_from(3), 1),
    ]];
    run_test(input, expected, |coll| {
        coll.postpone(
            coll.scope(),
            |(t, _d)| *t,
            |(t, _d)| *t - 1,
            |_| 0,
            false,
            false,
            Ok,
        )
        .unwrap()
        .arrange_by_key()
    });
}

#[test]
fn test_wrapper_two_times() {
    let input = vec![
        vec![
            ((1, (200, 100, 11)), Timestamp::original_from(0), 1),
            ((2, (200, 100, 22)), Timestamp::original_from(0), 1),
        ],
        vec![
            ((3, (200, 100, 33)), Timestamp::original_from(1), 1),
            ((4, (200, 100, 44)), Timestamp::original_from(1), 1),
        ],
        vec![
            ((5, (200, 200, 55)), Timestamp::original_from(2), 1),
            ((6, (300, 200, 66)), Timestamp::original_from(2), 1),
        ],
        vec![((7, (300, 300, 77)), Timestamp::original_from(3), 1)],
    ];
    let expected = vec![vec![
        ((1, (200, 100, 11)), Timestamp::original_from(3), 1),
        ((2, (200, 100, 22)), Timestamp::original_from(3), 1),
        ((3, (200, 100, 33)), Timestamp::original_from(3), 1),
        ((4, (200, 100, 44)), Timestamp::original_from(3), 1),
        ((5, (200, 200, 55)), Timestamp::original_from(3), 1),
    ]];
    run_test(input, expected, |coll| {
        coll.postpone(
            coll.scope(),
            |(t1, _t2, _d)| *t1,
            |(_t1, t2, _d)| *t2,
            |_| 0,
            false,
            false,
            Ok,
        )
        .unwrap()
        .arrange_by_key()
    });
}

#[test]
fn test_wrapper_two_times_replace_aggregate() {
    let input = vec![
        vec![
            ((1, (200, 100, 11)), Timestamp::original_from(0), 1),
            ((2, (200, 100, 22)), Timestamp::original_from(0), 1),
        ],
        vec![
            ((3, (200, 100, 33)), Timestamp::original_from(1), 1),
            ((1, (200, 100, 11)), Timestamp::original_from(1), -1),
        ],
        vec![
            ((1, (200, 200, 55)), Timestamp::original_from(2), 1),
            ((6, (300, 200, 66)), Timestamp::original_from(2), 1),
        ],
        vec![((7, (300, 300, 77)), Timestamp::original_from(3), 1)],
    ];
    let expected = vec![vec![
        ((1, (200, 200, 55)), Timestamp::original_from(3), 1),
        ((2, (200, 100, 22)), Timestamp::original_from(3), 1),
        ((3, (200, 100, 33)), Timestamp::original_from(3), 1),
    ]];
    run_test(input, expected, |coll| {
        coll.postpone(
            coll.scope(),
            |(t1, _t2, _d)| *t1,
            |(_t1, t2, _d)| *t2,
            |_| 0,
            false,
            false,
            Ok,
        )
        .unwrap()
        .arrange_by_key()
    });
}

#[test]
fn test_wrapper_two_times_forward_late() {
    let input = vec![
        vec![
            ((1, (100, 100, 11)), Timestamp::original_from(0), 1),
            ((2, (100, 100, 22)), Timestamp::original_from(0), 1),
        ],
        vec![
            ((3, (200, 100, 33)), Timestamp::original_from(1), 1),
            ((4, (200, 200, 44)), Timestamp::original_from(1), 1),
        ],
        vec![
            ((5, (100, 200, 55)), Timestamp::original_from(2), 1),
            ((6, (300, 200, 66)), Timestamp::original_from(2), 1),
        ],
        vec![((7, (300, 300, 77)), Timestamp::original_from(3), 1)],
    ];
    let expected = vec![
        vec![
            ((1, (100, 100, 11)), Timestamp::original_from(1), 1),
            ((2, (100, 100, 22)), Timestamp::original_from(1), 1),
        ],
        vec![
            ((3, (200, 100, 33)), Timestamp::original_from(2), 1),
            ((4, (200, 200, 44)), Timestamp::original_from(2), 1),
            ((5, (100, 200, 55)), Timestamp::original_from(2), 1),
        ],
    ];
    run_test(input, expected, |coll| {
        coll.postpone(
            coll.scope(),
            |(t1, _t2, _d)| *t1,
            |(_t1, t2, _d)| *t2,
            |_| 0,
            false,
            false,
            Ok,
        )
        .unwrap()
        .arrange_by_key()
    });
}

#[test]
fn test_forget() {
    let input = vec![
        vec![
            ((1, (100, 11)), Timestamp::original_from(0), 1),
            ((2, (100, 22)), Timestamp::original_from(0), 1),
        ],
        vec![
            ((3, (200, 33)), Timestamp::original_from(2), 1),
            ((4, (200, 44)), Timestamp::original_from(2), 1),
        ],
        vec![
            ((5, (300, 55)), Timestamp::original_from(4), 1),
            ((6, (300, 66)), Timestamp::original_from(4), 1),
        ],
    ];
    let expected = vec![
        vec![
            ((1, (100, 11)), Timestamp::original_from(0), 1),
            ((2, (100, 22)), Timestamp::original_from(0), 1),
        ],
        vec![
            ((3, (200, 33)), Timestamp::original_from(2), 1),
            ((4, (200, 44)), Timestamp::original_from(2), 1),
        ],
        vec![
            ((5, (300, 55)), Timestamp::original_from(4), 1),
            ((6, (300, 66)), Timestamp::original_from(4), 1),
            ((1, (100, 11)), Timestamp::original_from(4), -1),
            ((2, (100, 22)), Timestamp::original_from(4), -1),
        ],
    ];

    run_test(input, expected, |coll| {
        coll.forget(|(t, _d)| *t + 1, |(t, _d)| *t, |_| 0, false, Ok)
            .unwrap()
            .arrange_by_key()
    });
}

#[test]
fn test_forget_mark_forgetting_records() {
    let input = vec![
        vec![
            ((1, (100, 11)), Timestamp(0), 1),
            ((2, (100, 22)), Timestamp(0), 1),
        ],
        vec![
            ((3, (200, 33)), Timestamp(2), 1),
            ((4, (200, 44)), Timestamp(2), 1),
        ],
        vec![
            ((5, (300, 55)), Timestamp(4), 1),
            ((6, (300, 66)), Timestamp(4), 1),
        ],
        vec![
            ((7, (400, 77)), Timestamp(6), 1),
            ((8, (400, 88)), Timestamp(6), 1),
        ],
    ];
    let expected = vec![
        vec![
            ((1, (100, 11)), Timestamp(0), 1),
            ((2, (100, 22)), Timestamp(0), 1),
        ],
        vec![
            ((3, (200, 33)), Timestamp(2), 1),
            ((4, (200, 44)), Timestamp(2), 1),
        ],
        vec![
            ((5, (300, 55)), Timestamp(4), 1),
            ((6, (300, 66)), Timestamp(4), 1),
        ],
        vec![
            ((1, (100, 11)), Timestamp(5), -1),
            ((2, (100, 22)), Timestamp(5), -1),
        ],
        vec![
            ((7, (400, 77)), Timestamp(6), 1),
            ((8, (400, 88)), Timestamp(6), 1),
        ],
        vec![
            ((3, (200, 33)), Timestamp(7), -1),
            ((4, (200, 44)), Timestamp(7), -1),
        ],
    ];

    run_test(input, expected, |coll| {
        coll.forget(|(t, _d)| *t + 1, |(t, _d)| *t, |_| 0, true, Ok)
            .unwrap()
            .arrange_by_key()
    });
}

#[test]
fn test_cutoff() {
    let input = vec![
        vec![
            ((1, (100, 11)), Timestamp::original_from(0), 1),
            ((2, (100, 22)), Timestamp::original_from(0), 1),
        ],
        vec![
            ((3, (200, 33)), Timestamp::original_from(1), 1),
            ((4, (100, 44)), Timestamp::original_from(1), 1),
        ],
        vec![
            ((5, (300, 55)), Timestamp::original_from(2), 1),
            ((6, (300, 66)), Timestamp::original_from(2), 1),
        ],
    ];
    let expected = vec![
        vec![
            ((1, (100, 11)), Timestamp::original_from(0), 1),
            ((2, (100, 22)), Timestamp::original_from(0), 1),
        ],
        vec![
            ((3, (200, 33)), Timestamp::original_from(1), 1),
            ((4, (100, 44)), Timestamp::original_from(1), 1),
        ],
        vec![
            ((5, (300, 55)), Timestamp::original_from(2), 1),
            ((6, (300, 66)), Timestamp::original_from(2), 1),
        ],
    ];

    let expected_late = vec![];

    run_test(input.clone(), expected, |coll| {
        coll.freeze(|(t, _d)| *t + 1, |(t, _d)| *t, |_| 0)
            .0
            .arrange_by_key()
    });

    run_test(input, expected_late, |coll| {
        coll.freeze(|(t, _d)| *t + 1, |(t, _d)| *t, |_| 0)
            .1
            .arrange_by_key()
    });
}

//((4, (100, 44)), 1, 1) should be late
#[test]
fn test_cutoff_late() {
    let input = vec![
        vec![
            ((1, (100, 11)), Timestamp::original_from(0), 1),
            ((2, (200, 22)), Timestamp::original_from(0), 1),
        ],
        vec![
            ((3, (200, 33)), Timestamp::original_from(1), 1),
            ((4, (100, 44)), Timestamp::original_from(1), 1),
        ],
        vec![
            ((5, (300, 55)), Timestamp::original_from(2), 1),
            ((6, (300, 66)), Timestamp::original_from(2), 1),
        ],
    ];
    let expected = vec![
        vec![
            ((1, (100, 11)), Timestamp::original_from(0), 1),
            ((2, (200, 22)), Timestamp::original_from(0), 1),
        ],
        vec![((3, (200, 33)), Timestamp::original_from(1), 1)],
        vec![
            ((5, (300, 55)), Timestamp::original_from(2), 1),
            ((6, (300, 66)), Timestamp::original_from(2), 1),
        ],
    ];
    let expected_late = vec![vec![((4, (100, 44)), Timestamp::original_from(1), 1)]];

    run_test(input.clone(), expected, |coll| {
        coll.freeze(|(t, _d)| *t + 1, |(t, _d)| *t, |_| 0)
            .0
            .arrange_by_key()
    });

    run_test(input, expected_late, |coll| {
        coll.freeze(|(t, _d)| *t + 1, |(t, _d)| *t, |_| 0)
            .1
            .arrange_by_key()
    });
}

#[test]
fn test_cutoff_late_multiple() {
    let input = vec![
        vec![
            ((1, (100, 11)), Timestamp::original_from(0), 1),
            ((2, (200, 22)), Timestamp::original_from(0), 1),
        ],
        vec![
            ((3, (200, 33)), Timestamp::original_from(1), 1),
            ((4, (100, 44)), Timestamp::original_from(1), 1),
        ],
        vec![
            ((5, (300, 55)), Timestamp::original_from(2), 1),
            ((6, (100, 66)), Timestamp::original_from(2), 1),
            ((7, (-100, 77)), Timestamp::original_from(2), 1),
        ],
    ];
    let expected = vec![
        vec![
            ((1, (100, 11)), Timestamp::original_from(0), 1),
            ((2, (200, 22)), Timestamp::original_from(0), 1),
        ],
        vec![((3, (200, 33)), Timestamp::original_from(1), 1)],
        vec![((5, (300, 55)), Timestamp::original_from(2), 1)],
    ];

    let expected_late = vec![
        vec![((4, (100, 44)), Timestamp::original_from(1), 1)],
        vec![
            ((6, (100, 66)), Timestamp::original_from(2), 1),
            ((7, (-100, 77)), Timestamp::original_from(2), 1),
        ],
    ];
    run_test(input.clone(), expected, |coll| {
        coll.freeze(|(t, _d)| *t + 1, |(t, _d)| *t, |_| 0)
            .0
            .arrange_by_key()
    });

    run_test(input, expected_late, |coll| {
        coll.freeze(|(t, _d)| *t + 1, |(t, _d)| *t, |_| 0)
            .1
            .arrange_by_key()
    });
}
