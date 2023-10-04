#![allow(clippy::disallowed_methods)]
mod operator_test_utils;

use differential_dataflow::operators::arrange::ArrangeByKey;
use operator_test_utils::run_test;

use pathway_engine::engine::dataflow::operators::time_column::{
    postpone_core, SelfCompactionTime, TimeColumnBuffer, TimeColumnForget, TimeColumnFreeze,
    TimeKey,
};

#[test]
fn test_core_basic() {
    let input = vec![
        vec![
            (
                (TimeKey { time: 100, key: 1 }, (100, 11)),
                SelfCompactionTime::original(0),
                1,
            ),
            (
                (TimeKey { time: 100, key: 2 }, (100, 22)),
                SelfCompactionTime::original(0),
                1,
            ),
        ],
        vec![
            (
                (TimeKey { time: 200, key: 3 }, (200, 33)),
                SelfCompactionTime::original(1),
                1,
            ),
            (
                (TimeKey { time: 200, key: 4 }, (200, 44)),
                SelfCompactionTime::original(1),
                1,
            ),
        ],
        vec![
            (
                (TimeKey { time: 300, key: 5 }, (300, 55)),
                SelfCompactionTime::original(2),
                1,
            ),
            (
                (TimeKey { time: 300, key: 6 }, (300, 66)),
                SelfCompactionTime::original(2),
                1,
            ),
        ],
    ];
    let expected = vec![
        vec![
            ((1, (100, 11)), SelfCompactionTime::original(1), 1),
            ((2, (100, 22)), SelfCompactionTime::original(1), 1),
        ],
        vec![
            ((3, (200, 33)), SelfCompactionTime::original(2), 1),
            ((4, (200, 44)), SelfCompactionTime::original(2), 1),
        ],
    ];
    run_test(input, expected, |coll| {
        postpone_core(coll.arrange_by_key(), |(t, _d)| *t - 1).arrange_by_key()
    });
}

#[test]
fn test_core_late_forwarding() {
    let input = vec![
        vec![
            (
                (TimeKey { time: 100, key: 1 }, (100, 11)),
                SelfCompactionTime::original(0),
                1,
            ),
            (
                (TimeKey { time: 100, key: 2 }, (100, 22)),
                SelfCompactionTime::original(0),
                1,
            ),
        ],
        vec![
            (
                (TimeKey { time: 200, key: 3 }, (200, 33)),
                SelfCompactionTime::original(1),
                1,
            ),
            (
                (TimeKey { time: 200, key: 4 }, (200, 44)),
                SelfCompactionTime::original(1),
                1,
            ),
        ],
        vec![
            (
                (TimeKey { time: 300, key: 5 }, (300, 55)),
                SelfCompactionTime::original(2),
                1,
            ),
            (
                (TimeKey { time: 100, key: 6 }, (100, 66)),
                SelfCompactionTime::original(2),
                1,
            ),
        ],
    ];
    let expected = vec![
        vec![
            ((1, (100, 11)), SelfCompactionTime::original(1), 1),
            ((2, (100, 22)), SelfCompactionTime::original(1), 1),
        ],
        vec![
            ((3, (200, 33)), SelfCompactionTime::original(2), 1),
            ((4, (200, 44)), SelfCompactionTime::original(2), 1),
            ((6, (100, 66)), SelfCompactionTime::original(2), 1),
        ],
    ];
    run_test(input, expected, |coll| {
        postpone_core(coll.arrange_by_key(), |(t, _d)| *t - 1).arrange_by_key()
    });
}

#[test]
fn test_core_late_forwarding_ignore_retraction() {
    let input = vec![
        vec![
            (
                (TimeKey { time: 100, key: 1 }, (100, 11)),
                SelfCompactionTime::original(0),
                1,
            ),
            (
                (TimeKey { time: 100, key: 2 }, (100, 22)),
                SelfCompactionTime::original(0),
                1,
            ),
        ],
        vec![
            (
                (TimeKey { time: 300, key: 3 }, (300, 33)),
                SelfCompactionTime::original(1),
                1,
            ),
            (
                (TimeKey { time: 300, key: 4 }, (300, 44)),
                SelfCompactionTime::original(1),
                1,
            ),
        ],
        vec![(
            (TimeKey { time: 100, key: 6 }, (100, 66)),
            SelfCompactionTime::retraction(2),
            1,
        )],
        vec![
            (
                (TimeKey { time: 400, key: 5 }, (400, 55)),
                SelfCompactionTime::original(3),
                1,
            ),
            (
                (TimeKey { time: 100, key: 7 }, (100, 77)),
                SelfCompactionTime::original(3),
                1,
            ),
        ],
    ];
    let expected = vec![
        vec![
            ((1, (100, 11)), SelfCompactionTime::original(1), 1),
            ((2, (100, 22)), SelfCompactionTime::original(1), 1),
        ],
        vec![
            ((3, (300, 33)), SelfCompactionTime::original(3), 1),
            ((4, (300, 44)), SelfCompactionTime::original(3), 1),
            ((7, (100, 77)), SelfCompactionTime::original(3), 1),
        ],
    ];
    run_test(input, expected, |coll| {
        postpone_core(coll.arrange_by_key(), |(t, _d)| *t - 1).arrange_by_key()
    });
}

#[test]
fn test_core_aggregate_to_zero() {
    let input = vec![
        vec![
            (
                (TimeKey { time: 200, key: 1 }, (200, 11)),
                SelfCompactionTime::original(0),
                1,
            ),
            (
                (TimeKey { time: 200, key: 2 }, (200, 22)),
                SelfCompactionTime::original(0),
                1,
            ),
        ],
        vec![
            (
                (TimeKey { time: 200, key: 3 }, (200, 33)),
                SelfCompactionTime::original(1),
                1,
            ),
            (
                (TimeKey { time: 200, key: 1 }, (200, 11)),
                SelfCompactionTime::original(1),
                -1,
            ),
        ],
        vec![
            (
                (TimeKey { time: 300, key: 5 }, (300, 55)),
                SelfCompactionTime::original(2),
                1,
            ),
            (
                (TimeKey { time: 300, key: 6 }, (300, 66)),
                SelfCompactionTime::original(2),
                1,
            ),
        ],
    ];
    let expected = vec![vec![
        ((2, (200, 22)), SelfCompactionTime::original(2), 1),
        ((3, (200, 33)), SelfCompactionTime::original(2), 1),
    ]];
    run_test(input, expected, |coll| {
        postpone_core(coll.arrange_by_key(), |(t, _d)| *t - 1).arrange_by_key()
    });
}

#[test]
fn test_core_aggregate_replace() {
    let input = vec![
        vec![
            (
                (TimeKey { time: 200, key: 1 }, (200, 11)),
                SelfCompactionTime::original(0),
                1,
            ),
            (
                (TimeKey { time: 200, key: 2 }, (200, 22)),
                SelfCompactionTime::original(0),
                1,
            ),
        ],
        vec![
            (
                (TimeKey { time: 200, key: 3 }, (200, 33)),
                SelfCompactionTime::original(1),
                1,
            ),
            (
                (TimeKey { time: 200, key: 1 }, (200, 11)),
                SelfCompactionTime::original(1),
                -1,
            ),
        ],
        vec![
            (
                (TimeKey { time: 200, key: 1 }, (200, 55)),
                SelfCompactionTime::original(2),
                1,
            ),
            (
                (TimeKey { time: 300, key: 6 }, (300, 66)),
                SelfCompactionTime::original(2),
                1,
            ),
        ],
    ];
    let expected = vec![vec![
        ((1, (200, 55)), SelfCompactionTime::original(2), 1),
        ((2, (200, 22)), SelfCompactionTime::original(2), 1),
        ((3, (200, 33)), SelfCompactionTime::original(2), 1),
    ]];

    run_test(input, expected, |coll| {
        postpone_core(coll.arrange_by_key(), |(t, _d)| *t - 1).arrange_by_key()
    });
}

#[test]
fn test_wrapper_basic() {
    let input = vec![
        vec![((1, (100, 11)), 0, 1), ((2, (100, 22)), 0, 1)],
        vec![((3, (200, 33)), 1, 1), ((4, (200, 44)), 1, 1)],
        vec![((5, (300, 55)), 2, 1), ((6, (300, 66)), 2, 1)],
    ];
    let expected = vec![
        vec![((1, (100, 11)), 1, 1), ((2, (100, 22)), 1, 1)],
        vec![((3, (200, 33)), 2, 1), ((4, (200, 44)), 2, 1)],
    ];

    run_test(input, expected, |coll| {
        coll.postpone(coll.scope(), |(t, _d)| *t, |(t, _d)| *t - 1)
            .arrange_by_key()
    });
}

#[test]
fn test_wrapper_late_forwarding() {
    let input = vec![
        vec![((1, (100, 11)), 0, 1), ((2, (100, 22)), 0, 1)],
        vec![((3, (200, 33)), 1, 1), ((4, (200, 44)), 1, 1)],
        vec![((5, (300, 55)), 2, 1), ((6, (100, 66)), 2, 1)],
    ];
    let expected = vec![
        vec![((1, (100, 11)), 1, 1), ((2, (100, 22)), 1, 1)],
        vec![
            ((3, (200, 33)), 2, 1),
            ((4, (200, 44)), 2, 1),
            ((6, (100, 66)), 2, 1),
        ],
    ];

    run_test(input, expected, |coll| {
        coll.postpone(coll.scope(), |(t, _d)| *t, |(t, _d)| *t - 1)
            .arrange_by_key()
    });
}

#[test]
fn test_wrapper_aggregate_to_zero() {
    let input = vec![
        vec![((1, (200, 11)), 0, 1), ((2, (200, 22)), 0, 1)],
        vec![((3, (200, 33)), 1, 1), ((1, (200, 11)), 1, -1)],
        vec![((5, (300, 55)), 2, 1), ((6, (300, 66)), 2, 1)],
    ];
    let expected = vec![vec![((2, (200, 22)), 2, 1), ((3, (200, 33)), 2, 1)]];

    run_test(input, expected, |coll| {
        coll.postpone(coll.scope(), |(t, _d)| *t, |(t, _d)| *t - 1)
            .arrange_by_key()
    });
}

#[test]
fn test_wrapper_aggregate_replace() {
    let input = vec![
        vec![((1, (200, 11)), 0, 1), ((2, (200, 22)), 0, 1)],
        vec![((3, (200, 33)), 1, 1), ((1, (200, 11)), 1, -1)],
        vec![((1, (200, 55)), 2, 1), ((6, (300, 66)), 2, 1)],
    ];
    let expected = vec![vec![
        ((1, (200, 55)), 2, 1),
        ((2, (200, 22)), 2, 1),
        ((3, (200, 33)), 2, 1),
    ]];
    run_test(input, expected, |coll| {
        coll.postpone(coll.scope(), |(t, _d)| *t, |(t, _d)| *t - 1)
            .arrange_by_key()
    });
}

#[test]
fn test_wrapper_two_times() {
    let input = vec![
        vec![((1, (200, 100, 11)), 0, 1), ((2, (200, 100, 22)), 0, 1)],
        vec![((3, (200, 100, 33)), 1, 1), ((4, (200, 100, 44)), 1, 1)],
        vec![((5, (200, 200, 55)), 2, 1), ((6, (300, 200, 66)), 2, 1)],
    ];
    let expected = vec![vec![
        ((1, (200, 100, 11)), 2, 1),
        ((2, (200, 100, 22)), 2, 1),
        ((3, (200, 100, 33)), 2, 1),
        ((4, (200, 100, 44)), 2, 1),
        ((5, (200, 200, 55)), 2, 1),
    ]];
    run_test(input, expected, |coll| {
        coll.postpone(coll.scope(), |(t1, _t2, _d)| *t1, |(_t1, t2, _d)| *t2)
            .arrange_by_key()
    });
}

#[test]
fn test_wrapper_two_times_replace_aggregate() {
    let input = vec![
        vec![((1, (200, 100, 11)), 0, 1), ((2, (200, 100, 22)), 0, 1)],
        vec![((3, (200, 100, 33)), 1, 1), ((1, (200, 100, 11)), 1, -1)],
        vec![((1, (200, 200, 55)), 2, 1), ((6, (300, 200, 66)), 2, 1)],
    ];
    let expected = vec![vec![
        ((1, (200, 200, 55)), 2, 1),
        ((2, (200, 100, 22)), 2, 1),
        ((3, (200, 100, 33)), 2, 1),
    ]];
    run_test(input, expected, |coll| {
        coll.postpone(coll.scope(), |(t1, _t2, _d)| *t1, |(_t1, t2, _d)| *t2)
            .arrange_by_key()
    });
}

#[test]
fn test_wrapper_two_times_forward_late() {
    let input = vec![
        vec![((1, (100, 100, 11)), 0, 1), ((2, (100, 100, 22)), 0, 1)],
        vec![((3, (200, 100, 33)), 1, 1), ((4, (200, 200, 44)), 1, 1)],
        vec![((5, (100, 200, 55)), 2, 1), ((6, (300, 200, 66)), 2, 1)],
    ];
    let expected = vec![
        vec![((1, (100, 100, 11)), 0, 1), ((2, (100, 100, 22)), 0, 1)],
        vec![((3, (200, 100, 33)), 1, 1), ((4, (200, 200, 44)), 1, 1)],
        vec![((5, (100, 200, 55)), 2, 1)],
    ];
    run_test(input, expected, |coll| {
        coll.postpone(coll.scope(), |(t1, _t2, _d)| *t1, |(_t1, t2, _d)| *t2)
            .arrange_by_key()
    });
}

#[test]
fn test_forget() {
    let input = vec![
        vec![((1, (100, 11)), 0, 1), ((2, (100, 22)), 0, 1)],
        vec![((3, (200, 33)), 1, 1), ((4, (200, 44)), 1, 1)],
        vec![((5, (300, 55)), 2, 1), ((6, (300, 66)), 2, 1)],
    ];
    let expected = vec![
        vec![((1, (100, 11)), 0, 1), ((2, (100, 22)), 0, 1)],
        vec![
            ((1, (100, 11)), 1, -1),
            ((2, (100, 22)), 1, -1),
            ((3, (200, 33)), 1, 1),
            ((4, (200, 44)), 1, 1),
        ],
        vec![
            ((3, (200, 33)), 2, -1),
            ((4, (200, 44)), 2, -1),
            ((5, (300, 55)), 2, 1),
            ((6, (300, 66)), 2, 1),
        ],
    ];

    run_test(input, expected, |coll| {
        coll.forget(|(t, _d)| *t + 1, |(t, _d)| *t).arrange_by_key()
    });
}

#[test]
fn test_cutoff() {
    let input = vec![
        vec![((1, (100, 11)), 0, 1), ((2, (100, 22)), 0, 1)],
        vec![((3, (100, 33)), 1, 1), ((4, (200, 44)), 1, 1)],
        vec![((5, (300, 55)), 2, 1), ((6, (300, 66)), 2, 1)],
    ];
    let expected = vec![
        vec![((1, (100, 11)), 0, 1), ((2, (100, 22)), 0, 1)],
        vec![((4, (200, 44)), 1, 1)],
        vec![((5, (300, 55)), 2, 1), ((6, (300, 66)), 2, 1)],
    ];

    let expected_late = vec![vec![((3, (100, 33)), 1, 1)]];

    run_test(input.clone(), expected, |coll| {
        coll.freeze(|(t, _d)| *t + 1, |(t, _d)| *t)
            .0
            .arrange_by_key()
    });

    run_test(input, expected_late, |coll| {
        coll.freeze(|(t, _d)| *t + 1, |(t, _d)| *t)
            .1
            .arrange_by_key()
    });
}

#[test]
fn test_cutoff_late() {
    let input = vec![
        vec![((1, (100, 11)), 0, 1), ((2, (200, 22)), 0, 1)],
        vec![((3, (200, 33)), 1, 1), ((4, (100, 44)), 1, 1)],
        vec![((5, (300, 55)), 2, 1), ((6, (300, 66)), 2, 1)],
    ];
    let expected = vec![
        vec![((2, (200, 22)), 0, 1)],
        vec![((3, (200, 33)), 1, 1)],
        vec![((5, (300, 55)), 2, 1), ((6, (300, 66)), 2, 1)],
    ];
    let expected_late = vec![vec![((1, (100, 11)), 0, 1)], vec![((4, (100, 44)), 1, 1)]];

    run_test(input.clone(), expected, |coll| {
        coll.freeze(|(t, _d)| *t + 1, |(t, _d)| *t)
            .0
            .arrange_by_key()
    });

    run_test(input, expected_late, |coll| {
        coll.freeze(|(t, _d)| *t + 1, |(t, _d)| *t)
            .1
            .arrange_by_key()
    });
}

#[test]
fn test_cutoff_late_multiple() {
    let input = vec![
        vec![((1, (100, 11)), 0, 1), ((2, (200, 22)), 0, 1)],
        vec![((3, (200, 33)), 1, 1), ((4, (100, 44)), 1, 1)],
        vec![
            ((5, (300, 55)), 2, 1),
            ((6, (100, 66)), 2, 1),
            ((7, (-100, 77)), 2, 1),
        ],
    ];
    let expected = vec![
        vec![((2, (200, 22)), 0, 1)],
        vec![((3, (200, 33)), 1, 1)],
        vec![((5, (300, 55)), 2, 1)],
    ];

    let expected_late = vec![
        vec![((1, (100, 11)), 0, 1)],
        vec![((4, (100, 44)), 1, 1)],
        vec![((6, (100, 66)), 2, 1), ((7, (-100, 77)), 2, 1)],
    ];
    run_test(input.clone(), expected, |coll| {
        coll.freeze(|(t, _d)| *t + 1, |(t, _d)| *t)
            .0
            .arrange_by_key()
    });

    run_test(input, expected_late, |coll| {
        coll.freeze(|(t, _d)| *t + 1, |(t, _d)| *t)
            .1
            .arrange_by_key()
    });
}
