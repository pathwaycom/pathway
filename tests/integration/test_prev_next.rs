// Copyright © 2024 Pathway

#![allow(clippy::disallowed_methods)]

use super::operator_test_utils::run_test;

use pathway_engine::engine::dataflow::operators::prev_next::add_prev_next_pointers;

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::collections::{BTreeSet, HashSet};

use differential_dataflow::operators::arrange::ArrangeBySelf;
use timely::dataflow::operators::capture::capture::Capture;
use timely::dataflow::operators::capture::Extract;

use differential_dataflow::AsCollection;

use timely::dataflow::operators::ToStream;

type T = i32;
type D<K> = (K, (Option<K>, Option<K>));
type OutputBatch<K> = Vec<(D<K>, T, i32)>;

//single batch test, example of simple, standalone test setup
#[test]
#[allow(clippy::disallowed_methods)]
fn test_prev_next_insert_00() {
    let res = timely::example(|scope| {
        let col = vec![(1, 1, 1), (3, 1, 1), (5, 1, 1), (7, 1, 1)]
            .into_iter()
            .to_stream(scope)
            .as_collection();

        let arr = col.arrange_by_self();
        let sorted = add_prev_next_pointers(arr, &|_a, _b| true);
        sorted
            .as_collection(|key, val| (*key, *val))
            .inner
            .capture()
    });
    let to_print: Vec<_> = res.extract();
    let expected = vec![
        ((1, (None, Some(3))), 1, 1),
        ((3, (Some(1), Some(5))), 1, 1),
        ((5, (Some(3), Some(7))), 1, 1),
        ((7, (Some(5), None)), 1, 1),
    ];
    assert!(to_print[0].1.eq(&expected));
}

//two batches
#[test]
fn test_prev_next_insert_01() {
    let mut input = Vec::new();
    let size = 4;
    for item in 0..size {
        input.push((2 * item + 1, 1, 1));
    }
    let mut input2 = Vec::new();
    for item in 0..size {
        input2.push((2 * item, 2, 1));
    }

    let expected: OutputBatch<i32> = vec![
        ((1, (None, Some(3))), 1, 1),
        ((3, (Some(1), Some(5))), 1, 1),
        ((5, (Some(3), Some(7))), 1, 1),
        ((7, (Some(5), None)), 1, 1),
    ];
    let expected2: OutputBatch<i32> = vec![
        ((1, (None, Some(3))), 2, -1),
        ((3, (Some(1), Some(5))), 2, -1),
        ((5, (Some(3), Some(7))), 2, -1),
        ((7, (Some(5), None)), 2, -1),
        ((0, (None, Some(1))), 2, 1),
        ((1, (Some(0), Some(2))), 2, 1),
        ((2, (Some(1), Some(3))), 2, 1),
        ((3, (Some(2), Some(4))), 2, 1),
        ((4, (Some(3), Some(5))), 2, 1),
        ((5, (Some(4), Some(6))), 2, 1),
        ((6, (Some(5), Some(7))), 2, 1),
        ((7, (Some(6), None)), 2, 1),
    ];
    run_test(vec![input, input2], vec![expected, expected2], |coll| {
        add_prev_next_pointers(coll.arrange_by_self(), &|_a, _b| true)
    });
}

//consecutive entries in the first batch, no unchanged entries on update
#[test]
fn test_prev_next_insert_02() {
    let mut input = Vec::new();
    let size = 4;
    for item in 0..size {
        input.push((2 * item + 1, 1, 1));
    }
    let mut input2 = Vec::new();
    for item in 0..size {
        if item != 2 {
            input2.push((2 * item, 2, 1));
        }
    }

    let expected: OutputBatch<i32> = vec![
        ((1, (None, Some(3))), 1, 1),
        ((3, (Some(1), Some(5))), 1, 1),
        ((5, (Some(3), Some(7))), 1, 1),
        ((7, (Some(5), None)), 1, 1),
    ];

    let expected2: OutputBatch<i32> = vec![
        ((1, (None, Some(3))), 2, -1),
        ((3, (Some(1), Some(5))), 2, -1),
        ((5, (Some(3), Some(7))), 2, -1),
        ((7, (Some(5), None)), 2, -1),
        ((0, (None, Some(1))), 2, 1),
        ((1, (Some(0), Some(2))), 2, 1),
        ((2, (Some(1), Some(3))), 2, 1),
        ((3, (Some(2), Some(5))), 2, 1),
        ((5, (Some(3), Some(6))), 2, 1),
        ((6, (Some(5), Some(7))), 2, 1),
        ((7, (Some(6), None)), 2, 1),
    ];
    run_test(vec![input, input2], vec![expected, expected2], |coll| {
        add_prev_next_pointers(coll.arrange_by_self(), &|_a, _b| true)
    });
}

//consecutive entries in the first batch, unchanged entries on update
#[test]
fn test_prev_next_insert_03() {
    let mut input = Vec::new();
    let size = 6;
    for item in 0..size {
        input.push((2 * item + 1, 1, 1));
    }
    let mut input2 = Vec::new();
    for item in 0..size {
        if item != 2 && item != 3 {
            input2.push((2 * item, 2, 1));
        }
    }

    let expected: OutputBatch<i32> = vec![
        ((1, (None, Some(3))), 1, 1),
        ((3, (Some(1), Some(5))), 1, 1),
        ((5, (Some(3), Some(7))), 1, 1),
        ((7, (Some(5), Some(9))), 1, 1),
        ((9, (Some(7), Some(11))), 1, 1),
        ((11, (Some(9), None)), 1, 1),
    ];
    let expected2: OutputBatch<i32> = vec![
        ((1, (None, Some(3))), 2, -1),
        ((3, (Some(1), Some(5))), 2, -1),
        ((7, (Some(5), Some(9))), 2, -1),
        ((9, (Some(7), Some(11))), 2, -1),
        ((11, (Some(9), None)), 2, -1),
        ((0, (None, Some(1))), 2, 1),
        ((1, (Some(0), Some(2))), 2, 1),
        ((2, (Some(1), Some(3))), 2, 1),
        ((3, (Some(2), Some(5))), 2, 1),
        ((7, (Some(5), Some(8))), 2, 1),
        ((8, (Some(7), Some(9))), 2, 1),
        ((9, (Some(8), Some(10))), 2, 1),
        ((10, (Some(9), Some(11))), 2, 1),
        ((11, (Some(10), None)), 2, 1),
    ];
    run_test(vec![input, input2], vec![expected, expected2], |coll| {
        add_prev_next_pointers(coll.arrange_by_self(), &|_a, _b| true)
    });
}

#[test]
fn test_prev_next_insert_04() {
    let mut input = Vec::new();
    let size = 4;
    for item in 0..size {
        if item != 2 {
            input.push((2 * item + 1, 1, 1));
        }
    }
    let mut input2 = Vec::new();
    for item in 0..size {
        input2.push((2 * item, 2, 1));
    }

    let expected: OutputBatch<i32> = vec![
        ((1, (None, Some(3))), 1, 1),
        ((3, (Some(1), Some(7))), 1, 1),
        ((7, (Some(3), None)), 1, 1),
    ];
    // the way we derive times is not fixed yet;
    // at the moment it is the largest time that is involved in producing the update entry
    // it may be changed to something else, then this test needs to be adjusted
    let expected2: OutputBatch<i32> = vec![
        ((1, (None, Some(3))), 2, -1),
        ((3, (Some(1), Some(7))), 2, -1),
        ((7, (Some(3), None)), 2, -1),
        ((0, (None, Some(1))), 2, 1),
        ((1, (Some(0), Some(2))), 2, 1),
        ((2, (Some(1), Some(3))), 2, 1),
        ((3, (Some(2), Some(4))), 2, 1),
        ((4, (Some(3), Some(6))), 2, 1),
        ((6, (Some(4), Some(7))), 2, 1),
        ((7, (Some(6), None)), 2, 1),
    ];
    run_test(vec![input, input2], vec![expected, expected2], |coll| {
        add_prev_next_pointers(coll.arrange_by_self(), &|_a, _b| true)
    });
}

#[test]
fn test_prev_next_insert_05() {
    let mut input = Vec::new();
    let size = 6;
    for item in 0..size {
        if item != 2 && item != 3 {
            input.push((2 * item + 1, 1, 1));
        }
    }
    let mut input2 = Vec::new();
    for item in 0..size {
        input2.push((2 * item, 2, 1));
    }

    let expected: OutputBatch<i32> = vec![
        ((1, (None, Some(3))), 1, 1),
        ((3, (Some(1), Some(9))), 1, 1),
        ((9, (Some(3), Some(11))), 1, 1),
        ((11, (Some(9), None)), 1, 1),
    ];

    let expected2: OutputBatch<i32> = vec![
        ((1, (None, Some(3))), 2, -1),
        ((3, (Some(1), Some(9))), 2, -1),
        ((9, (Some(3), Some(11))), 2, -1),
        ((11, (Some(9), None)), 2, -1),
        ((0, (None, Some(1))), 2, 1),
        ((1, (Some(0), Some(2))), 2, 1),
        ((2, (Some(1), Some(3))), 2, 1),
        ((3, (Some(2), Some(4))), 2, 1),
        ((4, (Some(3), Some(6))), 2, 1),
        ((6, (Some(4), Some(8))), 2, 1),
        ((8, (Some(6), Some(9))), 2, 1),
        ((9, (Some(8), Some(10))), 2, 1),
        ((10, (Some(9), Some(11))), 2, 1),
        ((11, (Some(10), None)), 2, 1),
    ];

    run_test(vec![input, input2], vec![expected, expected2], |coll| {
        add_prev_next_pointers(coll.arrange_by_self(), &|_a, _b| true)
    });
}

// same data as test_prev_next_batch_splitting_01, but given as single batch with two times
#[test]
fn test_prev_next_multiple_times_00() {
    let mut input = Vec::new();
    let size = 4;
    for item in 0..size {
        input.push((2 * item + 1, 1, 1));
    }
    for item in 0..size {
        input.push((2 * item, 2, 1));
    }

    let mut expected: OutputBatch<i32> = vec![
        ((1, (None, Some(3))), 1, 1),
        ((3, (Some(1), Some(5))), 1, 1),
        ((5, (Some(3), Some(7))), 1, 1),
        ((7, (Some(5), None)), 1, 1),
    ];
    let expected2: OutputBatch<i32> = vec![
        ((1, (None, Some(3))), 2, -1),
        ((3, (Some(1), Some(5))), 2, -1),
        ((5, (Some(3), Some(7))), 2, -1),
        ((7, (Some(5), None)), 2, -1),
        ((0, (None, Some(1))), 2, 1),
        ((1, (Some(0), Some(2))), 2, 1),
        ((2, (Some(1), Some(3))), 2, 1),
        ((3, (Some(2), Some(4))), 2, 1),
        ((4, (Some(3), Some(5))), 2, 1),
        ((5, (Some(4), Some(6))), 2, 1),
        ((6, (Some(5), Some(7))), 2, 1),
        ((7, (Some(6), None)), 2, 1),
    ];

    expected.extend(expected2.iter());
    run_test(vec![input], vec![expected], |coll| {
        add_prev_next_pointers(coll.arrange_by_self(), &|_a, _b| true)
    });
}

//consecutive entries in the first batch, no unchanged entries on update
#[test]
fn test_prev_next_zero_entries_00() {
    let mut input = Vec::new();
    let size = 4;
    for item in 0..size {
        input.push((2 * item + 1, 1, 1));
    }
    let mut input2 = Vec::new();
    for item in 0..size {
        input2.push((2 * item, 2, if item != 2 { 1 } else { 0 }));
    }

    let expected: OutputBatch<i32> = vec![
        ((1, (None, Some(3))), 1, 1),
        ((3, (Some(1), Some(5))), 1, 1),
        ((5, (Some(3), Some(7))), 1, 1),
        ((7, (Some(5), None)), 1, 1),
    ];

    let expected2: OutputBatch<i32> = vec![
        ((1, (None, Some(3))), 2, -1),
        ((3, (Some(1), Some(5))), 2, -1),
        ((5, (Some(3), Some(7))), 2, -1),
        ((7, (Some(5), None)), 2, -1),
        ((0, (None, Some(1))), 2, 1),
        ((1, (Some(0), Some(2))), 2, 1),
        ((2, (Some(1), Some(3))), 2, 1),
        ((3, (Some(2), Some(5))), 2, 1),
        ((5, (Some(3), Some(6))), 2, 1),
        ((6, (Some(5), Some(7))), 2, 1),
        ((7, (Some(6), None)), 2, 1),
    ];
    run_test(vec![input, input2], vec![expected, expected2], |coll| {
        add_prev_next_pointers(coll.arrange_by_self(), &|_a, _b| true)
    });
}

//consecutive entries in the first batch, unchanged entries on update
#[test]
fn test_prev_next_zero_entries_01() {
    let mut input = Vec::new();
    let size = 6;
    for item in 0..size {
        input.push((2 * item + 1, 1, 1));
    }
    let mut input2 = Vec::new();
    for item in 0..size {
        input2.push((2 * item, 2, if item != 2 && item != 3 { 1 } else { 0 }));
    }

    let expected: OutputBatch<i32> = vec![
        ((1, (None, Some(3))), 1, 1),
        ((3, (Some(1), Some(5))), 1, 1),
        ((5, (Some(3), Some(7))), 1, 1),
        ((7, (Some(5), Some(9))), 1, 1),
        ((9, (Some(7), Some(11))), 1, 1),
        ((11, (Some(9), None)), 1, 1),
    ];
    let expected2: OutputBatch<i32> = vec![
        ((1, (None, Some(3))), 2, -1),
        ((3, (Some(1), Some(5))), 2, -1),
        ((7, (Some(5), Some(9))), 2, -1),
        ((9, (Some(7), Some(11))), 2, -1),
        ((11, (Some(9), None)), 2, -1),
        ((0, (None, Some(1))), 2, 1),
        ((1, (Some(0), Some(2))), 2, 1),
        ((2, (Some(1), Some(3))), 2, 1),
        ((3, (Some(2), Some(5))), 2, 1),
        ((7, (Some(5), Some(8))), 2, 1),
        ((8, (Some(7), Some(9))), 2, 1),
        ((9, (Some(8), Some(10))), 2, 1),
        ((10, (Some(9), Some(11))), 2, 1),
        ((11, (Some(10), None)), 2, 1),
    ];
    run_test(vec![input, input2], vec![expected, expected2], |coll| {
        add_prev_next_pointers(coll.arrange_by_self(), &|_a, _b| true)
    });
}

//remove every second
#[test]
fn test_prev_next_delete_00() {
    let mut input = Vec::new();
    let size = 10;
    for item in 0..size {
        input.push((item, 1, 1));
    }
    let mut input2 = Vec::new();
    for item in 0..(size / 2) {
        input2.push((2 * item, 2, -1));
    }

    let expected: OutputBatch<i32> = vec![
        ((0, (None, Some(1))), 1, 1),
        ((1, (Some(0), Some(2))), 1, 1),
        ((2, (Some(1), Some(3))), 1, 1),
        ((3, (Some(2), Some(4))), 1, 1),
        ((4, (Some(3), Some(5))), 1, 1),
        ((5, (Some(4), Some(6))), 1, 1),
        ((6, (Some(5), Some(7))), 1, 1),
        ((7, (Some(6), Some(8))), 1, 1),
        ((8, (Some(7), Some(9))), 1, 1),
        ((9, (Some(8), None)), 1, 1),
    ];
    let expected2: OutputBatch<i32> = vec![
        ((0, (None, Some(1))), 2, -1),
        ((1, (Some(0), Some(2))), 2, -1),
        ((2, (Some(1), Some(3))), 2, -1),
        ((3, (Some(2), Some(4))), 2, -1),
        ((4, (Some(3), Some(5))), 2, -1),
        ((5, (Some(4), Some(6))), 2, -1),
        ((6, (Some(5), Some(7))), 2, -1),
        ((7, (Some(6), Some(8))), 2, -1),
        ((8, (Some(7), Some(9))), 2, -1),
        ((9, (Some(8), None)), 2, -1),
        ((1, (None, Some(3))), 2, 1),
        ((3, (Some(1), Some(5))), 2, 1),
        ((5, (Some(3), Some(7))), 2, 1),
        ((7, (Some(5), Some(9))), 2, 1),
        ((9, (Some(7), None)), 2, 1),
    ];

    run_test(vec![input, input2], vec![expected, expected2], |coll| {
        add_prev_next_pointers(coll.arrange_by_self(), &|_a, _b| true)
    });
}

//remove block
#[test]

fn test_prev_next_delete_01() {
    let mut input = Vec::new();
    let size = 10;
    for item in 0..size {
        input.push((item, 1, 1));
    }
    let mut input2 = Vec::new();
    for item in 0..(size / 2) {
        input2.push((item + 3, 2, -1));
    }

    let expected: OutputBatch<i32> = vec![
        ((0, (None, Some(1))), 1, 1),
        ((1, (Some(0), Some(2))), 1, 1),
        ((2, (Some(1), Some(3))), 1, 1),
        ((3, (Some(2), Some(4))), 1, 1),
        ((4, (Some(3), Some(5))), 1, 1),
        ((5, (Some(4), Some(6))), 1, 1),
        ((6, (Some(5), Some(7))), 1, 1),
        ((7, (Some(6), Some(8))), 1, 1),
        ((8, (Some(7), Some(9))), 1, 1),
        ((9, (Some(8), None)), 1, 1),
    ];

    let expected2: OutputBatch<i32> = vec![
        ((2, (Some(1), Some(3))), 2, -1),
        ((3, (Some(2), Some(4))), 2, -1),
        ((4, (Some(3), Some(5))), 2, -1),
        ((5, (Some(4), Some(6))), 2, -1),
        ((6, (Some(5), Some(7))), 2, -1),
        ((7, (Some(6), Some(8))), 2, -1),
        ((8, (Some(7), Some(9))), 2, -1),
        ((2, (Some(1), Some(8))), 2, 1),
        ((8, (Some(2), Some(9))), 2, 1),
    ];

    run_test(vec![input, input2], vec![expected, expected2], |coll| {
        add_prev_next_pointers(coll.arrange_by_self(), &|_a, _b| true)
    });
}

//consecutive entries in the first batch, no unchanged entries on update
#[test]
fn test_prev_next_delete_02() {
    let input = vec![(1, 1, 1), (3, 1, 1), (5, 1, 1), (7, 1, 1)];
    let input2 = vec![(1, 2, -1), (8, 2, 1)];

    let expected: OutputBatch<i32> = vec![
        ((1, (None, Some(3))), 1, 1),
        ((3, (Some(1), Some(5))), 1, 1),
        ((5, (Some(3), Some(7))), 1, 1),
        ((7, (Some(5), None)), 1, 1),
    ];
    // the way we derive times is not fixed yet;
    // at the moment it is the largest time that is involved in producing the update entry
    // it may be changed to something else, then this test needs to be adjusted
    let expected2: OutputBatch<i32> = vec![
        ((1, (None, Some(3))), 2, -1),
        ((3, (Some(1), Some(5))), 2, -1),
        ((3, (None, Some(5))), 2, 1),
        ((7, (Some(5), None)), 2, -1),
        ((7, (Some(5), Some(8))), 2, 1),
        ((8, (Some(7), None)), 2, 1),
    ];
    run_test(vec![input, input2], vec![expected, expected2], |coll| {
        add_prev_next_pointers(coll.arrange_by_self(), &|_a, _b| true)
    });
}

#[test]
fn test_prev_next_delete_after_insert_with_gap() {
    let input = vec![
        vec![
            (4, 0, 1),
            (21, 0, 1),
            (27, 0, 1),
            (31, 0, 1),
            (32, 0, 1),
            (45, 0, 1),
            (55, 0, 1),
        ],
        vec![
            (8, 1, 1),
            (25, 1, 1),
            (45, 1, -1),
            (46, 1, 1),
            (50, 1, 1),
            (79, 1, 1),
            (95, 1, 1),
        ],
    ];

    let expected = vec![
        vec![
            ((4, (None, Some(21))), 0, 1),
            ((21, (Some(4), Some(27))), 0, 1),
            ((27, (Some(21), Some(31))), 0, 1),
            ((31, (Some(27), Some(32))), 0, 1),
            ((32, (Some(31), Some(45))), 0, 1),
            ((45, (Some(32), Some(55))), 0, 1),
            ((55, (Some(45), None)), 0, 1),
        ],
        vec![
            ((4, (None, Some(21))), 1, -1),
            ((21, (Some(4), Some(27))), 1, -1),
            ((27, (Some(21), Some(31))), 1, -1),
            ((32, (Some(31), Some(45))), 1, -1),
            ((45, (Some(32), Some(55))), 1, -1),
            ((55, (Some(45), None)), 1, -1),
            ((4, (None, Some(8))), 1, 1),
            ((8, (Some(4), Some(21))), 1, 1),
            ((21, (Some(8), Some(25))), 1, 1),
            ((25, (Some(21), Some(27))), 1, 1),
            ((27, (Some(25), Some(31))), 1, 1),
            ((32, (Some(31), Some(46))), 1, 1),
            ((46, (Some(32), Some(50))), 1, 1),
            ((50, (Some(46), Some(55))), 1, 1),
            ((55, (Some(50), Some(79))), 1, 1),
            ((79, (Some(55), Some(95))), 1, 1),
            ((95, (Some(79), None)), 1, 1),
        ],
    ];
    run_test(input, expected, |coll| {
        add_prev_next_pointers(coll.arrange_by_self(), &|_a, _b| true)
    });
}

#[test]
fn test_three_batches() {
    let input = vec![
        vec![(15, 0, 1), (16, 0, 1), (-4, 0, 1)],
        vec![(10, 1, 1), (17, 1, 1), (2, 1, 1)],
        vec![(15, 2, -1), (-3, 2, 1)],
    ];
    let expected = vec![
        vec![
            ((-4, (None, Some(15))), 0, 1),
            ((15, (Some(-4), Some(16))), 0, 1),
            ((16, (Some(15), None)), 0, 1),
        ],
        vec![
            ((-4, (None, Some(15))), 1, -1),
            ((15, (Some(-4), Some(16))), 1, -1),
            ((16, (Some(15), None)), 1, -1),
            ((-4, (None, Some(2))), 1, 1),
            ((2, (Some(-4), Some(10))), 1, 1),
            ((10, (Some(2), Some(15))), 1, 1),
            ((15, (Some(10), Some(16))), 1, 1),
            ((16, (Some(15), Some(17))), 1, 1),
            ((17, (Some(16), None)), 1, 1),
        ],
        vec![
            ((-4, (None, Some(2))), 2, -1),
            ((2, (Some(-4), Some(10))), 2, -1),
            ((10, (Some(2), Some(15))), 2, -1),
            ((15, (Some(10), Some(16))), 2, -1),
            ((16, (Some(15), Some(17))), 2, -1),
            ((-4, (None, Some(-3))), 2, 1),
            ((-3, (Some(-4), Some(2))), 2, 1),
            ((2, (Some(-3), Some(10))), 2, 1),
            ((10, (Some(2), Some(16))), 2, 1),
            ((16, (Some(10), Some(17))), 2, 1),
        ],
    ];
    run_test(input, expected, |coll| {
        add_prev_next_pointers(coll.arrange_by_self(), &|_a, _b| true)
    });
}

#[test]
/// in case some changes are applied to prev-next related code, this test
/// is to be run manually, with several different configurations (more test cases,
/// larger test cases etc)
fn test_random() {
    let n_tests = 10;
    let max_n_iterations = 5;
    let max_n_elements_per_iteration = 5;
    let min_val = -4;
    let max_val = 20;

    for i in 0..n_tests {
        println!("TEST {i}");
        let mut inputs = Vec::new();
        let mut expected = Vec::new();

        let mut current = BTreeSet::new();
        let mut previous_state = Vec::new();
        let mut previous_state_hashset = HashSet::new();

        let mut rng = StdRng::seed_from_u64(i);

        let n_iterations = rng.gen_range(2..=max_n_iterations);

        for t in 0..n_iterations {
            let n_elements = rng.gen_range(2..=max_n_elements_per_iteration);
            let mut input_current = Vec::new();

            for _ in 0..n_elements {
                let elem = rng.gen_range(min_val..=max_val);
                if current.remove(&elem) {
                    input_current.push((elem, t, -1));
                } else {
                    input_current.push((elem, t, 1));
                    current.insert(elem);
                }
            }
            inputs.push(input_current);

            let mut current_state = Vec::with_capacity(n_elements);
            let mut current_state_hashset = HashSet::with_capacity(n_elements);
            let mut prev_prev = None;
            let mut prev = None;
            for entry in &current {
                if let Some(prev_val) = prev {
                    current_state.push((prev_val, (prev_prev, Some(*entry))));
                    current_state_hashset.insert((prev_val, (prev_prev, Some(*entry))));
                }
                prev_prev = prev;
                prev = Some(*entry);
            }
            if let Some(prev_val) = prev {
                current_state.push((prev_val, (prev_prev, None)));
                current_state_hashset.insert((prev_val, (prev_prev, None)));
            }
            let mut expected_current = Vec::with_capacity(2 * n_elements);

            for entry in &previous_state {
                if !current_state_hashset.contains(entry) {
                    expected_current.push((*entry, t, -1));
                }
            }
            for entry in &current_state {
                if !previous_state_hashset.contains(entry) {
                    expected_current.push((*entry, t, 1));
                }
            }

            previous_state = current_state;
            previous_state_hashset = current_state_hashset;

            if !expected_current.is_empty() {
                expected.push(expected_current);
            }
        }

        println!("{:?}", inputs);
        println!("{:?}", expected);
        run_test(inputs, expected, |coll| {
            add_prev_next_pointers(coll.arrange_by_self(), &|_a, _b| true)
        });
    }
}

#[test]
fn test_instances_random() {
    let n_tests = 10;
    let max_n_iterations = 6;
    let max_n_elements_per_iteration = 10;
    let min_val = -4;
    let max_val = 20;
    let instances = 4;

    for i in 0..n_tests {
        println!("TEST {i}");
        let mut inputs = Vec::new();
        let mut expected = Vec::new();

        let mut current = BTreeSet::new();
        let mut previous_state = Vec::new();
        let mut previous_state_hashset = HashSet::new();

        let mut rng = StdRng::seed_from_u64(i);

        let n_iterations = rng.gen_range(2..=max_n_iterations);

        for t in 0..n_iterations {
            let n_elements = rng.gen_range(2..=max_n_elements_per_iteration);
            let mut input_current = Vec::new();

            for _ in 0..n_elements {
                let elem = (
                    rng.gen_range(1..=instances),
                    rng.gen_range(min_val..=max_val),
                );
                if current.remove(&elem) {
                    input_current.push((elem, t, -1));
                } else {
                    input_current.push((elem, t, 1));
                    current.insert(elem);
                }
            }
            inputs.push(input_current);

            let mut current_state: Vec<D<(i32, i32)>> = Vec::with_capacity(n_elements);
            let mut current_state_hashset: HashSet<D<(i32, i32)>> =
                HashSet::with_capacity(n_elements);
            let mut prev_prev: Option<(i32, i32)> = None;
            let mut prev: Option<(i32, i32)> = None;

            for entry in &current {
                if let Some(prev_val) = prev {
                    if prev_val.0 == entry.0 {
                        current_state.push((prev_val, (prev_prev, Some(*entry))));
                        current_state_hashset.insert((prev_val, (prev_prev, Some(*entry))));
                        prev_prev = prev
                    } else {
                        current_state.push((prev_val, (prev_prev, None)));
                        current_state_hashset.insert((prev_val, (prev_prev, None)));
                        prev_prev = None
                    }
                }
                prev = Some(*entry);
            }

            if let Some(prev_val) = prev {
                current_state.push((prev_val, (prev_prev, None)));
                current_state_hashset.insert((prev_val, (prev_prev, None)));
            }
            let mut expected_current = Vec::with_capacity(2 * n_elements);

            for entry in &previous_state {
                if !current_state_hashset.contains(entry) {
                    expected_current.push((*entry, t, -1));
                }
            }
            for entry in &current_state {
                if !previous_state_hashset.contains(entry) {
                    expected_current.push((*entry, t, 1));
                }
            }

            previous_state = current_state;
            previous_state_hashset = current_state_hashset;

            if !expected_current.is_empty() {
                expected.push(expected_current);
            }
        }

        println!("{:?}", inputs);
        println!("{:?}", expected);
        run_test(inputs, expected, |coll| {
            add_prev_next_pointers(coll.arrange_by_self(), &|a, b| a.0 == b.0)
        });
    }
}
