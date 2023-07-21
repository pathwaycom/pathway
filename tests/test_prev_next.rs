use pathway_engine::engine::dataflow::operators::prev_next::add_prev_next_pointers;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::collections::{BTreeSet, HashSet};
use std::sync::mpsc::Receiver;

use differential_dataflow::input::InputSession;
use differential_dataflow::operators::arrange::ArrangeBySelf;
use std::cmp::max;
use std::sync::{Arc, Mutex};
use timely::communication::allocator::Generic;
use timely::dataflow::operators::capture::capture::Capture;
use timely::dataflow::operators::capture::Extract;
use timely::dataflow::operators::capture::Replay;
use timely::dataflow::operators::capture::{EventCore, EventLinkCore};
use timely::worker::Worker;
use timely::Config;

use differential_dataflow::trace::TraceReader;
use differential_dataflow::{AsCollection, ExchangeData, Hashable};
use std::rc::Rc;

use timely::dataflow::operators::Inspect;
use timely::dataflow::operators::ToStream;

type T = i32;
type D<K> = (K, (Option<K>, Option<K>));
type OutputBatch<K> = Vec<(D<K>, T, i32)>;

#[allow(clippy::disallowed_methods)]
fn run_test<K: ExchangeData + Hashable + Copy>(
    input_batches: Vec<Vec<(K, i32, i32)>>,
    mut expected_output_batches: Vec<OutputBatch<K>>,
    instance_filter: &'static (impl Fn(&K, &K) -> bool + std::marker::Sync),
) {
    let (send, recv): (_, Receiver<EventCore<T, OutputBatch<K>>>) = ::std::sync::mpsc::channel();
    let send = Arc::new(Mutex::new(send));

    timely::execute(Config::thread(), move |worker: &mut Worker<Generic>| {
        // this is only to validate the output.
        let send = send.lock().unwrap().clone();
        // these are to capture/replay the stream.
        let handle1 = Rc::new(EventLinkCore::<T, OutputBatch<K>>::new());
        let handle2 = Some(handle1.clone());
        // create an input collection of data.
        let mut input: InputSession<i32, K, i32> = InputSession::new();

        // define a new computation.
        worker.dataflow(
            |scope: &mut timely::dataflow::scopes::Child<
                timely::worker::Worker<timely::communication::Allocator>,
                i32,
            >| {
                // create a new collection from our input.
                let sorter = input.to_collection(scope);
                let arranged = sorter.arrange_by_self();
                let res = add_prev_next_pointers(arranged.clone(), instance_filter);
                res.trace
                    .map_batches(|batch| println!("outer debug, res map batch{:?}", batch));
                res.stream.inspect(move |x| {
                    println!("outer debug, res inspect {:?}", &x);
                });
                arranged
                    .trace
                    .map_batches(|batch| println!("outer debug, input map batch{:?}", batch));
                arranged.stream.inspect(move |x| {
                    println!("outer debug, input inspect {:?}", &x);
                });
                res.as_collection(|key, val| (*key, *val))
                    .inner
                    .capture_into(handle1);
            },
        );

        worker.dataflow(|scope2| handle2.replay_into(scope2).capture_into(send));

        input.advance_to(0);
        let mut t = 0;
        for batch in input_batches.iter() {
            for (element, change, time) in batch.iter() {
                input.advance_to(*time);
                input.update(*element, *change);
                println!(
                    "time {:?} element {:?} change {:?}",
                    *time, *element, *change
                );
                t = max(*time, t);
            }
            input.advance_to(t + 1);
            input.flush();
            worker.step();
        }
        worker.step();
    })
    .expect("Computation terminated abnormally");

    let mut to_print: Vec<(T, OutputBatch<K>)> = recv.extract();
    println!("{:?}", to_print);

    for i in 0..to_print.len() {
        to_print[i].1.sort();
        expected_output_batches[i].sort();
        println!("results to compare for idx {i:?}");
        println!("{:?}", to_print[i].1);
        println!("{:?}", expected_output_batches[i]);
        assert!(to_print[i].1.eq(&expected_output_batches[i]));
    }
}

//single batch test
#[test]
#[allow(clippy::disallowed_methods)]
fn test_prev_next_insert_00() {
    let res = timely::example(|scope| {
        let col = vec![(1, 11, 1), (3, 13, 1), (5, 15, 1), (7, 17, 1)]
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
        ((1, (None, Some(3))), 13, 1),
        ((3, (Some(1), Some(5))), 15, 1),
        ((5, (Some(3), Some(7))), 17, 1),
        ((7, (Some(5), None)), 17, 1),
    ];
    assert!(to_print[0].1.eq(&expected));
}

//two batches
#[test]
fn test_prev_next_insert_01() {
    let mut input = Vec::new();
    let size = 4;
    for item in 0..size {
        input.push((2 * item + 1, 1, item));
    }
    let mut input2 = Vec::new();
    for item in 0..size {
        input2.push((2 * item, 1, size + item));
    }

    let expected: OutputBatch<i32> = vec![
        ((1, (None, Some(3))), 1, 1),
        ((3, (Some(1), Some(5))), 2, 1),
        ((5, (Some(3), Some(7))), 3, 1),
        ((7, (Some(5), None)), 3, 1),
    ];
    // the way we derive times is not fixed yet;
    // at the moment it is the largest time that is involved in producing the update entry
    // it may be changed to something else, then this test needs to be adjusted
    let expected2: OutputBatch<i32> = vec![
        ((1, (None, Some(3))), 5, -1),
        ((3, (Some(1), Some(5))), 6, -1),
        ((5, (Some(3), Some(7))), 7, -1),
        ((7, (Some(5), None)), 7, -1),
        ((0, (None, Some(1))), 4, 1),
        ((1, (Some(0), Some(2))), 5, 1),
        ((2, (Some(1), Some(3))), 5, 1),
        ((3, (Some(2), Some(4))), 6, 1),
        ((4, (Some(3), Some(5))), 6, 1),
        ((5, (Some(4), Some(6))), 7, 1),
        ((6, (Some(5), Some(7))), 7, 1),
        ((7, (Some(6), None)), 7, 1),
    ];

    run_test(vec![input, input2], vec![expected, expected2], &|_a, _b| {
        true
    });
}

//consecutive entries in the first batch, no unchanged entries on update
#[test]
fn test_prev_next_insert_02() {
    let mut input = Vec::new();
    let size = 4;
    for item in 0..size {
        input.push((2 * item + 1, 1, item));
    }
    let mut input2 = Vec::new();
    for item in 0..size {
        if item != 2 {
            input2.push((2 * item, 1, size + item));
        }
    }

    let expected: OutputBatch<i32> = vec![
        ((1, (None, Some(3))), 1, 1),
        ((3, (Some(1), Some(5))), 2, 1),
        ((5, (Some(3), Some(7))), 3, 1),
        ((7, (Some(5), None)), 3, 1),
    ];
    // the way we derive times is not fixed yet;
    // at the moment it is the largest time that is involved in producing the update entry
    // it may be changed to something else, then this test needs to be adjusted
    let expected2: OutputBatch<i32> = vec![
        ((1, (None, Some(3))), 5, -1),
        ((3, (Some(1), Some(5))), 5, -1),
        ((5, (Some(3), Some(7))), 7, -1),
        ((7, (Some(5), None)), 7, -1),
        ((0, (None, Some(1))), 4, 1),
        ((1, (Some(0), Some(2))), 5, 1),
        ((2, (Some(1), Some(3))), 5, 1),
        ((3, (Some(2), Some(5))), 5, 1),
        ((5, (Some(3), Some(6))), 7, 1),
        ((6, (Some(5), Some(7))), 7, 1),
        ((7, (Some(6), None)), 7, 1),
    ];
    run_test(vec![input, input2], vec![expected, expected2], &|_a, _b| {
        true
    });
}

//consecutive entries in the first batch, unchanged entries on update
#[test]
fn test_prev_next_insert_03() {
    let mut input = Vec::new();
    let size = 6;
    for item in 0..size {
        input.push((2 * item + 1, 1, item));
    }
    let mut input2 = Vec::new();
    for item in 0..size {
        if item != 2 && item != 3 {
            input2.push((2 * item, 1, size + item));
        }
    }

    let expected: OutputBatch<i32> = vec![
        ((1, (None, Some(3))), 1, 1),     // time(3) = 1
        ((3, (Some(1), Some(5))), 2, 1),  // time(5) = 2
        ((5, (Some(3), Some(7))), 3, 1),  // time(7) = 3
        ((7, (Some(5), Some(9))), 4, 1),  // time(9) = 4
        ((9, (Some(7), Some(11))), 5, 1), // time(11) = 5
        ((11, (Some(9), None)), 5, 1),    // time(11) = 5
    ];
    // the way we derive times is not fixed yet;
    // at the moment it is the largest time that is involved in producing the update entry
    // it may be changed to something else, then this test needs to be adjusted
    let expected2: OutputBatch<i32> = vec![
        ((1, (None, Some(3))), 7, -1),    // time(2) = 7
        ((3, (Some(1), Some(5))), 7, -1), // time(2) = 7, time(4)=8, 4 not in the test
        // ((5, (Some(3), Some(7))), 8, -1),    // time(6) = 9
        ((7, (Some(5), Some(9))), 10, -1),  // time(8) = 10
        ((9, (Some(7), Some(11))), 11, -1), // time(10) = 11
        ((11, (Some(9), None)), 11, -1),    // time(10) = 11
        ((0, (None, Some(1))), 6, 1),       // time(0) = 6
        ((1, (Some(0), Some(2))), 7, 1),    // time(2) = 7
        ((2, (Some(1), Some(3))), 7, 1),    // time(2) = 7
        ((3, (Some(2), Some(5))), 7, 1),    // time(2) = 7, time(4)=8 4 not present in the test
        //4
        //((5, (Some(3), Some(7))), _, 1),
        //6
        ((7, (Some(5), Some(8))), 10, 1),   // time(8) = 10
        ((8, (Some(7), Some(9))), 10, 1),   // time(8) = 10
        ((9, (Some(8), Some(10))), 11, 1),  // time(10) = 11
        ((10, (Some(9), Some(11))), 11, 1), // time(10) = 11
        ((11, (Some(10), None)), 11, 1),    // time(10) = 11
    ];
    run_test(vec![input, input2], vec![expected, expected2], &|_a, _b| {
        true
    });
}

#[test]
fn test_prev_next_insert_04() {
    let mut input = Vec::new();
    let size = 4;
    for item in 0..size {
        if item != 2 {
            input.push((2 * item + 1, 1, item));
        }
    }
    let mut input2 = Vec::new();
    for item in 0..size {
        input2.push((2 * item, 1, size + item));
    }

    let expected: OutputBatch<i32> = vec![
        ((1, (None, Some(3))), 1, 1),
        ((3, (Some(1), Some(7))), 3, 1),
        // ((5, (Some(3), Some(7))), 3, 1),
        ((7, (Some(3), None)), 3, 1),
    ];
    // the way we derive times is not fixed yet;
    // at the moment it is the largest time that is involved in producing the update entry
    // it may be changed to something else, then this test needs to be adjusted
    let expected2: OutputBatch<i32> = vec![
        ((1, (None, Some(3))), 5, -1),
        ((3, (Some(1), Some(7))), 6, -1),
        // ((5, (Some(3), Some(7))), 7, -1),
        ((7, (Some(3), None)), 7, -1),
        ((0, (None, Some(1))), 4, 1),
        ((1, (Some(0), Some(2))), 5, 1),
        ((2, (Some(1), Some(3))), 5, 1),
        ((3, (Some(2), Some(4))), 6, 1),
        ((4, (Some(3), Some(6))), 7, 1),
        // ((5, (Some(3), Some(6))), 7, 1),
        ((6, (Some(4), Some(7))), 7, 1),
        ((7, (Some(6), None)), 7, 1),
    ];
    run_test(vec![input, input2], vec![expected, expected2], &|_a, _b| {
        true
    });
}

#[test]
fn test_prev_next_insert_05() {
    let mut input = Vec::new();
    let size = 6;
    for item in 0..size {
        if item != 2 && item != 3 {
            input.push((2 * item + 1, 1, item));
        }
    }
    let mut input2 = Vec::new();
    for item in 0..size {
        input2.push((2 * item, 1, size + item));
    }

    let expected: OutputBatch<i32> = vec![
        ((1, (None, Some(3))), 1, 1),    // time(3) = 1
        ((3, (Some(1), Some(9))), 4, 1), // time(5) = 2, time(9) = 4
        // ((5, (Some(3), Some(7))), 3, 1),  // time(7) = 3
        // ((7, (Some(5), Some(9))), 4, 1),  // time(9) = 4
        ((9, (Some(3), Some(11))), 5, 1), // time(11) = 5
        ((11, (Some(9), None)), 5, 1),    // time(11) = 5
    ];
    // the way we derive times is not fixed yet;
    // at the moment it is the largest time that is involved in producing the update entry
    // it may be changed to something else, then this test needs to be adjusted
    let expected2: OutputBatch<i32> = vec![
        ((1, (None, Some(3))), 7, -1),    // time(2) = 7
        ((3, (Some(1), Some(9))), 8, -1), // time(4) = 8,
        // ((5, (Some(3), Some(7))), 3, -1),
        // ((7, (Some(5), Some(9))), 4, -1),
        ((9, (Some(3), Some(11))), 11, -1), // time(10) = 11
        ((11, (Some(9), None)), 11, -1),    // time(10) = 11
        ((0, (None, Some(1))), 6, 1),       // time(0) = 6
        ((1, (Some(0), Some(2))), 7, 1),    // time(2) = 7
        ((2, (Some(1), Some(3))), 7, 1),    // time(2) = 7
        ((3, (Some(2), Some(4))), 8, 1),    // time(4) = 8
        ((4, (Some(3), Some(6))), 9, 1),    // time(6) = 9
        // ((5, (Some(2), Some(5))), 7, 1),
        ((6, (Some(4), Some(8))), 10, 1),
        // ((7, (Some(5), Some(8))), 10, 1),// time(8) = 10
        ((8, (Some(6), Some(9))), 10, 1),   // time(8) = 10
        ((9, (Some(8), Some(10))), 11, 1),  // time(10) = 11
        ((10, (Some(9), Some(11))), 11, 1), // time(10) = 11
        ((11, (Some(10), None)), 11, 1),    // time(10) = 11
    ];

    run_test(vec![input, input2], vec![expected, expected2], &|_a, _b| {
        true
    });
}

//consecutive entries in the first batch, no unchanged entries on update
#[test]
fn test_prev_next_zero_entries_00() {
    let mut input = Vec::new();
    let size = 4;
    for item in 0..size {
        input.push((2 * item + 1, 1, item));
    }
    let mut input2 = Vec::new();
    for item in 0..size {
        input2.push((2 * item, if item != 2 { 1 } else { 0 }, size + item));
    }

    let expected: OutputBatch<i32> = vec![
        ((1, (None, Some(3))), 1, 1),
        ((3, (Some(1), Some(5))), 2, 1),
        ((5, (Some(3), Some(7))), 3, 1),
        ((7, (Some(5), None)), 3, 1),
    ];
    // the way we derive times is not fixed yet;
    // at the moment it is the largest time that is involved in producing the update entry
    // it may be changed to something else, then this test needs to be adjusted
    let expected2: OutputBatch<i32> = vec![
        ((1, (None, Some(3))), 5, -1),
        ((3, (Some(1), Some(5))), 5, -1),
        ((5, (Some(3), Some(7))), 7, -1),
        ((7, (Some(5), None)), 7, -1),
        ((0, (None, Some(1))), 4, 1),
        ((1, (Some(0), Some(2))), 5, 1),
        ((2, (Some(1), Some(3))), 5, 1),
        ((3, (Some(2), Some(5))), 5, 1),
        ((5, (Some(3), Some(6))), 7, 1),
        ((6, (Some(5), Some(7))), 7, 1),
        ((7, (Some(6), None)), 7, 1),
    ];
    run_test(vec![input, input2], vec![expected, expected2], &|_a, _b| {
        true
    });
}

//consecutive entries in the first batch, unchanged entries on update
#[test]
fn test_prev_next_zero_entries_01() {
    let mut input = Vec::new();
    let size = 6;
    for item in 0..size {
        input.push((2 * item + 1, 1, item));
    }
    let mut input2 = Vec::new();
    for item in 0..size {
        input2.push((
            2 * item,
            if item != 2 && item != 3 { 1 } else { 0 },
            size + item,
        ));
    }

    let expected: OutputBatch<i32> = vec![
        ((1, (None, Some(3))), 1, 1),     // time(3) = 1
        ((3, (Some(1), Some(5))), 2, 1),  // time(5) = 2
        ((5, (Some(3), Some(7))), 3, 1),  // time(7) = 3
        ((7, (Some(5), Some(9))), 4, 1),  // time(9) = 4
        ((9, (Some(7), Some(11))), 5, 1), // time(11) = 5
        ((11, (Some(9), None)), 5, 1),    // time(11) = 5
    ];
    // the way we derive times is not fixed yet;
    // at the moment it is the largest time that is involved in producing the update entry
    // it may be changed to something else, then this test needs to be adjusted
    let expected2: OutputBatch<i32> = vec![
        ((1, (None, Some(3))), 7, -1),    // time(2) = 7
        ((3, (Some(1), Some(5))), 7, -1), // time(2) = 7, time(4)=8, 4 not in the test
        // ((5, (Some(3), Some(7))), 8, -1),    // time(6) = 9
        ((7, (Some(5), Some(9))), 10, -1),  // time(8) = 10
        ((9, (Some(7), Some(11))), 11, -1), // time(10) = 11
        ((11, (Some(9), None)), 11, -1),    // time(10) = 11
        ((0, (None, Some(1))), 6, 1),       // time(0) = 6
        ((1, (Some(0), Some(2))), 7, 1),    // time(2) = 7
        ((2, (Some(1), Some(3))), 7, 1),    // time(2) = 7
        ((3, (Some(2), Some(5))), 7, 1),    // time(2) = 7, time(4)=8 4 not present in the test
        //4
        //((5, (Some(3), Some(7))), _, 1),
        //6
        ((7, (Some(5), Some(8))), 10, 1),   // time(8) = 10
        ((8, (Some(7), Some(9))), 10, 1),   // time(8) = 10
        ((9, (Some(8), Some(10))), 11, 1),  // time(10) = 11
        ((10, (Some(9), Some(11))), 11, 1), // time(10) = 11
        ((11, (Some(10), None)), 11, 1),    // time(10) = 11
    ];
    run_test(vec![input, input2], vec![expected, expected2], &|_a, _b| {
        true
    });
}

//remove every second
#[test]
fn test_prev_next_delete_00() {
    let mut input = Vec::new();
    let size = 10;
    for item in 0..size {
        input.push((item, 1, item));
    }
    let mut input2 = Vec::new();
    for item in 0..(size / 2) {
        input2.push((2 * item, -1, size + item));
    }

    let expected: OutputBatch<i32> = vec![
        ((0, (None, Some(1))), 1, 1),
        ((1, (Some(0), Some(2))), 2, 1),
        ((2, (Some(1), Some(3))), 3, 1),
        ((3, (Some(2), Some(4))), 4, 1),
        ((4, (Some(3), Some(5))), 5, 1),
        ((5, (Some(4), Some(6))), 6, 1),
        ((6, (Some(5), Some(7))), 7, 1),
        ((7, (Some(6), Some(8))), 8, 1),
        ((8, (Some(7), Some(9))), 9, 1),
        ((9, (Some(8), None)), 9, 1),
    ];
    // the way we derive times is not fixed yet;
    // at the moment it is the largest time that is involved in producing the update entry
    // it may be changed to something else, then this test needs to be adjusted
    let expected2: OutputBatch<i32> = vec![
        ((0, (None, Some(1))), 10, -1),
        ((1, (Some(0), Some(2))), 11, -1),
        ((2, (Some(1), Some(3))), 11, -1),
        ((3, (Some(2), Some(4))), 12, -1),
        ((4, (Some(3), Some(5))), 12, -1),
        ((5, (Some(4), Some(6))), 13, -1),
        ((6, (Some(5), Some(7))), 13, -1),
        ((7, (Some(6), Some(8))), 14, -1),
        ((8, (Some(7), Some(9))), 14, -1),
        ((9, (Some(8), None)), 14, -1),
        ((1, (None, Some(3))), 11, 1),    // time(2)=11
        ((3, (Some(1), Some(5))), 12, 1), // time(4)=12
        ((5, (Some(3), Some(7))), 13, 1), // time(6)=13
        ((7, (Some(5), Some(9))), 14, 1), // time(8)=14
        ((9, (Some(7), None)), 14, 1),    // time(8)=14
    ];

    run_test(vec![input, input2], vec![expected, expected2], &|_a, _b| {
        true
    });
}

//remove block
#[test]

fn test_prev_next_delete_01() {
    let mut input = Vec::new();
    let size = 10;
    for item in 0..size {
        input.push((item, 1, item));
    }
    let mut input2 = Vec::new();
    for item in 0..(size / 2) {
        input2.push((item + 3, -1, size + item));
    }

    let expected: OutputBatch<i32> = vec![
        ((0, (None, Some(1))), 1, 1),
        ((1, (Some(0), Some(2))), 2, 1),
        ((2, (Some(1), Some(3))), 3, 1),
        ((3, (Some(2), Some(4))), 4, 1),
        ((4, (Some(3), Some(5))), 5, 1),
        ((5, (Some(4), Some(6))), 6, 1),
        ((6, (Some(5), Some(7))), 7, 1),
        ((7, (Some(6), Some(8))), 8, 1),
        ((8, (Some(7), Some(9))), 9, 1),
        ((9, (Some(8), None)), 9, 1),
    ];
    // the way we derive times is not fixed yet;
    // at the moment it is the largest time that is involved in producing the update entry
    // it may be changed to something else, then this test needs to be adjusted
    let expected2: OutputBatch<i32> = vec![
        /*
            at the moment, the first entry has time 14;
            the logic behind is that 1-2-3
            was replaced by 1-2-4, then by 1-2-5, 1-2-6, 1-2-7, 1-2-8;
            time of 1-2-8 is 14; we aggregated all of those changes into one:
            1-2-3 -> 1,2,8, with time 14; it's far from clear whether this is
            right logic (alternative would be 10, as this is the time of first disappearance,
            but then there are no insert-delete entries related to intermediate states, which
            is somewhat inconsistent with this approach);

            in Pathway usage, that should not matter too much, as all times in a batch of updates
            should be the same
        */
        ((2, (Some(1), Some(3))), 14, -1),
        ((3, (Some(2), Some(4))), 10, -1), // time(3)=10
        ((4, (Some(3), Some(5))), 11, -1), // time(4)=11
        ((5, (Some(4), Some(6))), 12, -1), // time(5)=12
        ((6, (Some(5), Some(7))), 13, -1), // time(6)=13
        ((7, (Some(6), Some(8))), 14, -1),
        ((8, (Some(7), Some(9))), 14, -1),
        ((2, (Some(1), Some(8))), 14, 1), // time(7) = 14
        ((8, (Some(2), Some(9))), 14, 1), //time(7) = 14
    ];

    run_test(vec![input, input2], vec![expected, expected2], &|_a, _b| {
        true
    });
}

//consecutive entries in the first batch, no unchanged entries on update
#[test]
fn test_prev_next_delete_02() {
    let input = vec![(1, 1, 1), (3, 1, 1), (5, 1, 1), (7, 1, 1)];
    let input2 = vec![(1, -1, 2), (8, 1, 2)];

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
    run_test(vec![input, input2], vec![expected, expected2], &|_a, _b| {
        true
    });
}

#[test]
fn test_prev_next_delete_after_insert_with_gap() {
    let input = vec![
        vec![
            (4, 1, 0),
            (21, 1, 0),
            (27, 1, 0),
            (31, 1, 0),
            (32, 1, 0),
            (45, 1, 0),
            (55, 1, 0),
        ],
        vec![
            (8, 1, 1),
            (25, 1, 1),
            (45, -1, 1),
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
    run_test(input, expected, &|_a, _b| true);
}

#[test]
fn test_three_batches() {
    let input = vec![
        vec![(15, 1, 0), (16, 1, 0), (-4, 1, 0)],
        vec![(10, 1, 1), (17, 1, 1), (2, 1, 1)],
        vec![(15, -1, 2), (-3, 1, 2)],
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
    run_test(input, expected, &|_a, _b| true);
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
                    input_current.push((elem, -1, t));
                } else {
                    input_current.push((elem, 1, t));
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
        run_test(inputs, expected, &|_a, _b| true);
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
                    input_current.push((elem, -1, t));
                } else {
                    input_current.push((elem, 1, t));
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
        run_test(inputs, expected, &|a, b| a.0 == b.0);
    }
}
