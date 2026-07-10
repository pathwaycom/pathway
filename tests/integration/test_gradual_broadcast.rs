// Copyright © 2026 Pathway

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use differential_dataflow::input::InputSession;
use ordered_float::OrderedFloat;
use timely::dataflow::operators::Inspect;

use pathway_engine::engine::dataflow::operators::gradual_broadcast::GradualBroadcast;
use pathway_engine::engine::{Key, KeyImpl};

type Float = OrderedFloat<f64>;
type Triplet = (Float, Float, Float);
type OutputRecord = (Key, (i64, Float));
type CapturedStream = Arc<Mutex<Vec<(OutputRecord, u64, isize)>>>;

fn triplet(lower: f64, value: f64, upper: f64) -> Triplet {
    (
        OrderedFloat(lower),
        OrderedFloat(value),
        OrderedFloat(upper),
    )
}

// Emulates pathway.internals.api.squash_updates: replays the (consolidated)
// stream in time order and verifies it is temporally consistent — every
// retraction must match an insertion that happened at an earlier time (or at
// the same time, after consolidation). Returns the final state.
fn squash_updates(captured: &CapturedStream) -> HashMap<Key, (i64, Float)> {
    let mut consolidated: HashMap<(OutputRecord, u64), isize> = HashMap::new();
    for (record, time, diff) in captured.lock().unwrap().iter() {
        *consolidated.entry((*record, *time)).or_insert(0) += diff;
    }
    let mut events: Vec<(u64, isize, OutputRecord)> = consolidated
        .into_iter()
        .filter(|(_record_time, diff)| *diff != 0)
        .map(|((record, time), diff)| (time, diff, record))
        .collect();
    events.sort();

    let mut state: HashMap<Key, (i64, Float)> = HashMap::new();
    for (time, diff, (key, value)) in events {
        match diff {
            1 => assert!(
                state.insert(key, value).is_none(),
                "duplicated entries for key {key:?} at time {time}"
            ),
            -1 => assert_eq!(
                state.remove(&key),
                Some(value),
                "retraction not matching an earlier insertion for key {key:?} at time {time}"
            ),
            _ => panic!("invalid diff {diff} for key {key:?} at time {time}"),
        }
    }
    state
}

#[test]
fn test_threshold_update_arriving_after_newer_input_rows_keeps_stream_consistent() {
    // The threshold table and the broadcast target are fed through independent
    // dataflow paths, so a threshold update with time T may reach the operator
    // after input rows with times greater than T were already processed. The
    // output stream must stay temporally consistent in that case.
    let key_1 = Key(KeyImpl::from(1u32));
    let key_2 = Key(KeyImpl::from(2u32));
    let threshold_key = Key(KeyImpl::from(42u32));

    let captured: CapturedStream = Arc::new(Mutex::new(Vec::new()));
    let captured_in_worker = captured.clone();

    timely::execute_directly(move |worker| {
        let mut tab_input: InputSession<u64, (Key, i64), isize> = InputSession::new();
        let mut threshold_input: InputSession<u64, (Key, Triplet), isize> = InputSession::new();

        worker.dataflow(|scope| {
            let tab = tab_input.to_collection(scope);
            let threshold = threshold_input.to_collection(scope);
            tab.gradual_broadcast(&threshold).inner.inspect({
                let captured = captured_in_worker.clone();
                move |(data, time, diff)| {
                    captured.lock().unwrap().push((*data, *time, *diff));
                }
            });
        });

        // Threshold A (value == lower) at time 0: every key gets 10.5.
        threshold_input.update_at((threshold_key, triplet(10.5, 10.5, 20.5)), 0, 1);
        threshold_input.advance_to(1);
        threshold_input.flush();
        for _ in 0..10 {
            worker.step();
        }

        // A row arrives at time 0 and gets broadcast the current threshold.
        tab_input.update_at((key_1, 1), 0, 1);
        tab_input.advance_to(1);
        tab_input.flush();
        for _ in 0..10 {
            worker.step();
        }

        // Another row arrives at time 10 and is fully processed while the
        // threshold input is still lagging at time 1.
        tab_input.update_at((key_2, 2), 10, 1);
        tab_input.advance_to(11);
        tab_input.flush();
        for _ in 0..10 {
            worker.step();
        }

        // Only now the threshold update with time 5 (a time smaller than the
        // already processed input rows) reaches the operator. Threshold B
        // (value == upper): every key gets 20.5.
        threshold_input.update_at((threshold_key, triplet(10.5, 10.5, 20.5)), 5, -1);
        threshold_input.update_at((threshold_key, triplet(10.5, 20.5, 20.5)), 5, 1);
        threshold_input.advance_to(6);
        threshold_input.flush();
        for _ in 0..10 {
            worker.step();
        }
    });

    let final_state = squash_updates(&captured);
    let expected: HashMap<Key, (i64, Float)> = HashMap::from([
        (key_1, (1, OrderedFloat(20.5))),
        (key_2, (2, OrderedFloat(20.5))),
    ]);
    assert_eq!(final_state, expected);
}
