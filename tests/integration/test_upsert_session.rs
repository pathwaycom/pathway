// Copyright © 2024 Pathway

use super::helpers::get_entries_in_receiver;

use std::sync::{mpsc, Arc, Mutex};

use timely::dataflow::operators::Inspect;

use pathway_engine::connectors::adaptors::{InputAdaptor, UpsertSession};
use pathway_engine::engine::dataflow::operators::output::ConsolidateForOutput;
use pathway_engine::engine::{Key, Value};

#[test]
fn test_upsert_session_replacement() {
    let k1 = Key::random();
    let k2 = Key::random();

    let (sender, receiver) = mpsc::channel();
    let sender = Arc::new(Mutex::new(sender));
    timely::execute_from_args(std::env::args(), move |worker| {
        let mut input = UpsertSession::new();
        worker.dataflow(
            |scope: &mut timely::dataflow::scopes::Child<
                timely::worker::Worker<timely::communication::Allocator>,
                u64,
            >| {
                let sender = sender.lock().unwrap().clone();
                let table = input.to_collection(scope);
                table.consolidate_for_output(true).inspect(move |batch| {
                    for (data, diff) in &batch.data {
                        sender
                            .send((data.clone(), batch.time, *diff))
                            .expect("inspected entry sending failed");
                    }
                });
            },
        );
        input.upsert(k1, Some(Value::from("one")));
        input.advance_to(123);
        input.upsert(k2, Some(Value::from("two")));
        input.advance_to(246);
        input.upsert(k1, Some(Value::from("three")));
        input.advance_to(369);
    })
    .expect("Computation terminated abnormally");

    assert_eq!(
        get_entries_in_receiver(receiver),
        vec![
            ((k1, Value::from("one")), 0, 1),
            ((k2, Value::from("two")), 123, 1),
            ((k1, Value::from("one")), 246, -1),
            ((k1, Value::from("three")), 246, 1),
        ]
    );
}

#[test]
fn test_removal_by_key() {
    let k1 = Key::random();
    let k2 = Key::random();

    let (sender, receiver) = mpsc::channel();
    let sender = Arc::new(Mutex::new(sender));
    timely::execute_from_args(std::env::args(), move |worker| {
        let mut input = UpsertSession::new();
        worker.dataflow(
            |scope: &mut timely::dataflow::scopes::Child<
                timely::worker::Worker<timely::communication::Allocator>,
                u64,
            >| {
                let sender = sender.lock().unwrap().clone();
                let table = input.to_collection(scope);
                table.consolidate_for_output(true).inspect(move |batch| {
                    for (data, diff) in &batch.data {
                        sender
                            .send((data.clone(), batch.time, *diff))
                            .expect("inspected entry sending failed");
                    }
                });
            },
        );
        input.upsert(k1, Some(Value::from("one")));
        input.advance_to(123);
        input.upsert(k2, Some(Value::from("two")));
        input.advance_to(246);
        input.upsert(k1, None);
        input.advance_to(369);
    })
    .expect("Computation terminated abnormally");

    assert_eq!(
        get_entries_in_receiver(receiver),
        vec![
            ((k1, Value::from("one")), 0, 1),
            ((k2, Value::from("two")), 123, 1),
            ((k1, Value::from("one")), 246, -1),
        ]
    );
}
