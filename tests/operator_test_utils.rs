#![allow(dead_code)]
use differential_dataflow::input::InputSession;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::{Arranged, TraceAgent};
use differential_dataflow::trace::implementations::ord::OrdValBatch;
use differential_dataflow::trace::implementations::spine_fueled::Spine;
use differential_dataflow::trace::TraceReader;
use differential_dataflow::{Collection, ExchangeData, Hashable};
use itertools::zip_eq;
use pathway_engine::engine::dataflow::operators::time_column::SelfCompactionTime;
use std::cmp::max;
use std::rc::Rc;
use std::sync::{Arc, Mutex};
use timely::communication::allocator::Generic;
use timely::dataflow::operators::capture::capture::Capture;
use timely::dataflow::operators::capture::{EventLinkCore, Extract, Replay};
use timely::dataflow::scopes::Child;
use timely::progress::timestamp::Refines;
use timely::progress::Timestamp;
use timely::worker::Worker;
use timely::Config;

type R = i32;
type OutputBatch<K, V, T> = Vec<((K, V), T, R)>;

pub trait Incrementable {
    fn incremented(&self) -> Self;
}

impl Incrementable for i32 {
    fn incremented(&self) -> i32 {
        *self + 1
    }
}

impl Incrementable for SelfCompactionTime<i32> {
    fn incremented(&self) -> SelfCompactionTime<i32> {
        SelfCompactionTime::original(
            <SelfCompactionTime<i32> as Refines<i32>>::to_outer(self.clone()) + 1,
        )
    }
}

pub fn run_test<
    D: ExchangeData + Hashable + Clone,
    K2: ExchangeData + Hashable + Clone,
    V2: ExchangeData + Hashable + Clone,
    T: Timestamp
        + Lattice
        + timely::order::TotalOrder
        + timely::progress::timestamp::Refines<()>
        + Incrementable,
>(
    input_batches: Vec<Vec<(D, T, R)>>,
    expected_output_batches: Vec<OutputBatch<K2, V2, T>>,
    op: impl Fn(
            Collection<Child<'_, Worker<Generic>, T>, D, R>,
        )
            -> Arranged<Child<Worker<Generic>, T>, TraceAgent<Spine<Rc<OrdValBatch<K2, V2, T, R>>>>>
        + std::marker::Send
        + std::marker::Sync
        + 'static,
) {
    let (send, recv) = ::std::sync::mpsc::channel();
    let send = Arc::new(Mutex::new(send));

    timely::execute(Config::thread(), move |worker: &mut Worker<Generic>| {
        // this is only to validate the output.
        let send = send.lock().unwrap().clone();
        // these are to capture/replay the stream.
        let handle1 = Rc::new(EventLinkCore::<T, OutputBatch<K2, V2, T>>::new());
        let handle2 = Some(handle1.clone());
        // create an input collection of data.
        let mut input: InputSession<T, D, R> = InputSession::new();

        // define a new computation.
        let probe = worker.dataflow(
            |scope: &mut timely::dataflow::scopes::Child<
                timely::worker::Worker<timely::communication::Allocator>,
                T,
            >| {
                // create a new collection from our input.
                let collection = input.to_collection(scope);
                let res = op(collection);
                res.trace
                    .map_batches(|batch| eprintln!("outer debug, res map batch{:?}", batch));

                res.as_collection(|key, val| (key.clone(), val.clone()))
                    .inner
                    .capture_into(handle1);
                res.as_collection(|key, val| (key.clone(), val.clone()))
                    .probe()
            },
        );

        worker.dataflow(|scope2| handle2.replay_into(scope2).capture_into(send));

        input.advance_to(T::minimum());
        let mut t = T::minimum();
        for batch in input_batches.iter() {
            for (data, time, change) in batch.iter() {
                input.advance_to(time.clone());
                input.update(data.clone(), *change);
                eprintln!("time {:?} element {:?} change {:?}", *time, *data, *change);
                t = max(time.clone(), t);
            }
            input.advance_to(t.incremented());
            eprintln!("{:?} {:?}", t, t.incremented());
            input.flush();
            worker.step_while(|| probe.less_than(input.time()))
        }
        worker.step();
    })
    .expect("Computation terminated abnormally");

    let to_print: Vec<(T, OutputBatch<K2, V2, T>)> = recv.extract();
    eprintln!("results: {:?}", to_print);
    eprintln!("expected: {:?}", expected_output_batches);
    assert!(to_print.len() == expected_output_batches.len());

    for (i, (mut returned, mut expected)) in zip_eq(to_print, expected_output_batches).enumerate() {
        returned.1.sort();
        expected.sort();
        eprintln!("results to compare for idx {i:?}");
        eprintln!("{:?}", returned.1);
        eprintln!("{:?}", expected);
        assert!(returned.1.eq(&expected));
    }
}
