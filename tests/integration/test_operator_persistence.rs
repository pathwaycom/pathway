// Copyright © 2024 Pathway

use bincode::{deserialize, serialize};
use differential_dataflow::difference::Abelian;
use differential_dataflow::input::InputSession;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::{ExchangeData, Hashable};
use futures::channel::oneshot;
use mockall::mock;
use mockall::predicate::eq;
use pathway_engine::engine::dataflow::persist::Persist;
use pathway_engine::engine::dataflow::shard::Shard;
use pathway_engine::engine::{Timestamp, TotalFrontier};
use pathway_engine::persistence::backends::{
    BackendPutFuture, Error as BackendError, PersistenceBackend,
};
use pathway_engine::persistence::operator_snapshot::{
    ConcreteSnapshotReader, ConcreteSnapshotWriter, MultiConcreteSnapshotReader,
    OperatorSnapshotReader, OperatorSnapshotWriter,
};
use pathway_engine::persistence::PersistenceTime;

use std::fmt::Debug;
use std::ops::ControlFlow;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, SystemTime};
use timely::communication::allocator::Generic;
use timely::order::TotalOrder;
use timely::worker::Worker;
use timely::Config;

use itertools::Itertools;

struct MockSnapshotReader<D, R> {
    data: Vec<(D, R)>,
}

impl<D, R> MockSnapshotReader<D, R> {
    fn new(data: Vec<(D, R)>) -> Self {
        Self { data }
    }
}

impl<D, R> OperatorSnapshotReader<D, R> for MockSnapshotReader<D, R>
where
    D: Clone,
    R: Clone,
{
    fn load_persisted(&mut self) -> Result<Vec<(D, R)>, BackendError> {
        Ok(self.data.clone())
    }
}

struct MockSnapshotWriter<D, T, R> {
    buffer: Vec<(D, R)>,
    expected_data: Vec<(T, Vec<(D, R)>)>,
    index: usize,
}

impl<D, T, R> MockSnapshotWriter<D, T, R> {
    fn new(expected_data: Vec<(T, Vec<(D, R)>)>) -> Self {
        Self {
            buffer: Vec::new(),
            expected_data,
            index: 0,
        }
    }
}

impl<T, D, R> OperatorSnapshotWriter<T, D, R> for MockSnapshotWriter<D, T, R>
where
    T: Debug + Ord,
    D: Debug + Clone + Ord,
    R: Debug + Clone + Ord,
{
    fn persist(&mut self, time: T, mut data: Vec<(D, R)>) {
        let (expected_time, expected_data) = &self.expected_data[self.index];
        let mut expected_data = expected_data.clone();
        self.index += 1;
        assert_eq!(time, *expected_time);
        data.sort();
        expected_data.sort();
        assert_eq!(data, expected_data);
    }
    fn persist_single(&mut self, data: (D, R)) {
        self.buffer.push(data);
    }
    fn close_time(&mut self, time: T) {
        let data = std::mem::take(&mut self.buffer);
        self.persist(time, data);
    }
    fn flush(&mut self, _time: TotalFrontier<T>) -> Vec<BackendPutFuture> {
        Vec::new()
    }
}

impl<D, T, R> Drop for MockSnapshotWriter<D, T, R> {
    fn drop(&mut self) {
        assert_eq!(self.index, self.expected_data.len());
    }
}

fn run_test<D, T, R>(
    input_data: Vec<Vec<(D, T, R)>>,
    peristed_data: Vec<(D, R)>,
    expected_output: Vec<(D, T, R)>,
    expected_persisted: Vec<(T, Vec<(D, R)>)>,
) where
    T: timely::progress::Timestamp
        + TotalOrder
        + timely::progress::timestamp::Refines<()>
        + PersistenceTime
        + Lattice,
    D: ExchangeData + Ord + Shard + Hashable,
    R: ExchangeData + Abelian,
{
    let guards = timely::execute(Config::thread(), move |worker: &mut Worker<Generic>| {
        let snapshot_writer = MockSnapshotWriter::new(expected_persisted.clone());
        let snapshot_reader = MockSnapshotReader::new(peristed_data.clone());
        let mut input: InputSession<T, D, R> = InputSession::new();
        let mut expected_output_session: InputSession<T, D, R> = InputSession::new();
        let (probe, mut poller, thread_handle) = worker.dataflow(|scope| {
            let collection = input.to_collection(scope);
            let (persisted, poller, thread_handle) = collection.persist(
                Box::new(snapshot_reader),
                Arc::new(Mutex::new(snapshot_writer)),
            );
            let probe = persisted
                .concat(&expected_output_session.to_collection(scope).negate())
                .consolidate()
                .inspect(move |entry| {
                    panic!("Entry {entry:?} not matched");
                })
                .probe();
            (probe, poller, thread_handle)
        });

        while let ControlFlow::Continue(time) = poller() {
            let d = time.map_or(Duration::ZERO, |time| {
                time.duration_since(SystemTime::now())
                    .unwrap_or(Duration::ZERO)
            });
            thread::sleep(d);
        }
        drop(poller);
        input.advance_to(T::minimum());
        for (data, time, diff) in &expected_output {
            expected_output_session.update_at(data.clone(), time.clone(), diff.clone());
        }
        drop(expected_output_session);
        for batch in &input_data {
            let min_time = batch
                .iter()
                .map(|(_data, time, _change)| time)
                .min()
                .expect("vectors with data shouldn't be empty")
                .clone();

            input.advance_to(min_time);
            input.flush();
            worker.step_while(|| probe.less_than(input.time()));

            for (data, time, diff) in batch {
                input.update_at(data.clone(), time.clone(), diff.clone());
            }
        }
        thread_handle
            .join()
            .expect("persistence read thread should finish gracefully");
    })
    .expect("computation should finish gracefully");

    guards
        .join()
        .into_iter()
        .try_collect()
        .expect("computation should finish gracefully")
}

#[test]
fn test_ordered() {
    let input_data = vec![
        vec![
            (1, Timestamp(2), 1),
            (2, Timestamp(2), 1),
            (3, Timestamp(2), 1),
        ],
        vec![
            (1, Timestamp(3), 1),
            (2, Timestamp(3), 1),
            (3, Timestamp(3), 1),
        ],
        vec![(5, Timestamp(5), 1), (6, Timestamp(5), 1)],
    ];
    let persisted_data = vec![(1, 1), (2, 1), (3, 3), (4, 1)];
    let expected_persisted = vec![
        (Timestamp(2), vec![(1, 1), (2, 1), (3, 1)]),
        (Timestamp(3), vec![(1, 1), (2, 1), (3, 1)]),
        (Timestamp(5), vec![(5, 1), (6, 1)]),
    ];
    let expected_output = vec![
        (1, Timestamp(0), 1),
        (2, Timestamp(0), 1),
        (3, Timestamp(0), 3),
        (4, Timestamp(0), 1),
        (1, Timestamp(2), 1),
        (2, Timestamp(2), 1),
        (3, Timestamp(2), 1),
        (1, Timestamp(3), 1),
        (2, Timestamp(3), 1),
        (3, Timestamp(3), 1),
        (5, Timestamp(5), 1),
        (6, Timestamp(5), 1),
    ];
    run_test(
        input_data,
        persisted_data,
        expected_output,
        expected_persisted,
    );
}

#[test]
fn test_unordered() {
    let input_data = vec![
        vec![
            (1, Timestamp(2), 1),
            (2, Timestamp(4), 1),
            (3, Timestamp(2), 1),
        ],
        vec![
            (1, Timestamp(3), 1),
            (2, Timestamp(4), 1),
            (3, Timestamp(3), 1),
        ],
        vec![(5, Timestamp(4), 1), (6, Timestamp(5), 1)],
    ];
    let persisted_data = vec![(1, 1), (2, 1), (3, 3), (4, 1)];
    let expected_persisted = vec![
        (Timestamp(2), vec![(1, 1), (3, 1)]),
        (Timestamp(3), vec![(1, 1), (3, 1)]),
        (Timestamp(4), vec![(2, 1), (2, 1), (5, 1)]),
        (Timestamp(5), vec![(6, 1)]),
    ];
    let expected_output = vec![
        (1, Timestamp(0), 1),
        (2, Timestamp(0), 1),
        (3, Timestamp(0), 3),
        (4, Timestamp(0), 1),
        (1, Timestamp(2), 1),
        (2, Timestamp(4), 1),
        (3, Timestamp(2), 1),
        (1, Timestamp(3), 1),
        (2, Timestamp(4), 1),
        (3, Timestamp(3), 1),
        (5, Timestamp(4), 1),
        (6, Timestamp(5), 1),
    ];
    run_test(
        input_data,
        persisted_data,
        expected_output,
        expected_persisted,
    );
}

#[test]
fn test_empty_input() {
    let input_data = vec![];
    let persisted_data = vec![(1, 1), (2, 1), (3, 3), (4, 1)];
    let expected_persisted = vec![];
    let expected_output = vec![
        (1, Timestamp(0), 1),
        (2, Timestamp(0), 1),
        (3, Timestamp(0), 3),
        (4, Timestamp(0), 1),
    ];
    run_test(
        input_data,
        persisted_data,
        expected_output,
        expected_persisted,
    );
}

#[test]
fn test_nothing_persisted_before() {
    let input_data = vec![
        vec![
            (1, Timestamp(2), 1),
            (2, Timestamp(5), 1),
            (3, Timestamp(2), 1),
        ],
        vec![
            (1, Timestamp(3), 1),
            (2, Timestamp(4), 1),
            (3, Timestamp(3), 1),
        ],
        vec![(5, Timestamp(4), 1), (6, Timestamp(5), 1)],
    ];
    let persisted_data = vec![];
    let expected_persisted = vec![
        (Timestamp(2), vec![(1, 1), (3, 1)]),
        (Timestamp(3), vec![(1, 1), (3, 1)]),
        (Timestamp(4), vec![(2, 1), (5, 1)]),
        (Timestamp(5), vec![(2, 1), (6, 1)]),
    ];
    let expected_output = vec![
        (1, Timestamp(2), 1),
        (2, Timestamp(5), 1),
        (3, Timestamp(2), 1),
        (1, Timestamp(3), 1),
        (2, Timestamp(4), 1),
        (3, Timestamp(3), 1),
        (5, Timestamp(4), 1),
        (6, Timestamp(5), 1),
    ];
    run_test(
        input_data,
        persisted_data,
        expected_output,
        expected_persisted,
    );
}

#[test]
fn test_unordered_2() {
    let input_data = vec![
        vec![
            (1, Timestamp(2), 1),
            (2, Timestamp(4), 1),
            (3, Timestamp(6), 1),
        ],
        vec![
            (4, Timestamp(3), 1),
            (5, Timestamp(5), 1),
            (6, Timestamp(8), 1),
        ],
        vec![(5, Timestamp(4), 1), (6, Timestamp(5), 1)],
    ];
    let persisted_data = vec![(1, 1), (2, 1), (3, 3), (4, 1)];
    let expected_persisted = vec![
        (Timestamp(2), vec![(1, 1)]),
        (Timestamp(3), vec![(4, 1)]),
        (Timestamp(4), vec![(2, 1), (5, 1)]),
        (Timestamp(5), vec![(5, 1), (6, 1)]),
        (Timestamp(6), vec![(3, 1)]),
        (Timestamp(8), vec![(6, 1)]),
    ];
    let expected_output = vec![
        (1, Timestamp(0), 1),
        (2, Timestamp(0), 1),
        (3, Timestamp(0), 3),
        (4, Timestamp(0), 1),
        (1, Timestamp(2), 1),
        (2, Timestamp(4), 1),
        (3, Timestamp(6), 1),
        (4, Timestamp(3), 1),
        (5, Timestamp(5), 1),
        (6, Timestamp(8), 1),
        (5, Timestamp(4), 1),
        (6, Timestamp(5), 1),
    ];
    run_test(
        input_data,
        persisted_data,
        expected_output,
        expected_persisted,
    );
}

mock! {
    #[derive(Debug)]
    Backend {}
    impl PersistenceBackend for Backend {
        fn list_keys(&self) -> Result<Vec<String>, BackendError>;
        fn get_value(&self, key: &str) -> Result<Vec<u8>, BackendError>;
        fn put_value(&mut self, key: &str, value: Vec<u8>) -> BackendPutFuture;
        fn remove_key(&mut self, key: &str) -> Result<(), BackendError>;
    }
}

#[test]
fn test_operator_snapshot_reader_reads_correct_files_1() {
    let mut backend = MockBackend::new();
    backend.expect_list_keys().times(1).returning(|| {
        Ok(vec![
            "0-37-2".to_string(),
            "0-34-3".to_string(),
            "0-33-3".to_string(),
            "0-32-1".to_string(),
            "0-31-2".to_string(),
            "0-27-2".to_string(),
            "0-26-3".to_string(),
            "0-23-4".to_string(),
            "0-22-5".to_string(),
            "3-27-5".to_string(),
            "4-23-9".to_string(),
            "4-13-10".to_string(),
            "6-20-82".to_string(),
            "7-12-130".to_string(),
            "8-18-280".to_string(),
        ])
    });
    backend
        .expect_get_value()
        .times(7)
        .returning(|_key| serialize(&vec![(3, 1)]).map_err(|e| BackendError::Bincode(*e)));
    let keys_to_remove = vec![
        "0-37-2", "0-34-3", "0-27-2", "0-26-3", "0-23-4", "0-22-5", "4-13-10", "7-12-130",
    ];
    for key in keys_to_remove {
        backend
            .expect_remove_key()
            .with(eq(key))
            .times(1)
            .returning(|_key| Ok(()));
    }
    let mut reader = MultiConcreteSnapshotReader::new(vec![ConcreteSnapshotReader::new(
        Box::new(backend),
        TotalFrontier::At(Timestamp(34)),
    )]);
    assert_eq!(reader.load_persisted().unwrap(), vec![(3, 7)]);
}

#[test]
fn test_operator_snapshot_reader_consolidates() {
    let mut backend = MockBackend::new();
    backend.expect_list_keys().times(1).returning(|| {
        Ok(vec![
            "0-23-1".to_string(),
            "0-22-2".to_string(),
            "0-21-1".to_string(),
            "0-20-2".to_string(),
            "0-16-2".to_string(),
            "2-18-3".to_string(),
            "2-12-4".to_string(),
            "3-14-5".to_string(),
        ])
    });
    backend
        .expect_get_value()
        .with(eq("0-21-1"))
        .returning(|_key| serialize(&vec![((2, 4), 1)]).map_err(|e| BackendError::Bincode(*e)));
    backend
        .expect_get_value()
        .with(eq("0-20-2"))
        .returning(|_key| {
            serialize(&vec![((3, 5), 1), ((3, 3), -1)]).map_err(|e| BackendError::Bincode(*e))
        });
    backend
        .expect_get_value()
        .with(eq("2-18-3"))
        .returning(|_key| {
            serialize(&vec![((2, 1), -1), ((1, 10), 1), ((8, 1), 1)])
                .map_err(|e| BackendError::Bincode(*e))
        });
    backend
        .expect_get_value()
        .with(eq("3-14-5"))
        .returning(|_key| {
            serialize(&vec![
                ((2, 1), 1),
                ((3, 3), 1),
                ((7, 2), 1),
                ((6, 3), 1),
                ((5, 1), 1),
            ])
            .map_err(|e| BackendError::Bincode(*e))
        });
    let keys_to_remove = vec!["0-23-1", "0-22-2", "0-16-2", "2-12-4"];
    for key in keys_to_remove {
        backend
            .expect_remove_key()
            .with(eq(key))
            .times(1)
            .returning(|_key| Ok(()));
    }
    let mut reader = MultiConcreteSnapshotReader::new(vec![ConcreteSnapshotReader::new(
        Box::new(backend),
        TotalFrontier::At(Timestamp(22)),
    )]);
    let mut result = reader.load_persisted().unwrap();
    result.sort();
    assert_eq!(
        result,
        vec![
            ((1, 10), 1),
            ((2, 4), 1),
            ((3, 5), 1),
            ((5, 1), 1),
            ((6, 3), 1),
            ((7, 2), 1),
            ((8, 1), 1),
        ]
    );
}

#[test]
fn test_operator_snapshot_writer() {
    let mut backend = MockBackend::new();
    backend
        .expect_put_value()
        .times(2)
        .withf(|key, data| {
            let mut v: Vec<(i64, isize)> = deserialize(data).unwrap();
            v.sort();
            if key == "0-1700-3" {
                assert_eq!(v, vec![(2, 1), (3, 3), (4, 1)]);
                true
            } else if key == "0-2900-2" {
                assert_eq!(v, vec![(1, 1), (2, 3)]);
                true
            } else {
                panic!("key {key} shouldn't get created")
            }
        })
        .returning(|_key, _data| {
            let (sender, receiver) = oneshot::channel();
            sender.send(Ok(())).unwrap();
            receiver
        });
    let mut writer: ConcreteSnapshotWriter<i64, isize> =
        ConcreteSnapshotWriter::new(Box::new(backend), Duration::from_millis(1000));
    writer.persist(Timestamp(1200), vec![(2, 1), (3, 1)]);
    writer.persist(Timestamp(1700), vec![(4, 1), (3, 2)]);
    writer.persist(Timestamp(2100), vec![(1, 1), (2, 2)]);
    writer.persist(Timestamp(2900), vec![(2, 1)]);
    writer.persist(Timestamp(3000), vec![(5, 1), (4, 2)]);
    writer.persist(Timestamp(3200), vec![(1, 1)]);
    let futures = writer.flush(TotalFrontier::At(Timestamp(3200)));
    futures::executor::block_on(futures::future::try_join_all(futures)).unwrap();
}
