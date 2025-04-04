// Copyright © 2025 Pathway

use std::cell::RefCell;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::mem::take;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use differential_dataflow::input::InputSession;
use differential_dataflow::{AsCollection, Collection};
use timely::dataflow::operators::{Exchange, Inspect, Map};
use timely::progress::Timestamp as _;

use crate::connectors::adaptors::InputAdaptor;
use crate::connectors::data_format::Parser;
use crate::connectors::data_storage::ReaderBuilder;
use crate::connectors::{Connector, PersistenceMode, SnapshotAccess};
use crate::engine::graph::{SubscribeCallbacks, SubscribeConfig};
use crate::engine::{
    ColumnPath, Error, Key, OriginalOrRetraction, Result, TableHandle, TableProperties, Timestamp,
    Value,
};

use super::maybe_total::MaybeTotalScope;
use super::operators::output::ConsolidateForOutput;
use super::operators::{MapWrapped, MaybeTotal, Reshard};
use super::{DataflowGraphInner, MaybePersist, Table, Tuple};

/// `AsyncTransformer` allows for fully asynchronous computation on the python side.
/// Computation results are returned to the engine in a later (or equal) time
/// than the entry that triggered the computation.
///
/// To achieve full asynchronicity, `AsyncTransformer` utilizes
/// python output connector (subscribe) and python input connector.
/// The computation happens in two streams:
///
/// 1. stream to subscribe operator:
/// - output columns extraction (in subscribe)
/// - computation of final time in the input collection
///   (used to call `on_end` in subscribe when the input stream finishes)
/// - concatenation of the forgetting stream with input rows that have their computation finished
/// - persisting the stream. On restart used to produce rows for which `AsyncTransformer` didn't finish in the previous run.
/// - saving values of the input stream in `values_currently_processed` so that we can get input values when we get a result.
///   To get an appropriate input entry, `task_id` is generated. Every task has a different `task_id` (even rows with the same key).
///   In this step also forgetting entries are removed (`is_original` check).
/// - consolidation (in subscribe)
/// - sending data to python (in subscribe),
///   `on_frontier` is used to call `on_end` when the original stream finishes.
///   Without that, we would never finish as the forgetting stream would provide new time updates forever.
///   When we call `on_end`, the python connector finishes and that makes the forgetting stream finish.
///
/// 2. stream from python input connector
/// - special input session (`AsyncTransformerSession`) used to assign `seq_id` for each entry.
///   It is used to deduplicate values with the same (key, time) pair. The deduplication cannot
///   be done in the input session itself because we need all values to produce the forgetting stream.
/// - running `PythonConnector`/`TransparentParser` pair with this input session
/// - extraction of a true row key and enriching the stream with input values from `values_currently_processed`.
/// - creation of the forgetting stream using the key and input values retrieved earlier.
///   The time is moved to the next retraction time so that it is easy to filter out forgetting entries later.
/// - grouping of the stream as batches (`consolidate_for_output`)
/// - keeping only the last entry (with greatest `seq_id`) for each key in each batch.
///   Having more entries for a single (key, time) pair would result in inconsistency in upserting later.
///   The final produced entry for some (key, time) pairs could not be the one with the greatest `seq_id`.
/// - upsert operator with persistence. Upsert is there to avoid recomputation for deletions. Without upserting,
///   there could be problems with inconsistency as the `AsyncTransformer` can be non-deterministic.
///   Persistence is there to be able to update values computed in previous runs.

struct AsyncTransformerSession {
    input_session: InputSession<Timestamp, (Key, Value, i64), isize>,
    sequential_id: i64,
}

impl AsyncTransformerSession {
    #[allow(clippy::wrong_self_convention)] // consistent with other InputAdaptor implementors
    fn to_collection<S: MaybeTotalScope<MaybeTotalTimestamp = Timestamp>>(
        &mut self,
        scope: &mut S,
    ) -> Collection<S, (Key, Value, i64)> {
        self.input_session.to_collection(scope)
    }
}

impl InputAdaptor<Timestamp> for AsyncTransformerSession {
    /// The implementation below mostly reuses differetial dataflow's `InputSession` internals.
    ///
    /// It adds a sequential id for each entry so that it is possible later to
    /// deduplicate entries for a single (key, time) pair leaving only the last one.

    fn new() -> Self {
        AsyncTransformerSession {
            input_session: InputSession::new(),
            sequential_id: 1,
        }
    }

    fn flush(&mut self) {
        self.input_session.flush();
    }

    fn advance_to(&mut self, time: Timestamp) {
        if *self.time() < time {
            self.sequential_id = 0;
        }
        self.input_session.advance_to(time);
    }

    fn insert(&mut self, key: Key, value: Value) {
        self.input_session.insert((key, value, self.sequential_id));
        self.sequential_id += 1;
    }

    fn remove(&mut self, key: Key, value: Value) {
        self.input_session.remove((key, value, self.sequential_id));
        self.sequential_id += 1;
    }

    fn time(&self) -> &Timestamp {
        self.input_session.time()
    }
}

struct StreamCloseData {
    max_time: Timestamp,
    stream_closed: bool,
    on_end_called: bool,
}

impl StreamCloseData {
    fn new(max_time: Timestamp, stream_closed: bool, on_end_called: bool) -> Self {
        Self {
            max_time,
            stream_closed,
            on_end_called,
        }
    }
}

fn run_input_connector<S>(
    graph: &mut DataflowGraphInner<S>,
    reader: Box<dyn ReaderBuilder>,
    parser: Box<dyn Parser>,
    commit_duration: Option<Duration>,
    input_session: Box<dyn InputAdaptor<Timestamp>>,
) -> Result<()>
where
    S: MaybeTotalScope<MaybeTotalTimestamp = Timestamp>,
{
    let connector = Connector::new(
        commit_duration,
        parser.column_count(),
        graph.terminate_on_error,
        graph.create_error_logger()?.into(),
    );
    let state = connector.run(
        reader,
        parser,
        input_session,
        move |values, _offset| {
            let values = values.expect("key should be present");
            if let [Value::Pointer(key)] = values[..] {
                key
            } else {
                panic!("values should contain exactly one key")
            }
        },
        graph.output_probe.clone(),
        None,
        None,
        true,
        None,
        PersistenceMode::Batch, // default value from connector_table
        SnapshotAccess::Full,   // default value from connector_table
        graph.error_reporter.clone(),
        None,
    )?;

    graph.pollers.push(state.poller);
    graph.connector_threads.push(state.input_thread_handle);

    Ok(())
}

fn run_output_connector<S>(
    graph: &mut DataflowGraphInner<S>,
    table_handle: TableHandle,
    column_paths: Vec<ColumnPath>,
    python_input_values: &Collection<S, (Key, Tuple, Value, i64)>,
    mut callbacks: SubscribeCallbacks,
    values_currently_processed: Rc<RefCell<HashMap<Key, (Key, Tuple)>>>,
    skip_errors: bool,
) -> Result<()>
where
    S: MaybeTotalScope<MaybeTotalTimestamp = Timestamp>,
{
    let forgetting_stream = python_input_values
        .inner
        .map(move |((key, input_values, _values, _seq_id), time, diff)| {
            // move time to retraction time so that it can be filtered out easily later
            ((key, input_values), time.next_retraction_time(), diff)
        })
        .as_collection()
        .negate();

    // safe because it is created inside a timely worker and a single worker exectues all operators in a single thread
    let max_time_in_original_stream = Rc::new(RefCell::new(StreamCloseData::new(
        Timestamp::minimum(),
        false,
        false,
    )));

    let logic = {
        let max_time_in_original_stream = max_time_in_original_stream.clone();
        move |graph: &mut DataflowGraphInner<S>, collection: Collection<S, (Key, Tuple)>| {
            Ok(collection
                .inner
                .exchange(|_data| 0) // Move all data to worker 0 to compute max time present in data.
                // Currently async transformer is run only on worker 0 so it is not a bottleneck.
                // If parallelization is needed, count the number of entries in each worker
                // and only run ``on_end`` when all entries are sent.
                .inspect_core(move |data_or_frontier| match data_or_frontier {
                    // needed for a hack to run ``on_end`` at the end of the true input stream
                    Ok(data) => {
                        let mut max_time_in_original_stream =
                            max_time_in_original_stream.borrow_mut();
                        for (_key_val, timestamp, _diff) in data.1 {
                            max_time_in_original_stream.max_time =
                                std::cmp::max(*timestamp, max_time_in_original_stream.max_time);
                        }
                    }
                    Err(frontier) => {
                        if frontier.is_empty() {
                            max_time_in_original_stream.borrow_mut().stream_closed = true;
                        }
                    }
                })
                .as_collection()
                .concat(&forgetting_stream)
                .maybe_persist(graph, "async_transformer")? // distributes data between workers if persistence is enabled
                .reshard_to_first_worker() // if persistence is disabled, we need to reshard anyway
                .inner
                .flat_map(move |((key, tuple), time, diff)| {
                    // filter out retractions
                    // put new values in values_currently_processed so that they can be retracted later
                    if time.is_original() {
                        let task_id = Key::random();
                        values_currently_processed
                            .borrow_mut()
                            .insert(task_id, (key, tuple.clone()));
                        Some((
                            (key, tuple.with_appended(Value::Pointer(task_id))),
                            time,
                            diff,
                        ))
                    } else {
                        None
                    }
                })
                .as_collection())
        }
    };

    // hack to run ``on_end`` at the end of the true input stream (ignoring forgetting stream from input reader)
    let on_end = take(&mut callbacks.on_end);
    if let Some(mut on_end) = on_end {
        callbacks.on_frontier = Some(Box::new(move |timestamp| {
            let mut max_time_in_original_stream = max_time_in_original_stream.borrow_mut();
            if max_time_in_original_stream.stream_closed
                && max_time_in_original_stream.max_time < timestamp // strict comparison because in on_frontier timestamp is still on input
                && !max_time_in_original_stream.on_end_called
            {
                max_time_in_original_stream.on_end_called = true;
                on_end()?;
            }
            Ok(())
        }));
    }

    graph.subscribe_table(
        table_handle,
        column_paths,
        callbacks,
        SubscribeConfig {
            skip_persisted_batch: false,
            skip_errors,
            skip_pending: true,
        },
        None,
        None,
        logic,
    )
}

#[allow(clippy::too_many_arguments)]
pub fn async_transformer<S>(
    graph: &mut DataflowGraphInner<S>,
    table_handle: TableHandle,
    column_paths: Vec<ColumnPath>,
    callbacks: SubscribeCallbacks,
    reader: Box<dyn ReaderBuilder>,
    parser: Box<dyn Parser>,
    commit_duration: Option<Duration>,
    table_properties: Arc<TableProperties>,
    skip_errors: bool,
) -> Result<TableHandle>
where
    S: MaybeTotalScope<MaybeTotalTimestamp = Timestamp>,
{
    let mut input_session = AsyncTransformerSession::new();
    let python_input_values = input_session.to_collection(&mut graph.scope);

    if graph.scope.index() == 0 {
        run_input_connector(
            graph,
            reader,
            parser,
            commit_duration,
            Box::new(input_session),
        )?;
    }

    // safe because it is created inside a single timely worker and a single worker exectues all operators in a single thread
    let values_currently_processed: Rc<RefCell<HashMap<Key, (Key, Tuple)>>> =
        Rc::new(RefCell::new(HashMap::new()));

    let values = python_input_values
        .reshard_to_first_worker()
        .inner
        .map({
            let values_currently_processed = values_currently_processed.clone();
            move |((task_id, values, seq_id), time, diff)| {
                // get input values (they have to be the same to remove values from persisted input)
                let (input_key, input_values) = values_currently_processed
                    .borrow_mut()
                    .remove(&task_id)
                    .expect("task_id has to be present");

                ((input_key, input_values, values, seq_id), time, diff)
            }
        })
        .as_collection();

    run_output_connector(
        graph,
        table_handle,
        column_paths,
        &values,
        callbacks,
        values_currently_processed,
        skip_errors,
    )?;

    let table = graph
        .tables
        .get(table_handle)
        .ok_or(Error::InvalidTableHandle)?;

    let pending = MaybeTotal::distinct(
        &table
            .values()
            .clone()
            .maybe_persist(graph, "AsyncTransformer: Pending")?,
    )
    .filter_out_persisted(&mut graph.persistence_wrapper)?
    .map_named("AsyncTransformer: Pending", |(key, _values)| {
        (key, Tuple::Zero, Value::Pending, 0)
    });

    let output_values = values
        .concat(&pending)
        .consolidate_for_output(false)
        .flat_map(|batch| {
            let mut keep: HashMap<Key, (Value, isize, i64)> = HashMap::new();
            for ((key, _input_values, values, seq_id), diff) in batch.data {
                match keep.entry(key) {
                    Entry::Occupied(mut entry) => {
                        let (_, _, entry_seq_id) = entry.get();
                        if seq_id > *entry_seq_id {
                            entry.insert((values, diff, seq_id));
                        }
                    }
                    Entry::Vacant(entry) => {
                        entry.insert((values, diff, seq_id));
                    }
                }
            }
            keep.into_iter()
                .map(move |(key, (value, diff, _seq_id))| ((key, value), batch.time, diff))
        })
        .as_collection();

    let output_values_maybe_persisted = graph.maybe_persisted_upsert_collection(&output_values)?;
    Ok(graph.tables.alloc(
        Table::from_collection(output_values_maybe_persisted).with_properties(table_properties),
    ))
}
