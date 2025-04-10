// Copyright © 2024 Pathway

use adaptors::InputAdaptor;
use differential_dataflow::input::InputSession;
use itertools::Itertools;
use log::{error, info, warn};
use scopeguard::guard;
use std::cell::RefCell;
use std::env;
use std::mem::take;
use std::ops::ControlFlow;
use std::rc::Rc;
use std::sync::mpsc::{self, Sender, TryRecvError};
use std::sync::{Arc, Mutex};
use std::thread;
use std::thread::Thread;
use std::time::{Duration, SystemTime};
use timely::dataflow::operators::probe::Handle;

pub mod adaptors;
pub mod data_format;
pub mod data_lake;
pub mod data_storage;
pub mod data_tokenize;
pub mod metadata;
pub mod monitoring;
pub mod offset;
pub mod posix_like;
pub mod scanner;
pub mod synchronization;

use crate::connectors::monitoring::ConnectorMonitor;
use crate::engine::error::{DynError, Trace};
use crate::engine::report_error::{
    LogError, ReportError, SpawnWithReporter, UnwrapWithErrorLogger,
};
use crate::engine::{DataError, Key, Value};

use crate::connectors::synchronization::ConnectorGroupAccessor;
use crate::engine::Error as EngineError;
use crate::engine::Timestamp;
use crate::persistence::config::ReadersQueryPurpose;
use crate::persistence::frontier::OffsetAntichain;
use crate::persistence::input_snapshot::{Event as SnapshotEvent, SnapshotMode};
use crate::persistence::tracker::{RequiredPersistenceMode, WorkerPersistentStorage};
use crate::persistence::{PersistentId, SharedSnapshotWriter, UniqueName};

use data_format::{ParseError, ParseResult, ParsedEvent, ParsedEventWithErrors, Parser};
use data_storage::{
    DataEventType, ReadError, ReadResult, Reader, ReaderBuilder, ReaderContext, WriteError, Writer,
};

pub use adaptors::SessionType;
pub use data_storage::StorageType;
pub use offset::{Offset, OffsetKey, OffsetValue};

/*
    Below is the custom reader stuff.
    In most cases, the input can be separated into raw data reads and parsing.
    The custom reader is needed, when this is not the case, for example, there is a
    library, already returning some tuples of ints (or whatever, which is not a kind
    of random data).
*/
pub trait CustomReader {
    fn acquire_custom_data(&mut self) -> (ParseResult, Option<Offset>);
}

/*
    Below is the connector stuff.
    Connector is a constructor, taking the reader, the parser, the input session and proving the parsed data to the input session for the
    data source defined by reader.
*/

pub struct StartedConnectorState {
    pub poller: Box<dyn FnMut() -> ControlFlow<(), Option<SystemTime>>>,
    pub input_thread_handle: std::thread::JoinHandle<()>,
    pub connector_monitor: Rc<RefCell<ConnectorMonitor>>,
}

impl StartedConnectorState {
    pub fn new(
        poller: Box<dyn FnMut() -> ControlFlow<(), Option<SystemTime>>>,
        input_thread_handle: std::thread::JoinHandle<()>,
        connector_monitor: Rc<RefCell<ConnectorMonitor>>,
    ) -> Self {
        Self {
            poller,
            input_thread_handle,
            connector_monitor,
        }
    }
}

const MAX_PARSE_ERRORS_IN_LOG: usize = 128;

pub struct Connector {
    commit_duration: Option<Duration>,
    current_timestamp: Timestamp,
    num_columns: usize,
    current_frontier: OffsetAntichain,
    skip_all_errors: bool,
    error_logger: Rc<dyn LogError>,
    n_parse_attempts: usize,
    n_parse_errors_in_log: usize,
}

#[derive(Debug)]
pub enum Entry {
    Snapshot(SnapshotEvent),
    RewindFinishSentinel(OffsetAntichain),
    RealtimeEntries(Vec<ParsedEventWithErrors>, Offset),
    RealtimeEvent(ReadResult),
    RealtimeParsingError(DynError),
}

#[derive(Debug, Clone, Copy)]
pub enum PersistenceMode {
    RealtimeReplay,
    SpeedrunReplay,
    Batch,
    Persisting,
    SelectivePersisting,
    UdfCaching,
    OperatorPersisting,
}

impl PersistenceMode {
    fn on_before_reading_snapshot(self, sender: &Sender<Entry>) {
        // In case of Batch replay we need to start with AdvanceTime to set a new timestamp
        if matches!(self, PersistenceMode::Batch) {
            let timestamp = Timestamp::new_from_current_time();
            let send_res = sender.send(Entry::Snapshot(SnapshotEvent::AdvanceTime(
                timestamp,
                OffsetAntichain::new(),
            )));
            if let Err(e) = send_res {
                panic!("Failed to initialize time for batch replay: {e}");
            }
        }
    }

    fn handle_snapshot_time_advancement(self, sender: &Sender<Entry>, entry_read: SnapshotEvent) {
        match self {
            PersistenceMode::Batch
            | PersistenceMode::Persisting
            | PersistenceMode::SelectivePersisting
            | PersistenceMode::UdfCaching
            | PersistenceMode::OperatorPersisting => {}
            PersistenceMode::SpeedrunReplay => {
                let send_res = sender.send(Entry::Snapshot(entry_read));
                if let Err(e) = send_res {
                    error!("Failed to send rewind entry: {e}");
                }
            }
            PersistenceMode::RealtimeReplay => {
                unreachable!()
            }
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum SnapshotAccess {
    Replay,
    Record,
    Full,
    OffsetsOnly,
}

impl SnapshotAccess {
    fn is_replay_allowed(self) -> bool {
        match self {
            SnapshotAccess::Full | SnapshotAccess::Replay | SnapshotAccess::OffsetsOnly => true,
            SnapshotAccess::Record => false,
        }
    }

    fn is_snapshot_writing_allowed(self) -> bool {
        match self {
            SnapshotAccess::Full | SnapshotAccess::Record | SnapshotAccess::OffsetsOnly => true,
            SnapshotAccess::Replay => false,
        }
    }

    fn snapshot_mode(self) -> SnapshotMode {
        match self {
            SnapshotAccess::Full | SnapshotAccess::Record | SnapshotAccess::Replay => {
                SnapshotMode::Full
            }
            SnapshotAccess::OffsetsOnly => SnapshotMode::OffsetsOnly,
        }
    }
}

impl Connector {
    /*
        The implementation for pull model of data acquisition: we explicitly inquiry the source about the newly
        arrived data.
    */
    pub fn new(
        commit_duration: Option<Duration>,
        num_columns: usize,
        skip_all_errors: bool,
        error_logger: Rc<dyn LogError>,
    ) -> Self {
        Connector {
            commit_duration,
            current_timestamp: Timestamp(0), // default is 0 now. If changing, make sure it is even (required for alt-neu).
            num_columns,
            current_frontier: OffsetAntichain::new(),
            skip_all_errors,
            error_logger,
            n_parse_attempts: 0,
            n_parse_errors_in_log: 0,
        }
    }

    fn advance_time(&mut self, input_session: &mut dyn InputAdaptor<Timestamp>) -> Timestamp {
        let new_timestamp = Timestamp::new_from_current_time();
        let timestamp_updated = self.current_timestamp <= new_timestamp;
        if timestamp_updated {
            self.current_timestamp = new_timestamp;
        } else {
            warn!("The current timestamp is lower than the last one saved");
        }

        input_session.advance_to(self.current_timestamp);
        input_session.flush();

        self.current_timestamp
    }

    pub fn rewind_from_disk_snapshot(
        persistent_id: PersistentId,
        persistent_storage: &Arc<Mutex<WorkerPersistentStorage>>,
        sender: &Sender<Entry>,
        persistence_mode: PersistenceMode,
    ) {
        // TODO: note that here we read snapshots again.
        // If it's slow, some kind of snapshot reader memoization may be a good idea
        // (also note it will require some communication between workers)
        let snapshot_readers = persistent_storage
            .lock()
            .unwrap()
            .create_snapshot_readers(persistent_id, ReadersQueryPurpose::ReadSnapshot);

        if let Ok(snapshot_readers) = snapshot_readers {
            for mut snapshot_reader in snapshot_readers {
                let mut entries_read = 0;
                loop {
                    let entry_read = snapshot_reader.read();
                    let Ok(entry_read) = entry_read else { break };
                    match entry_read {
                        SnapshotEvent::Finished => {
                            info!("Reached the end of the snapshot. Exiting the rewind after {entries_read} entries");
                            break;
                        }
                        SnapshotEvent::Insert(_, _) | SnapshotEvent::Delete(_, _) => {
                            entries_read += 1;
                            let send_res = sender.send(Entry::Snapshot(entry_read));
                            if let Err(e) = send_res {
                                error!("Failed to send rewind entry: {e}");
                            }
                        }
                        SnapshotEvent::AdvanceTime(_, _) => {
                            persistence_mode.handle_snapshot_time_advancement(sender, entry_read);
                        }
                    }
                }
            }
        }
    }

    pub fn frontier_for(
        reader: &mut dyn Reader,
        persistent_id: PersistentId,
        persistent_storage: &Arc<Mutex<WorkerPersistentStorage>>,
    ) -> OffsetAntichain {
        let snapshot_readers = persistent_storage
            .lock()
            .unwrap()
            .create_snapshot_readers(persistent_id, ReadersQueryPurpose::ReconstructFrontier);

        let mut frontier = OffsetAntichain::new();

        if let Ok(snapshot_readers) = snapshot_readers {
            for mut snapshot_reader in snapshot_readers {
                loop {
                    let entry_read = snapshot_reader.read();
                    let Ok(entry_read) = entry_read else { break };
                    if matches!(entry_read, SnapshotEvent::Finished) {
                        break;
                    }
                }

                frontier = reader
                    .storage_type()
                    .merge_two_frontiers(&frontier, snapshot_reader.last_frontier());
            }
        }

        frontier
    }

    #[allow(clippy::too_many_lines)]
    pub fn read_realtime_updates(
        reader: &mut dyn Reader,
        parser: &mut dyn Parser,
        sender: &Sender<Entry>,
        main_thread: &Thread,
        error_reporter: &(impl ReportError + 'static),
        mut group: Option<&mut ConnectorGroupAccessor>,
    ) {
        let use_rare_wakeup = env::var("PATHWAY_YOLO_RARE_WAKEUPS") == Ok("1".to_string());
        let mut amt_send = 0;
        let mut consecutive_errors = 0;
        loop {
            let row_read_result = reader.read();
            let finished = matches!(row_read_result, Ok(ReadResult::Finished));

            match row_read_result {
                Ok(ReadResult::Data(reader_context, offset)) => {
                    match parser.parse(&reader_context) {
                        Ok(entries) => {
                            if let Some(group) = group.as_mut() {
                                let mut entries_for_sending = Vec::new();
                                let mut approvals = Vec::new();
                                let mut disconnected = false;
                                for entry in entries {
                                    let mut can_be_sent = group.can_entry_be_sent(&entry);
                                    while can_be_sent.is_wait() {
                                        if !entries_for_sending.is_empty() {
                                            let send_res = sender.send(Entry::RealtimeEntries(
                                                take(&mut entries_for_sending),
                                                offset.clone(),
                                            ));
                                            if send_res.is_err() {
                                                disconnected = true;
                                                break;
                                            }
                                            if !approvals.is_empty() {
                                                group.report_entries_sent(take(&mut approvals));
                                            }
                                        }
                                        let retry_future = can_be_sent.expect_wait();
                                        futures::executor::block_on(retry_future)
                                            .expect("retry sender must not drop");
                                        can_be_sent = group.can_entry_be_sent(&entry);
                                    }
                                    if disconnected {
                                        break;
                                    }
                                    let approval = can_be_sent.expect_approved();
                                    entries_for_sending.push(entry);
                                    approvals.push(approval);
                                }
                                let send_res = sender.send(Entry::RealtimeEntries(
                                    take(&mut entries_for_sending),
                                    offset,
                                ));
                                if disconnected || send_res.is_err() {
                                    break;
                                }
                                group.report_entries_sent(take(&mut approvals));
                            } else {
                                let send_res = sender.send(Entry::RealtimeEntries(entries, offset));
                                if send_res.is_err() {
                                    break;
                                }
                            }
                        }
                        Err(e) => {
                            let send_res = sender.send(Entry::RealtimeParsingError(e));
                            if send_res.is_err() {
                                break;
                            }
                        }
                    };
                }
                Ok(other_read_result) => {
                    if let ReadResult::NewSource(ref metadata) = other_read_result {
                        parser.on_new_source_started(metadata);
                    }
                    let send_res = sender.send(Entry::RealtimeEvent(other_read_result));
                    if send_res.is_err() {
                        break;
                    }
                    consecutive_errors = 0;
                }
                Err(error) => {
                    error!("There had been an error processing the row read result: {error}");
                    consecutive_errors += 1;
                    if consecutive_errors > reader.max_allowed_consecutive_errors() {
                        error_reporter.report(EngineError::ReaderFailed(error));
                    }
                }
            };

            if finished {
                break;
            }

            if use_rare_wakeup {
                amt_send += 1;
                if amt_send % 50 == 0 {
                    main_thread.unpark();
                }
            } else {
                main_thread.unpark();
            }
        }
    }

    pub fn read_snapshot(
        reader: &mut dyn Reader,
        persistent_storage: Option<&Arc<Mutex<WorkerPersistentStorage>>>,
        persistent_id: Option<PersistentId>,
        sender: &Sender<Entry>,
        persistence_mode: PersistenceMode,
        snapshot_access: SnapshotAccess,
        realtime_reader_needed: bool,
    ) -> Result<(), ReadError> {
        info!(
            "Enter read_snapshot method with reader {:?}",
            reader.storage_type()
        );
        let mut frontier = OffsetAntichain::new();
        if snapshot_access.is_replay_allowed() {
            persistence_mode.on_before_reading_snapshot(sender);
        }
        if let Some(persistent_storage) = persistent_storage {
            if let Some(persistent_id) = persistent_id {
                reader.initialize_cached_objects_storage(
                    &persistent_storage.lock().unwrap(),
                    persistent_id,
                )?;
                if snapshot_access.is_replay_allowed() {
                    if persistent_storage
                        .lock()
                        .unwrap()
                        .input_persistence_enabled()
                    {
                        Self::rewind_from_disk_snapshot(
                            persistent_id,
                            persistent_storage,
                            sender,
                            persistence_mode,
                        );
                    }

                    if realtime_reader_needed {
                        frontier = Self::frontier_for(reader, persistent_id, persistent_storage);
                        info!("Seek the data source to the frontier {frontier:?}");
                        reader.seek(&frontier)?;
                    }
                }
            }
        }
        // Report that rewind has finished, so that autocommits start to work
        let send_res = sender.send(Entry::RewindFinishSentinel(frontier));
        if let Err(e) = send_res {
            panic!("Failed to switch from persisted to realtime: {e}");
        }

        Ok(())
    }

    pub fn snapshot_writer(
        reader: &dyn ReaderBuilder,
        persistent_id: Option<PersistentId>,
        persistent_storage: Option<&Arc<Mutex<WorkerPersistentStorage>>>,
        snapshot_access: SnapshotAccess,
    ) -> Result<Option<SharedSnapshotWriter>, WriteError> {
        if !snapshot_access.is_snapshot_writing_allowed() {
            Ok(None)
        } else if let Some(persistent_storage) = &persistent_storage {
            if let Some(persistent_id) = persistent_id {
                Ok(Some(
                    persistent_storage
                        .lock()
                        .unwrap()
                        .create_snapshot_writer(persistent_id, snapshot_access.snapshot_mode())?,
                ))
            } else {
                let persistent_ids_enforced = persistent_storage
                    .lock()
                    .unwrap()
                    .persistent_id_generation_enabled(
                        RequiredPersistenceMode::InputOrOperatorPersistence,
                    );
                assert!(!persistent_ids_enforced || reader.is_internal());
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }

    #[allow(clippy::too_many_arguments)]
    #[allow(clippy::too_many_lines)]
    pub fn run(
        mut self,
        reader: Box<dyn ReaderBuilder>,
        mut parser: Box<dyn Parser>,
        mut input_session: Box<dyn InputAdaptor<Timestamp>>,
        mut values_to_key: impl FnMut(Option<&Vec<Value>>, Option<&Offset>) -> Key + 'static,
        probe: Handle<Timestamp>,
        persistent_storage: Option<Arc<Mutex<WorkerPersistentStorage>>>,
        persistent_id: Option<PersistentId>,
        realtime_reader_needed: bool,
        unique_name: Option<&UniqueName>,
        persistence_mode: PersistenceMode,
        snapshot_access: SnapshotAccess,
        error_reporter: impl ReportError + 'static,
        mut group: Option<ConnectorGroupAccessor>,
    ) -> Result<StartedConnectorState, EngineError> {
        assert_eq!(self.num_columns, parser.column_count());

        let main_thread = thread::current();
        let (sender, receiver) = mpsc::channel();

        let thread_name = format!(
            "pathway:connector-{}-{}",
            reader.short_description(),
            parser.short_description()
        );
        let reader_name = reader.name(unique_name);
        let session_type = parser.session_type();
        let in_connector_group = group.is_some();

        let mut snapshot_writer = Self::snapshot_writer(
            reader.as_ref(),
            persistent_id,
            persistent_storage.as_ref(),
            snapshot_access,
        )
        .map_err(EngineError::SnapshotWriterError)?;

        let input_thread_handle = thread::Builder::new()
            .name(thread_name)
            .spawn_with_reporter(error_reporter, move |reporter| {
                let sender = guard(sender, |sender| {
                    // ensure that we always unpark the main thread after dropping the sender, so it
                    // notices we are done sending
                    drop(sender);
                    main_thread.unpark();
                });

                let mut reader = reader.build()?;
                Self::read_snapshot(
                    &mut *reader,
                    persistent_storage.as_ref(),
                    persistent_id,
                    &sender,
                    persistence_mode,
                    snapshot_access,
                    realtime_reader_needed,
                )
                .map_err(EngineError::ReaderFailed)?;
                if realtime_reader_needed {
                    Self::read_realtime_updates(
                        &mut *reader,
                        &mut *parser,
                        &sender,
                        &main_thread,
                        reporter,
                        group.as_mut(),
                    );
                }

                Ok(())
            })
            .expect("connector thread creation failed");

        let mut next_commit_at = self.commit_duration.map(|x| SystemTime::now() + x);
        let mut backfilling_finished = false;

        let connector_monitor = Rc::new(RefCell::new(ConnectorMonitor::new(reader_name)));
        let cloned_connector_monitor = connector_monitor.clone();
        let mut commit_allowed = true;
        let mut deferred_events = Vec::new();
        let poller = Box::new(move || {
            let iteration_start = SystemTime::now();
            if matches!(persistence_mode, PersistenceMode::SpeedrunReplay)
                && !backfilling_finished
                && probe.less_than(input_session.time())
            {
                return ControlFlow::Continue(Some(iteration_start));
            }

            if let Some(next_commit_at_timestamp) = next_commit_at {
                if next_commit_at_timestamp <= iteration_start {
                    if backfilling_finished && commit_allowed {
                        /*
                            We don't auto-commit for the initial batch, which consists of the
                            data, which shouldn't trigger any output.

                            The end of this batch is determined by the AdvanceTime event.
                        */
                        let parsed_entries = vec![ParsedEventWithErrors::AdvanceTime];
                        self.on_parsed_data(
                            parsed_entries,
                            None, // no key generation for time advancement
                            input_session.as_mut(),
                            &mut values_to_key,
                            &mut snapshot_writer,
                            &mut Some(&mut *connector_monitor.borrow_mut()),
                            session_type,
                        );
                    }

                    next_commit_at = Some(next_commit_at_timestamp + self.commit_duration.unwrap());
                }
            }

            let mut n_entries_in_batch = 0;
            loop {
                n_entries_in_batch += 1;
                if n_entries_in_batch == 100_000 {
                    return ControlFlow::Continue(next_commit_at);
                }
                match receiver.try_recv() {
                    Ok(Entry::RealtimeEvent(ReadResult::Finished)) => {
                        if let Some(snapshot_writer) = &snapshot_writer {
                            let snapshot_event = SnapshotEvent::AdvanceTime(
                                Timestamp(self.current_timestamp.0 + 2),
                                self.current_frontier.clone(),
                            );
                            info!("Input source has ended. Terminating with snapshot event: {snapshot_event:?}");
                            snapshot_writer.lock().unwrap().write(&snapshot_event);
                        }
                        if backfilling_finished {
                            (*connector_monitor).borrow_mut().finish();
                            return ControlFlow::Break(());
                        }
                    }
                    Ok(Entry::Snapshot(SnapshotEvent::AdvanceTime(new_timestamp, _))) => {
                        input_session.flush();
                        let new_timestamp_even = (new_timestamp.0 / 2) * 2; //use only even times (required by alt-neu)
                        let new_timestamp_even = Timestamp(new_timestamp_even);
                        input_session.advance_to(new_timestamp_even);
                        input_session.flush();
                        return ControlFlow::Continue(Some(iteration_start));
                    }
                    Ok(entry) => {
                        let need_to_defer_processing = match entry {
                            Entry::RealtimeEvent(ReadResult::NewSource(ref metadata)) => {
                                // Deferring events is only necessary when the data source
                                // belongs to a connector group. In such cases, the data might be
                                // partially read and waiting to proceed, which can block autocommits
                                // from happening for an indefinite amount of time.
                                in_connector_group && !metadata.commits_allowed_in_between()
                            }
                            Entry::RealtimeEvent(ReadResult::FinishedSource { .. }) => false,
                            _ => !deferred_events.is_empty(),
                        };
                        deferred_events.push(entry);
                        if need_to_defer_processing {
                            continue;
                        }
                        for entry in take(&mut deferred_events) {
                            self.handle_input_entry(
                                entry,
                                &mut backfilling_finished,
                                session_type,
                                input_session.as_mut(),
                                &mut values_to_key,
                                &mut snapshot_writer,
                                &mut Some(&mut *connector_monitor.borrow_mut()),
                                &mut commit_allowed,
                            );
                        }
                    }
                    Err(TryRecvError::Empty) => return ControlFlow::Continue(next_commit_at),
                    Err(TryRecvError::Disconnected) => {
                        (*connector_monitor).borrow_mut().finish();
                        return ControlFlow::Break(());
                    }
                }
            }
        });
        Ok(StartedConnectorState::new(
            poller,
            input_thread_handle,
            cloned_connector_monitor,
        ))
    }

    #[allow(clippy::too_many_arguments)]
    fn handle_input_entry(
        &mut self,
        entry: Entry,
        backfilling_finished: &mut bool,
        session_type: SessionType,
        input_session: &mut dyn InputAdaptor<Timestamp>,
        values_to_key: impl FnMut(Option<&Vec<Value>>, Option<&Offset>) -> Key,
        snapshot_writer: &mut Option<SharedSnapshotWriter>,
        connector_monitor: &mut Option<&mut ConnectorMonitor>,
        commit_allowed: &mut bool,
    ) {
        let has_persistent_storage = snapshot_writer.is_some();

        match entry {
            Entry::RealtimeEvent(read_result) => match read_result {
                ReadResult::Finished => {}
                ReadResult::FinishedSource {
                    commit_allowed: commit_allowed_external,
                } => {
                    *commit_allowed = commit_allowed_external;
                    if *commit_allowed {
                        let parsed_entries = vec![ParsedEventWithErrors::AdvanceTime];
                        self.on_parsed_data(
                            parsed_entries,
                            None, // no key generation for time advancement
                            input_session,
                            values_to_key,
                            snapshot_writer,
                            connector_monitor,
                            session_type,
                        );
                    }
                }
                ReadResult::NewSource(metadata) => {
                    *commit_allowed &= metadata.commits_allowed_in_between();
                }
                ReadResult::Data(_, _) => {
                    unreachable!("ReadResult::Data must be a part of RealtimeEntries event")
                }
            },
            Entry::RealtimeParsingError(e) => {
                self.log_parse_error(e);
            }
            Entry::RealtimeEntries(mut parsed_entries, offset) => {
                if !*backfilling_finished {
                    parsed_entries.retain(|x| !matches!(x, ParsedEventWithErrors::AdvanceTime));
                }

                self.on_parsed_data(
                    parsed_entries,
                    Some(&offset.clone()),
                    input_session,
                    values_to_key,
                    snapshot_writer,
                    connector_monitor,
                    session_type,
                );

                let (offset_key, offset_value) = offset;
                if has_persistent_storage {
                    assert!(*backfilling_finished);
                    self.current_frontier
                        .advance_offset(offset_key, offset_value);
                }
            }
            Entry::RewindFinishSentinel(restored_frontier) => {
                assert!(!*backfilling_finished);
                *backfilling_finished = true;
                self.current_frontier = restored_frontier;
                let parsed_entries = vec![ParsedEventWithErrors::AdvanceTime];
                self.on_parsed_data(
                    parsed_entries,
                    None, // no key generation for time advancement
                    input_session,
                    values_to_key,
                    snapshot_writer,
                    connector_monitor,
                    session_type,
                );
            }
            Entry::Snapshot(snapshot) => {
                assert!(!*backfilling_finished);
                match snapshot {
                    SnapshotEvent::Insert(key, value) => {
                        Self::on_insert(key, value, input_session);
                    }
                    SnapshotEvent::Delete(key, value) => {
                        Self::on_remove(key, value, input_session);
                    }
                    SnapshotEvent::AdvanceTime(_, _) | SnapshotEvent::Finished => {
                        unreachable!()
                    }
                };
            }
        }
    }

    fn on_insert(key: Key, values: Vec<Value>, input_session: &mut dyn InputAdaptor<Timestamp>) {
        input_session.insert(key, Value::Tuple(values.into()));
    }

    fn on_remove(key: Key, values: Vec<Value>, input_session: &mut dyn InputAdaptor<Timestamp>) {
        input_session.remove(key, Value::Tuple(values.into()));
    }

    #[allow(clippy::too_many_arguments)]
    fn on_parsed_data(
        &mut self,
        parsed_entries: Vec<ParsedEventWithErrors>,
        offset: Option<&Offset>,
        input_session: &mut dyn InputAdaptor<Timestamp>,
        mut values_to_key: impl FnMut(Option<&Vec<Value>>, Option<&Offset>) -> Key,
        snapshot_writer: &mut Option<SharedSnapshotWriter>,
        connector_monitor: &mut Option<&mut ConnectorMonitor>,
        session_type: SessionType,
    ) {
        let error_logger = self.error_logger.clone();
        let error_handling_logic: data_format::ErrorRemovalLogic = if self.skip_all_errors {
            Box::new(move |values| values.into_iter().try_collect())
        } else {
            Box::new(move |values| {
                Ok(values
                    .into_iter()
                    .map(|value| value.unwrap_or_log(error_logger.as_ref(), Value::Error))
                    .collect())
            })
        }; // logic to handle errors in values
        for entry in parsed_entries {
            let entry = match entry.remove_errors(&error_handling_logic) {
                Ok(entry) => {
                    self.log_parse_success();
                    entry
                }
                Err(err) => {
                    let err = if self.skip_all_errors {
                        err
                    } else {
                        // if there is an error in key
                        ParseError::ErrorInKey(err).into()
                    };
                    self.log_parse_error(err);
                    continue;
                }
            };
            let key = entry.key(&mut values_to_key, offset);
            if let Some(key) = key {
                // true for Insert, Delete
                if let Some(ref mut connector_monitor) = connector_monitor {
                    connector_monitor.increment();
                }

                if let Some(snapshot_writer) = snapshot_writer {
                    // TODO: if the usage of Mutex+Arc hits the performance, add a buffered accessor here
                    // It must accumulate the data to the extent of the chunk size, and then unlock the mutex
                    // once and send the full chunk
                    let snapshot_event = entry
                        .snapshot_event(key)
                        .expect("Snapshot event not constructed");
                    snapshot_writer.lock().unwrap().write(&snapshot_event);
                }
            }

            match entry {
                ParsedEvent::Insert((_, values)) => {
                    if values.len() != self.num_columns {
                        error!("There are {} tokens in the entry, but the expected number of tokens was {}", values.len(), self.num_columns);
                        continue;
                    }
                    Self::on_insert(key.expect("No key"), values, input_session);
                }
                ParsedEvent::Delete((_, values)) => {
                    if matches!(session_type, SessionType::Native)
                        && values.len() != self.num_columns
                    {
                        error!("There are {} tokens in the entry, but the expected number of tokens was {}", values.len(), self.num_columns);
                        continue;
                    }
                    Self::on_remove(key.expect("No key"), values, input_session);
                }
                ParsedEvent::AdvanceTime => {
                    let time_advanced = self.advance_time(input_session);
                    if let Some(ref mut connector_monitor) = connector_monitor {
                        connector_monitor.commit();
                    }
                    if let Some(snapshot_writer) = snapshot_writer {
                        snapshot_writer
                            .lock()
                            .unwrap()
                            .write(&SnapshotEvent::AdvanceTime(
                                time_advanced,
                                self.current_frontier.clone(),
                            ));
                    }
                }
            };
        }
    }

    fn log_parse_error(&mut self, error: DynError) {
        self.n_parse_attempts += 1;
        if self.skip_all_errors {
            self.n_parse_errors_in_log += 1;
            let needs_error_log = self.n_parse_errors_in_log <= MAX_PARSE_ERRORS_IN_LOG
                || self.n_parse_errors_in_log * 10 <= self.n_parse_attempts;
            if needs_error_log {
                error!("Parse error: {error}");
            } else if self.n_parse_errors_in_log == MAX_PARSE_ERRORS_IN_LOG + 1 {
                error!("Too many parse errors, some of them will be omitted...");
            }
        } else {
            self.error_logger.log_error(error.into());
        }
    }

    fn log_parse_success(&mut self) {
        self.n_parse_attempts += 1;
    }
}

pub struct SnapshotReaderState {
    pub poller: Box<dyn FnMut() -> ControlFlow<(), Option<SystemTime>>>,
    pub input_thread_handle: std::thread::JoinHandle<()>,
}

pub fn read_persisted_state(
    mut input_session: InputSession<Timestamp, (Key, Vec<Value>), isize>,
    persistent_storage: Arc<Mutex<WorkerPersistentStorage>>,
    unique_name: &UniqueName,
    persistent_id: PersistentId,
) -> SnapshotReaderState {
    let (sender, receiver) = mpsc::channel();
    let thread_name = format!("pathway:{unique_name}");

    let input_thread_handle = thread::Builder::new()
        .name(thread_name)
        .spawn(move || {
            Connector::rewind_from_disk_snapshot(
                persistent_id,
                &persistent_storage,
                &sender,
                PersistenceMode::Persisting,
            );
            let send_res = sender.send(Entry::RewindFinishSentinel(OffsetAntichain::new()));
            if let Err(e) = send_res {
                panic!("Failed to read the state of deduplicate operator: {e}");
            }
        })
        .expect("deduplication thread creation failed");

    let poller = Box::new(move || loop {
        match receiver.recv() {
            Ok(Entry::Snapshot(entry)) => match entry {
                SnapshotEvent::Insert(key, values) => input_session.insert((key, values)),
                SnapshotEvent::Delete(key, values) => input_session.remove((key, values)),
                SnapshotEvent::AdvanceTime(_, _) | SnapshotEvent::Finished => unreachable!(),
            },
            Ok(
                Entry::RealtimeEvent(_)
                | Entry::RealtimeEntries(_, _)
                | Entry::RealtimeParsingError(_),
            ) => unreachable!(),
            Ok(Entry::RewindFinishSentinel(_)) | Err(_) => return ControlFlow::Break(()),
        }
    });

    SnapshotReaderState {
        poller,
        input_thread_handle,
    }
}

impl<T> UnwrapWithErrorLogger<T> for Result<T, ParseError> {
    #[track_caller]
    fn unwrap_or_else_log<F: FnOnce() -> T>(
        self,
        error_logger: &(impl LogError + ?Sized),
        op: F,
    ) -> T {
        self.unwrap_or_else(|err| {
            error_logger.log_error(DataError::Other(err.into()));
            op()
        })
    }
    #[track_caller]
    fn unwrap_or_log_with_trace(
        self,
        error_logger: &(impl LogError + ?Sized),
        trace: &Trace,
        default: T,
    ) -> T {
        self.unwrap_or_else(|err| {
            error_logger.log_error_with_trace(err.into(), trace);
            default
        })
    }
    fn ok_with_logger(self, error_logger: &(impl LogError + ?Sized)) -> Option<T> {
        Some(self).transpose().unwrap_or_log(error_logger, None)
    }
}
