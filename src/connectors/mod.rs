use log::{error, info, warn};
use std::cell::RefCell;
use std::collections::HashMap;
use std::env;
use std::iter::zip;
use std::ops::ControlFlow;
use std::rc::Rc;
use std::sync::mpsc::{self, Sender, TryRecvError};
use std::sync::{Arc, Mutex};
use std::thread;
use std::thread::Thread;
use std::time::{Duration, SystemTime};

use differential_dataflow::input::InputSession;
use scopeguard::guard;
use timely::progress::Timestamp as TimelyTimestamp;

pub mod data_format;
pub mod data_storage;
pub mod monitoring;
pub mod offset;
pub mod snapshot;

use crate::connectors::monitoring::ConnectorMonitor;
use crate::engine::report_error::{ReportError, SpawnWithReporter};
use crate::engine::{Key, Value};

use crate::connectors::snapshot::Event as SnapshotEvent;
use crate::engine::Error as EngineError;
use crate::persistence::frontier::OffsetAntichain;
use crate::persistence::tracker::SingleWorkerPersistentStorage;
use crate::persistence::{ExternalPersistentId, PersistentId, SharedSnapshotWriter};

use data_format::{ParseResult, ParsedEvent, Parser};
use data_storage::{DataEventType, ReadResult, Reader, ReaderBuilder, ReaderContext, WriteError};

pub use data_storage::StorageType;
pub use offset::{Offset, OffsetKey, OffsetValue};

pub const ARTIFICIAL_TIME_ON_REWIND_START: u64 = 0;

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

pub struct StartedConnectorState<Timestamp> {
    pub poller: Box<dyn FnMut() -> ControlFlow<(), Option<SystemTime>>>,
    pub input_thread_handle: std::thread::JoinHandle<()>,
    pub offsets_by_time: Arc<Mutex<HashMap<Timestamp, OffsetAntichain>>>,
    pub connector_monitor: Rc<RefCell<ConnectorMonitor>>,
}

impl<Timestamp> StartedConnectorState<Timestamp> {
    pub fn new(
        poller: Box<dyn FnMut() -> ControlFlow<(), Option<SystemTime>>>,
        input_thread_handle: std::thread::JoinHandle<()>,
        offsets_by_time: Arc<Mutex<HashMap<Timestamp, OffsetAntichain>>>,
        connector_monitor: Rc<RefCell<ConnectorMonitor>>,
    ) -> Self {
        Self {
            poller,
            input_thread_handle,
            offsets_by_time,
            connector_monitor,
        }
    }
}

pub struct Connector<Timestamp> {
    commit_duration: Option<Duration>,
    current_timestamp: Timestamp,
}

#[derive(Debug, Eq, PartialEq)]
pub enum Entry {
    Snapshot(SnapshotEvent),
    RewindFinishSentinel,
    Realtime(ReadResult),
}

impl<Timestamp> Connector<Timestamp>
where
    Timestamp: TimelyTimestamp + Default + From<u64>,
    u64: From<Timestamp>,
{
    /*
        The implementation for pull model of data acquisition: we explicitly inquiry the source about the newly
        arrived data.
    */
    pub fn new(commit_duration: Option<Duration>) -> Self {
        Connector {
            commit_duration,
            current_timestamp: Default::default(),
        }
    }

    fn advance_time(
        &mut self,
        input_sessions: &mut [InputSession<Timestamp, (Key, Value), isize>],
        universe_session: &mut InputSession<Timestamp, Key, isize>,
    ) -> u64 {
        let current_timestamp = SystemTime::now();
        let unix_timestamp = current_timestamp
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("System time should be after the Unix epoch");

        let new_timestamp = u64::try_from(unix_timestamp.as_millis())
            .expect("number of milliseconds should fit in 64 bits");

        if self.current_timestamp.less_equal(&new_timestamp.into()) {
            self.current_timestamp = new_timestamp.into();
        } else {
            warn!("The current timestamp is lower than the last one saved. Commits won't work.");
        }

        universe_session.advance_to(self.current_timestamp.clone());
        universe_session.flush();

        for input_session in &mut *input_sessions {
            input_session.advance_to(self.current_timestamp.clone());
            input_session.flush();
        }

        self.current_timestamp.clone().into()
    }

    pub fn rewind_from_disk_snapshot(
        persistent_id: PersistentId,
        persistent_storage: &Arc<Mutex<SingleWorkerPersistentStorage>>,
        sender: &Sender<Entry>,
    ) {
        let snapshot_readers = persistent_storage
            .lock()
            .unwrap()
            .create_snapshot_readers(persistent_id);

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
                        SnapshotEvent::AdvanceTime(_) => {}
                    }
                }
            }
        }
    }

    pub fn read_realtime_updates(
        reader: &mut dyn Reader,
        sender: &Sender<Entry>,
        main_thread: &Thread,
    ) {
        let use_rare_wakeup = env::var("PATHWAY_YOLO_RARE_WAKEUPS") == Ok("1".to_string());
        let mut amt_send = 0;
        loop {
            let row_read_result = reader.read();
            let finished = matches!(row_read_result, Ok(ReadResult::Finished));

            match row_read_result {
                Ok(read_result) => {
                    let send_res = sender.send(Entry::Realtime(read_result));
                    if send_res.is_err() {
                        break;
                    }
                }
                Err(error) => {
                    error!("There had been an error processing the row read result {error}");
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
        persistent_storage: &Option<Arc<Mutex<SingleWorkerPersistentStorage>>>,
        sender: &Sender<Entry>,
    ) {
        // Rewind the data source
        if let Some(persistent_storage) = persistent_storage {
            if let Some(persistent_id) = reader.persistent_id() {
                Self::rewind_from_disk_snapshot(persistent_id, persistent_storage, sender);

                let frontier = persistent_storage
                    .lock()
                    .unwrap()
                    .frontier_for(persistent_id);

                info!("Seek the data source to the frontier {frontier:?}");
                if let Err(e) = reader.seek(&frontier) {
                    error!("Failed to seek to frontier: {e}");
                }
            }
        }

        // Report that rewind has finished, so that autocommits start to work
        {
            let send_res = sender.send(Entry::RewindFinishSentinel);
            if let Err(e) = send_res {
                panic!("Failed to switch from persisted to realtime: {e}");
            }
        }
    }

    pub fn snapshot_writer(
        reader: &dyn ReaderBuilder,
        persistent_storage: &Option<Arc<Mutex<SingleWorkerPersistentStorage>>>,
    ) -> Result<Option<SharedSnapshotWriter>, WriteError> {
        if let Some(persistent_storage) = &persistent_storage {
            if let Some(persistent_id) = reader.persistent_id() {
                Ok(Some(
                    persistent_storage
                        .lock()
                        .unwrap()
                        .create_snapshot_writer(persistent_id)?,
                ))
            } else {
                assert!(reader.is_internal());
                Ok(None)
            }
        } else {
            assert!(reader.persistent_id().is_none());
            Ok(None)
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn run(
        mut self,
        reader: Box<dyn ReaderBuilder>,
        mut parser: Box<dyn Parser>,
        mut input_sessions: Vec<InputSession<Timestamp, (Key, Value), isize>>,
        mut universe_session: InputSession<Timestamp, Key, isize>,
        mut values_to_key: impl FnMut(Option<Vec<Value>>, Option<&Offset>) -> Key + 'static,
        persistent_storage: Option<Arc<Mutex<SingleWorkerPersistentStorage>>>,
        connector_id: usize,
        realtime_reader_needed: bool,
        external_persistent_id: &Option<ExternalPersistentId>,
        error_reporter: impl ReportError + 'static,
    ) -> Result<StartedConnectorState<Timestamp>, EngineError> {
        assert_eq!(input_sessions.len(), parser.column_count());

        let main_thread = thread::current();
        let (sender, receiver) = mpsc::channel();

        let thread_name = format!(
            "pathway:connector-{}-{}",
            reader.short_description(),
            parser.short_description()
        );
        let reader_name = reader.name(external_persistent_id, connector_id);

        let mut snapshot_writer = Self::snapshot_writer(reader.as_ref(), &persistent_storage)
            .map_err(EngineError::SnapshotWriterError)?;

        let input_thread_handle = thread::Builder::new()
            .name(thread_name)
            .spawn_with_reporter(error_reporter, move |_reporter| {
                let sender = guard(sender, |sender| {
                    // ensure that we always unpark the main thread after dropping the sender, so it
                    // notices we are done sending
                    drop(sender);
                    main_thread.unpark();
                });

                let mut reader = reader.build()?;
                Self::read_snapshot(&mut *reader, &persistent_storage, &sender);
                if realtime_reader_needed {
                    Self::read_realtime_updates(&mut *reader, &sender, &main_thread);
                }

                Ok(())
            })
            .expect("connector thread creation failed");

        let offsets_by_time = Arc::new(Mutex::new(HashMap::<Timestamp, OffsetAntichain>::new()));
        let offsets_by_time_writer = offsets_by_time.clone();

        let mut next_commit_at = self.commit_duration.map(|x| SystemTime::now() + x);
        let mut backfilling_finished = false;

        let connector_monitor = Rc::new(RefCell::new(ConnectorMonitor::new(reader_name)));
        let cloned_connector_monitor = connector_monitor.clone();
        let poller = Box::new(move || {
            let iteration_start = SystemTime::now();

            if let Some(next_commit_at_timestamp) = next_commit_at {
                if next_commit_at_timestamp <= iteration_start {
                    if backfilling_finished {
                        /*
                            We don't auto-commit for the initial batch, which consists of the
                            data, which shouldn't trigger any output.

                            The end of this batch is determined by the AdvanceTime event.
                        */
                        let parsed_entries = vec![ParsedEvent::AdvanceTime];
                        self.on_parsed_data(
                            parsed_entries,
                            None, // no key generation for time advancement
                            &mut input_sessions,
                            &mut universe_session,
                            &mut values_to_key,
                            &mut snapshot_writer,
                            &mut Some(&mut *connector_monitor.borrow_mut()),
                        );
                    }

                    next_commit_at = Some(next_commit_at_timestamp + self.commit_duration.unwrap());
                }
            }

            loop {
                match receiver.try_recv() {
                    Ok(Entry::Realtime(ReadResult::Finished)) => {
                        if backfilling_finished {
                            (*connector_monitor).borrow_mut().finish();
                            return ControlFlow::Break(());
                        }
                    }
                    Ok(entry) => {
                        self.handle_input_entry(
                            entry,
                            &mut backfilling_finished,
                            &mut parser,
                            &mut input_sessions,
                            &mut universe_session,
                            &mut values_to_key,
                            &mut snapshot_writer,
                            &offsets_by_time_writer,
                            &mut Some(&mut *connector_monitor.borrow_mut()),
                        );
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
            offsets_by_time,
            cloned_connector_monitor,
        ))
    }

    #[allow(clippy::too_many_arguments)]
    fn handle_input_entry(
        &mut self,
        entry: Entry,
        backfilling_finished: &mut bool,
        parser: &mut Box<dyn Parser>,
        input_sessions: &mut [InputSession<Timestamp, (Key, Value), isize>],
        universe_session: &mut InputSession<Timestamp, Key, isize>,
        values_to_key: impl FnMut(Option<Vec<Value>>, Option<&Offset>) -> Key,
        snapshot_writer: &mut Option<SharedSnapshotWriter>,
        offsets_by_time_writer: &Mutex<HashMap<Timestamp, OffsetAntichain>>,
        connector_monitor: &mut Option<&mut ConnectorMonitor>,
    ) {
        let has_persistent_storage = snapshot_writer.is_some();

        match entry {
            Entry::Realtime(read_result) => match read_result {
                ReadResult::Finished => {}
                ReadResult::NewSource => {
                    parser.on_new_source_started();

                    let parsed_entries = vec![ParsedEvent::AdvanceTime];
                    self.on_parsed_data(
                        parsed_entries,
                        None, // no key generation for time advancement
                        input_sessions,
                        universe_session,
                        values_to_key,
                        snapshot_writer,
                        connector_monitor,
                    );
                }
                ReadResult::Data(reader_context, offset) => {
                    let mut parsed_entries = match parser.parse(&reader_context) {
                        Ok(entries) => entries,
                        Err(e) => {
                            error!("Read data parsed unsuccessfully. {e}");
                            return;
                        }
                    };

                    if !*backfilling_finished {
                        parsed_entries.retain(|x| !matches!(x, ParsedEvent::AdvanceTime));
                    }

                    self.on_parsed_data(
                        parsed_entries,
                        Some(&offset.clone()),
                        input_sessions,
                        universe_session,
                        values_to_key,
                        snapshot_writer,
                        connector_monitor,
                    );

                    let (offset_key, offset_value) = offset;
                    if has_persistent_storage && *backfilling_finished {
                        offsets_by_time_writer
                            .lock()
                            .unwrap()
                            .entry(self.current_timestamp.clone())
                            .or_insert(OffsetAntichain::new())
                            .advance_offset(offset_key, offset_value);
                    }
                }
            },
            Entry::RewindFinishSentinel => {
                *backfilling_finished = true;
                let parsed_entries = vec![ParsedEvent::AdvanceTime];
                self.on_parsed_data(
                    parsed_entries,
                    None, // no key generation for time advancement
                    input_sessions,
                    universe_session,
                    values_to_key,
                    snapshot_writer,
                    connector_monitor,
                );
            }
            Entry::Snapshot(snapshot) => match snapshot {
                SnapshotEvent::Insert(key, value) => {
                    Self::on_insert(key, value, input_sessions, universe_session);
                }
                SnapshotEvent::Delete(key, value) => {
                    Self::on_remove(key, value, input_sessions, universe_session);
                }
                SnapshotEvent::AdvanceTime(_) | SnapshotEvent::Finished => {
                    unreachable!()
                }
            },
        }
    }

    /*
        The implementation for non-str pulls.
    */
    pub fn run_with_custom_reader(
        &mut self,
        custom_reader: &mut dyn CustomReader,
        input_sessions: &mut [InputSession<Timestamp, (Key, Value), isize>],
        universe_session: &mut InputSession<Timestamp, Key, isize>,
        mut values_to_key: impl FnMut(Option<Vec<Value>>, Option<&Offset>) -> Key,
        snapshot_writer: &mut Option<SharedSnapshotWriter>,
    ) {
        loop {
            match custom_reader.acquire_custom_data() {
                (Ok(entries), maybe_offset) => self.on_parsed_data(
                    entries,
                    maybe_offset.as_ref(),
                    input_sessions,
                    universe_session,
                    &mut values_to_key,
                    snapshot_writer,
                    &mut None,
                ),
                (Err(e), _) => {
                    error!("Read data parsed unsuccessfully. {e}");
                }
            };
        }
    }

    /*
        The implementation for push model of data acquisition: the source of the data notifies us, when the new
        data arrive.

        The callback takes the read result, which is basically the string of raw data.
    */
    #[allow(clippy::too_many_arguments)]
    pub fn on_data(
        &mut self,
        raw_read_data: &ReaderContext,
        offset: Option<&Offset>,
        parser: &mut dyn Parser,
        input_sessions: &mut [InputSession<Timestamp, (Key, Value), isize>],
        universe_session: &mut InputSession<Timestamp, Key, isize>,
        values_to_key: impl FnMut(Option<Vec<Value>>, Option<&Offset>) -> Key,
        snapshot_writer: &mut Option<SharedSnapshotWriter>,
    ) {
        match parser.parse(raw_read_data) {
            Ok(entries) => self.on_parsed_data(
                entries,
                offset,
                input_sessions,
                universe_session,
                values_to_key,
                snapshot_writer,
                &mut None,
            ),
            Err(e) => {
                error!("Read data parsed unsuccessfully. {e}");
            }
        }
    }

    fn on_insert(
        key: Key,
        values: Vec<Value>,
        input_sessions: &mut [InputSession<Timestamp, (Key, Value), isize>],
        universe_session: &mut InputSession<Timestamp, Key, isize>,
    ) {
        universe_session.insert(key);
        for (input_session, value) in zip(input_sessions.iter_mut(), values) {
            input_session.insert((key, value));
        }
    }

    fn on_remove(
        key: Key,
        values: Vec<Value>,
        input_sessions: &mut [InputSession<Timestamp, (Key, Value), isize>],
        universe_session: &mut InputSession<Timestamp, Key, isize>,
    ) {
        universe_session.remove(key);
        for (input_session, value) in zip(input_sessions.iter_mut(), values) {
            input_session.remove((key, value));
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn on_parsed_data(
        &mut self,
        parsed_entries: Vec<ParsedEvent>,
        offset: Option<&Offset>,
        input_sessions: &mut [InputSession<Timestamp, (Key, Value), isize>],
        universe_session: &mut InputSession<Timestamp, Key, isize>,
        mut values_to_key: impl FnMut(Option<Vec<Value>>, Option<&Offset>) -> Key,
        snapshot_writer: &mut Option<SharedSnapshotWriter>,
        connector_monitor: &mut Option<&mut ConnectorMonitor>,
    ) {
        for entry in parsed_entries {
            match entry {
                ParsedEvent::Insert((raw_key, values)) => {
                    if values.len() != input_sessions.len() {
                        error!("There are {} tokens in the entry, but the count of the provided input sessions was {}", values.len(), input_sessions.len());
                        continue;
                    }
                    let key = values_to_key(raw_key, offset);
                    if let Some(snapshot_writer) = snapshot_writer {
                        // TODO: if the usage of Mutex+Arc hits the performance, add a buffered accessor here
                        // It must accumulate the data to the extent of the chunk size, and then unlock the mutex
                        // once and send the full chunk
                        if let Err(e) = snapshot_writer
                            .lock()
                            .unwrap()
                            .write(&SnapshotEvent::Insert(key, values.clone()))
                        {
                            error!("Failed to save row ({key}, {values:?}) in persistent buffer. Error: {e}");
                        }
                    }
                    Self::on_insert(key, values, input_sessions, universe_session);
                    if let Some(ref mut connector_monitor) = connector_monitor {
                        connector_monitor.increment();
                    }
                }
                ParsedEvent::Delete((raw_key, values)) => {
                    if values.len() != input_sessions.len() {
                        error!("There are {} tokens in the entry, but the count of the provided input sessions was {}", values.len(), input_sessions.len());
                        continue;
                    }
                    let key = values_to_key(raw_key, offset);
                    if let Some(snapshot_writer) = snapshot_writer {
                        if let Err(e) = snapshot_writer
                            .lock()
                            .unwrap()
                            .write(&SnapshotEvent::Delete(key, values.clone()))
                        {
                            error!("Failed to save row ({key}, {values:?}) in persistent buffer. Error: {e}");
                        }
                    }
                    Self::on_remove(key, values, input_sessions, universe_session);
                    if let Some(ref mut connector_monitor) = connector_monitor {
                        connector_monitor.increment();
                    }
                }
                ParsedEvent::AdvanceTime => {
                    let time_advanced = self.advance_time(input_sessions, universe_session);
                    if let Some(ref mut connector_monitor) = connector_monitor {
                        connector_monitor.commit();
                    }
                    if let Some(snapshot_writer) = snapshot_writer {
                        if let Err(e) = snapshot_writer
                            .lock()
                            .unwrap()
                            .write(&SnapshotEvent::AdvanceTime(time_advanced))
                        {
                            error!("Failed to save time advancement ({time_advanced}) in persistent buffer. Error: {e}");
                        }
                    }
                }
            };
        }
    }
}
