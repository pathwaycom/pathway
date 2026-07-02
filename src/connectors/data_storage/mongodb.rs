use log::warn;
use std::borrow::Cow;
use std::collections::HashMap;
use std::mem::take;
use std::time::Duration;

use mongodb::bson::{doc, Document as BsonDocument, Timestamp};
use mongodb::change_stream::event::{ChangeStreamEvent, OperationType, ResumeToken};
use mongodb::options::{
    DeleteOneModel as MongoDeleteOneModel, FullDocumentType, UpdateOneModel as MongoUpdateOneModel,
    WriteModel as MongoWriteModel,
};
use mongodb::options::{ReadConcern, ReadPreference, SelectionCriteria};
use mongodb::{
    sync::ChangeStream, sync::Client as MongoClient, sync::Collection as MongoCollection,
    Namespace as MongoNamespace,
};

use crate::connectors::data_format::bson::serialize_value_to_bson;
use crate::connectors::data_format::FormatterContext;
use crate::connectors::data_storage::{
    CommitPossibility, ConnectorMode, DataEventType, ReaderContext,
};
use crate::connectors::metadata::MongoDbMetadata;
use crate::connectors::offset::{OffsetKey, OffsetValue};
use crate::connectors::{ReadError, ReadResult, Reader, StorageType, WriteError, Writer};
use crate::engine::{Key, Value};
use crate::persistence::frontier::OffsetAntichain;

#[derive(Debug)]
enum BufferedMongoEvent {
    Upsert {
        key: BsonDocument,
        value: BsonDocument,
    },
    Delete {
        key: BsonDocument,
    },
}

/// Errors specific to `MongoDB` connector operations.
///
/// Using a dedicated enum rather than wrapping `mongodb::error::Error` directly
/// lets the engine surface actionable messages for the most common failure
/// modes that users encounter when persistence is enabled.
#[derive(Debug, thiserror::Error)]
pub enum MongoDbError {
    /// General `MongoDB` driver or server error not covered by a more specific variant.
    #[error(transparent)]
    Driver(#[from] mongodb::error::Error),

    /// The persisted change stream resume token could not be decoded from the
    /// stored bytes, or was rejected by the server as syntactically invalid
    /// (`MongoDB` error code 260 `InvalidResumeToken`).  The persistence data may
    /// be corrupted.  To recover, delete the persistence directory and restart.
    #[error(
        "the persisted MongoDB change stream resume token is corrupt and cannot be decoded; \
         delete the persistence directory and restart to trigger a full collection rescan"
    )]
    CorruptResumeToken,

    /// The `MongoDB` oplog history at the persisted position has been purged
    /// (`MongoDB` error code 286 `ChangeStreamHistoryLost`).  The change stream
    /// cannot resume from the saved token.  To recover, delete the persistence
    /// directory and restart.
    #[error(
        "the MongoDB oplog history at the persisted position has been purged (oplog rotated); \
         delete the persistence directory and restart to trigger a full collection rescan"
    )]
    OplogRotated,

    /// The collection is not backed by a replica set, so change streams are
    /// unavailable.  Streaming and persistence both require a replica set.
    #[error("replication is not configured; MongoDB change streams require a replica set")]
    ReplicationNotConfigured,

    /// The change stream delivered a DDL event that permanently terminates the
    /// stream: the collection was dropped, renamed, the database was dropped,
    /// or the server explicitly invalidated the cursor.  The persisted resume
    /// token is no longer meaningful — events that arrive on a future
    /// (re)created collection with the same name are not visible through this
    /// token.  To recover, delete the persistence directory and restart to
    /// trigger a full collection rescan.
    #[error(
        "MongoDB change stream terminated: a {0} event was emitted on the watched \
         namespace, which permanently ends the underlying cursor (this happens on \
         collection drop, collection rename, database drop, and server-issued \
         invalidate). The saved resume token cannot be extended past this point — \
         starting a new change stream from it would only re-emit the terminating \
         event, so any further inserts/updates on a recreated collection with the \
         same name would be silently lost. Pathway therefore stops the pipeline \
         instead of continuing with stale state. To recover, delete the persistence \
         directory and restart the pipeline; this triggers a full rescan of \
         whatever collection now lives at the same namespace."
    )]
    StreamTerminated(String),
}

const MONGODB_SET_PARAMETER: &str = "$set";
const MONGODB_PRIMARY_KEY_NAME: &str = "_id";

/// `MongoDB` server error code: the supplied resume token is syntactically or
/// semantically invalid.  Maps to [`MongoDbError::CorruptResumeToken`].
const MONGO_ERR_INVALID_RESUME_TOKEN: i32 = 260;

/// `MongoDB` server error code: the oplog at the persisted position has been
/// purged.  Maps to [`MongoDbError::OplogRotated`].
const MONGO_ERR_CHANGE_STREAM_HISTORY_LOST: i32 = 286;

/// Classify a raw `MongoDB` driver error into one of the more specific
/// [`MongoDbError`] variants when the server error code is known.
fn classify_mongo_error(e: mongodb::error::Error) -> MongoDbError {
    if let mongodb::error::ErrorKind::Command(ref cmd_err) = *e.kind {
        match cmd_err.code {
            MONGO_ERR_CHANGE_STREAM_HISTORY_LOST => return MongoDbError::OplogRotated,
            MONGO_ERR_INVALID_RESUME_TOKEN => return MongoDbError::CorruptResumeToken,
            _ => {}
        }
    }
    MongoDbError::Driver(e)
}

/// Buffer strategy depends on the output mode:
///
/// - In `stream_of_changes` mode every minibatch event is preserved — updates
///   emit both the `-1` retraction of the old row and the `+1` insertion of the
///   new row, so we must not deduplicate by key.
/// - In `snapshot` mode we only want the *net* effect per key within a batch,
///   so we collapse by key and let the last +1 win over earlier deletes.
enum WriteBuffer {
    StreamOfChanges(Vec<BsonDocument>),
    Snapshot(HashMap<Key, BufferedMongoEvent>),
}

impl WriteBuffer {
    fn is_empty(&self) -> bool {
        match self {
            Self::StreamOfChanges(v) => v.is_empty(),
            Self::Snapshot(m) => m.is_empty(),
        }
    }

    fn len(&self) -> usize {
        match self {
            Self::StreamOfChanges(v) => v.len(),
            Self::Snapshot(m) => m.len(),
        }
    }

    fn clear(&mut self) {
        match self {
            Self::StreamOfChanges(v) => v.clear(),
            Self::Snapshot(m) => m.clear(),
        }
    }
}

pub struct MongoWriter {
    namespace: MongoNamespace,
    client: MongoClient,
    collection: MongoCollection<BsonDocument>,
    buffer: WriteBuffer,
    max_batch_size: Option<usize>,
    single_threaded: bool,
}

impl MongoWriter {
    pub fn new(
        namespace: MongoNamespace,
        client: MongoClient,
        collection: MongoCollection<BsonDocument>,
        max_batch_size: Option<usize>,
        snapshot_mode: bool,
        sorted_output: bool,
    ) -> Self {
        let buffer = if snapshot_mode {
            WriteBuffer::Snapshot(HashMap::new())
        } else {
            WriteBuffer::StreamOfChanges(Vec::new())
        };
        Self {
            namespace,
            client,
            collection,
            buffer,
            max_batch_size,
            // A global `sort_by` order spans the whole minibatch, so it can only
            // be honored by a single writer; otherwise MongoDB writes shard
            // freely across workers (see `single_threaded`).
            single_threaded: sorted_output,
        }
    }

    fn flush_stream_of_changes(&mut self, docs: &mut [BsonDocument]) -> Result<(), WriteError> {
        let prepared_buffer: Vec<_> = docs.iter_mut().collect();
        let command = self.collection.insert_many(prepared_buffer);
        let _ = command.run()?;
        Ok(())
    }

    fn flush_snapshot(
        &mut self,
        entries: &mut HashMap<Key, BufferedMongoEvent>,
    ) -> Result<(), WriteError> {
        let prepared_buffer: Vec<_> = entries
            .values_mut()
            .map(|entry| match entry {
                BufferedMongoEvent::Upsert { key, value } => {
                    let mut update_payload = BsonDocument::new();
                    update_payload.insert(MONGODB_SET_PARAMETER, value);

                    MongoWriteModel::UpdateOne(
                        MongoUpdateOneModel::builder()
                            .namespace(self.namespace.clone())
                            .filter(take(key))
                            .update(update_payload)
                            .upsert(true)
                            .build(),
                    )
                }
                BufferedMongoEvent::Delete { key } => MongoWriteModel::DeleteOne(
                    MongoDeleteOneModel::builder()
                        .namespace(self.namespace.clone())
                        .filter(take(key))
                        .build(),
                ),
            })
            .collect();

        let command = self.client.bulk_write(prepared_buffer);
        let _ = command.run()?;
        Ok(())
    }
}

impl Writer for MongoWriter {
    fn write(&mut self, data: FormatterContext) -> Result<(), WriteError> {
        for payload in data.payloads {
            match &mut self.buffer {
                WriteBuffer::Snapshot(map) => {
                    // Last-event-wins per key within a minibatch. Overriding on
                    // both `+1` and `-1` gives the correct net effect for any
                    // sequence: `+1,-1` collapses to Delete, `-1,+1` collapses
                    // to Upsert, `-1,+1,-1` collapses to Delete, and so on.
                    let mut key = BsonDocument::new();
                    let _ = key.insert(
                        MONGODB_PRIMARY_KEY_NAME,
                        serialize_value_to_bson(&Value::Pointer(data.key))?,
                    );
                    let event = if data.diff == 1 {
                        BufferedMongoEvent::Upsert {
                            key,
                            value: payload.into_bson_document()?,
                        }
                    } else {
                        BufferedMongoEvent::Delete { key }
                    };
                    map.insert(data.key, event);
                }
                WriteBuffer::StreamOfChanges(docs) => {
                    docs.push(payload.into_bson_document()?);
                }
            }
        }
        if let Some(max_batch_size) = self.max_batch_size {
            if self.buffer.len() >= max_batch_size {
                self.flush(true)?;
            }
        }
        Ok(())
    }

    fn flush(&mut self, _forced: bool) -> Result<(), WriteError> {
        if self.buffer.is_empty() {
            return Ok(());
        }

        let result = match &mut self.buffer {
            WriteBuffer::StreamOfChanges(docs) => {
                let mut docs = take(docs);
                self.flush_stream_of_changes(&mut docs)
            }
            WriteBuffer::Snapshot(map) => {
                let mut map = take(map);
                self.flush_snapshot(&mut map)
            }
        };
        self.buffer.clear();

        result
    }

    fn single_threaded(&self) -> bool {
        // MongoDB writes shard freely across workers: the engine shards output by
        // row key (`Shard for (Key, _)` hashes the key only), so every event for a
        // given `_id` — insert, delete, and both halves of an update — is handled
        // by one worker, and `MongoWriter` keeps no cross-worker global state
        // (stream-of-changes appends independent documents; snapshot issues
        // per-`_id` upserts/deletes via `bulk_write`). Disjoint `_id` ownership
        // rules out conflicting writes.
        //
        // The only exception is a global `sort_by` order: it spans the whole
        // minibatch, so a single writer is required to produce it. That case is
        // captured at construction time in `self.single_threaded` (set from whether
        // the sink was given a `sort_by`).
        self.single_threaded
    }

    fn name(&self) -> String {
        format!("MongoDB({})", self.collection.name())
    }
}

#[derive(Debug)]
pub enum MongoSnapshotEntry {
    Upsert { key: String, document: BsonDocument },
    Delete { key: String },
}

impl MongoSnapshotEntry {
    fn into_context(self) -> ReaderContext {
        match self {
            Self::Upsert { key, document } => {
                ReaderContext::Bson((DataEventType::Insert, key, document))
            }
            Self::Delete { key } => {
                ReaderContext::Bson((DataEventType::Delete, key, BsonDocument::new()))
            }
        }
    }
}

/// Where the change-stream cursor should begin.
enum StreamStart {
    /// Cold start: anchor to a definite cluster time captured *before* the
    /// collection dump.  Unlike opening a cursor with no anchor (which starts
    /// at the fuzzy "cursor established" moment and can miss events committed
    /// around that point under load), this guarantees every event at or after
    /// the timestamp is delivered, with no gap against the dump.
    OperationTime(Timestamp),
    /// Persistence resume: continue strictly after the saved oplog position.
    AfterToken(ResumeToken),
}

pub struct MongoReader {
    namespace: MongoNamespace,
    client: MongoClient,
    collection: MongoCollection<BsonDocument>,
    snapshot: Vec<ReadResult>,
    change_stream: Option<ChangeStream<ChangeStreamEvent<BsonDocument>>>,
    mode: ConnectorMode,
    is_initialized: bool,
    /// Persisted oplog position from a previous run. When set, `initialize()`
    /// skips the full collection dump and resumes the change stream directly.
    saved_resume_token: Option<Vec<u8>>,
}

impl MongoReader {
    pub fn new(
        namespace: MongoNamespace,
        client: MongoClient,
        collection: MongoCollection<BsonDocument>,
        mode: ConnectorMode,
    ) -> Result<Self, ReadError> {
        client
            .database(&namespace.db)
            .run_command(doc! { "ping": 1 })
            .run()?;

        Ok(Self {
            namespace,
            client,
            collection,
            mode,
            snapshot: Vec::new(),
            change_stream: None,
            is_initialized: false,
            saved_resume_token: None,
        })
    }

    fn collection(&self) -> MongoCollection<BsonDocument> {
        self.client
            .database(&self.namespace.db)
            .collection(self.collection.name())
    }

    /// Read a change stream's current oplog position (resume token) without
    /// assuming anything about pending events.  The initial `watch` aggregate
    /// usually carries a `postBatchResumeToken`, returned immediately; if it
    /// doesn't, drive `next_if_any` a few times so an empty getMore populates
    /// it.  Used on a cold start to anchor the snapshot offset to the *already
    /// open* live cursor's starting point — so nothing can fall between the
    /// position read here and live tailing.  If the token never appears the
    /// collection is not on a replica set and change streams are unavailable.
    fn fetch_resume_token(
        stream: &mut ChangeStream<ChangeStreamEvent<BsonDocument>>,
    ) -> Result<ResumeToken, ReadError> {
        const MAX_TOKEN_FETCH_ATTEMPTS: usize = 5;
        if let Some(token) = stream.resume_token() {
            return Ok(token);
        }
        for _ in 0..MAX_TOKEN_FETCH_ATTEMPTS {
            let _ = stream.next_if_any()?;
            if let Some(token) = stream.resume_token() {
                return Ok(token);
            }
        }
        Err(MongoDbError::ReplicationNotConfigured.into())
    }

    /// Read the current cluster operation time, used to anchor a cold-start
    /// cursor to a definite point before the dump (see [`StreamStart`]).  On a
    /// replica set every command response carries `operationTime`; its absence
    /// means change streams are unavailable.
    fn current_operation_time(&self) -> Result<Timestamp, ReadError> {
        let response = self
            .client
            .database(&self.namespace.db)
            .run_command(doc! { "ping": 1 })
            .run()?;
        response
            .get_timestamp("operationTime")
            .map_err(|_| MongoDbError::ReplicationNotConfigured.into())
    }

    /// Open the change stream used for both catch-up and live tailing.
    /// `UpdateLookup` makes replace/update events carry the full post-image, and
    /// the await time bounds how long a live `next()` blocks between polls.
    fn open_change_stream(
        &self,
        start: StreamStart,
    ) -> Result<ChangeStream<ChangeStreamEvent<BsonDocument>>, ReadError> {
        let collection = self.collection();
        let builder = collection
            .watch()
            .full_document(FullDocumentType::UpdateLookup)
            .max_await_time(Duration::from_secs(1));
        let stream = match start {
            StreamStart::OperationTime(ts) => builder.start_at_operation_time(ts).run()?,
            StreamStart::AfterToken(token) => builder.start_after(token).run()?,
        };
        Ok(stream)
    }

    fn doc_id_string(doc: &BsonDocument) -> String {
        doc.get("_id")
            .map_or_else(|| "<no-id>".to_owned(), ToString::to_string)
    }

    fn dump_collection(&self) -> Result<Vec<MongoSnapshotEntry>, ReadError> {
        let cursor = self
            .collection()
            .find(doc! {})
            .read_concern(ReadConcern::majority())
            .selection_criteria(SelectionCriteria::ReadPreference(
                ReadPreference::SecondaryPreferred {
                    options: Option::default(),
                },
            ))
            .batch_size(1000u32)
            .run()?;

        let mut snapshot = Vec::new();
        for result in cursor {
            let document = result?;
            snapshot.push(MongoSnapshotEntry::Upsert {
                key: Self::doc_id_string(&document),
                document,
            });
        }
        Ok(snapshot)
    }

    /// Serialize a `ResumeToken` to BSON bytes for storage in offset snapshots.
    /// These bytes can be round-tripped via `bson::from_slice::<ResumeToken>`.
    fn token_bytes(token: &ResumeToken) -> Vec<u8> {
        mongodb::bson::to_vec(token).unwrap_or_default()
    }

    fn apply_event_to_snapshot(
        snapshot: &mut Vec<MongoSnapshotEntry>,
        event: &ChangeStreamEvent<BsonDocument>,
    ) -> Result<(), ReadError> {
        let id = event
            .document_key
            .as_ref()
            .map_or_else(String::default, Self::doc_id_string);

        match &event.operation_type {
            OperationType::Insert | OperationType::Replace | OperationType::Update => {
                if let Some(doc) = &event.full_document {
                    snapshot.push(MongoSnapshotEntry::Upsert {
                        key: id,
                        document: doc.clone(),
                    });
                }
                Ok(())
            }
            OperationType::Delete => {
                snapshot.push(MongoSnapshotEntry::Delete { key: id });
                Ok(())
            }
            // Stream-terminating DDL events: the saved resume token cannot be
            // meaningfully extended past these, so any further events — including
            // events on a future recreated collection with the same name — would
            // be silently lost.  Surface this as an error so the user gets an
            // actionable message instead of empty output.
            terminating @ (OperationType::Drop
            | OperationType::Rename
            | OperationType::DropDatabase
            | OperationType::Invalidate) => {
                Err(MongoDbError::StreamTerminated(format!("{terminating:?}")).into())
            }
            // Any other future event type we don't know about: ignore with a
            // warning to stay forward-compatible with the driver.
            other => {
                warn!("Unhandled operation type in snapshot: {other:?}");
                Ok(())
            }
        }
    }

    /// Fold the change-stream backlog into `entries` until the cursor is caught
    /// up to the current oplog tip, then leave it positioned for live tailing.
    ///
    /// Catch-up events are applied to the in-memory snapshot rather than emitted
    /// as live changes so that, on a cold start, events overlapping the
    /// collection dump are consolidated with it inside a single minibatch (a
    /// re-applied unchanged upsert nets to nothing) instead of producing
    /// spurious retraction/insert churn in a later minibatch.  "Caught up" is
    /// detected by consecutive empty getMores: a change stream returns empty
    /// only once it has delivered every event up to the majority-committed tip
    /// (`next_if_any` already blocks up to the stream's `max_await_time`).  The
    /// same cursor is then reused by `read()` for live events, so — unlike the
    /// previous capture-token/re-open design — there is no resume-token handoff
    /// for a concurrently arriving event to slip through.
    fn drain_catchup(
        stream: &mut ChangeStream<ChangeStreamEvent<BsonDocument>>,
        entries: &mut Vec<MongoSnapshotEntry>,
        start_token: ResumeToken,
    ) -> Result<ResumeToken, ReadError> {
        const MAX_CONSECUTIVE_EMPTIES: usize = 2;
        let mut last_token = start_token;
        let mut empties = 0usize;
        loop {
            match stream.next_if_any() {
                Ok(Some(event)) => {
                    empties = 0;
                    last_token = event.id.clone();
                    Self::apply_event_to_snapshot(entries, &event)?;
                }
                Ok(None) => {
                    // `resume_token` advances on every getMore, including empty
                    // ones, so it tracks the oplog position even with no events.
                    if let Some(rt) = stream.resume_token() {
                        last_token = rt;
                    }
                    empties += 1;
                    if empties >= MAX_CONSECUTIVE_EMPTIES {
                        break;
                    }
                }
                Err(e) => return Err(ReadError::MongoDb(classify_mongo_error(e))),
            }
        }
        Ok(last_token)
    }

    fn build_snapshot_block(
        entries: Vec<MongoSnapshotEntry>,
        token: &ResumeToken,
    ) -> Vec<ReadResult> {
        let token_bytes = Self::token_bytes(token);
        let mut prepared = Vec::with_capacity(entries.len() + 2);
        prepared.push(ReadResult::NewSource(MongoDbMetadata::new().into()));
        for entry in entries {
            prepared.push(ReadResult::Data(
                entry.into_context(),
                (
                    OffsetKey::MongoDb,
                    OffsetValue::MongoDbOplogToken(token_bytes.clone()),
                ),
            ));
        }
        prepared.push(ReadResult::FinishedSource {
            commit_possibility: CommitPossibility::Possible,
        });
        // Reverse so we can serve entries in order via pop().
        prepared.reverse();
        prepared
    }

    fn initialize(&mut self) -> Result<(), ReadError> {
        // One durable change-stream cursor is opened up front and used for BOTH
        // the initial catch-up and subsequent live tailing.  Opening it before
        // the snapshot is taken (cold start) or at the saved position (resume)
        // guarantees there is no window between "snapshot taken" and "live
        // stream attached" in which a concurrently committed event could be
        // lost: the earlier design opened short-lived streams to capture oplog
        // tokens and then re-opened a separate live stream with `start_after`,
        // and under concurrent load an event landing around that handoff could
        // slip through and never be delivered.
        //
        // Catch-up events are folded into the snapshot block (delivered as one
        // atomic NewSource/FinishedSource transaction) so the dump overlap is
        // consolidated within a single minibatch.
        let (start_token, mut stream, mut entries) = if let Some(token_bytes) =
            self.saved_resume_token.take()
        {
            // Resume: tail strictly after the persisted position and replay
            // only the delta — the full collection dump is not repeated.
            let saved_token = mongodb::bson::from_slice::<ResumeToken>(&token_bytes)
                .map_err(|_| MongoDbError::CorruptResumeToken)?;
            let stream = self.open_change_stream(StreamStart::AfterToken(saved_token.clone()))?;
            (saved_token, stream, Vec::new())
        } else {
            // Cold start: capture a definite cluster time, open the cursor
            // anchored at it, THEN dump.  The anchor sits at or before the
            // dump's snapshot, so the union of "dump" and "cursor from the
            // anchor" leaves no gap — every event is in one, the other, or
            // both (the overlap is folded into the snapshot block).
            let op_time = self.current_operation_time()?;
            let mut stream = self.open_change_stream(StreamStart::OperationTime(op_time))?;
            let start_token = Self::fetch_resume_token(&mut stream)?;
            let entries = self.dump_collection()?;
            (start_token, stream, entries)
        };

        // The resulting token is carried by every snapshot entry so seek() can
        // restore the exact oplog position on restart (the resume path above).
        let resume_token = Self::drain_catchup(&mut stream, &mut entries, start_token)?;
        self.snapshot = Self::build_snapshot_block(entries, &resume_token);
        self.is_initialized = true;

        if self.mode.is_polling_enabled() {
            self.change_stream = Some(stream);
        }

        Ok(())
    }
}

impl Reader for MongoReader {
    fn read(&mut self) -> Result<ReadResult, ReadError> {
        while self.snapshot.is_empty() {
            match self.change_stream.as_mut() {
                None => {
                    if self.is_initialized {
                        return Ok(ReadResult::Finished);
                    }
                    self.initialize()?;
                }
                Some(change_stream) => match change_stream.next() {
                    Some(Ok(event)) => {
                        let token_bytes = Self::token_bytes(
                            &change_stream
                                .resume_token()
                                .unwrap_or_else(|| event.id.clone()),
                        );
                        let offset = (
                            OffsetKey::MongoDb,
                            OffsetValue::MongoDbOplogToken(token_bytes),
                        );
                        let mut entries = Vec::new();
                        Self::apply_event_to_snapshot(&mut entries, &event)?;
                        for entry in entries {
                            self.snapshot
                                .push(ReadResult::Data(entry.into_context(), offset.clone()));
                        }
                    }
                    Some(Err(e)) => {
                        return Err(ReadError::MongoDb(classify_mongo_error(e)));
                    }
                    None => {}
                },
            }
        }

        Ok(self.snapshot.pop().unwrap())
    }

    fn seek(&mut self, frontier: &OffsetAntichain) -> Result<(), ReadError> {
        if let Some(OffsetValue::MongoDbOplogToken(bytes)) =
            frontier.get_offset(&OffsetKey::MongoDb)
        {
            self.saved_resume_token = Some(bytes.clone());
        }
        Ok(())
    }

    fn short_description(&self) -> Cow<'static, str> {
        format!("MongoDB({})", self.collection.name()).into()
    }

    fn storage_type(&self) -> StorageType {
        StorageType::MongoDb
    }
}
