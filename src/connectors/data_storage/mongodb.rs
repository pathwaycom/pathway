use log::warn;
use std::borrow::Cow;
use std::collections::HashMap;
use std::mem::take;
use std::time::Duration;

use mongodb::bson::{doc, Document as BsonDocument};
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
}

impl MongoWriter {
    pub fn new(
        namespace: MongoNamespace,
        client: MongoClient,
        collection: MongoCollection<BsonDocument>,
        max_batch_size: Option<usize>,
        snapshot_mode: bool,
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

    fn capture_start_token(&self) -> Result<ResumeToken, ReadError> {
        const MAX_TOKEN_FETCH_ATTEMPTS: usize = 5;
        // Open a fresh change stream just to read the current oplog position.
        // We use `start_at_operation_time(now)` so the cursor is anchored to
        // a definite point in time even if the initial aggregate response
        // happens to omit `postBatchResumeToken`.  We then drive `next_if_any`
        // up to a few times: each call ends with the driver having processed
        // either an event or an empty getMore response, and in both cases the
        // `postBatchResumeToken` carried by that response is cached as the
        // stream's current resume token.  Any event the cursor returns is
        // safe to discard — `dump_collection` reads the post-event state and
        // `catchup_replay` starts strictly after the returned token.
        let mut stream = self
            .collection()
            .watch()
            .max_await_time(Duration::from_millis(200))
            .run()?;

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

    /// Extract the `_data` hex string bytes from a `ResumeToken` for ordering
    /// comparisons. `MongoDB` guarantees that `_data` values are lexicographically
    /// ordered by oplog position. We must compare only `_data`, not the full BSON
    /// document, because `kHighWaterMark` tokens (from `capture_start_token`) and
    /// `kEventCandidate` tokens (from event `_id`s) have different `_data` string
    /// lengths. Their serialized BSON documents therefore have different lengths, so
    /// a raw `bson::to_vec` byte comparison is dominated by the document-length
    /// prefix rather than the actual oplog position.
    fn token_order_key(token: &ResumeToken) -> Vec<u8> {
        let raw = Self::token_bytes(token);
        mongodb::bson::from_slice::<mongodb::bson::Document>(&raw)
            .ok()
            .and_then(|doc| doc.get_str("_data").ok().map(|s| s.as_bytes().to_vec()))
            .unwrap_or(raw)
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

    fn catchup_replay(
        &self,
        snapshot: &mut Vec<MongoSnapshotEntry>,
        start_token: ResumeToken,
    ) -> Result<ResumeToken, ReadError> {
        const MAX_CONSECUTIVE_EMPTIES: usize = 4;
        // Capture "end mark" — the resume token of the current oplog tip.
        // We open a second stream just for this, then close it immediately.
        let end_token = self.capture_start_token()?;
        if Self::token_order_key(&start_token) >= Self::token_order_key(&end_token) {
            return Ok(end_token);
        }

        // Use a large batch_size so a single getMore can carry many historical
        // events.  The driver still splits into multiple batches if needed;
        // the loop below polls `next_if_any` until the stream's resume token
        // has advanced past `end_token`.  `max_await_time` is kept short so
        // that the empty-tail case (no events between runs) returns quickly.
        let mut stream = self
            .collection()
            .watch()
            .start_after(start_token)
            .full_document(FullDocumentType::UpdateLookup)
            .max_await_time(Duration::from_millis(500))
            .batch_size(10_000_u32)
            .run()?;

        let mut last_token: Option<ResumeToken> = None;

        // Drive the stream until its resume token has advanced past `end_token`.
        //
        // `next_if_any` returns `None` for either `BatchValue::Empty` (the
        // server returned a getMore with no documents within `max_await_time`,
        // typically meaning we've drained the historical backlog and are now
        // tailing the live oplog tip) or `BatchValue::Exhausted` (cursor closed).
        // For both we re-check `resume_token`: MongoDB advances it on every
        // getMore, including empty ones, so once it has crossed `end_token` we
        // know all events up to that mark have been observed and it's safe to
        // stop.  Otherwise we keep polling, with a small sleep between empty
        // attempts and a hard cap on consecutive empties so a stuck server
        // can't hang the connector.
        let mut empties = 0usize;
        loop {
            match stream.next_if_any() {
                Ok(Some(event)) => {
                    empties = 0;
                    let current_token = event.id.clone();

                    Self::apply_event_to_snapshot(snapshot, &event)?;
                    last_token = Some(current_token.clone());

                    // Stop once we've reached (or passed) the end mark.
                    if Self::token_order_key(&current_token) >= Self::token_order_key(&end_token) {
                        break;
                    }
                }
                Ok(None) => {
                    // `resume_token` reflects the latest oplog position the
                    // stream has observed, even on empty getMore responses.
                    if let Some(rt) = stream.resume_token() {
                        if Self::token_order_key(&rt) >= Self::token_order_key(&end_token) {
                            last_token = Some(rt);
                            break;
                        }
                    }
                    empties += 1;
                    if empties >= MAX_CONSECUTIVE_EMPTIES {
                        // The server is taking too long to advance.  Return
                        // whatever progress we made; the live change stream
                        // (in streaming mode) will keep filling the gap, and
                        // a subsequent persistence resume in static mode will
                        // continue from the last applied event.
                        break;
                    }
                    std::thread::sleep(Duration::from_millis(50));
                }
                Err(e) => return Err(ReadError::MongoDb(classify_mongo_error(e))),
            }
        }

        Ok(last_token.unwrap_or(end_token))
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
        if let Some(token_bytes) = self.saved_resume_token.take() {
            // Resuming from a persisted oplog position.  Find the current oplog tip,
            // then replay every change-stream event that arrived between the saved
            // token and that tip.  All replayed entries share the single `end_token`
            // offset, so they must be delivered as an atomic transactional block —
            // otherwise a mid-batch commit would persist `end_token` before the
            // remaining in-memory entries are processed, and a crash at that point
            // would drop them (the main change stream resumes strictly after
            // `end_token`).  Wrapping in NewSource/FinishedSource tells the engine
            // not to commit until the whole block is consumed.
            let saved_token = mongodb::bson::from_slice::<ResumeToken>(&token_bytes)
                .map_err(|_| MongoDbError::CorruptResumeToken)?;
            let mut entries = Vec::new();
            let end_token = self.catchup_replay(&mut entries, saved_token)?;
            self.snapshot = Self::build_snapshot_block(entries, &end_token);
            self.is_initialized = true;

            if self.mode.is_polling_enabled() {
                self.change_stream = Some(
                    self.collection()
                        .watch()
                        .start_after(end_token)
                        .full_document(FullDocumentType::UpdateLookup)
                        .max_await_time(Duration::from_secs(1))
                        .run()?,
                );
            }
            return Ok(());
        }

        // Cold start: capture the current oplog position, dump the full collection,
        // then replay any writes that arrived during the dump.
        let start_token = self.capture_start_token()?;
        let mut entries = self.dump_collection()?;
        let resume_token = self.catchup_replay(&mut entries, start_token)?;

        // Each entry carries the oplog token so seek() can restore the exact position
        // on restart, enabling the catch-up replay path above.
        self.snapshot = Self::build_snapshot_block(entries, &resume_token);
        self.is_initialized = true;

        if self.mode.is_polling_enabled() {
            self.change_stream = Some(
                self.collection()
                    .watch()
                    .start_after(resume_token)
                    .full_document(FullDocumentType::UpdateLookup)
                    .max_await_time(Duration::from_secs(1))
                    .run()?,
            );
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
