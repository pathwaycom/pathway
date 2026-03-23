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

use crate::connectors::bson::serialize_value_to_bson;
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
    Insert {
        value: BsonDocument,
    },
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

pub struct MongoWriter {
    namespace: MongoNamespace,
    client: MongoClient,
    collection: MongoCollection<BsonDocument>,
    buffer: HashMap<Key, BufferedMongoEvent>,
    snapshot_mode: bool,
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
        Self {
            namespace,
            client,
            collection,
            max_batch_size,
            snapshot_mode,
            buffer: HashMap::new(),
        }
    }

    fn flush_stream_of_changes(&mut self) -> Result<(), WriteError> {
        let prepared_buffer: Vec<_> = self
            .buffer
            .values_mut()
            .map(|entry| match entry {
                BufferedMongoEvent::Insert { value } => value,
                other => unreachable!(
                    "Unexpected type of buffered entry for a stream of changes mode: {other:?}"
                ),
            })
            .collect();
        let command = self.collection.insert_many(prepared_buffer);
        let _ = command.run()?;
        Ok(())
    }

    fn flush_snapshot(&mut self) -> Result<(), WriteError> {
        let prepared_buffer: Vec<_> = self
            .buffer
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
                other @ BufferedMongoEvent::Insert { .. } => {
                    unreachable!("Unexpected type of buffered entry for a snapshot mode: {other:?}")
                }
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
            if self.snapshot_mode {
                let mut key = BsonDocument::new();
                let _ = key.insert(
                    MONGODB_PRIMARY_KEY_NAME,
                    serialize_value_to_bson(&Value::Pointer(data.key))?,
                );
                if data.diff == 1 {
                    self.buffer.insert(
                        data.key,
                        BufferedMongoEvent::Upsert {
                            key,
                            value: payload.into_bson_document()?,
                        },
                    );
                } else {
                    self.buffer
                        .entry(data.key)
                        .or_insert(BufferedMongoEvent::Delete { key });
                }
            } else {
                self.buffer.insert(
                    data.key,
                    BufferedMongoEvent::Insert {
                        value: payload.into_bson_document()?,
                    },
                );
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

        let result = if self.snapshot_mode {
            self.flush_snapshot()
        } else {
            self.flush_stream_of_changes()
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
        let stream = self.collection().watch().run()?;
        stream
            .resume_token()
            .ok_or(MongoDbError::ReplicationNotConfigured.into())
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
    ) {
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
            }
            OperationType::Delete => {
                snapshot.push(MongoSnapshotEntry::Delete { key: id });
            }
            // DDL-level events (drop, rename, ...). Log and skip for now.
            other => {
                warn!("Unhandled operation type in snapshot: {other:?}");
            }
        }
    }

    fn catchup_replay(
        &self,
        snapshot: &mut Vec<MongoSnapshotEntry>,
        start_token: ResumeToken,
    ) -> Result<ResumeToken, ReadError> {
        // Capture "end mark" — the resume token of the current oplog tip.
        // We open a second stream just for this, then close it immediately.
        let end_token = self.capture_start_token()?;
        if Self::token_order_key(&start_token) >= Self::token_order_key(&end_token) {
            return Ok(end_token);
        }

        let mut stream = self
            .collection()
            .watch()
            .start_after(start_token)
            .full_document(FullDocumentType::UpdateLookup)
            .max_await_time(Duration::from_millis(500))
            .run()?;

        let mut last_token: Option<ResumeToken> = None;

        // Use next_if_any() instead of next() because next() on a MongoDB sync
        // change stream never returns None — it keeps issuing getMore commands
        // indefinitely (the driver's stream_poll_next loops on BatchValue::Empty).
        // next_if_any() returns None as soon as the current batch is exhausted,
        // which is what we want: the historical events are in the initial aggregate
        // batch when startAfter is used with a high-watermark token, so draining
        // that batch is sufficient to collect all events up to end_token.
        loop {
            match stream.next_if_any() {
                Ok(Some(event)) => {
                    let current_token = event.id.clone();

                    Self::apply_event_to_snapshot(snapshot, &event);
                    last_token = Some(current_token.clone());

                    // Stop once we've reached (or passed) the end mark.
                    if Self::token_order_key(&current_token) >= Self::token_order_key(&end_token) {
                        break;
                    }
                }
                // Batch exhausted — no more buffered events available right now.
                Ok(None) => break,
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
            // Resuming from a persisted oplog position.
            // Find the current oplog tip, then replay every change-stream event
            // that arrived between the saved token and that tip. The entries are
            // placed directly into the snapshot queue as plain Data items — no
            // NewSource/FinishedSource wrapping. Live reading then continues from
            // the captured tip so there are no gaps and no duplicate delivery.
            let saved_token = mongodb::bson::from_slice::<ResumeToken>(&token_bytes)
                .map_err(|_| MongoDbError::CorruptResumeToken)?;
            let mut entries = Vec::new();
            let end_token = self.catchup_replay(&mut entries, saved_token)?;
            let end_token_bytes = Self::token_bytes(&end_token);

            let mut snapshot = Vec::with_capacity(entries.len());
            for entry in entries {
                snapshot.push(ReadResult::Data(
                    entry.into_context(),
                    (
                        OffsetKey::MongoDb,
                        OffsetValue::MongoDbOplogToken(end_token_bytes.clone()),
                    ),
                ));
            }
            snapshot.reverse();
            self.snapshot = snapshot;
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
                        Self::apply_event_to_snapshot(&mut entries, &event);
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
