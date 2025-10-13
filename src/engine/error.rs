// Copyright © 2024 Pathway

use std::any::Any;
use std::error;
use std::fmt;
use std::result;

use super::ColumnPath;
use super::{Key, Value};
use crate::connectors::synchronization::Error as InputSynchronizationError;
use crate::persistence::Error as PersistenceBackendError;

use crate::connectors::data_storage::{ReadError, WriteError};
use crate::external_integration::IndexingError;

#[allow(clippy::module_name_repetitions)]
pub type DynError = Box<dyn error::Error + Send + Sync>;
pub type DynResult<T> = result::Result<T, DynError>;

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    #[error("iteration limit too small")]
    IterationLimitTooSmall,

    #[error("invalid universe handle")]
    InvalidUniverseHandle,

    #[error("invalid column handle")]
    InvalidColumnHandle,

    #[error("invalid table handle")]
    InvalidTableHandle,

    #[error("invalid error log handle")]
    InvalidErrorLogHandle,

    #[error("invalid grouper handle")]
    InvalidGrouperHandle,

    #[error("invalid joiner handle")]
    InvalidJoinerHandle,

    #[error("invalid ixer handle")]
    InvalidIxerHandle,

    #[error("invalid concat handle")]
    InvalidConcatHandle,

    #[error("invalid flatten handle")]
    InvalidFlattenHandle,

    #[error("invalid venn universes handle")]
    InvalidVennUniversesHandle,

    #[error("invalid column path: {0:?}")]
    InvalidColumnPath(ColumnPath),

    #[error("properties of two columns with the same path are not equal")]
    InconsistentColumnProperties,

    #[error("it is not allowed to use ids when creating table properties")]
    IdInTableProperties,

    #[error("graph not in scope")]
    GraphNotInScope,

    #[error("wrong join type")]
    BadJoinType,

    #[error("wrong ix key policy")]
    BadIxKeyPolicy,

    #[error("context not in scope")]
    ContextNotInScope,

    #[error("graph is not capable of IO")]
    IoNotPossible,

    #[error("graph is not capable of iteration")]
    IterationNotPossible,

    #[error("operation is not supported inside iterate")]
    NotSupportedInIteration,

    #[error("length mismatch")]
    LengthMismatch,

    #[error("different lengths of join condition")]
    DifferentJoinConditionLengths,

    #[error("universe mismatch")]
    UniverseMismatch,

    #[error("worker panic: {0}")]
    WorkerPanic(String),

    #[error("other worker panicked")]
    OtherWorkerPanic,

    #[error("dataflow error: {0}")]
    Dataflow(String),

    #[error("index out of bounds")]
    IndexOutOfBounds,

    #[error("this method cannot extract from key, use extract instead")]
    ExtractFromValueNotSupportedForKey,

    #[error("persistence backend failed: {0}")]
    PersistentStorageError(#[from] PersistenceBackendError),

    #[error(transparent)]
    Other(DynError),

    #[error("{inner}\n{trace}")]
    WithTrace {
        #[source]
        inner: DynError,
        trace: Trace,
    },

    #[error("snapshot writer failed: {0}")]
    SnapshotWriterError(#[source] Box<WriteError>),

    #[error("reader failed: {0:?}")]
    ReaderFailed(#[source] Box<ReadError>),

    #[error("computation of imported table failed")]
    ImportedTableFailed,

    #[error("operator_id not set")]
    OperatorIdNotSet,

    #[error(transparent)]
    DataError(DataError),

    #[error("column {name} is not present in schema. Schema keys are: {schema_keys:?}")]
    FieldNotInSchema {
        name: String,
        schema_keys: Vec<String>,
    },

    #[error("input synchronization failed: {0}")]
    InputSynchronization(#[from] InputSynchronizationError),

    #[error("indexing has failed: {0}")]
    Indexing(#[from] IndexingError),

    #[error("precision for HyperLogLogPlus should be between 4 and 18 but is {0}")]
    HyperLogLogPlusInvalidPrecision(usize),

    #[error("exactly once join is not supported in iteration")]
    ExactlyOnceJoinNotSupportedInIteration,
}

const OTHER_WORKER_ERROR_MESSAGES: [&str; 3] = [
    "MergeQueue poisoned.",
    "timely communication error: reading data: socket closed",
    "Send thread panic: Any { .. }",
];

impl Error {
    pub fn from_panic_payload(panic_payload: Box<dyn Any + Send + 'static>) -> Self {
        let message = match panic_payload.downcast::<&'static str>() {
            Ok(message) => message.to_string(),
            Err(panic_payload) => match panic_payload.downcast::<String>() {
                Ok(message) => *message,
                Err(panic_payload) => format!("{panic_payload:?}"),
            },
        };
        if OTHER_WORKER_ERROR_MESSAGES.contains(&message.as_str()) {
            Self::OtherWorkerPanic
        } else {
            Self::WorkerPanic(message)
        }
    }

    pub fn downcast<E: error::Error + 'static>(self) -> Result<E, Self> {
        match self {
            Self::Other(inner) => match inner.downcast::<E>() {
                Ok(error) => Ok(*error),
                Err(other) => Err(Self::Other(other)),
            },
            other => Err(other),
        }
    }

    pub fn with_trace(error: impl Into<DynError>, trace: Trace) -> Self {
        Self::WithTrace {
            inner: error.into(),
            trace,
        }
    }
}

impl From<DynError> for Error {
    fn from(value: DynError) -> Self {
        match value.downcast::<Self>() {
            Ok(this) => *this,
            Err(other) => match other.downcast::<DataError>() {
                Ok(data_error) => Self::DataError(*data_error),
                Err(other) => Self::Other(other),
            },
        }
    }
}

impl From<DataError> for Error {
    fn from(value: DataError) -> Self {
        match value {
            DataError::Other(error) => Self::Other(error),
            value => Self::DataError(value),
        }
    }
}

pub type Result<T, E = Error> = result::Result<T, E>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Trace {
    Frame {
        line: String,
        file_name: String,
        line_number: u32,
        function: String,
    },
    Empty,
}

impl fmt::Display for Trace {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match &self {
            Self::Frame {
                line,
                file_name,
                line_number,
                function,
            } => write!(
                f,
                "Occurred here:\n \tLine: {line}\n \tFile: {file_name}:{line_number}\n \tFunction: {function}"
            ),
            Self::Empty => write!(f, ""),
        }
    }
}

pub const STANDARD_OBJECT_LENGTH_LIMIT: usize = 500;

pub fn limit_length(s: String, max_length: usize) -> String {
    if s.len() > max_length {
        s.chars().take(max_length - 3).collect::<String>() + "..."
    } else {
        s
    }
}

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
#[allow(clippy::module_name_repetitions)]
pub enum DataError {
    #[error("value missing")]
    ValueMissing,

    #[error("key missing in input table: {0}")]
    KeyMissingInInputTable(Key),

    #[error("key missing in output table: {0}")]
    KeyMissingInOutputTable(Key),

    #[error("missing key: {0}")]
    MissingKey(Key),

    #[error("duplicate key: {0}")]
    DuplicateKey(Key),

    #[error("value error: {0}")]
    ValueError(String),

    #[error("type mismatch: expected {expected}, got {value:?}")]
    TypeMismatch {
        expected: &'static str,
        value: Value,
    },

    #[error("column type mismatch: expected {expected}, got {actual}")]
    ColumnTypeMismatch {
        expected: &'static str,
        actual: &'static str,
    },

    #[error("index out of bounds")]
    IndexOutOfBounds,

    #[error("division by zero")]
    DivisionByZero,

    #[error("parse error: {0}")]
    ParseError(String),

    #[error("date time conversion error")]
    DateTimeConversionError,

    #[error("Error value in column")]
    ErrorInValue,

    #[error("Error value encountered in filter condition, skipping the row")]
    ErrorInFilter,

    #[error("Error value encountered in reindex as new id, skipping the row")]
    ErrorInReindex,

    #[error("Error value encountered in join condition, skipping the row")]
    ErrorInJoin,

    #[error("Error value encountered in grouping columns, skipping the row")]
    ErrorInGroupby,

    #[error("Error value encountered in deduplicate instance, skipping the row")]
    ErrorInDeduplicate,

    #[error("Error value encountered in output, skipping the row")]
    ErrorInOutput,

    #[error("Error value encountered in index update, skipping the row")]
    ErrorInIndexUpdate,

    #[error("Error value encountered in index search, can't answer the query")]
    ErrorInIndexSearch,

    #[error("{reducer_type}::init() failed for {value:?} of key {source_key:?}")]
    ReducerInitializationError {
        reducer_type: String,
        value: Value,
        source_key: Key,
    },

    #[error("More than one distinct value passed to the unique reducer: {value_1:?}, {value_2:?}")]
    MoreThanOneValueInUniqueReducer { value_1: Value, value_2: Value },

    #[error("mixing types in npsum is not allowed")]
    MixingTypesInNpSum,

    #[error("updating a row that does not exist, key: {0}")]
    UpdatingNonExistingRow(Key),

    #[error("Expected deletion of a row with key: {0}, but got insertion instead.")]
    ExpectedDeletion(Key),

    #[error("Expected table to be append-only, but got deletion for key: {0}.")]
    ExpectedAppendOnly(Key),

    #[error("Expected table to be append-only, but got diff={1} for key: {0}.")]
    AppendOnlyViolation(Key, isize),

    #[error("Repeated entry in a batch.")]
    RepeatedEntryInBatch,

    #[error(transparent)]
    Other(DynError),
}

pub type DataResult<T, E = DataError> = result::Result<T, E>;

impl From<DynError> for DataError {
    fn from(value: DynError) -> Self {
        match value.downcast::<Self>() {
            Ok(this) => *this,
            Err(other) => Self::Other(other),
        }
    }
}

pub fn register_custom_panic_hook() {
    // custom hook to avoid polluting output with "MergeQueue poisoned"
    // messages that result from a different worker failure
    let prev = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |panic_info| {
        let payload = panic_info.payload();
        let message = match payload.downcast_ref::<&'static str>() {
            Some(message) => Some(*message),
            None => payload.downcast_ref::<String>().map(String::as_str),
        };
        if message.is_none_or(|message| !OTHER_WORKER_ERROR_MESSAGES.contains(&message)) {
            prev(panic_info);
        }
    }));
}
