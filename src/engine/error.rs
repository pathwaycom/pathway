use std::any::Any;
use std::error;
use std::fmt;
use std::result;

use super::{Key, Value};

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

    #[error("invalid requested columns")]
    InvalidRequestedColumns,

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

    #[error("wrong id assignment")]
    BadIdAssign,

    #[error("cannot compute empty intersection")]
    EmptyIntersection,

    #[error("length mismatch")]
    LengthMismatch,

    #[error("value missing")]
    ValueMissing,

    #[error("different lengths of join condition")]
    DifferentJoinConditionLengths,

    #[error("universe mismatch")]
    UniverseMismatch,

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

    #[error("key missing in universe: {0:#}")]
    KeyMissingInUniverse(Key),

    #[error("key missing in column: {0:#}")]
    KeyMissingInColumn(Key),

    #[error("duplicate key: {0:#}")]
    DuplicateKey(Key),

    #[error("worker panic: {0}")]
    WorkerPanic(String),

    #[error("dataflow error: {0}")]
    Dataflow(String),

    #[error("index out of bounds")]
    IndexOutOfBounds,

    #[error("division by zero")]
    DivisionByZero,

    #[error("parse error: {0}")]
    ParseError(String),

    #[error("date time conversion error")]
    DateTimeConversionError,

    #[error("value error: {0}")]
    ValueError(String),

    #[error(transparent)]
    Other(DynError),

    #[error("{inner}\n{trace}")]
    WithTrace {
        #[source]
        inner: DynError,
        trace: Trace,
    },
}

impl Error {
    pub fn from_panic_payload(panic_payload: Box<dyn Any + Send + 'static>) -> Self {
        let message = match panic_payload.downcast::<&'static str>() {
            Ok(message) => message.to_string(),
            Err(panic_payload) => match panic_payload.downcast::<String>() {
                Ok(message) => *message,
                Err(panic_payload) => format!("{panic_payload:?}"),
            },
        };
        Self::WorkerPanic(message)
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

    pub fn with_trace(error: DynError, trace: Trace) -> Self {
        Self::WithTrace {
            inner: error,
            trace,
        }
    }
}

impl From<DynError> for Error {
    fn from(value: DynError) -> Self {
        match value.downcast::<Self>() {
            Ok(this) => *this,
            Err(other) => Self::Other(other),
        }
    }
}

pub type Result<T, E = Error> = result::Result<T, E>;

#[derive(Debug, Clone)]
pub enum Trace {
    Frame {
        line: String,
        file_name: String,
        line_number: u32,
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
            } => write!(
                f,
                "Occurred here:\n \tLine: {line}\n \tFile: {file_name}:{line_number}"
            ),
            Self::Empty => write!(f, ""),
        }
    }
}
