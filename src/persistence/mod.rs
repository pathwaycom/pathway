use std::num::ParseIntError;

pub mod frontier;
pub mod storage;
pub mod tracker;

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    #[error("failed to deserialize offset from json: {0}")]
    IncorrectSerializedOffset(serde_json::Error),

    #[error("failed to deserialize last advanced timestamp: {0}")]
    IncorrectSerializedTimestamp(ParseIntError),

    #[error("failed to deserialize storage types: {0}")]
    IncorrectSerializedStorageTypes(serde_json::Error),

    #[error("unable to initialize offsets storage: {0}")]
    StorageInitializationFailed(&'static str, Option<std::io::Error>),

    #[error("the operated offsets aren't forming an antichain")]
    NotValidAntichain,

    #[error("the offsets dump contains duplicated persistent ids")]
    DuplicatePersistentId,

    #[error("the parsed offset storage entry doesn't form a key-value pair")]
    KeyValueIncorrect,

    #[error("persistent id is not an int")]
    PersistentIdNotUInt,

    #[error("offsets information file is broken: {0}")]
    OffsetsFileBroken(&'static str, Option<std::io::Error>),

    #[error("failed to save current offsets to a file: {0}")]
    SavingOffsetsFailed(&'static str, std::io::Error),
}

pub type PersistentId = u128;
