pub mod file_like;
pub mod kafka;
pub mod sqlite;

#[allow(clippy::module_name_repetitions)]
pub use file_like::FileLikeMetadata;

#[allow(clippy::module_name_repetitions)]
pub use kafka::KafkaMetadata;

#[allow(clippy::module_name_repetitions)]
pub use sqlite::SQLiteMetadata;

#[allow(clippy::module_name_repetitions)]
#[derive(Debug)]
pub enum SourceMetadata {
    FileLike(FileLikeMetadata),
    Kafka(KafkaMetadata),
    SQLite(SQLiteMetadata),
}

impl From<FileLikeMetadata> for SourceMetadata {
    fn from(impl_: FileLikeMetadata) -> Self {
        Self::FileLike(impl_)
    }
}

impl From<KafkaMetadata> for SourceMetadata {
    fn from(impl_: KafkaMetadata) -> Self {
        Self::Kafka(impl_)
    }
}

impl From<SQLiteMetadata> for SourceMetadata {
    fn from(impl_: SQLiteMetadata) -> Self {
        Self::SQLite(impl_)
    }
}

impl SourceMetadata {
    pub fn serialize(&self) -> serde_json::Value {
        match self {
            Self::FileLike(meta) => serde_json::to_value(meta),
            Self::Kafka(meta) => serde_json::to_value(meta),
            Self::SQLite(meta) => serde_json::to_value(meta),
        }
        .expect("Internal JSON serialization error")
    }

    pub fn commits_allowed_in_between(&self) -> bool {
        match self {
            Self::FileLike(_) | Self::SQLite(_) => false,
            Self::Kafka(_) => true,
        }
    }
}
