pub mod file_like;
pub mod iceberg;
pub mod kafka;
pub mod mongodb;
pub mod parquet;
pub mod postgres;
pub mod sqlite;

#[allow(clippy::module_name_repetitions)]
pub use file_like::FileLikeMetadata;

#[allow(clippy::module_name_repetitions)]
pub use kafka::KafkaMetadata;

#[allow(clippy::module_name_repetitions)]
pub use iceberg::IcebergMetadata;

#[allow(clippy::module_name_repetitions)]
pub use mongodb::MongoDbMetadata;

#[allow(clippy::module_name_repetitions)]
pub use parquet::ParquetMetadata;

#[allow(clippy::module_name_repetitions)]
pub use postgres::PostgresMetadata;

#[allow(clippy::module_name_repetitions)]
pub use sqlite::SQLiteMetadata;

#[allow(clippy::module_name_repetitions)]
#[derive(Debug)]
pub enum SourceMetadata {
    FileLike(FileLikeMetadata),
    Kafka(KafkaMetadata),
    MongoDb(MongoDbMetadata),
    SQLite(SQLiteMetadata),
    Iceberg(IcebergMetadata),
    Parquet(ParquetMetadata),
    Postgres(PostgresMetadata),
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

impl From<IcebergMetadata> for SourceMetadata {
    fn from(impl_: IcebergMetadata) -> Self {
        Self::Iceberg(impl_)
    }
}

impl From<ParquetMetadata> for SourceMetadata {
    fn from(impl_: ParquetMetadata) -> Self {
        Self::Parquet(impl_)
    }
}

impl From<MongoDbMetadata> for SourceMetadata {
    fn from(impl_: MongoDbMetadata) -> Self {
        Self::MongoDb(impl_)
    }
}

impl From<PostgresMetadata> for SourceMetadata {
    fn from(impl_: PostgresMetadata) -> Self {
        Self::Postgres(impl_)
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
            Self::MongoDb(meta) => serde_json::to_value(meta),
            Self::SQLite(meta) => serde_json::to_value(meta),
            Self::Iceberg(meta) => serde_json::to_value(meta),
            Self::Parquet(meta) => serde_json::to_value(meta),
            Self::Postgres(meta) => serde_json::to_value(meta),
        }
        .expect("Internal JSON serialization error")
    }

    pub fn commits_allowed_in_between(&self) -> bool {
        match self {
            Self::FileLike(_)
            | Self::MongoDb(_)
            | Self::SQLite(_)
            | Self::Iceberg(_)
            | Self::Parquet(_)
            | Self::Postgres(_) => false,
            Self::Kafka(_) => true,
        }
    }
}
