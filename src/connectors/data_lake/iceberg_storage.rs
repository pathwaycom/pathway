// Iceberg 0.9 split storage backends out of the core crate. Catalogs no longer
// pick a backend automatically from the path scheme; they require an explicit
// `StorageFactory`. Pathway's connectors handle paths from multiple schemes
// (local fs for `tabulario/iceberg-rest` test setups, `s3://` for Glue and
// production REST → S3 deployments), so this factory builds a `Storage` that
// dispatches per call by URI scheme.

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use iceberg::io::{
    FileMetadata, FileRead, FileWrite, InputFile, LocalFsStorage, OutputFile, Storage,
    StorageConfig, StorageFactory,
};
use iceberg::Result as IcebergResult;
use iceberg_storage_opendal::OpenDalStorageFactory;
use serde::{Deserialize, Serialize};

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct PathwayStorageFactory;

#[typetag::serde(name = "PathwayStorageFactory")]
impl StorageFactory for PathwayStorageFactory {
    fn build(&self, config: &StorageConfig) -> IcebergResult<Arc<dyn Storage>> {
        Ok(Arc::new(PathwayMultiStorage {
            config: config.clone(),
        }))
    }
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct PathwayMultiStorage {
    config: StorageConfig,
}

impl PathwayMultiStorage {
    fn dispatch(&self, path: &str) -> IcebergResult<Arc<dyn Storage>> {
        if path.starts_with("s3://") {
            OpenDalStorageFactory::S3 {
                configured_scheme: "s3".to_string(),
                customized_credential_load: None,
            }
            .build(&self.config)
        } else if path.starts_with("s3a://") {
            OpenDalStorageFactory::S3 {
                configured_scheme: "s3a".to_string(),
                customized_credential_load: None,
            }
            .build(&self.config)
        } else {
            Ok(Arc::new(LocalFsStorage::new()))
        }
    }
}

#[typetag::serde(name = "PathwayMultiStorage")]
#[async_trait]
impl Storage for PathwayMultiStorage {
    async fn exists(&self, path: &str) -> IcebergResult<bool> {
        self.dispatch(path)?.exists(path).await
    }

    async fn metadata(&self, path: &str) -> IcebergResult<FileMetadata> {
        self.dispatch(path)?.metadata(path).await
    }

    async fn read(&self, path: &str) -> IcebergResult<Bytes> {
        self.dispatch(path)?.read(path).await
    }

    async fn reader(&self, path: &str) -> IcebergResult<Box<dyn FileRead>> {
        self.dispatch(path)?.reader(path).await
    }

    async fn write(&self, path: &str, bs: Bytes) -> IcebergResult<()> {
        self.dispatch(path)?.write(path, bs).await
    }

    async fn writer(&self, path: &str) -> IcebergResult<Box<dyn FileWrite>> {
        self.dispatch(path)?.writer(path).await
    }

    async fn delete(&self, path: &str) -> IcebergResult<()> {
        self.dispatch(path)?.delete(path).await
    }

    async fn delete_prefix(&self, path: &str) -> IcebergResult<()> {
        self.dispatch(path)?.delete_prefix(path).await
    }

    fn new_input(&self, path: &str) -> IcebergResult<InputFile> {
        self.dispatch(path)?.new_input(path)
    }

    fn new_output(&self, path: &str) -> IcebergResult<OutputFile> {
        self.dispatch(path)?.new_output(path)
    }
}
