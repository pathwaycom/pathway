// Copyright © 2026 Pathway

use std::io::{Error as IoError, ErrorKind};
use std::time::Duration;

use opendal::layers::RetryLayer;
use opendal::raw::oio::{Delete, List, Read, Write};
use opendal::raw::{Access, Accessor, HttpClient, OpDelete, OpList, OpRead, OpWrite};
use opendal::services::Azblob;
use opendal::Operator;
use tokio::runtime::Runtime as TokioRuntime;

use crate::async_runtime::create_async_tokio_runtime;
use crate::persistence::backends::PersistenceBackend;
use crate::persistence::Error;

use super::{BackendPutFuture, BackgroundObjectUploader};

const DEFAULT_CONTENT_TYPE: &str = "application/x-binary";

#[allow(clippy::module_name_repetitions)]
pub struct AzureKVStorage {
    root_path: String,
    // Use the raw accessor: Operator normalizes repeated slashes and whitespace,
    // but those characters are significant in existing Azure checkpoint names.
    store: Accessor,
    runtime: TokioRuntime,
    background_uploader: BackgroundObjectUploader,
}

impl std::fmt::Debug for AzureKVStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AzureKVStorage")
            .field("root_path", &self.root_path)
            .finish_non_exhaustive()
    }
}

impl AzureKVStorage {
    pub fn new(
        root_path: &str,
        account: &str,
        container: &str,
        account_key: &str,
    ) -> Result<Self, Error> {
        if root_path.is_empty()
            || root_path.starts_with('/')
            || root_path.split('/').any(|part| matches!(part, "." | ".."))
        {
            return Err(IoError::new(
                ErrorKind::InvalidInput,
                "Azure persistence requires a nonempty relative root path without dot segments",
            )
            .into());
        }
        let root_path = if root_path.ends_with('/') {
            root_path.to_owned()
        } else {
            format!("{root_path}/")
        };
        let store = Self::create_store(account, container, account_key)?;
        let uploader_runtime = create_async_tokio_runtime()?;
        let uploader_root_path = root_path.clone();
        // Each runtime owns its HTTP pool, including its connection-driving tasks.
        let uploader_store = Self::create_store(account, container, account_key)?;
        let upload_object = move |key: String, value: Vec<u8>| {
            let path = format!("{uploader_root_path}{key}");
            uploader_runtime.block_on(async {
                let options = OpWrite::default().with_content_type(DEFAULT_CONTENT_TYPE);
                let (_, mut writer) = uploader_store.write(&path, options).await?;
                writer.write(value.into()).await?;
                writer.close().await?;
                Ok(())
            })
        };

        Ok(Self {
            root_path,
            store,
            runtime: create_async_tokio_runtime()?,
            background_uploader: BackgroundObjectUploader::new(upload_object),
        })
    }

    fn create_store(
        account: &str,
        container: &str,
        account_key: &str,
    ) -> Result<Accessor, opendal::Error> {
        let http_client = reqwest_opendal::Client::builder()
            .timeout(Duration::from_secs(60))
            .build()
            .map_err(|error| {
                opendal::Error::new(
                    opendal::ErrorKind::Unexpected,
                    "could not create Azure HTTP client",
                )
                .set_source(error)
            })?;
        let builder = Azblob::default()
            .endpoint(&format!("https://{account}.blob.core.windows.net"))
            .container(container)
            .account_name(account)
            .account_key(account_key);
        let store = Operator::new(builder)?
            .layer(
                RetryLayer::new()
                    .with_min_delay(Duration::from_millis(200))
                    .with_max_times(8)
                    .with_jitter(),
            )
            .finish()
            .into_inner();
        store
            .info()
            .update_http_client(|_| HttpClient::with(http_client));
        Ok(store)
    }
}

impl PersistenceBackend for AzureKVStorage {
    fn list_keys(&self) -> Result<Vec<String>, Error> {
        self.runtime.block_on(async {
            let mut result = Vec::new();
            // The lister follows continuation tokens until every page is read.
            let (_, mut blobs) = self
                .store
                .list(&self.root_path, OpList::default().with_recursive(true))
                .await?;
            while let Some(blob) = blobs.next().await? {
                let key = blob.path().strip_prefix(&self.root_path).ok_or_else(|| {
                    IoError::new(
                        ErrorKind::InvalidData,
                        "Azure returned a blob outside the requested prefix",
                    )
                })?;
                result.push(key.to_owned());
            }
            Ok(result)
        })
    }

    fn get_value(&self, key: &str) -> Result<Vec<u8>, Error> {
        let path = format!("{}{key}", self.root_path);
        self.runtime.block_on(async {
            let (_, mut reader) = self.store.read(&path, OpRead::default()).await?;
            Ok(reader.read_all().await?.to_vec())
        })
    }

    fn put_value(&self, key: &str, value: Vec<u8>) -> BackendPutFuture {
        self.background_uploader
            .upload_object(key.to_string(), value)
    }

    fn remove_key(&self, key: &str) -> Result<(), Error> {
        let path = format!("{}{key}", self.root_path);
        self.runtime.block_on(async {
            let (_, mut deleter) = self.store.delete().await?;
            deleter.delete(&path, OpDelete::default()).await?;
            deleter.close().await?;
            Ok(())
        })
    }
}
