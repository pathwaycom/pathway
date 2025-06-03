// Copyright © 2024 Pathway

use azure_storage::StorageCredentials;
use azure_storage_blobs::prelude::{BlobClient, ClientBuilder};
use futures::stream::StreamExt;
use tokio::runtime::Runtime as TokioRuntime;

use crate::async_runtime::create_async_tokio_runtime;
use crate::persistence::backends::PersistenceBackend;
use crate::persistence::Error;
use crate::retry::{execute_with_retries, RetryConfig};

use super::{BackendPutFuture, BackgroundObjectUploader};

const MAX_AZURE_RETRIES: usize = 2;
const DEFAULT_CONTENT_TYPE: &str = "application/x-binary";

#[derive(Debug)]
#[allow(clippy::module_name_repetitions)]
pub struct AzureKVStorage {
    root_path: String,
    account: String,
    container: String,
    credentials: StorageCredentials,
    runtime: TokioRuntime,
    background_uploader: BackgroundObjectUploader,
}

impl AzureKVStorage {
    pub fn new(
        root_path: &str,
        account: String,
        container: String,
        credentials: StorageCredentials,
    ) -> Result<Self, Error> {
        let mut root_path_prepared = root_path.to_string();
        if !root_path.ends_with('/') {
            root_path_prepared += "/";
        }
        let root_path = root_path_prepared;

        let uploader_runtime = create_async_tokio_runtime()?;

        let uploader_root_path = root_path.clone();
        let uploader_account = account.clone();
        let uploader_container = container.clone();
        let uploader_credentials = credentials.clone();
        let upload_object = move |key: String, value: Vec<u8>| {
            let blob_client = Self::create_blob_client_with_credentials(
                &uploader_root_path,
                &uploader_account,
                &uploader_container,
                uploader_credentials.clone(),
                &key,
            );

            let _ = execute_with_retries(
                || {
                    uploader_runtime.block_on(async {
                        blob_client
                            .put_block_blob(value.clone())
                            .content_type(DEFAULT_CONTENT_TYPE)
                            .await
                    })
                },
                RetryConfig::default(),
                MAX_AZURE_RETRIES,
            )?;

            Ok(())
        };

        Ok(Self {
            root_path,
            account,
            container,
            credentials,
            runtime: create_async_tokio_runtime()?,
            background_uploader: BackgroundObjectUploader::new(upload_object),
        })
    }

    fn create_blob_client_with_credentials(
        root_path: &str,
        account: &str,
        container: &str,
        credentials: StorageCredentials,
        key: &str,
    ) -> BlobClient {
        ClientBuilder::new(account, credentials).blob_client(container, format!("{root_path}{key}"))
    }

    fn create_blob_client(&self, key: &str) -> BlobClient {
        Self::create_blob_client_with_credentials(
            &self.root_path,
            &self.account,
            &self.container,
            self.credentials.clone(),
            key,
        )
    }
}

impl PersistenceBackend for AzureKVStorage {
    fn list_keys(&self) -> Result<Vec<String>, Error> {
        let container_client = ClientBuilder::new(&self.account, self.credentials.clone())
            .container_client(&self.container);
        self.runtime.block_on(async {
            let mut result = Vec::new();
            let mut stream = container_client
                .list_blobs()
                .prefix(self.root_path.clone())
                .into_stream();

            while let Some(next_blobs) = stream.next().await {
                let blob_list = next_blobs?;
                let blobs = blob_list.blobs.blobs();
                for blob in blobs {
                    if blob.deleted.unwrap_or(false) {
                        continue;
                    }
                    result.push(blob.name[self.root_path.len()..].to_string());
                }
            }

            Ok(result)
        })
    }

    fn get_value(&self, key: &str) -> Result<Vec<u8>, Error> {
        let blob_client = self.create_blob_client(key);
        let mut result: Vec<u8> = vec![];
        let mut stream = blob_client.get().into_stream();
        self.runtime.block_on(async {
            while let Some(value) = stream.next().await {
                let mut body = value?.data;
                while let Some(value) = body.next().await {
                    let value = value?;
                    result.extend(&value);
                }
            }
            Ok(result)
        })
    }

    fn put_value(&mut self, key: &str, value: Vec<u8>) -> BackendPutFuture {
        self.background_uploader
            .upload_object(key.to_string(), value)
    }

    fn remove_key(&mut self, key: &str) -> Result<(), Error> {
        let blob_client = self.create_blob_client(key);
        self.runtime.block_on(async {
            let _ = blob_client.delete().await?;
            Ok(())
        })
    }
}
