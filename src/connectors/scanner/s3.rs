use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt::Debug;
use std::str::from_utf8;
use std::time::SystemTime;

use arcstr::ArcStr;
use log::{info, warn};
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
use rayon::{ThreadPool, ThreadPoolBuilder};

use crate::connectors::metadata::FileLikeMetadata;
use crate::connectors::scanner::{PosixLikeScanner, QueuedAction};
use crate::connectors::ReadError;
use crate::persistence::cached_object_storage::CachedObjectStorage;
use crate::retry::{execute_with_retries, RetryConfig};

use s3::bucket::Bucket as S3Bucket;
use s3::request::request_trait::ResponseData as S3ResponseData;
use s3::serde_types::ListBucketResult as S3ListBucketResult;

const MAX_S3_RETRIES: usize = 2;
const S3_PATH_PREFIXES: [&str; 2] = ["s3://", "s3a://"];

struct S3DownloadedObject {
    path: ArcStr,
    contents: Vec<u8>,
    metadata: Option<FileLikeMetadata>,
}

impl S3DownloadedObject {
    fn new(path: ArcStr, contents: Vec<u8>, metadata: Option<FileLikeMetadata>) -> Self {
        Self {
            path,
            contents,
            metadata,
        }
    }

    fn set_metadata(mut self, metadata: FileLikeMetadata) -> Self {
        self.metadata = Some(metadata);
        self
    }
}

type S3DownloadResult = Result<S3DownloadedObject, ReadError>;

#[derive(Debug)]
#[allow(clippy::module_name_repetitions)]
pub enum S3CommandName {
    ListObjectsV2,
    GetObject,
    DeleteObject,
    InitiateMultipartUpload,
    PutMultipartChunk,
    CompleteMultipartUpload,
}

#[allow(clippy::module_name_repetitions)]
pub struct S3Scanner {
    /*
        This class takes responsibility over S3 object selection and streaming.
        In encapsulates the selection of the next object to stream and streaming
        the object and provides reader end of the pipe to the outside user.
    */
    bucket: S3Bucket,
    objects_prefix: String,
    pending_modifications: HashMap<String, Vec<u8>>,
    downloader_pool: ThreadPool,
}

impl PosixLikeScanner for S3Scanner {
    fn object_metadata(
        &mut self,
        object_path: &[u8],
    ) -> Result<Option<FileLikeMetadata>, ReadError> {
        let path = from_utf8(object_path).expect("S3 path are expected to be UTF-8 strings");
        let object_lists = execute_with_retries(
            || self.bucket.list(path.to_string(), None),
            RetryConfig::default(),
            MAX_S3_RETRIES,
        )
        .map_err(|e| ReadError::S3(S3CommandName::ListObjectsV2, e))?;
        for list in object_lists {
            for object in &list.contents {
                if object.key != path {
                    continue;
                }
                let metadata = FileLikeMetadata::from_s3_object(object);
                if metadata.modified_at.is_some() {
                    return Ok(Some(metadata));
                }
            }
        }
        Ok(None)
    }

    fn read_object(&mut self, object_path: &[u8]) -> Result<Vec<u8>, ReadError> {
        let path = from_utf8(object_path).expect("S3 path are expected to be UTF-8 strings");
        if let Some(prepared_object) = self.pending_modifications.remove(path) {
            Ok(prepared_object)
        } else {
            let downloaded_object = Self::stream_object_from_path_and_bucket(path, &self.bucket)?;
            Ok(downloaded_object.contents)
        }
    }

    fn next_scanner_actions(
        &mut self,
        are_deletions_enabled: bool,
        cached_object_storage: &CachedObjectStorage,
    ) -> Result<Vec<QueuedAction>, ReadError> {
        let object_lists: Vec<S3ListBucketResult> = execute_with_retries(
            || self.bucket.list(self.objects_prefix.to_string(), None),
            RetryConfig::default(),
            MAX_S3_RETRIES,
        )
        .map_err(|e| ReadError::S3(S3CommandName::ListObjectsV2, e))?;
        let mut result = Vec::new();
        let mut seen_object_keys = HashSet::new();
        let mut pending_modification_download_tasks = Vec::new();
        for list in object_lists {
            for object in &list.contents {
                seen_object_keys.insert(object.key.clone());
                let actual_metadata = FileLikeMetadata::from_s3_object(object);
                let object_key = object.key.as_bytes();
                if let Some(stored_metadata) = cached_object_storage.stored_metadata(object_key) {
                    let is_updated = stored_metadata.is_changed(&actual_metadata);
                    if is_updated && are_deletions_enabled {
                        pending_modification_download_tasks.push(actual_metadata);
                    }
                } else {
                    pending_modification_download_tasks.push(actual_metadata);
                }
            }
        }
        let pending_modification_objects = self.download_bulk(&pending_modification_download_tasks);
        for object in pending_modification_objects {
            match object {
                Ok(object) => {
                    let object_path = object.path.to_string();
                    let is_update = cached_object_storage.contains_object(object_path.as_bytes());
                    if is_update {
                        result.push(QueuedAction::Update(
                            object_path.as_bytes().into(),
                            object.metadata.as_ref().unwrap().clone(),
                        ));
                    } else {
                        result.push(QueuedAction::Read(
                            object_path.as_bytes().into(),
                            object.metadata.as_ref().unwrap().clone(),
                        ));
                    }
                    self.pending_modifications
                        .insert(object_path.clone(), object.contents);
                }
                Err(e) => {
                    warn!("Failed to fetch the modified version of the object: {e}. It will be retried with the next bulk of updates.");
                }
            }
        }
        if are_deletions_enabled {
            for (object_path, _) in cached_object_storage.get_iter() {
                let object_path = from_utf8(object_path).expect("S3 paths must be UTF8-compatible");
                if !seen_object_keys.contains(object_path) {
                    result.push(QueuedAction::Delete(object_path.as_bytes().into()));
                }
            }
        }
        Ok(result)
    }

    fn short_description(&self) -> String {
        format!("S3({})", self.objects_prefix)
    }
}

#[allow(clippy::module_name_repetitions)]
impl S3Scanner {
    pub fn new(
        bucket: S3Bucket,
        objects_prefix: impl Into<String>,
        downloader_threads_count: usize,
    ) -> Result<Self, ReadError> {
        let objects_prefix = objects_prefix.into();

        let object_lists = execute_with_retries(
            || bucket.list(objects_prefix.clone(), None),
            RetryConfig::default(),
            MAX_S3_RETRIES,
        )
        .map_err(|e| ReadError::S3(S3CommandName::ListObjectsV2, e))?;

        let mut has_nonempty_list = false;
        for list in object_lists {
            if !list.contents.is_empty() {
                has_nonempty_list = true;
                break;
            }
        }
        if !has_nonempty_list {
            return Err(ReadError::NoObjectsToRead);
        }

        Ok(S3Scanner {
            bucket,
            objects_prefix,
            downloader_pool: ThreadPoolBuilder::new()
                .num_threads(downloader_threads_count)
                .build()
                .expect("Failed to create downloader pool"),
            pending_modifications: HashMap::new(),
        })
    }

    pub fn deduce_bucket_and_path(s3_path: &str) -> (Option<String>, String) {
        for prefix in S3_PATH_PREFIXES {
            let Some(bucket_and_path) = s3_path.strip_prefix(prefix) else {
                continue;
            };
            let (bucket, path) = bucket_and_path
                .split_once('/')
                .unwrap_or((bucket_and_path, ""));
            return (Some(bucket.to_string()), path.to_string());
        }

        (None, s3_path.to_string())
    }

    pub fn download_object_from_path_and_bucket(
        object_path_ref: &str,
        bucket: &S3Bucket,
    ) -> Result<S3ResponseData, ReadError> {
        let (_, deduced_path) = Self::deduce_bucket_and_path(object_path_ref);
        execute_with_retries(
            || bucket.get_object(&deduced_path), // returns Err on incorrect status code because fail-on-err feature is enabled
            RetryConfig::default(),
            MAX_S3_RETRIES,
        )
        .map_err(|e| ReadError::S3(S3CommandName::GetObject, e))
    }

    fn stream_object_from_path_and_bucket(
        object_path_ref: &str,
        bucket: &S3Bucket,
    ) -> S3DownloadResult {
        let object_path = object_path_ref.to_string();
        let response = Self::download_object_from_path_and_bucket(&object_path, bucket)?;

        Ok(S3DownloadedObject::new(
            object_path_ref.to_string().into(),
            response.bytes().to_vec(),
            None,
        ))
    }

    fn download_bulk(&mut self, new_objects: &[FileLikeMetadata]) -> Vec<S3DownloadResult> {
        if new_objects.is_empty() {
            return Vec::with_capacity(0);
        }
        info!("Downloading a bulk of {} objects", new_objects.len());
        let downloading_started_at = SystemTime::now();
        let new_objects_downloaded: Vec<S3DownloadResult> = self.downloader_pool.install(|| {
            new_objects
                .par_iter()
                .map(|task| {
                    Self::stream_object_from_path_and_bucket(&task.path, &self.bucket)
                        .map(|result| result.set_metadata(task.clone()))
                })
                .collect()
        });
        info!("Downloading done in {:?}", downloading_started_at.elapsed());
        new_objects_downloaded
    }
}
