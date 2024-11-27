use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::fmt::Debug;
use std::io::Cursor;
use std::io::Read;
use std::mem::take;
use std::str::from_utf8;
use std::sync::Arc;
use std::thread::sleep;
use std::time::{Duration, SystemTime};

use arcstr::ArcStr;
use chrono::DateTime;
use log::{error, info};
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
use rayon::{ThreadPool, ThreadPoolBuilder};

use crate::connectors::data_storage::ConnectorMode;
use crate::connectors::scanner::PosixLikeScanner;
use crate::connectors::{DataEventType, ReadError, ReadResult};
use crate::deepcopy::DeepCopy;
use crate::retry::{execute_with_retries, RetryConfig};

use s3::bucket::Bucket as S3Bucket;
use s3::request::request_trait::ResponseData as S3ResponseData;

const MAX_S3_RETRIES: usize = 2;
const S3_PATH_PREFIXES: [&str; 2] = ["s3://", "s3a://"];
type S3DownloadedObject = (ArcStr, Cursor<Vec<u8>>);
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
    streaming_mode: ConnectorMode,
    current_object_path: Option<ArcStr>,
    processed_objects: HashSet<String>,
    objects_for_processing: VecDeque<String>,
    downloader_pool: ThreadPool,
    predownloaded_objects: HashMap<String, Cursor<Vec<u8>>>,
    is_queue_initialized: bool,
}

impl PosixLikeScanner for S3Scanner {
    fn data_event_type(&self) -> Option<DataEventType> {
        if self.current_object_path.is_some() {
            Some(DataEventType::Insert)
        } else {
            None
        }
    }

    fn current_object_reader(
        &mut self,
    ) -> Result<Option<Box<dyn Read + Send + 'static>>, ReadError> {
        let Some(path) = self.current_object_path.clone() else {
            return Ok(None);
        };
        let selected_object_path = from_utf8(path.as_bytes())
            .unwrap_or_else(|_| panic!("s3 path is not UTF-8 serializable: {path:?}"))
            .to_string();
        if let Some(prepared_object) = self.predownloaded_objects.remove(&selected_object_path) {
            let reader = Box::new(prepared_object);
            Ok(Some(reader))
        } else {
            // Expected to happen once for each case of persistent recovery.
            // If repeats multiple times, something is wrong.
            info!("No prepared object for {selected_object_path}. Downloading directly from S3.");
            let (_, object_contents) =
                Self::stream_object_from_path_and_bucket(&selected_object_path, &self.bucket)?;
            let reader = Box::new(object_contents);
            Ok(Some(reader))
        }
    }

    fn current_offset_object(&self) -> Option<Arc<[u8]>> {
        self.current_object_path
            .clone()
            .map(|x| x.as_bytes().into())
    }

    fn seek_to_object(&mut self, path: &[u8]) -> Result<(), ReadError> {
        let path: String = from_utf8(path)
            .unwrap_or_else(|_| panic!("s3 path is not UTF-8 serializable: {path:?}"))
            .to_string();
        self.processed_objects.clear();

        /*
            S3 bucket-list calls are considered expensive, because of that we do one.
            Then, two linear passes detect the objects which should be marked.
        */
        let object_lists = execute_with_retries(
            || self.bucket.list(self.objects_prefix.to_string(), None),
            RetryConfig::default(),
            MAX_S3_RETRIES,
        )
        .map_err(|e| ReadError::S3(S3CommandName::ListObjectsV2, e))?;

        let mut threshold_modification_time = None;
        for list in &object_lists {
            for object in &list.contents {
                if object.key == path {
                    let Ok(last_modified) = DateTime::parse_from_rfc3339(&object.last_modified)
                    else {
                        continue;
                    };
                    threshold_modification_time = Some(last_modified);
                }
            }
        }
        if let Some(threshold_modification_time) = threshold_modification_time {
            let path = path.to_string();
            for list in object_lists {
                for object in list.contents {
                    let Ok(last_modified) = DateTime::parse_from_rfc3339(&object.last_modified)
                    else {
                        continue;
                    };
                    if (last_modified, &object.key) < (threshold_modification_time, &path) {
                        self.processed_objects.insert(object.key);
                    }
                }
            }
            self.processed_objects.insert(path.clone());
            self.current_object_path = Some(path.into());
        }

        Ok(())
    }

    fn next_scanner_action(&mut self) -> Result<Option<ReadResult>, ReadError> {
        if take(&mut self.current_object_path).is_some() {
            return Ok(Some(ReadResult::FinishedSource {
                commit_allowed: true,
            }));
        }

        let is_polling_enabled = self.streaming_mode.is_polling_enabled();
        loop {
            if let Some(selected_object_path) = self.objects_for_processing.pop_front() {
                if self
                    .predownloaded_objects
                    .contains_key(&selected_object_path)
                {
                    self.processed_objects.insert(selected_object_path.clone());
                    self.current_object_path = Some(selected_object_path.into());
                    return Ok(Some(ReadResult::NewSource(None)));
                }
            } else {
                let is_queue_refresh_needed = !self.is_queue_initialized
                    || (self.objects_for_processing.is_empty() && is_polling_enabled);
                if is_queue_refresh_needed {
                    self.find_new_objects_for_processing()?;
                    if self.objects_for_processing.is_empty() && is_polling_enabled {
                        // Even after the queue refresh attempt, it's still empty
                        // Sleep before the next poll
                        sleep(Self::sleep_duration());
                    }
                } else {
                    // No elements and no further queue refreshes,
                    // the connector can stop at this point
                    return Ok(None);
                }
            }
        }
    }
}

#[allow(clippy::module_name_repetitions)]
impl S3Scanner {
    pub fn new(
        bucket: S3Bucket,
        objects_prefix: impl Into<String>,
        streaming_mode: ConnectorMode,
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
            streaming_mode,

            current_object_path: None,
            processed_objects: HashSet::new(),
            objects_for_processing: VecDeque::new(),
            downloader_pool: ThreadPoolBuilder::new()
                .num_threads(downloader_threads_count)
                .build()
                .expect("Failed to create downloader pool"), // TODO: configure number of threads
            predownloaded_objects: HashMap::new(),
            is_queue_initialized: false,
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

    pub fn stream_object_from_path_and_bucket(
        object_path_ref: &str,
        bucket: &S3Bucket,
    ) -> S3DownloadResult {
        let object_path = object_path_ref.to_string();
        let response = Self::download_object_from_path_and_bucket(&object_path, bucket)?;
        let readable_data = Cursor::new(response.bytes().to_vec());
        Ok((object_path_ref.to_string().into(), readable_data))
    }

    pub fn stream_object_from_path(
        &mut self,
        object_path_ref: &str,
    ) -> Result<Cursor<Vec<u8>>, ReadError> {
        let (current_object_path, reader_impl) =
            Self::stream_object_from_path_and_bucket(object_path_ref, &self.bucket.deep_copy())?;
        self.current_object_path = Some(current_object_path);
        Ok(reader_impl)
    }

    fn find_new_objects_for_processing(&mut self) -> Result<(), ReadError> {
        let object_lists = execute_with_retries(
            || self.bucket.list(self.objects_prefix.to_string(), None),
            RetryConfig::default(),
            MAX_S3_RETRIES,
        )
        .map_err(|e| ReadError::S3(S3CommandName::ListObjectsV2, e))?;

        let mut new_objects = Vec::new();
        for list in &object_lists {
            for object in &list.contents {
                if self.processed_objects.contains(&object.key) {
                    continue;
                }
                let Ok(last_modified) = DateTime::parse_from_rfc3339(&object.last_modified) else {
                    continue;
                };
                new_objects.push((last_modified, object.key.clone()));
            }
        }
        new_objects.sort_unstable();
        info!("Found {} new objects to process", new_objects.len());
        let downloading_started_at = SystemTime::now();
        let new_objects_downloaded: Vec<S3DownloadResult> = self.downloader_pool.install(|| {
            new_objects
                .par_iter()
                .map(|(_, path)| Self::stream_object_from_path_and_bucket(path, &self.bucket))
                .collect()
        });
        info!("Downloading done in {:?}", downloading_started_at.elapsed());

        for downloaded_object in new_objects_downloaded {
            match downloaded_object {
                Ok((path, prepared_reader)) => {
                    self.predownloaded_objects
                        .insert(path.to_string(), prepared_reader);
                }
                Err(e) => {
                    error!("Error while downloading an object from S3: {e}");
                }
            }
        }
        for (_, object_key) in new_objects {
            self.objects_for_processing.push_back(object_key);
        }

        info!(
            "The {} new objects have been enqueued for further processing",
            self.objects_for_processing.len()
        );
        self.is_queue_initialized = true;
        Ok(())
    }

    pub fn expect_current_object_path(&self) -> ArcStr {
        self.current_object_path
            .as_ref()
            .expect("current object should be present")
            .clone()
    }

    fn sleep_duration() -> Duration {
        Duration::from_millis(10000)
    }
}
