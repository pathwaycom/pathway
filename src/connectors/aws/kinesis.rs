use log::{error, warn};
use std::borrow::Cow;
use std::collections::{HashMap, HashSet, VecDeque};
use std::mem::take;
use std::time::{Duration, Instant};

use aws_sdk_kinesis::operation::describe_stream_summary::DescribeStreamSummaryError;
use aws_sdk_kinesis::operation::get_records::GetRecordsError;
use aws_sdk_kinesis::operation::get_shard_iterator::GetShardIteratorError;
use aws_sdk_kinesis::operation::list_shards::ListShardsError;
use aws_sdk_kinesis::operation::list_streams::ListStreamsError;
use aws_sdk_kinesis::operation::put_records::PutRecordsError;
use aws_sdk_kinesis::primitives::Blob;
use aws_sdk_kinesis::types::PutRecordsRequestEntry;
use aws_sdk_kinesis::types::ShardIteratorType;
use aws_sdk_kinesis::types::StreamStatus;
use aws_sdk_kinesis::Client;
use aws_smithy_runtime_api::client::result::SdkError;
use aws_smithy_runtime_api::http::Response as AwsHttpResponse;
use rand::rng;
use rand::seq::SliceRandom;
use tokio::runtime::Runtime as TokioRuntime;
use xxhash_rust::xxh3::xxh3_64;

use crate::connectors::data_format::FormatterContext;
use crate::connectors::data_storage::{MessageQueueTopic, ReaderContext};
use crate::connectors::{OffsetKey, OffsetValue};
use crate::connectors::{ReadError, ReadResult, Reader, StorageType, WriteError, Writer};
use crate::persistence::frontier::OffsetAntichain;
use crate::retry::RetryConfig;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("List shards error, error details: {0:?}")]
    ListShards(#[from] SdkError<ListShardsError, AwsHttpResponse>),

    #[error("List streams error, error details: {0:?}")]
    ListStreams(#[from] SdkError<ListStreamsError, AwsHttpResponse>),

    #[error("Describe stream error, error details: {0:?}")]
    DescribeStreamSummary(#[from] SdkError<DescribeStreamSummaryError, AwsHttpResponse>),

    #[error("Get shard iterator error, error details: {0:?}")]
    GetShardIterator(#[from] SdkError<GetShardIteratorError, AwsHttpResponse>),

    #[error("Get shard records error, error details: {0:?}")]
    GetShardRecords(#[from] SdkError<GetRecordsError, AwsHttpResponse>),

    #[error("Put records error, error details: {0:?}")]
    PutRecords(#[from] SdkError<PutRecordsError, AwsHttpResponse>),

    #[error("Iterator not found for shard {0}")]
    IteratorNotFound(String),

    #[error("Stream '{0}' does not exist")]
    StreamDoesntExist(String),

    #[error(
        "The data stream '{0}' is not ready to accept data. Status: '{1}', but 'Active' was expected"
    )]
    StreamNotReady(String, StreamStatus),
}

const MAX_KINESIS_RECORDS_PER_REQUEST: i32 = 10_000;
const SHARD_ITERATOR_TTL_SECONDS: u64 = 300;

struct Shard {
    shard_id: String,
    last_record_id: Option<String>,
}

struct CachedShardIterator {
    iterator: String,
    received_at: Instant,
}

impl CachedShardIterator {
    fn new(iterator: String) -> Self {
        Self {
            iterator,
            received_at: Instant::now(),
        }
    }

    fn is_expired(&self, queried_at: &Instant) -> bool {
        let iterator_age = queried_at.duration_since(self.received_at);
        iterator_age <= Duration::from_secs(SHARD_ITERATOR_TTL_SECONDS)
    }
}

struct ShardSet {
    worker_index: u64,
    worker_count: u64,
    stream_name: String,
    last_updated_at: Instant,
    round_robin_duration: Duration,
    assigned_shards: VecDeque<Shard>,
    exhausted_shards: HashSet<String>,
    had_full_round_robin: bool,
    last_shard_id: String,
}

impl ShardSet {
    fn new(
        worker_index: u64,
        worker_count: u64,
        stream_name: String,
        round_robin_duration: Duration,
        runtime: &TokioRuntime,
        client: &Client,
    ) -> Result<Self, Error> {
        let mut shard_set = Self {
            worker_index,
            worker_count,
            stream_name,
            round_robin_duration,
            last_updated_at: Instant::now(),
            assigned_shards: VecDeque::new(),
            exhausted_shards: HashSet::new(),
            had_full_round_robin: false,
            last_shard_id: String::default(),
        };
        shard_set.update_assigned_shards(runtime, client)?;
        Ok(shard_set)
    }

    fn update_assigned_shards(
        &mut self,
        runtime: &TokioRuntime,
        client: &Client,
    ) -> Result<(), Error> {
        runtime.block_on(async {
            let mut shards = Vec::new();
            let mut next_token = None;

            loop {
                let response = client
                    .list_shards()
                    .stream_name(&self.stream_name)
                    .set_next_token(next_token)
                    .send()
                    .await?;

                next_token = response.next_token().map(std::string::ToString::to_string);
                for shard in response.shards.unwrap_or_default() {
                    if self.exhausted_shards.contains(&shard.shard_id) {
                        continue;
                    }
                    let assigned_worker_index =
                        xxh3_64(shard.shard_id.as_bytes()) % self.worker_count;
                    if assigned_worker_index != self.worker_index {
                        continue;
                    }
                    shards.push(Shard {
                        last_record_id: shard
                            .sequence_number_range()
                            .map_or_else(|| None, |range| range.ending_sequence_number())
                            .map(std::string::ToString::to_string),
                        shard_id: shard.shard_id,
                    });
                }

                if next_token.is_none() {
                    break;
                }
            }

            self.last_updated_at = Instant::now();
            self.last_shard_id = shards
                .last()
                .map(|shard| shard.shard_id.clone())
                .unwrap_or_default();
            self.had_full_round_robin = false;

            // After the first round-robin, there may still be some time left (for example, if some shards were empty
            // and their reads were quick). In that case, we continue reading in a round-robin manner until the time runs out.
            // Since the first shards would otherwise have an advantage, we shuffle the list of shards to give each one
            // an equal chance to utilize this extra time.
            shards.shuffle(&mut rng());
            self.assigned_shards = shards.into();

            Ok(())
        })
    }

    fn suspend_current_shard(&mut self) {
        // The current shard will be scheduled after the end of the current
        // cycle, hence `pop_back()` removes it.
        self.assigned_shards.pop_back();
    }

    fn max_shard_query_duration(&self, queried_at: &Instant) -> Duration {
        if self.assigned_shards.is_empty() {
            unreachable!("if there are no shards, the method shouldn't be called")
        }

        let round_robin_duration_ns = u64::try_from(self.round_robin_duration.as_nanos())
            .expect("round robin duration in nanoseconds must fit u64");
        let fair_share =
            Duration::from_nanos(round_robin_duration_ns / (self.assigned_shards.len() as u64));

        let elapsed = queried_at.duration_since(self.last_updated_at);
        let remaining_duration = self.round_robin_duration.saturating_sub(elapsed);

        std::cmp::min(fair_share, remaining_duration)
    }

    fn next_shard_id(&mut self, runtime: &TokioRuntime, client: &Client) -> String {
        loop {
            let mut update_is_needed = false;
            if self.assigned_shards.is_empty() {
                // Update, case 1: we've read everything we could.
                let iteration_elapsed = self.last_updated_at.elapsed();
                if iteration_elapsed < self.round_robin_duration {
                    std::thread::sleep(
                        self.round_robin_duration
                            .checked_sub(iteration_elapsed)
                            .unwrap(),
                    );
                }
                update_is_needed = true;
            } else if self.had_full_round_robin
                && self.last_updated_at.elapsed() >= self.round_robin_duration
            {
                // Update, case 2: we've done some round robins, not everything was read,
                // but it's time to check if there are more shards to work on.
                update_is_needed = true;
            }

            if update_is_needed {
                if let Err(e) = self.update_assigned_shards(runtime, client) {
                    error!(
                        "Failed to update assigned shards for stream {}: {e}. The shard set remains unchanged.",
                        self.stream_name,
                    );
                }
            } else {
                break;
            }
        }

        let shard = self
            .assigned_shards
            .pop_front()
            .expect("at least one shard must be present at this point");
        let shard_id = shard.shard_id.clone();

        // If this rotation logic changes, change also the logic in `suspend_current_shard`.
        self.assigned_shards.push_back(shard);
        self.had_full_round_robin |= shard_id == self.last_shard_id;

        shard_id
    }
}

pub struct KinesisReader {
    runtime: TokioRuntime,
    client: Client,
    stream_name: String,
    operated_set: ShardSet,
    shard_iterators: HashMap<String, CachedShardIterator>,
    shard_offsets: HashMap<String, String>,
    entries_read: VecDeque<ReadResult>,
    shard_offset_keys: HashMap<String, OffsetKey>,
}

impl KinesisReader {
    pub fn new(
        runtime: TokioRuntime,
        client: Client,
        stream_name: String,
        worker_index: usize,
        worker_count: usize,
        round_robin_duration: Duration,
    ) -> Result<Self, ReadError> {
        Ok(Self {
            operated_set: ShardSet::new(
                worker_index as u64,
                worker_count as u64,
                stream_name.clone(),
                round_robin_duration,
                &runtime,
                &client,
            )?,
            stream_name,
            runtime,
            client,
            shard_iterators: HashMap::new(),
            shard_offsets: HashMap::new(),
            entries_read: VecDeque::new(),
            shard_offset_keys: HashMap::new(),
        })
    }

    async fn get_shard_iterator(
        client: &Client,
        shard_iterators: &mut HashMap<String, CachedShardIterator>,
        shard_offsets: &HashMap<String, String>,
        stream_name: &str,
        shard_id: &str,
        queried_at: &Instant,
    ) -> Result<String, ReadError> {
        if let Some(iterator) = shard_iterators.remove(shard_id) {
            if !iterator.is_expired(queried_at) {
                return Ok(iterator.iterator);
            }
        }

        let mut iterator_builder = client
            .get_shard_iterator()
            .stream_name(stream_name)
            .shard_id(shard_id);

        if let Some(shard_offset) = shard_offsets.get(shard_id) {
            iterator_builder = iterator_builder
                .shard_iterator_type(ShardIteratorType::AfterSequenceNumber)
                .starting_sequence_number(shard_offset);
        } else {
            iterator_builder = iterator_builder.shard_iterator_type(ShardIteratorType::TrimHorizon);
        }

        let response = iterator_builder
            .send()
            .await
            .map_err(Error::GetShardIterator)?;
        let iterator = response
            .shard_iterator()
            .ok_or(Error::IteratorNotFound(shard_id.to_string()))?
            .to_string();

        Ok::<_, ReadError>(iterator)
    }

    fn offset_key_for_shard(
        shard_offset_keys: &mut HashMap<String, OffsetKey>,
        shard_id: &str,
    ) -> OffsetKey {
        shard_offset_keys
            .entry(shard_id.to_string())
            .or_insert_with(|| OffsetKey::Kinesis(shard_id.to_string().into()))
            .clone()
    }
}

impl Reader for KinesisReader {
    fn seek(&mut self, frontier: &OffsetAntichain) -> Result<(), ReadError> {
        self.operated_set
            .update_assigned_shards(&self.runtime, &self.client)?;

        // This way we get closing times only for assigned shards, but that's OK for us
        let mut shard_closing_offsets: HashMap<String, String> = HashMap::new();
        for shard in &self.operated_set.assigned_shards {
            if let Some(last_record_id) = &shard.last_record_id {
                shard_closing_offsets.insert(shard.shard_id.clone(), last_record_id.clone());
            }
        }

        self.shard_iterators.clear();
        self.shard_offsets.clear();
        self.entries_read.clear();
        self.operated_set.exhausted_shards.clear();
        for (offset_key, offset_value) in frontier {
            let OffsetKey::Kinesis(shard_id) = offset_key else {
                error!("Unexpected key type in kinesis offset: {offset_key:?}");
                continue;
            };
            let OffsetValue::KinesisOffset(offset) = offset_value else {
                error!("Unexpected value type in kinesis offset: {offset_value:?}");
                continue;
            };
            self.shard_offsets
                .insert(shard_id.to_string(), offset.clone());
            if let Some(closing_offset) = shard_closing_offsets.get(shard_id.as_str()) {
                if closing_offset == offset {
                    self.operated_set
                        .exhausted_shards
                        .insert(shard_id.to_string());
                }
            }
        }

        Ok(())
    }

    fn read(&mut self) -> Result<ReadResult, ReadError> {
        while self.entries_read.is_empty() {
            let shard_id = self.operated_set.next_shard_id(&self.runtime, &self.client);

            let started_at = Instant::now();
            let max_query_duration = self.operated_set.max_shard_query_duration(&started_at);

            self.runtime.block_on(async {
                let mut shard_iterator = Self::get_shard_iterator(
                    &self.client,
                    &mut self.shard_iterators,
                    &self.shard_offsets,
                    &self.stream_name,
                    &shard_id,
                    &started_at,
                )
                .await?;
                loop {
                    let records_response = self
                        .client
                        .get_records()
                        .shard_iterator(shard_iterator.clone())
                        .limit(MAX_KINESIS_RECORDS_PER_REQUEST)
                        .send()
                        .await
                        .map_err(|e| ReadError::Kinesis(e.into()))?;

                    for record in records_response.records() {
                        let key = record.partition_key.as_bytes();
                        let value = record.data.clone().into_inner();
                        let context =
                            ReaderContext::from_key_value(Some(key.to_vec()), Some(value));

                        let offset = (
                            Self::offset_key_for_shard(&mut self.shard_offset_keys, &shard_id),
                            OffsetValue::KinesisOffset(record.sequence_number.clone()),
                        );
                        self.entries_read
                            .push_back(ReadResult::Data(context, offset));
                        self.shard_offsets
                            .insert(shard_id.clone(), record.sequence_number.clone());
                    }

                    let Some(next_shard_iterator) = records_response.next_shard_iterator else {
                        // The shard has been closed and the portion of the messages we
                        // received is the last one.
                        self.operated_set.suspend_current_shard();
                        self.operated_set.exhausted_shards.insert(shard_id);
                        break;
                    };

                    let entries_exhausted_for_now = records_response.records.len()
                        < MAX_KINESIS_RECORDS_PER_REQUEST.try_into().unwrap();
                    let query_is_too_long = started_at.elapsed() > max_query_duration;
                    if entries_exhausted_for_now || query_is_too_long {
                        // The shard can still provide the new messages, but we've either exceeded the allocated
                        // time limit querying it, or it currently has no new data.
                        self.shard_iterators
                            .insert(shard_id, CachedShardIterator::new(next_shard_iterator));
                        if entries_exhausted_for_now {
                            // If there are currently no entries in the shard, don't poll it until next time
                            // quant for scheduling to avoid busy waits.
                            self.operated_set.suspend_current_shard();
                        }
                        break;
                    }

                    shard_iterator = next_shard_iterator;
                }

                Ok::<_, ReadError>(())
            })?;
        }

        let entry = self
            .entries_read
            .pop_front()
            .expect("must have been read already");
        Ok(entry)
    }

    fn short_description(&self) -> Cow<'static, str> {
        format!("Kinesis({})", self.stream_name).into()
    }

    fn storage_type(&self) -> StorageType {
        StorageType::Kinesis
    }
}

// https://docs.aws.amazon.com/kinesis/latest/APIReference/API_PutRecords.html
// Note that the partition keys are also included in the length. We also leave
// a small gap for the JSON formatting elements.
const MAX_KINESIS_BULK_REQUEST_ENTRIES: usize = 500;
const MAX_KINESIS_BULK_REQUEST_LENGTH: usize = 5_000_000;
const N_SEND_ATTEMPTS: usize = 5;

#[derive(Default, Debug)]
struct BufferedShardEntries {
    total_length: usize,
    entries: Vec<PutRecordsRequestEntry>,
}

impl BufferedShardEntries {
    fn add_entry(
        &mut self,
        runtime: &TokioRuntime,
        client: &Client,
        stream: &str,
        entry: PutRecordsRequestEntry,
    ) -> Result<(), WriteError> {
        let entry_length = entry.data.as_ref().len() + entry.partition_key.len();
        let entry_limit_reached = self.entries.len() == MAX_KINESIS_BULK_REQUEST_ENTRIES;
        let size_limit_reached = self.total_length + entry_length > MAX_KINESIS_BULK_REQUEST_LENGTH;
        if entry_limit_reached || size_limit_reached {
            runtime.block_on(async { self.flush(client, stream).await })?;
        }
        self.total_length += entry_length;
        self.entries.push(entry);
        Ok(())
    }

    async fn flush(&mut self, client: &Client, stream: &str) -> Result<(), WriteError> {
        if self.entries.is_empty() {
            return Ok(());
        }

        let mut retry = RetryConfig::default();

        for _ in 0..N_SEND_ATTEMPTS {
            let response = client
                .put_records()
                .stream_name(stream)
                .set_records(Some(take(&mut self.entries)))
                .send()
                .await;

            match response {
                Ok(response) => {
                    if response.failed_record_count().unwrap_or(0) == 0 {
                        break;
                    }

                    let mut total_entries_to_retry = 0;
                    for (index, record) in response.records.into_iter().enumerate() {
                        if record.error_code.is_some() {
                            self.entries.swap(total_entries_to_retry, index);
                            total_entries_to_retry += 1;
                        }
                    }
                    self.entries.truncate(total_entries_to_retry);
                    if total_entries_to_retry > 0 {
                        warn!("There are {total_entries_to_retry} objects requiring retry. If you see this message often, it means that the writing rate is too high. Consider increasing the number of shards.");
                    }
                }
                Err(e) => {
                    error!(
                        "An attempt to save item batch for stream {stream} has failed: {}",
                        Error::from(e)
                    );
                }
            }

            retry.sleep_after_error();
        }

        self.total_length = 0;
        if self.entries.is_empty() {
            Ok(())
        } else {
            self.entries.clear();
            Err(WriteError::SomeItemsNotDelivered(self.entries.len()))
        }
    }
}

pub struct KinesisWriter {
    runtime: TokioRuntime,
    client: Client,
    stream_name: MessageQueueTopic,
    key_field_index: Option<usize>,
    buffered_entries: HashMap<String, BufferedShardEntries>,
}

impl KinesisWriter {
    pub fn new(
        runtime: TokioRuntime,
        client: Client,
        stream_name: MessageQueueTopic,
        key_field_index: Option<usize>,
    ) -> Result<Self, WriteError> {
        runtime.block_on(async {
            match stream_name {
                MessageQueueTopic::Fixed(ref stream_name) => {
                    let stream = client
                        .describe_stream_summary()
                        .stream_name(stream_name)
                        .send()
                        .await
                        .map_err(Error::DescribeStreamSummary)?;
                    let stream_status = stream
                        .stream_description_summary()
                        .ok_or(Error::StreamDoesntExist(stream_name.clone()))?
                        .stream_status();
                    if stream_status != &StreamStatus::Active {
                        return Err(Error::StreamNotReady(
                            stream_name.clone(),
                            stream_status.clone(),
                        ));
                    }
                }
                MessageQueueTopic::Dynamic(_) => {
                    let _ = client
                        .list_streams()
                        .limit(1)
                        .send()
                        .await
                        .map_err(Error::ListStreams)?;
                }
            }
            Ok::<_, Error>(())
        })?;

        Ok(Self {
            runtime,
            client,
            stream_name,
            key_field_index,
            buffered_entries: HashMap::new(),
        })
    }
}

impl Writer for KinesisWriter {
    fn write(&mut self, data: FormatterContext) -> Result<(), WriteError> {
        let partition_key = match self.key_field_index {
            Some(index) => data.values[index].to_string(),
            None => data.key.0.to_string(),
        };

        for payload in data.payloads {
            let payload = payload.into_raw_bytes()?;
            let effective_stream = self.stream_name.get_for_posting(&data.values)?;
            let entry = PutRecordsRequestEntry::builder()
                .partition_key(partition_key.clone())
                .data(Blob::new(payload))
                .build()?;
            self.buffered_entries
                .entry(effective_stream.clone())
                .or_default()
                .add_entry(&self.runtime, &self.client, &effective_stream, entry)?;
        }

        Ok(())
    }

    fn flush(&mut self, _forced: bool) -> Result<(), WriteError> {
        let mut global_result = Ok(());
        self.runtime.block_on(async {
            for (stream_name, mut buffered_entries) in self.buffered_entries.drain() {
                let stream_result = buffered_entries.flush(&self.client, &stream_name).await;
                if stream_result.is_err() && global_result.is_ok() {
                    global_result = stream_result;
                }
            }
            global_result
        })
    }

    fn name(&self) -> String {
        format!("Kinesis({})", self.stream_name)
    }

    fn single_threaded(&self) -> bool {
        false
    }
}
