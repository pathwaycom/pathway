/// Below is a brief description of how a connector group functions.
/// The user begins working with connector synchronization by registering
/// a connector group using the `register_new_source` method in a coordinator object.
/// This method returns an accessor object that provides simple access to the group itself.
///
/// This accessor is aware of both the group (holding a shared pointer to it)
/// and the source ID. Therefore, after registering multiple connectors within a group,
/// there will be multiple distinct accessors - each sharing the same pointer to the group
/// implementation but holding a unique source ID and metadata. Essentially, an accessor
/// is just syntactic sugar over the group.
///
/// The group implementation provides an interface to perform two key actions:
/// - `can_entry_be_sent(source_id, value)`: Determines whether `value` can be sent now
///   without violating windowing constraints. If not, it returns an oneshot receiver
///   (`futures::oneshot::receiver`) that will be notified once the structure can retry.
/// - `report_send(source_id, value)`: Reports that `value` has been sent from the given
///   `source_id`.
///
/// Within the group, there is a `max_possible_value` threshold, which defines the maximum
/// value that can be sent at a given time. If `can_entry_be_sent` checks a value lower than
/// this threshold, it passes. Initially, `max_possible_value` may be unset.
///
/// The group also tracks the last sent values. The `report_send` method verifies whether
/// the `max_possible_value` should be updated after sending a value, and updates it if needed.
///
/// However, `max_possible_value` alone does not always determine whether data can be sent.
/// For instance, at startup, it is unset, and multiple sources may be available.
/// Therefore, `can_entry_be_sent` follows this algorithm:
///
/// The group maintains a `next_proposed_value` vector, which stores values seen by
/// connectors and submitted to `can_entry_be_sent` but not yet approved. Once approved,
/// they are removed (set to `None`).
///
/// Now, when an entry (`source_id`, `value`) arrives, two cases arise:
///
/// 1. **`max_possible_value` is undefined**:
///    - Update `next_proposed_value` for this `source_id`, leading to two scenarios:
///      - **Not all sources have `max_possible_value` set yet**: No streams can proceed.
///        Some connectors may have earlier data that must be processed first.
///        The connector must wait.
///      - **All sources have `max_possible_value` defined**: Find the minimum
///        `next_proposed_value`. This value, plus the allowed maximum difference, defines
///        the new `max_possible_value`. Now, proceed to case 2.
///
/// 2. **`max_possible_value` is defined**:
///    - If `value` matches the threshold, it is allowed.
///    - Otherwise, to prevent deadlocks:
///      - Consider two sources with integer data: `[1, 2, 21, 22]` and `[3, 4, 23, 24]`
///        and a maximum allowed difference of `5`.
///      - The system allows `{1, 2, 3, 4}` but then both sources are blocked because
///        their next values exceed the allowed difference.
///      - To fix this, `next_proposed_value` is updated for `source_id`. If all sources
///        have a `next_proposed_value`, `max_possible_value` is recalculated using the
///        minimum of these values plus `max_difference`.
///
/// The algorithm operates under these constraints:
/// - If `can_entry_be_sent` returns `false` for a `source_id`, that source must not
///   attempt further checks. Instead, it must retry with the same entry until approval
///   (although this changes slightly for the sake of multithreaded connectors - see below).
/// - `report_send` must be called only for entries that previously received a `true`
///   response from `can_entry_be_sent`.
///
/// ### Handling Multithreaded Connectors
/// Some connectors (e.g., Kafka, NATS) are multithreaded, meaning multiple instances of
/// `ConnectorGroupAccessor` exist and are treated separately. This can create issues.
///
/// Example: Reading from a Kafka topic with two partitions, where only one contains data.
/// If treated separately, the connector group would wait for data from the empty partition,
/// causing a deadlock.
///
/// To prevent this, we modify the algorithm so that:
/// - `source_id` remains the same across all timely workers.
/// - `next_proposed_value` is updated when a smaller value arrives from another worker.
/// - However, workers are not forced to wait for all others to update their values. We
///   only do the best effort for the synchronization, without waiting for all that would
///   probably stop the execution because of an empty partition.
///
/// Overall, with a minor adjustment, reading the multithreaded source is the same as
/// reading the single-threaded one, however with a peculiarity that we don't know the exact
/// order of events: it's up to the Kafka broker and reader, what to provide us next.
///
/// ### Optimizations
/// Acquiring a mutex can be expensive, so we want to minimize how often it's done.
/// To achieve this, each accessor caches a threshold value - `max_possible_value` - which
/// represents the most recent upper bound received from the connector group.
///
/// If a value is below this cached threshold, it can be sent without needing to lock
/// the mutex or inspect the shared structure again.
///
/// Similarly, the accessor uses this same threshold to avoid redundant reports: it
/// only reports new values if they exceed the last value it reported for its source.
use log::warn;
use std::cmp::{max, min};
use std::collections::HashMap;
use std::mem::take;
use std::sync::{Arc, Mutex};

use futures::channel::oneshot;
use futures::channel::oneshot::{Receiver as OneShotReceiver, Sender as OneShotSender};

use crate::connectors::ParsedEventWithErrors;
use crate::engine::error::DynResult;
use crate::engine::Value;

#[derive(Clone, Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    #[error("window length differs for two definitions of the same synchronization group")]
    InconsistentWindowLength,

    #[error("connector groups are not yet supported in multiprocess runs")]
    MultiprocessingNotSupported,
}

#[derive(Clone, Debug)]
pub struct EntrySendApproval {
    source_id: Option<usize>,
    value: Value,
}

impl EntrySendApproval {
    pub fn new(source_id: Option<usize>, value: Value) -> Self {
        Self { source_id, value }
    }
}

#[derive(Debug)]
pub enum EntryCheckResponse {
    Approved(EntrySendApproval),
    Wait(OneShotReceiver<()>),
}

impl EntryCheckResponse {
    pub fn is_wait(&self) -> bool {
        matches!(self, Self::Wait(_))
    }

    pub fn expect_wait(self) -> OneShotReceiver<()> {
        if let Self::Wait(receiver) = self {
            receiver
        } else {
            panic!("expect_wait called for {self:?}")
        }
    }

    pub fn expect_approved(self) -> EntrySendApproval {
        if let Self::Approved(receiver) = self {
            receiver
        } else {
            panic!("expect_approved called for {self:?}")
        }
    }
}

#[derive(Debug)]
pub struct WaitingSource {
    sender: Option<OneShotSender<()>>,
    requested_value: Value,
    source_id: usize,
}

#[derive(Clone, Debug)]
pub struct ConnectorGroupDescriptor {
    pub name: String,
    pub column_index: usize,
    pub max_difference: Value,
}

impl ConnectorGroupDescriptor {
    pub fn new(name: String, column_index: usize, max_difference: Value) -> Self {
        Self {
            name,
            column_index,
            max_difference,
        }
    }
}

#[derive(Debug)]
pub struct ConnectorGroup {
    max_difference: Value,
    next_proposed_value: Vec<Option<Value>>,
    last_reported_value: Vec<Option<Value>>,
    max_possible_value: Option<Value>,
    waiting_sources: Vec<WaitingSource>,
    group_source_ids: HashMap<usize, usize>,
}

impl ConnectorGroup {
    pub fn new(max_difference: Value) -> Self {
        Self {
            max_difference,
            next_proposed_value: Vec::new(),
            last_reported_value: Vec::new(),
            max_possible_value: None,
            waiting_sources: Vec::new(),
            group_source_ids: HashMap::new(),
        }
    }

    pub fn register_new_source(&mut self, source_id: usize) -> usize {
        let next_spare_id = self.next_proposed_value.len();
        let group_source_id = self
            .group_source_ids
            .entry(source_id)
            .or_insert(next_spare_id);
        if *group_source_id == next_spare_id {
            self.next_proposed_value.push(None);
            self.last_reported_value.push(None);
        }
        *group_source_id
    }

    pub fn report_entry_sent(&mut self, entry: EntrySendApproval) {
        if let Some(source_id) = entry.source_id {
            // Since the entry is sent, the source doesn't currently propose any value
            self.drop_next_proposed_value(source_id);

            // Update the last value reported by this source and possibly recalculate
            // the threshold for the max allowed value
            self.update_last_reported_value(source_id, entry.value);
        }
    }

    pub fn can_entry_be_sent(&mut self, source_id: usize, candidate: &Value) -> EntryCheckResponse {
        if let Some(max_possible_value) = &self.max_possible_value {
            // The first case: there is already an interval of the allowed values
            // We need to check if the candidate fits in this interval, and if so,
            // it can be reported upstream
            if candidate <= max_possible_value {
                return EntryCheckResponse::Approved(EntrySendApproval::new(
                    Some(source_id),
                    candidate.clone(),
                ));
            }
        }

        // The candidate doesn't fit the current interval. It becomes a proposed
        // value for the source. Note that in the multithreaded cases there can
        // be several proposed values (from several timely workers), and the
        // minimal must be taken into consideration
        if self.next_proposed_value[source_id]
            .as_ref()
            .map_or(true, |current| current > candidate)
        {
            self.next_proposed_value[source_id] = Some(candidate.clone());
        }

        // If all workers are stuck, each of them will have the NPV set.
        // The new allowed threshold must be computed as a minimum value
        // from the current NPVs.
        let next_proposed_threshold = self.next_proposed_threshold();
        if let Some(next_proposed_threshold) = next_proposed_threshold {
            let new_max_possible_value = self.offset_by_max_difference(&next_proposed_threshold);
            self.max_possible_value = Some(new_max_possible_value.clone());
            Self::wake_waiting_sources(&mut self.waiting_sources, &new_max_possible_value);
            if candidate <= &new_max_possible_value {
                return EntryCheckResponse::Approved(EntrySendApproval::new(
                    Some(source_id),
                    candidate.clone(),
                ));
            }
        }

        // If it is impossible to let the value pass now, create an one-shot receiver that
        // will wake it up when the value is accepted.
        let receiver =
            Self::enqueue_waiting_source(&mut self.waiting_sources, candidate, source_id);
        EntryCheckResponse::Wait(receiver)
    }

    // Enqueue a source that can't send the value now and its threshold value
    // When sending the value becomes possible, the `OneShotReceiver` receives
    // a value
    fn enqueue_waiting_source(
        waiting_sources: &mut Vec<WaitingSource>,
        requested_value: &Value,
        source_id: usize,
    ) -> OneShotReceiver<()> {
        let (sender, receiver) = oneshot::channel();
        waiting_sources.push(WaitingSource {
            sender: Some(sender),
            requested_value: requested_value.clone(),
            source_id,
        });
        receiver
    }

    // This method is called when the `max_possible_value` is advanced forward.
    // It wakes up the data sources that are waiting to send values, but are
    // suspended because the value didn't satisfy the threshold conditions
    fn wake_waiting_sources(waiting_sources: &mut Vec<WaitingSource>, max_possible_value: &Value) {
        for ws in waiting_sources.iter_mut() {
            let wait_is_over = max_possible_value >= &ws.requested_value;
            if wait_is_over {
                // If the source no longer needs to wait, notify it and drop the sender
                let sender = take(&mut ws.sender).unwrap();
                let send_res = sender.send(());
                if send_res.is_err() {
                    warn!("The reader wakeup receiver has dropped.");
                }
            }
        }
        waiting_sources.retain(|ws| ws.sender.is_some());
    }

    // Drops the `next_proposed_value` for the source and wakes the smallest
    // possible waiting sender for this source, if there is any
    fn drop_next_proposed_value(&mut self, source_id: usize) {
        self.next_proposed_value[source_id] = None;
        let mut chosen_ws_ref: Option<&mut WaitingSource> = None;
        for ws in &mut self.waiting_sources {
            if ws.source_id != source_id {
                continue;
            }
            if chosen_ws_ref
                .as_ref()
                .map_or(true, |current| current.requested_value > ws.requested_value)
            {
                chosen_ws_ref = Some(ws);
            }
        }
        if let Some(chosen_ws_ref) = chosen_ws_ref {
            let sender = take(&mut chosen_ws_ref.sender).unwrap();
            let send_res = sender.send(());
            if send_res.is_err() {
                warn!("The reader wakeup receiver has been dropped.");
            }
            self.waiting_sources.retain(|ws| ws.sender.is_some());
        }
    }

    // Update the last value reported by a data source
    fn update_last_reported_value(&mut self, source_id: usize, value: Value) {
        if let Some(last_reported_value) = &self.last_reported_value[source_id] {
            if &value <= last_reported_value {
                return;
            }
        }
        if Some(self.offset_by_max_difference(&value)) <= self.max_possible_value {
            self.last_reported_value[source_id] = Some(value);
            return;
        }
        self.last_reported_value[source_id] = Some(value);

        let mut new_minimum = None;
        for source_idx in 0..self.last_reported_value.len() - 1 {
            let last_reported_value = &self.last_reported_value[source_idx];
            let next_proposed_value = &self.next_proposed_value[source_idx];
            // There are two possibilities for the source: it can either have
            // bigger last_reported_value, or it may wait with a greater
            // `next_proposed_value`.
            //
            // If `next_proposed_value` is bigger, we take that. If it results
            // in the advancement of the threshold up to this `next_proposed_value`,
            // the source will be woken up and will be allowed to advance.
            let source_threshold = max(last_reported_value, next_proposed_value);
            if source_idx == 0 {
                new_minimum = source_threshold.as_ref();
            } else {
                new_minimum = min(new_minimum, source_threshold.as_ref());
            }
        }

        if let Some(new_minimum) = new_minimum {
            let new_max_possible_value = self.offset_by_max_difference(new_minimum);
            if self.max_possible_value < Some(new_max_possible_value.clone()) {
                self.max_possible_value = Some(new_max_possible_value);
                Self::wake_waiting_sources(
                    &mut self.waiting_sources,
                    self.max_possible_value.as_ref().unwrap(),
                );
            }
        }
    }

    // Calculates the threshold value, if the maximum sent value is `value`
    fn offset_by_max_difference(&self, value: &Value) -> Value {
        match value {
            Value::Int(int_value) => Value::Int(int_value + self.max_difference.as_int().unwrap()),
            Value::Duration(duration_value) => {
                Value::Duration(*duration_value + self.max_difference.as_duration().unwrap())
            }
            Value::DateTimeNaive(dt_value) => {
                Value::DateTimeNaive(*dt_value + self.max_difference.as_duration().unwrap())
            }
            Value::DateTimeUtc(dt_value) => {
                Value::DateTimeUtc(*dt_value + self.max_difference.as_duration().unwrap())
            }
            _ => panic!("Unsupported type for sync group field"),
        }
    }

    fn next_proposed_threshold(&self) -> Option<Value> {
        self.next_proposed_value.iter().min().cloned()?
    }
}

pub type SharedConnectorGroup = Arc<Mutex<ConnectorGroup>>;

#[derive(Debug)]
pub struct ConnectorGroupAccessor {
    group: SharedConnectorGroup,
    source_id: usize,
    target_value_idx: usize,
    max_possible_value: Option<Value>,
}

impl ConnectorGroupAccessor {
    pub fn new(group: SharedConnectorGroup, source_id: usize, target_value_idx: usize) -> Self {
        Self {
            group,
            source_id,
            target_value_idx,
            max_possible_value: None,
        }
    }

    pub fn can_entry_be_sent(&self, event: &ParsedEventWithErrors) -> EntryCheckResponse {
        let target_value = match event {
            ParsedEventWithErrors::AdvanceTime => {
                return EntryCheckResponse::Approved(EntrySendApproval::new(None, Value::None))
            }
            ParsedEventWithErrors::Insert((_, values))
            | ParsedEventWithErrors::Delete((_, values)) => self.extract_target_value(values),
        };
        if Some(target_value) <= self.max_possible_value.as_ref() {
            let approval = EntrySendApproval::new(Some(self.source_id), target_value.clone());
            return EntryCheckResponse::Approved(approval);
        }
        self.group
            .lock()
            .unwrap()
            .can_entry_be_sent(self.source_id, target_value)
    }

    pub fn report_entries_sent(&mut self, approvals: Vec<EntrySendApproval>) {
        // This method is done so that the events can be saved in bulk,
        // with just a single mutex acquisition
        let mut group = self.group.lock().unwrap();
        for approval in approvals {
            group.report_entry_sent(approval);
        }
        self.max_possible_value = group.max_possible_value.clone();
    }

    fn extract_target_value<'a>(&self, parsed_row: &'a [DynResult<Value>]) -> &'a Value {
        if let Ok(value) = &parsed_row[self.target_value_idx] {
            value
        } else {
            panic!("rows with error values are not allowed in the connector groups")
        }
    }
}

pub struct ConnectorSynchronizer {
    is_multiprocessed: bool,
    groups: HashMap<String, SharedConnectorGroup>,
}

impl ConnectorSynchronizer {
    pub fn new(is_multiprocessed: bool) -> Self {
        Self {
            is_multiprocessed,
            groups: HashMap::new(),
        }
    }

    pub fn ensure_synchronization_group(
        &mut self,
        desc: &ConnectorGroupDescriptor,
        source_id: usize,
    ) -> Result<ConnectorGroupAccessor, Error> {
        if self.is_multiprocessed {
            return Err(Error::MultiprocessingNotSupported);
        }

        let group = self
            .groups
            .entry(desc.name.clone())
            .or_insert(Arc::new(Mutex::new(ConnectorGroup::new(
                desc.max_difference.clone(),
            ))));

        let mut group_impl = group.lock().unwrap();
        if group_impl.max_difference != desc.max_difference {
            return Err(Error::InconsistentWindowLength);
        }
        let group_source_id = group_impl.register_new_source(source_id);

        Ok(ConnectorGroupAccessor::new(
            group.clone(),
            group_source_id,
            desc.column_index,
        ))
    }
}

pub type SharedConnectorSynchronizer = Arc<Mutex<ConnectorSynchronizer>>;
