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
/// The synchronization algorithm operates with the following state for each source:
/// - `last_reported_value` – the last value that was confirmed and sent.
/// - `next_proposed_value` – a value submitted by the source but not yet confirmed.
/// - `priority` – the priority of the source.
/// - `is_idle` – indicates whether the source is temporarily inactive.
///
/// The key value is `max_possible_value`, which determines whether a value can be sent:
/// - If a value from a source is <= `max_possible_value`
/// - And all higher-priority sources have `last_reported_value`s that are not lagging behind,
///   then the value is allowed to proceed.
///
/// `max_possible_value` is recalculated:
/// 1. After an attempt to add a new value (`next_proposed_value` updated).
/// 2. After a confirmed entry (`last_reported_value` updated).
///
/// Recalculation of `max_possible_value`:
/// 1. Only active sources (`is_idle == false`) are considered.
/// 2. For each source, compute two values:
///    - `last_reported_value + offset` if `last_reported_value` exists.
///      This represents the maximum value that can be safely allowed based on what has already been confirmed for this source.
///    - `next_proposed_value` if it exists.
///      This is considered because it represents the next value that is expected to be produced by this source.
/// 3. Take the maximum for each source.
/// 4. Set `max_possible_value` as the minimum of all these maximums.
///
/// Additional considerations:
/// - `max_possible_value` must never contradict already confirmed entries.
///   Therefore, `max_possible_value` is never updated to be less than the maximum of all `last_reported_value`s.
///   This ensures consistency even if some sources are currently idle but have higher confirmed values.
/// - If a lower-priority source exceeds `max_possible_value`, but a higher-priority source has a larger value,
///   the group of highest-priority sources is used to recalculate the minimum from their `next_proposed_value`.
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
use log::warn;
use std::cmp::{max, min};
use std::collections::HashMap;
use std::mem::take;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::channel::oneshot;
use futures::channel::oneshot::{Receiver as OneShotReceiver, Sender as OneShotSender};

use crate::connectors::ParsedEventWithErrors;
use crate::engine::error::DynResult;
use crate::engine::{Timestamp, Value};

#[derive(Clone, Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    #[error("window length differs for two definitions of the same synchronization group")]
    InconsistentWindowLength,

    #[error("the number of reader workers can't exceed {0}")]
    WorkerSetTooLarge(u32),
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
    priority: u64,
}

#[derive(Clone, Debug)]
pub struct ConnectorGroupDescriptor {
    pub name: String,
    pub column_index: usize,
    pub max_difference: Value,
    pub priority: u64,
    pub idle_duration: Option<Duration>,
}

impl ConnectorGroupDescriptor {
    pub fn new(
        name: String,
        column_index: usize,
        max_difference: Value,
        priority: u64,
        idle_duration: Option<Duration>,
    ) -> Self {
        Self {
            name,
            column_index,
            max_difference,
            priority,
            idle_duration,
        }
    }
}

type WorkerSetMask = u64;

#[derive(Debug)]
pub struct TrackedSource {
    next_proposed_value: Option<Value>,
    last_reported_value: Option<Value>,
    number_of_reader_workers: usize,
    priority: u64,

    // The variables below conceptually represent HashSets of worker IDs.
    // They are on the hot path: idleness is checked for every incoming event,
    // and force-advancement state can be queried a comparable number of times
    // when the max backlog size is limited.
    //
    // To avoid HashSet overhead on this hot path, we represent these sets as
    // bitmasks instead.
    //
    // Currently, the number of workers is limited to 8. For future growth,
    // we use a 64-bit value, allowing up to 64 workers. If this limit is ever
    // exceeded, we fail fast by returning an error.
    idle_reader_worker_mask: WorkerSetMask,
    completed_force_advancement_mask: WorkerSetMask,

    // A helper for checking the completeness of the sets.
    full_worker_set_mask: WorkerSetMask,
}

impl TrackedSource {
    fn new(priority: u64) -> Self {
        Self {
            next_proposed_value: None,
            last_reported_value: None,
            number_of_reader_workers: 0,
            priority,
            idle_reader_worker_mask: 0,
            completed_force_advancement_mask: 0,
            full_worker_set_mask: 0,
        }
    }

    fn register_new_reader_worker(&mut self) -> Result<usize, Error> {
        if u32::try_from(self.number_of_reader_workers).expect("the number of workers must fit u32")
            == WorkerSetMask::BITS
        {
            return Err(Error::WorkerSetTooLarge(WorkerSetMask::BITS));
        }

        self.full_worker_set_mask += 1u64 << self.number_of_reader_workers;

        self.number_of_reader_workers += 1;
        Ok(self.number_of_reader_workers - 1)
    }

    fn set_next_proposed_value_from_waiters(
        &mut self,
        waiting_sources: &[WaitingSource],
        source_id: usize,
    ) {
        self.next_proposed_value = waiting_sources
            .iter()
            .filter(|ws| ws.source_id == source_id)
            .map(|ws| &ws.requested_value)
            .min()
            .cloned();
    }

    fn maybe_update_next_proposed_value(&mut self, candidate: &Value) {
        if self
            .next_proposed_value
            .as_ref()
            .is_none_or(|current| current > candidate)
        {
            self.next_proposed_value = Some(candidate.clone());
        }
    }

    fn report_idle_reader_worker(&mut self, reader_worker_id: usize) -> bool {
        let worker_was_active = (self.idle_reader_worker_mask & (1u64 << reader_worker_id)) == 0;
        self.idle_reader_worker_mask |= 1u64 << reader_worker_id;
        worker_was_active
    }

    fn report_active_reader_worker(&mut self, reader_worker_id: usize) {
        self.idle_reader_worker_mask &= !(1u64 << reader_worker_id);
    }

    fn is_idle(&self) -> bool {
        self.next_proposed_value.is_none()
            && self.idle_reader_worker_mask == self.full_worker_set_mask
    }

    fn force_advancement_is_done_for_worker(&self, reader_worker_id: usize) -> bool {
        (self.completed_force_advancement_mask & (1u64 << reader_worker_id)) != 0
    }

    fn clear_completed_force_advancements(&mut self) {
        self.completed_force_advancement_mask = 0;
    }

    fn mark_completed_force_advancement(&mut self, reader_worker_id: usize) {
        self.completed_force_advancement_mask |= 1u64 << reader_worker_id;
    }

    fn force_advancement_done_in_all_sources(&self) -> bool {
        self.completed_force_advancement_mask == self.full_worker_set_mask
    }
}

#[derive(Debug)]
pub struct ConnectorGroup {
    max_difference: Value,
    max_possible_value: Option<Value>,
    waiting_sources: Vec<WaitingSource>,
    group_source_ids: HashMap<usize, usize>,
    sources: Vec<TrackedSource>,
    forced_time_advancement: Option<Timestamp>,
    max_active_priority: u64,
}

impl ConnectorGroup {
    pub fn new(max_difference: Value) -> Self {
        Self {
            max_difference,
            max_possible_value: None,
            waiting_sources: Vec::new(),
            group_source_ids: HashMap::new(),
            sources: Vec::new(),
            forced_time_advancement: None,
            max_active_priority: u64::MIN,
        }
    }

    pub fn register_new_source(
        &mut self,
        source_id: usize,
        priority: u64,
    ) -> Result<(usize, usize), Error> {
        let next_spare_id = self.sources.len();
        let group_source_id = self
            .group_source_ids
            .entry(source_id)
            .or_insert(next_spare_id);
        if *group_source_id == next_spare_id {
            self.sources.push(TrackedSource::new(priority));
        }
        let source_reader_worker_id =
            self.sources[*group_source_id].register_new_reader_worker()?;

        // All sources are initially active, hence updating.
        self.max_active_priority = max(self.max_active_priority, priority);

        Ok((*group_source_id, source_reader_worker_id))
    }

    pub fn forced_time_advancement_for_source(
        &self,
        source_id: usize,
        source_reader_worker_id: usize,
    ) -> Option<Timestamp> {
        if self.forced_time_advancement.is_none()
            || self.sources[source_id].force_advancement_is_done_for_worker(source_reader_worker_id)
        {
            return None;
        }
        self.forced_time_advancement
    }

    pub fn report_time_advancement(
        &mut self,
        timestamp: Timestamp,
        source_id: usize,
        source_reader_worker_id: usize,
    ) {
        if self.forced_time_advancement != Some(timestamp) {
            for source in &mut self.sources {
                source.clear_completed_force_advancements();
            }
            self.forced_time_advancement = Some(timestamp);
        }
        self.sources[source_id].mark_completed_force_advancement(source_reader_worker_id);

        let mut all_sources_advanced = true;
        for source in &self.sources {
            if !source.force_advancement_done_in_all_sources() {
                all_sources_advanced = false;
                break;
            }
        }

        if all_sources_advanced {
            self.forced_time_advancement = None;
        }
    }

    pub fn report_source_is_idle(
        &mut self,
        source_id: usize,
        source_reader_worker_id: usize,
    ) -> bool {
        let worker_was_active =
            self.sources[source_id].report_idle_reader_worker(source_reader_worker_id);
        if self.sources[source_id].is_idle() && !self.sources.iter().all(TrackedSource::is_idle) {
            self.update_max_possible_value();
        }

        self.max_active_priority = u64::MIN;
        for source in &self.sources {
            if !source.is_idle() && source.priority > self.max_active_priority {
                self.max_active_priority = source.priority;
            }
        }

        worker_was_active
    }

    pub fn report_entry_sent(&mut self, entry: &EntrySendApproval) {
        if let Some(source_id) = entry.source_id {
            // Update the last value reported by this source and possibly recalculate
            // the threshold for the max allowed value
            if self.sources[source_id]
                .last_reported_value
                .as_ref()
                .is_none_or(|current| entry.value > *current)
            {
                self.sources[source_id].last_reported_value = Some(entry.value.clone());
            }

            // Pick the minimum about the remaining proposed values for this source.
            self.sources[source_id]
                .set_next_proposed_value_from_waiters(self.waiting_sources.as_slice(), source_id);
        }
    }

    // This version is needed to avoid Rust compilation which prevents some
    // of the cases when &mut self is passed into another method.
    pub fn higher_prioritized_source_is_behind_external(
        sources: &[TrackedSource],
        candidate: &Value,
        source_priority: u64,
    ) -> bool {
        // An entry can be sent if there is no other source with the smaller
        // `last_reported_value` that is behind.
        for other_source in sources {
            if other_source.is_idle() || other_source.priority <= source_priority {
                continue;
            }
            match &other_source.last_reported_value {
                None => return true,
                Some(value) if value < candidate => return true,
                _ => {}
            }
        }
        false
    }

    pub fn higher_prioritized_source_is_behind(
        &self,
        candidate: &Value,
        source_priority: u64,
    ) -> bool {
        Self::higher_prioritized_source_is_behind_external(
            self.sources.as_slice(),
            candidate,
            source_priority,
        )
    }

    pub fn can_entry_be_sent(
        &mut self,
        source_id: usize,
        source_reader_worker_id: usize,
        candidate: &Value,
    ) -> EntryCheckResponse {
        // A new value means that the source is no longer idle.
        let source_was_already_active = !self.sources[source_id].is_idle();
        self.sources[source_id].report_active_reader_worker(source_reader_worker_id);
        let current_priority = self.sources[source_id].priority;

        // If the source stops being idle, the max_active_priority needs to be
        // recalculated.
        if self.max_active_priority < current_priority {
            self.max_active_priority = current_priority;
        }

        // The source now proposes the value `candidate`.
        self.sources[source_id].maybe_update_next_proposed_value(candidate);

        // Do a quick check for the hot path to avoid a potentially heavy invocation
        // of `update_max_possible_value`.
        if let Some(max_possible_value) = &self.max_possible_value {
            if source_was_already_active  // the set of active sources hasn't changed
                && candidate <= max_possible_value  // the value is allowed even according to the current (stricter) state
                && (current_priority == self.max_active_priority  // priority constraints are respected
                    || !self.higher_prioritized_source_is_behind(candidate, current_priority))
            {
                return EntryCheckResponse::Approved(EntrySendApproval::new(
                    Some(source_id),
                    candidate.clone(),
                ));
            }
        }

        // It's possible that the threshold has been moved, so we recalculate it.
        //
        // If all workers are stuck, each of them will have the NPV set.
        // The new allowed threshold must be computed as a minimum value
        // from the current NPVs.
        self.update_max_possible_value();

        if let Some(max_possible_value) = &self.max_possible_value {
            // If the value fits the allowed interval right now and can be doesn't
            // break the hierarchy, it can be allowed.
            if candidate <= max_possible_value
                && (current_priority == self.max_active_priority
                    || !self.higher_prioritized_source_is_behind(candidate, current_priority))
            {
                return EntryCheckResponse::Approved(EntrySendApproval::new(
                    Some(source_id),
                    candidate.clone(),
                ));
            }
        }

        // If it is impossible to let the value pass now, create an one-shot receiver that
        // will wake it up when the value is accepted.
        let receiver = Self::enqueue_waiting_source(
            &mut self.waiting_sources,
            candidate,
            source_id,
            self.sources[source_id].priority,
        );

        EntryCheckResponse::Wait(receiver)
    }

    // Enqueue a source that can't send the value now and its threshold value
    // When sending the value becomes possible, the `OneShotReceiver` receives
    // a value
    fn enqueue_waiting_source(
        waiting_sources: &mut Vec<WaitingSource>,
        requested_value: &Value,
        source_id: usize,
        priority: u64,
    ) -> OneShotReceiver<()> {
        let (sender, receiver) = oneshot::channel();
        waiting_sources.push(WaitingSource {
            sender: Some(sender),
            requested_value: requested_value.clone(),
            source_id,
            priority,
        });
        receiver
    }

    // This method is called when the `max_possible_value` is advanced forward.
    // It wakes up the data sources that are waiting to send values, but are
    // suspended because the value didn't satisfy the threshold conditions
    fn wake_waiting_sources(
        waiting_sources: &mut Vec<WaitingSource>,
        max_possible_value: &Value,
        sources: &[TrackedSource],
    ) {
        for ws in waiting_sources.iter_mut() {
            let priority_is_respected = !Self::higher_prioritized_source_is_behind_external(
                sources,
                &ws.requested_value,
                ws.priority,
            );
            let value_close_enough = max_possible_value >= &ws.requested_value;
            if priority_is_respected && value_close_enough {
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

    fn update_max_possible_value(&mut self) {
        let mut new_minimum = None;
        let mut is_first_source = true;

        // In any case, if a value `X` has ever been allowed (even in a currently
        // idle source), the new threshold must be at least `X` in order
        // not to break the invariant in which all last reported times are
        // less or equal to the current boundary.
        //
        // Normally, it would have not required any extra care. However, it is
        // possible that a source has been advanced far (possible, when it has the
        // highest priority) and other sources are behind. Then, in any case, the
        // other sources must be able to catch up with the factual processing time.
        self.max_active_priority = u64::MIN;
        let mut max_last_reported_value = None;
        let mut all_next_proposed_values_are_set = true;

        for source in &self.sources {
            max_last_reported_value =
                max(max_last_reported_value, source.last_reported_value.clone());
            if source.is_idle() {
                continue;
            }

            self.max_active_priority = max(self.max_active_priority, source.priority);

            let bound_by_last_reported_value = source
                .last_reported_value
                .as_ref()
                .map(|x| self.offset_by_max_difference(x));
            let next_proposed_value = &source.next_proposed_value;
            if next_proposed_value.is_none() {
                all_next_proposed_values_are_set = false;
            }

            // The synchronization groups are done in an assumption that the time behaves
            // in a monotonically increasing way. Therefore, to estimate the minimum element
            // that may come from the source, we take either the current non-accepted proposition
            // or the last element that had already been sent into the engine.
            let source_threshold = max(bound_by_last_reported_value, next_proposed_value.clone());
            if is_first_source {
                new_minimum = source_threshold;
                is_first_source = false;
            } else {
                new_minimum = min(new_minimum, source_threshold);
            }
        }

        let Some(mut new_threshold) = new_minimum else {
            // If the `new_minimum` isn't defined, it means that there is a source
            // from which we haven't yet seen anything. Therefore we wait for it
            // and can't set the global threshold.
            return;
        };
        if let Some(max_last_reported_value) = max_last_reported_value {
            // As stated above, not to contradict previously allowed elements,
            // we make sure that the new threshold allows the maximal one
            // from them.
            if max_last_reported_value > new_threshold {
                new_threshold = max_last_reported_value;
            }
        }

        let mut has_proposed_value_in_range = false;
        for source in &self.sources {
            if source.is_idle() {
                continue;
            }

            if let Some(next_proposed_value) = &source.next_proposed_value {
                if *next_proposed_value <= new_threshold
                    && !self
                        .higher_prioritized_source_is_behind(next_proposed_value, source.priority)
                {
                    has_proposed_value_in_range = true;
                    break;
                }
            }
        }

        if all_next_proposed_values_are_set && !has_proposed_value_in_range {
            // If all next_proposed_values are set, it means that all of them are
            // unable to send a value. In the weighted case, we will most likely
            // take the minimum among `next_proposed_value` - unless there was a
            // combination of far-advanced sources going idle.
            //
            // But taking the minimum among `next_proposed_value` in this case may
            // not be enough because the minimum may correspond to the less-priority
            // source, which won't pass since it can't update the maximum when we have
            // someone higher.
            //
            // So in this case, we need to take the minimum among the `next_proposed_value`
            // for the group with the highest priority and use it - this will ensure that
            // the high-pri source will advance and in its turn unblock others.
            let mut min_next_proposed_value = None;
            is_first_source = true;
            for source in &self.sources {
                if source.priority != self.max_active_priority || source.is_idle() {
                    continue;
                }
                if is_first_source {
                    min_next_proposed_value = source.next_proposed_value.as_ref();
                    is_first_source = false;
                } else {
                    min_next_proposed_value =
                        min(min_next_proposed_value, source.next_proposed_value.as_ref());
                }
            }

            if let Some(min_next_proposed_value) = min_next_proposed_value {
                new_threshold = max(new_threshold, min_next_proposed_value.clone());
            }
        }
        self.max_possible_value = Some(new_threshold);

        // The `max_possible_value` may be unchanged, but some sources with
        // smaller priority may be become unlocked and proceed.
        Self::wake_waiting_sources(
            &mut self.waiting_sources,
            self.max_possible_value.as_ref().unwrap(),
            self.sources.as_slice(),
        );
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
}

pub type SharedConnectorGroup = Arc<Mutex<ConnectorGroup>>;

#[derive(Clone, Debug)]
pub struct ConnectorGroupAccessor {
    group: SharedConnectorGroup,
    pub source_id: usize,
    pub source_reader_worker_id: usize,
    target_value_idx: usize,
    pub idle_duration: Option<Duration>,
}

impl ConnectorGroupAccessor {
    pub fn new(
        group: SharedConnectorGroup,
        source_id: usize,
        source_reader_worker_id: usize,
        target_value_idx: usize,
        idle_duration: Option<Duration>,
    ) -> Self {
        Self {
            group,
            source_id,
            source_reader_worker_id,
            target_value_idx,
            idle_duration,
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
        self.group.lock().unwrap().can_entry_be_sent(
            self.source_id,
            self.source_reader_worker_id,
            target_value,
        )
    }

    pub fn report_source_is_idle(&mut self) -> bool {
        let mut group = self.group.lock().unwrap();
        group.report_source_is_idle(self.source_id, self.source_reader_worker_id)
    }

    pub fn report_entries_sent(&mut self, approvals: Vec<EntrySendApproval>) {
        // This method is done so that the events can be saved in bulk,
        // with just a single mutex acquisition
        let mut group = self.group.lock().unwrap();
        for approval in approvals {
            group.report_entry_sent(&approval);
        }

        // Do a potentially heavy invocation, but only once per batch.
        group.update_max_possible_value();
    }

    pub fn report_time_advancement(&mut self, timestamp: Timestamp) {
        self.group.lock().unwrap().report_time_advancement(
            timestamp,
            self.source_id,
            self.source_reader_worker_id,
        );
    }

    pub fn forced_time_advancement(&self) -> Option<Timestamp> {
        self.group
            .lock()
            .unwrap()
            .forced_time_advancement_for_source(self.source_id, self.source_reader_worker_id)
    }

    fn extract_target_value<'a>(&self, parsed_row: &'a [DynResult<Value>]) -> &'a Value {
        if let Ok(value) = &parsed_row[self.target_value_idx] {
            value
        } else {
            panic!("rows with error values are not allowed in the connector groups")
        }
    }
}

#[derive(Debug, Default)]
pub struct ConnectorSynchronizer {
    groups: HashMap<String, SharedConnectorGroup>,
}

impl ConnectorSynchronizer {
    pub fn new() -> Self {
        Self {
            groups: HashMap::new(),
        }
    }

    pub fn ensure_synchronization_group(
        &mut self,
        desc: &ConnectorGroupDescriptor,
        source_id: usize,
    ) -> Result<ConnectorGroupAccessor, Error> {
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
        let (group_source_id, source_reader_worker_id) =
            group_impl.register_new_source(source_id, desc.priority)?;

        Ok(ConnectorGroupAccessor::new(
            group.clone(),
            group_source_id,
            source_reader_worker_id,
            desc.column_index,
            desc.idle_duration,
        ))
    }
}

pub type SharedConnectorSynchronizer = Arc<Mutex<ConnectorSynchronizer>>;
