//! Traits, implementations, and macros related to logging timely events.

/// Type alias for logging timely events.
pub type WorkerIdentifier = usize;
/// Logger type for worker-local logging.
pub type Logger<Event> = crate::logging_core::Logger<Event, WorkerIdentifier>;
/// Logger for timely dataflow system events.
pub type TimelyLogger = Logger<TimelyEvent>;
/// Logger for timely dataflow progress events (the "timely/progress" log stream).
pub type TimelyProgressLogger = Logger<TimelyProgressEvent>;

use crate::dataflow::operators::capture::{Event, EventPusher};
use std::time::Duration;

/// Logs events as a timely stream, with progress statements.
pub struct BatchLogger<T, E, P>
where
    P: EventPusher<Duration, (Duration, E, T)>,
{
    // None when the logging stream is closed
    time: Duration,
    event_pusher: P,
    _phantom: ::std::marker::PhantomData<(E, T)>,
}

impl<T, E, P> BatchLogger<T, E, P>
where
    P: EventPusher<Duration, (Duration, E, T)>,
{
    /// Creates a new batch logger.
    pub fn new(event_pusher: P) -> Self {
        BatchLogger {
            time: Default::default(),
            event_pusher,
            _phantom: ::std::marker::PhantomData,
        }
    }
    /// Publishes a batch of logged events and advances the capability.
    pub fn publish_batch(&mut self, &time: &Duration, data: &mut Vec<(Duration, E, T)>) {
        if !data.is_empty() {
            self.event_pusher
                .push(Event::Messages(self.time, data.drain(..).collect()));
        }
        if self.time < time {
            let new_frontier = time;
            let old_frontier = self.time;
            self.event_pusher
                .push(Event::Progress(vec![(new_frontier, 1), (old_frontier, -1)]));
        }
        self.time = time;
    }
}
impl<T, E, P> Drop for BatchLogger<T, E, P>
where
    P: EventPusher<Duration, (Duration, E, T)>,
{
    fn drop(&mut self) {
        self.event_pusher
            .push(Event::Progress(vec![(self.time, -1)]));
    }
}

#[derive(
    Serialize, Deserialize, Abomonation, Debug, Clone, Hash, Eq, PartialEq, Ord, PartialOrd,
)]
/// The creation of an `Operate` implementor.
pub struct OperatesEvent {
    /// Worker-unique identifier for the operator.
    pub id: usize,
    /// Sequence of nested scope identifiers indicating the path from the root to this instance.
    pub addr: Vec<usize>,
    /// A helpful name.
    pub name: String,
}

#[derive(
    Serialize, Deserialize, Abomonation, Debug, Clone, Hash, Eq, PartialEq, Ord, PartialOrd,
)]
/// The creation of a channel between operators.
pub struct ChannelsEvent {
    /// Worker-unique identifier for the channel
    pub id: usize,
    /// Sequence of nested scope identifiers indicating the path from the root to this instance.
    pub scope_addr: Vec<usize>,
    /// Source descriptor, indicating operator index and output port.
    pub source: (usize, usize),
    /// Target descriptor, indicating operator index and input port.
    pub target: (usize, usize),
}

/// Encapsulates Any and Debug for dynamically typed timestamps in logs
pub trait ProgressEventTimestamp: std::fmt::Debug + std::any::Any {
    /// Upcasts this `ProgressEventTimestamp` to `Any`.
    ///
    /// NOTE: This is required until <https://github.com/rust-lang/rfcs/issues/2765> is fixed
    ///
    /// # Example
    /// ```rust
    /// let ts = vec![(0usize, 0usize, (23u64, 10u64), -4i64), (0usize, 0usize, (23u64, 11u64), 1i64)];
    /// let ts: &timely::logging::ProgressEventTimestampVec = &ts;
    /// for (n, p, t, d) in ts.iter() {
    ///     print!("{:?}, ", (n, p, t.as_any().downcast_ref::<(u64, u64)>(), d));
    /// }
    /// println!();
    /// ```
    fn as_any(&self) -> &dyn std::any::Any;

    /// Returns the name of the concrete type of this object.
    ///
    /// # Note
    ///
    /// This is intended for diagnostic use. The exact contents and format of the
    /// string returned are not specified, other than being a best-effort
    /// description of the type. For example, amongst the strings
    /// that `type_name::<Option<String>>()` might return are `"Option<String>"` and
    /// `"std::option::Option<std::string::String>"`.
    fn type_name(&self) -> &'static str;
}
impl<T: crate::Data + std::fmt::Debug + std::any::Any> ProgressEventTimestamp for T {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn type_name(&self) -> &'static str {
        std::any::type_name::<T>()
    }
}

/// A vector of progress updates in logs
///
/// This exists to support upcasting of the concrecte progress update vectors to
/// `dyn ProgressEventTimestamp`. Doing so at the vector granularity allows us to
/// use a single allocation for the entire vector (as opposed to a `Box` allocation
/// for each dynamically typed element).
pub trait ProgressEventTimestampVec: std::fmt::Debug + std::any::Any {
    /// Iterate over the contents of the vector
    fn iter<'a>(
        &'a self,
    ) -> Box<
        dyn Iterator<
                Item = (
                    &'a usize,
                    &'a usize,
                    &'a dyn ProgressEventTimestamp,
                    &'a i64,
                ),
            > + 'a,
    >;
}

impl<T: ProgressEventTimestamp> ProgressEventTimestampVec for Vec<(usize, usize, T, i64)> {
    fn iter<'a>(
        &'a self,
    ) -> Box<
        dyn Iterator<
                Item = (
                    &'a usize,
                    &'a usize,
                    &'a dyn ProgressEventTimestamp,
                    &'a i64,
                ),
            > + 'a,
    > {
        Box::new(
            <[(usize, usize, T, i64)]>::iter(&self[..]).map(|(n, p, t, d)| {
                let t: &dyn ProgressEventTimestamp = t;
                (n, p, t, d)
            }),
        )
    }
}

#[derive(Debug)]
/// Send or receive of progress information.
pub struct TimelyProgressEvent {
    /// `true` if the event is a send, and `false` if it is a receive.
    pub is_send: bool,
    /// Source worker index.
    pub source: usize,
    /// Communication channel identifier
    pub channel: usize,
    /// Message sequence number.
    pub seq_no: usize,
    /// Sequence of nested scope identifiers indicating the path from the root to this instance.
    pub addr: Vec<usize>,
    /// List of message updates, containing Target descriptor, timestamp as string, and delta.
    pub messages: Box<dyn ProgressEventTimestampVec>,
    /// List of capability updates, containing Source descriptor, timestamp as string, and delta.
    pub internal: Box<dyn ProgressEventTimestampVec>,
}

#[derive(
    Serialize, Deserialize, Abomonation, Debug, Clone, Hash, Eq, PartialEq, Ord, PartialOrd,
)]
/// External progress pushed onto an operator
pub struct PushProgressEvent {
    /// Worker-unique operator identifier
    pub op_id: usize,
}

#[derive(
    Serialize, Deserialize, Abomonation, Debug, Clone, Hash, Eq, PartialEq, Ord, PartialOrd,
)]
/// Message send or receive event
pub struct MessagesEvent {
    /// `true` if send event, `false` if receive event.
    pub is_send: bool,
    /// Channel identifier
    pub channel: usize,
    /// Source worker index.
    pub source: usize,
    /// Target worker index.
    pub target: usize,
    /// Message sequence number.
    pub seq_no: usize,
    /// Number of typed records in the message.
    pub length: usize,
}

/// Records the starting and stopping of an operator.
#[derive(
    Serialize, Deserialize, Abomonation, Debug, Clone, Hash, PartialEq, Eq, Ord, PartialOrd,
)]
pub enum StartStop {
    /// Operator starts.
    Start,
    /// Operator stops.
    Stop,
}

#[derive(
    Serialize, Deserialize, Abomonation, Debug, Clone, Hash, Eq, PartialEq, Ord, PartialOrd,
)]
/// Operator start or stop.
pub struct ScheduleEvent {
    /// Worker-unique identifier for the operator, linkable to the identifiers in `OperatesEvent`.
    pub id: usize,
    /// `Start` if the operator is starting, `Stop` if it is stopping.
    /// activity is true if it looks like some useful work was performed during this call (data was
    /// read or written, notifications were requested / delivered)
    pub start_stop: StartStop,
}

impl ScheduleEvent {
    /// Creates a new start scheduling event.
    pub fn start(id: usize) -> Self {
        ScheduleEvent {
            id,
            start_stop: StartStop::Start,
        }
    }
    /// Creates a new stop scheduling event and reports whether work occurred.
    pub fn stop(id: usize) -> Self {
        ScheduleEvent {
            id,
            start_stop: StartStop::Stop,
        }
    }
}

#[derive(
    Serialize, Deserialize, Abomonation, Debug, Clone, Hash, Eq, PartialEq, Ord, PartialOrd,
)]
/// Operator shutdown.
pub struct ShutdownEvent {
    /// Worker-unique identifier for the operator, linkable to the identifiers in `OperatesEvent`.
    pub id: usize,
}

#[derive(
    Serialize, Deserialize, Abomonation, Debug, Clone, Hash, Eq, PartialEq, Ord, PartialOrd,
)]
/// Application-defined code start or stop
pub struct ApplicationEvent {
    /// Unique event type identifier
    pub id: usize,
    /// True when activity begins, false when it stops
    pub is_start: bool,
}

#[derive(
    Serialize, Deserialize, Abomonation, Debug, Clone, Hash, Eq, PartialEq, Ord, PartialOrd,
)]
/// Application-defined code start or stop
pub struct GuardedMessageEvent {
    /// True when activity begins, false when it stops
    pub is_start: bool,
}

#[derive(
    Serialize, Deserialize, Abomonation, Debug, Clone, Hash, Eq, PartialEq, Ord, PartialOrd,
)]
/// Application-defined code start or stop
pub struct GuardedProgressEvent {
    /// True when activity begins, false when it stops
    pub is_start: bool,
}

#[derive(Serialize, Deserialize, Abomonation, Debug, PartialEq, Eq, Hash, Clone, Copy)]
/// Identifier of the worker that generated a log line
pub struct TimelySetup {
    /// Worker index
    pub index: usize,
}

#[derive(
    Serialize, Deserialize, Abomonation, Debug, Clone, Hash, Eq, PartialEq, Ord, PartialOrd,
)]
/// Kind of communication channel
pub enum CommChannelKind {
    /// Communication channel carrying progress information
    Progress,
    /// Communication channel carrying data
    Data,
}

#[derive(
    Serialize, Deserialize, Abomonation, Debug, Clone, Hash, Eq, PartialEq, Ord, PartialOrd,
)]
/// Event on a communication channel
pub struct CommChannelsEvent {
    /// Communication channel identifier
    pub identifier: usize,
    /// Kind of communication channel (progress / data)
    pub kind: CommChannelKind,
}

#[derive(
    Serialize, Deserialize, Abomonation, Debug, Clone, Hash, Eq, PartialEq, Ord, PartialOrd,
)]
/// Input logic start/stop
pub struct InputEvent {
    /// True when activity begins, false when it stops
    pub start_stop: StartStop,
}

/// Records the starting and stopping of an operator.
#[derive(
    Serialize, Deserialize, Abomonation, Debug, Clone, Hash, PartialEq, Eq, Ord, PartialOrd,
)]
pub enum ParkEvent {
    /// Worker parks.
    Park(Option<Duration>),
    /// Worker unparks.
    Unpark,
}

impl ParkEvent {
    /// Creates a new park event from the supplied duration.
    pub fn park(duration: Option<Duration>) -> Self {
        ParkEvent::Park(duration)
    }
    /// Creates a new unpark event.
    pub fn unpark() -> Self {
        ParkEvent::Unpark
    }
}

#[derive(
    Serialize, Deserialize, Debug, Clone, Abomonation, Hash, Eq, PartialEq, Ord, PartialOrd,
)]
/// An event in a timely worker
pub enum TimelyEvent {
    /// Operator creation.
    Operates(OperatesEvent),
    /// Channel creation.
    Channels(ChannelsEvent),
    /// Progress propagation (reasoning).
    PushProgress(PushProgressEvent),
    /// Message send or receive.
    Messages(MessagesEvent),
    /// Operator start or stop.
    Schedule(ScheduleEvent),
    /// Operator shutdown.
    Shutdown(ShutdownEvent),
    /// No clue.
    Application(ApplicationEvent),
    /// Per-message computation.
    GuardedMessage(GuardedMessageEvent),
    /// Per-notification computation.
    GuardedProgress(GuardedProgressEvent),
    /// Communication channel event.
    CommChannels(CommChannelsEvent),
    /// Input event.
    Input(InputEvent),
    /// Park event.
    Park(ParkEvent),
    /// Unstructured event.
    Text(String),
}

impl From<OperatesEvent> for TimelyEvent {
    fn from(v: OperatesEvent) -> TimelyEvent {
        TimelyEvent::Operates(v)
    }
}

impl From<ChannelsEvent> for TimelyEvent {
    fn from(v: ChannelsEvent) -> TimelyEvent {
        TimelyEvent::Channels(v)
    }
}

impl From<PushProgressEvent> for TimelyEvent {
    fn from(v: PushProgressEvent) -> TimelyEvent {
        TimelyEvent::PushProgress(v)
    }
}

impl From<MessagesEvent> for TimelyEvent {
    fn from(v: MessagesEvent) -> TimelyEvent {
        TimelyEvent::Messages(v)
    }
}

impl From<ScheduleEvent> for TimelyEvent {
    fn from(v: ScheduleEvent) -> TimelyEvent {
        TimelyEvent::Schedule(v)
    }
}

impl From<ShutdownEvent> for TimelyEvent {
    fn from(v: ShutdownEvent) -> TimelyEvent {
        TimelyEvent::Shutdown(v)
    }
}

impl From<ApplicationEvent> for TimelyEvent {
    fn from(v: ApplicationEvent) -> TimelyEvent {
        TimelyEvent::Application(v)
    }
}

impl From<GuardedMessageEvent> for TimelyEvent {
    fn from(v: GuardedMessageEvent) -> TimelyEvent {
        TimelyEvent::GuardedMessage(v)
    }
}

impl From<GuardedProgressEvent> for TimelyEvent {
    fn from(v: GuardedProgressEvent) -> TimelyEvent {
        TimelyEvent::GuardedProgress(v)
    }
}

impl From<CommChannelsEvent> for TimelyEvent {
    fn from(v: CommChannelsEvent) -> TimelyEvent {
        TimelyEvent::CommChannels(v)
    }
}

impl From<InputEvent> for TimelyEvent {
    fn from(v: InputEvent) -> TimelyEvent {
        TimelyEvent::Input(v)
    }
}

impl From<ParkEvent> for TimelyEvent {
    fn from(v: ParkEvent) -> TimelyEvent {
        TimelyEvent::Park(v)
    }
}
