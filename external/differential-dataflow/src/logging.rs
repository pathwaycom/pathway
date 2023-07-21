//! Loggers and logging events for differential dataflow.

/// Logger for differential dataflow events.
pub type Logger = ::timely::logging::Logger<DifferentialEvent>;

/// Enables logging of differential dataflow events.
pub fn enable<A, W>(worker: &mut timely::worker::Worker<A>, writer: W) -> Option<Box<dyn std::any::Any+'static>>
where
    A: timely::communication::Allocate,
    W: std::io::Write+'static,
{
    let writer = ::timely::dataflow::operators::capture::EventWriter::new(writer);
    let mut logger = ::timely::logging::BatchLogger::new(writer);
    worker
        .log_register()
        .insert::<DifferentialEvent,_>("differential/arrange", move |time, data| logger.publish_batch(time, data))
}

/// Possible different differential events.
#[derive(Debug, Clone, Abomonation, Ord, PartialOrd, Eq, PartialEq)]
pub enum DifferentialEvent {
    /// Batch creation.
    Batch(BatchEvent),
    /// Merge start and stop events.
    Merge(MergeEvent),
    /// Batch dropped when trace dropped.
    Drop(DropEvent),
    /// A merge failed to complete in time.
    MergeShortfall(MergeShortfall),
    /// Trace sharing event.
    TraceShare(TraceShare),
}

/// Either the start or end of a merge event.
#[derive(Debug, Clone, Abomonation, Ord, PartialOrd, Eq, PartialEq)]
pub struct BatchEvent {
    /// Operator identifier.
    pub operator: usize,
    /// Which order of magnitude.
    pub length: usize,
}

impl From<BatchEvent> for DifferentialEvent { fn from(e: BatchEvent) -> Self { DifferentialEvent::Batch(e) } }


/// Either the start or end of a merge event.
#[derive(Debug, Clone, Abomonation, Ord, PartialOrd, Eq, PartialEq)]
pub struct DropEvent {
    /// Operator identifier.
    pub operator: usize,
    /// Which order of magnitude.
    pub length: usize,
}

impl From<DropEvent> for DifferentialEvent { fn from(e: DropEvent) -> Self { DifferentialEvent::Drop(e) } }

/// Either the start or end of a merge event.
#[derive(Debug, Clone, Abomonation, Ord, PartialOrd, Eq, PartialEq)]
pub struct MergeEvent {
    /// Operator identifier.
    pub operator: usize,
    /// Which order of magnitude.
    pub scale: usize,
    /// Length of first trace.
    pub length1: usize,
    /// Length of second trace.
    pub length2: usize,
    /// None implies a start.
    pub complete: Option<usize>,
}

impl From<MergeEvent> for DifferentialEvent { fn from(e: MergeEvent) -> Self { DifferentialEvent::Merge(e) } }

/// A merge failed to complete in time.
#[derive(Debug, Clone, Abomonation, Ord, PartialOrd, Eq, PartialEq)]
pub struct MergeShortfall {
    /// Operator identifer.
    pub operator: usize,
    /// Which order of magnitude.
    pub scale: usize,
    /// By how much were we short.
    pub shortfall: usize,
}

impl From<MergeShortfall> for DifferentialEvent { fn from(e: MergeShortfall) -> Self { DifferentialEvent::MergeShortfall(e) } }

/// Either the start or end of a merge event.
#[derive(Debug, Clone, Abomonation, Ord, PartialOrd, Eq, PartialEq)]
pub struct TraceShare {
    /// Operator identifier.
    pub operator: usize,
    /// Change in number of shares.
    pub diff: isize,
}

impl From<TraceShare> for DifferentialEvent { fn from(e: TraceShare) -> Self { DifferentialEvent::TraceShare(e) } }
