//! Write endpoint for a sequence of batches.
//!
//! A `TraceWriter` accepts a sequence of batches and distributes them
//! to both a shared trace and to a sequence of private queues.

use std::rc::{Rc, Weak};
use std::cell::RefCell;

use lattice::Lattice;
use trace::{Trace, Batch, BatchReader};
use timely::progress::{Antichain, Timestamp};

use trace::wrappers::rc::TraceBox;

use super::TraceAgentQueueWriter;
use super::TraceReplayInstruction;

/// Write endpoint for a sequence of batches.
///
/// A `TraceWriter` accepts a sequence of batches and distributes them
/// to both a shared trace and to a sequence of private queues.
pub struct TraceWriter<Tr>
where
    Tr: Trace,
    Tr::Time: Lattice+Timestamp+Ord+Clone+std::fmt::Debug+'static,
    Tr::Batch: Batch,
{
    /// Current upper limit.
    upper: Antichain<Tr::Time>,
    /// Shared trace, possibly absent (due to weakness).
    trace: Weak<RefCell<TraceBox<Tr>>>,
    /// A sequence of private queues into which batches are written.
    queues: Rc<RefCell<Vec<TraceAgentQueueWriter<Tr>>>>,
}

impl<Tr> TraceWriter<Tr>
where
    Tr: Trace,
    Tr::Time: Lattice+Timestamp+Ord+Clone+std::fmt::Debug+'static,
    Tr::Batch: Batch,
{
    /// Creates a new `TraceWriter`.
    pub fn new(
        upper: Vec<Tr::Time>,
        trace: Weak<RefCell<TraceBox<Tr>>>,
        queues: Rc<RefCell<Vec<TraceAgentQueueWriter<Tr>>>>
    ) -> Self
    {
        let mut temp = Antichain::new();
        temp.extend(upper.into_iter());
        Self { upper: temp, trace, queues }
    }

    /// Exerts merge effort, even without additional updates.
    pub fn exert(&mut self, fuel: &mut isize) {
        if let Some(trace) = self.trace.upgrade() {
            trace.borrow_mut().trace.exert(fuel);
        }
    }

    /// Advances the trace by `batch`.
    ///
    /// The `hint` argument is either `None` in the case of an empty batch,
    /// or is `Some(time)` for a time less or equal to all updates in the
    /// batch and which is suitable for use as a capability.
    pub fn insert(&mut self, batch: Tr::Batch, hint: Option<Tr::Time>) {

        // Something is wrong if not a sequence.
        if !(&self.upper == batch.lower()) {
            println!("{:?} vs {:?}", self.upper, batch.lower());
        }
        assert!(&self.upper == batch.lower());
        assert!(batch.lower() != batch.upper());

        self.upper.clone_from(batch.upper());

        // push information to each listener that still exists.
        let mut borrow = self.queues.borrow_mut();
        for queue in borrow.iter_mut() {
            if let Some(pair) = queue.upgrade() {
                pair.1.borrow_mut().push_back(TraceReplayInstruction::Batch(batch.clone(), hint.clone()));
                pair.1.borrow_mut().push_back(TraceReplayInstruction::Frontier(batch.upper().clone()));
                pair.0.activate();
            }
        }
        borrow.retain(|w| w.upgrade().is_some());

        // push data to the trace, if it still exists.
        if let Some(trace) = self.trace.upgrade() {
            trace.borrow_mut().trace.insert(batch);
        }

    }

    /// Inserts an empty batch up to `upper`.
    pub fn seal(&mut self, upper: Antichain<Tr::Time>) {
        if self.upper != upper {
            use trace::Builder;
            let builder = <Tr::Batch as Batch>::Builder::new();
            let batch = builder.done(self.upper.clone(), upper, Antichain::from_elem(Tr::Time::minimum()));
            self.insert(batch, None);
        }
    }
}

impl<Tr> Drop for TraceWriter<Tr>
where
    Tr: Trace,
    Tr::Time: Lattice+Timestamp+Ord+Clone+std::fmt::Debug+'static,
    Tr::Batch: Batch,
{
    fn drop(&mut self) {
        self.seal(Antichain::new())
    }
}
