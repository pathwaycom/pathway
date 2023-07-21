//! Methods which describe an operators topology, and the progress it makes.

use std::rc::Rc;
use std::cell::RefCell;

use crate::scheduling::Schedule;
use crate::progress::{Timestamp, ChangeBatch, Antichain};

/// Methods for describing an operators topology, and the progress it makes.
pub trait Operate<T: Timestamp> : Schedule {

    /// Indicates if the operator is strictly local to this worker.
    ///
    /// A parent scope must understand whether the progress information returned by the worker
    /// reflects only this worker's progress, so that it knows whether to send and receive the
    /// corresponding progress messages to its peers. If the operator is strictly local, it must
    /// exchange this information, whereas if the operator is itself implemented by the same set
    /// of workers, the parent scope understands that progress information already reflects the
    /// aggregate information among the workers.
    ///
    /// This is a coarse approximation to refined worker sets. In a future better world, operators
    /// would explain how their implementations are partitioned, so that a parent scope knows what
    /// progress information to exchange with which peers. Right now the two choices are either
    /// "all" or "none", but it could be more detailed. In the more detailed case, this method
    /// should / could return a pair (index, peers), indicating the group id of the worker out of
    /// how many groups. This becomes complicated, as a full all-to-all exchange would result in
    /// multiple copies of the same progress messages (but aggregated variously) arriving at
    /// arbitrary times.
    fn local(&self) -> bool { true }

    /// The number of inputs.
    fn inputs(&self) -> usize;
    /// The number of outputs.
    fn outputs(&self) -> usize;

    /// Fetches summary information about internal structure of the operator.
    ///
    /// Each operator must summarize its internal structure by a map from pairs `(input, output)`
    /// to an antichain of timestamp summaries, indicating how a timestamp on any of its inputs may
    /// be transformed to timestamps on any of its outputs.
    ///
    /// Each operator must also indicate whether it initially holds any capabilities on any of its
    /// outputs, so that the parent operator can properly initialize its progress information.
    ///
    /// The default behavior is to indicate that timestamps on any input can emerge unchanged on
    /// any output, and no initial capabilities are held.
    fn get_internal_summary(&mut self) -> (Vec<Vec<Antichain<T::Summary>>>, Rc<RefCell<SharedProgress<T>>>);

    /// Signals that external frontiers have been set.
    ///
    /// By default this method does nothing, and leaves all changes in the `frontiers` element
    /// of the shared progress state. An operator should be able to consult `frontiers` at any
    /// point and read out the current frontier information, or the changes from the last time
    /// that `frontiers` was drained.
    fn set_external_summary(&mut self) { }

    /// Indicates of whether the operator requires `push_external_progress` information or not.
    fn notify_me(&self) -> bool { true }
}

/// Progress information shared between parent and child.
#[derive(Debug)]
pub struct SharedProgress<T: Timestamp> {
    /// Frontier capability changes reported by the parent scope.
    pub frontiers: Vec<ChangeBatch<T>>,
    /// Consumed message changes reported by the child operator.
    pub consumeds: Vec<ChangeBatch<T>>,
    /// Internal capability changes reported by the child operator.
    pub internals: Vec<ChangeBatch<T>>,
    /// Produced message changes reported by the child operator.
    pub produceds: Vec<ChangeBatch<T>>,
}

impl<T: Timestamp> SharedProgress<T> {
    /// Allocates a new shared progress structure.
    pub fn new(inputs: usize, outputs: usize) -> Self {
        SharedProgress {
            frontiers: vec![ChangeBatch::new(); inputs],
            consumeds: vec![ChangeBatch::new(); inputs],
            internals: vec![ChangeBatch::new(); outputs],
            produceds: vec![ChangeBatch::new(); outputs],
        }
    }
}
