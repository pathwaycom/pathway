//! Barrier synchronization.

use crate::communication::Allocate;
use crate::dataflow::{InputHandle, ProbeHandle};
use crate::worker::Worker;

/// A re-usable barrier synchronization mechanism.
pub struct Barrier<A: Allocate> {
    input: InputHandle<usize, ()>,
    probe: ProbeHandle<usize>,
    worker: Worker<A>,
}

impl<A: Allocate> Barrier<A> {
    /// Allocates a new barrier.
    pub fn new(worker: &mut Worker<A>) -> Self {
        use crate::dataflow::operators::{Input, Probe};
        let (input, probe) = worker.dataflow(|scope| {
            let (handle, stream) = scope.new_input::<()>();
            (handle, stream.probe())
        });
        Barrier {
            input,
            probe,
            worker: worker.clone(),
        }
    }

    /// Blocks until all other workers have reached this barrier.
    ///
    /// This method does *not* block dataflow execution, which continues
    /// to execute while we await the arrival of the other workers.
    pub fn wait(&mut self) {
        self.advance();
        while !self.reached() {
            self.worker.step();
        }
    }

    /// Advances this worker to the next barrier stage.
    ///
    /// This change is not communicated until `worker.step()` is called.
    #[inline]
    pub fn advance(&mut self) {
        let round = *self.input.time();
        self.input.advance_to(round + 1);
    }

    /// Indicates that the barrier has been reached by all workers.
    ///
    /// This method may not change until `worker.step()` is called.
    #[inline]
    pub fn reached(&mut self) -> bool {
        !self.probe.less_than(self.input.time())
    }
}
