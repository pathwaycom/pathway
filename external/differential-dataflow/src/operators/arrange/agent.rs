//! Shared read access to a trace.

use std::rc::{Rc, Weak};
use std::cell::RefCell;
use std::collections::VecDeque;

use timely::dataflow::Scope;
use timely::dataflow::operators::generic::source;
use timely::progress::Timestamp;
use timely::progress::{Antichain, frontier::AntichainRef};
use timely::dataflow::operators::CapabilitySet;

use lattice::Lattice;
use trace::{Trace, TraceReader, Batch, BatchReader, Cursor};

use trace::wrappers::rc::TraceBox;

use timely::scheduling::Activator;

use super::{TraceWriter, TraceAgentQueueWriter, TraceAgentQueueReader, Arranged};
use super::TraceReplayInstruction;

use crate::trace::wrappers::frontier::{TraceFrontier, BatchFrontier};


/// A `TraceReader` wrapper which can be imported into other dataflows.
///
/// The `TraceAgent` is the default trace type produced by `arranged`, and it can be extracted
/// from the dataflow in which it was defined, and imported into other dataflows.
pub struct TraceAgent<Tr>
where
    Tr: TraceReader,
    Tr::Time: Lattice+Ord+Clone+'static,
{
    pub(crate) trace: Rc<RefCell<TraceBox<Tr>>>,
    queues: Weak<RefCell<Vec<TraceAgentQueueWriter<Tr>>>>,
    logical_compaction: Antichain<Tr::Time>,
    physical_compaction: Antichain<Tr::Time>,
    temp_antichain: Antichain<Tr::Time>,

    operator: ::timely::dataflow::operators::generic::OperatorInfo,
    logging: Option<::logging::Logger>,
}

impl<Tr> TraceReader for TraceAgent<Tr>
where
    Tr: TraceReader,
    Tr::Time: Lattice+Ord+Clone+'static,
{
    type Key = Tr::Key;
    type Val = Tr::Val;
    type Time = Tr::Time;
    type R = Tr::R;

    type Batch = Tr::Batch;
    type Cursor = Tr::Cursor;

    fn set_logical_compaction(&mut self, frontier: AntichainRef<Tr::Time>) {
        // This method does not enforce that `frontier` is greater or equal to `self.logical_compaction`.
        // Instead, it determines the joint consequences of both guarantees and moves forward with that.
        crate::lattice::antichain_join_into(&self.logical_compaction.borrow()[..], &frontier[..], &mut self.temp_antichain);
        self.trace.borrow_mut().adjust_logical_compaction(self.logical_compaction.borrow(), self.temp_antichain.borrow());
        ::std::mem::swap(&mut self.logical_compaction, &mut self.temp_antichain);
        self.temp_antichain.clear();
    }
    fn get_logical_compaction(&mut self) -> AntichainRef<Tr::Time> {
        self.logical_compaction.borrow()
    }
    fn set_physical_compaction(&mut self, frontier: AntichainRef<Tr::Time>) {
        // This method does not enforce that `frontier` is greater or equal to `self.physical_compaction`.
        // Instead, it determines the joint consequences of both guarantees and moves forward with that.
        crate::lattice::antichain_join_into(&self.physical_compaction.borrow()[..], &frontier[..], &mut self.temp_antichain);
        self.trace.borrow_mut().adjust_physical_compaction(self.physical_compaction.borrow(), self.temp_antichain.borrow());
        ::std::mem::swap(&mut self.physical_compaction, &mut self.temp_antichain);
        self.temp_antichain.clear();
    }
    fn get_physical_compaction(&mut self) -> AntichainRef<Tr::Time> {
        self.physical_compaction.borrow()
    }
    fn cursor_through(&mut self, frontier: AntichainRef<Tr::Time>) -> Option<(Tr::Cursor, <Tr::Cursor as Cursor>::Storage)> {
        self.trace.borrow_mut().trace.cursor_through(frontier)
    }
    fn map_batches<F: FnMut(&Self::Batch)>(&self, f: F) { self.trace.borrow().trace.map_batches(f) }
}

impl<Tr> TraceAgent<Tr>
where
    Tr: TraceReader,
    Tr::Time: Timestamp+Lattice,
{
    /// Creates a new agent from a trace reader.
    pub fn new(trace: Tr, operator: ::timely::dataflow::operators::generic::OperatorInfo, logging: Option<::logging::Logger>) -> (Self, TraceWriter<Tr>)
    where
        Tr: Trace,
        Tr::Batch: Batch,
    {
        let trace = Rc::new(RefCell::new(TraceBox::new(trace)));
        let queues = Rc::new(RefCell::new(Vec::new()));

        if let Some(logging) = &logging {
            logging.log(
                ::logging::TraceShare { operator: operator.global_id, diff: 1 }
            );
        }

        let reader = TraceAgent {
            trace: trace.clone(),
            queues: Rc::downgrade(&queues),
            logical_compaction: trace.borrow().logical_compaction.frontier().to_owned(),
            physical_compaction: trace.borrow().physical_compaction.frontier().to_owned(),
            temp_antichain: Antichain::new(),
            operator,
            logging,
        };

        let writer = TraceWriter::new(
            vec![<Tr::Time as Timestamp>::minimum()],
            Rc::downgrade(&trace),
            queues,
        );

        (reader, writer)
    }

    /// Attaches a new shared queue to the trace.
    ///
    /// The queue is first populated with existing batches from the trace,
    /// The queue will be immediately populated with existing historical batches from the trace, and until the reference
    /// is dropped the queue will receive new batches as produced by the source `arrange` operator.
    pub fn new_listener(&mut self, activator: Activator) -> TraceAgentQueueReader<Tr>
    {
        // create a new queue for progress and batch information.
        let mut new_queue = VecDeque::new();

        // add the existing batches from the trace
        let mut upper = None;
        self.trace
            .borrow_mut()
            .trace
            .map_batches(|batch| {
                new_queue.push_back(TraceReplayInstruction::Batch(batch.clone(), Some(<Tr::Time as Timestamp>::minimum())));
                upper = Some(batch.upper().clone());
            });

        if let Some(upper) = upper {
            new_queue.push_back(TraceReplayInstruction::Frontier(upper));
        }

        let reference = Rc::new((activator, RefCell::new(new_queue)));

        // wraps the queue in a ref-counted ref cell and enqueue/return it.
        if let Some(queue) = self.queues.upgrade() {
            queue.borrow_mut().push(Rc::downgrade(&reference));
        }
        reference.0.activate();
        reference
    }
}

impl<Tr> TraceAgent<Tr>
where
    Tr: TraceReader+'static,
    Tr::Time: Lattice+Ord+Clone+'static,
{
    /// Copies an existing collection into the supplied scope.
    ///
    /// This method creates an `Arranged` collection that should appear indistinguishable from applying `arrange`
    /// directly to the source collection brought into the local scope. The only caveat is that the initial state
    /// of the collection is its current state, and updates occur from this point forward. The historical changes
    /// the collection experienced in the past are accumulated, and the distinctions from the initial collection
    /// are no longer evident.
    ///
    /// The current behavior is that the introduced collection accumulates updates to some times less or equal
    /// to `self.get_logical_compaction()`. There is *not* currently a guarantee that the updates are accumulated *to*
    /// the frontier, and the resulting collection history may be weirdly partial until this point. In particular,
    /// the historical collection may move through configurations that did not actually occur, even if eventually
    /// arriving at the correct collection. This is probably a bug; although we get to the right place in the end,
    /// the intermediate computation could do something that the original computation did not, like diverge.
    ///
    /// I would expect the semantics to improve to "updates are advanced to `self.get_logical_compaction()`", which
    /// means the computation will run as if starting from exactly this frontier. It is not currently clear whose
    /// responsibility this should be (the trace/batch should only reveal these times, or an operator should know
    /// to advance times before using them).
    ///
    /// # Examples
    ///
    /// ```
    /// extern crate timely;
    /// extern crate differential_dataflow;
    ///
    /// use timely::Config;
    /// use differential_dataflow::input::Input;
    /// use differential_dataflow::operators::arrange::ArrangeBySelf;
    /// use differential_dataflow::operators::reduce::Reduce;
    /// use differential_dataflow::trace::Trace;
    /// use differential_dataflow::trace::implementations::ord::OrdValSpine;
    ///
    /// fn main() {
    ///     ::timely::execute(Config::thread(), |worker| {
    ///
    ///         // create a first dataflow
    ///         let mut trace = worker.dataflow::<u32,_,_>(|scope| {
    ///             // create input handle and collection.
    ///             scope.new_collection_from(0 .. 10).1
    ///                  .arrange_by_self()
    ///                  .trace
    ///         });
    ///
    ///         // do some work.
    ///         worker.step();
    ///         worker.step();
    ///
    ///         // create a second dataflow
    ///         worker.dataflow(move |scope| {
    ///             trace.import(scope)
    ///                  .reduce(move |_key, src, dst| dst.push((*src[0].0, 1)));
    ///         });
    ///
    ///     }).unwrap();
    /// }
    /// ```
    pub fn import<G>(&mut self, scope: &G) -> Arranged<G, TraceAgent<Tr>>
    where
        G: Scope<Timestamp=Tr::Time>,
        Tr::Time: Timestamp,
    {
        self.import_named(scope, "ArrangedSource")
    }

    /// Same as `import`, but allows to name the source.
    pub fn import_named<G>(&mut self, scope: &G, name: &str) -> Arranged<G, TraceAgent<Tr>>
    where
        G: Scope<Timestamp=Tr::Time>,
        Tr::Time: Timestamp,
    {
        // Drop ShutdownButton and return only the arrangement.
        self.import_core(scope, name).0
    }

    /// Imports an arrangement into the supplied scope.
    ///
    /// # Examples
    ///
    /// ```
    /// extern crate timely;
    /// extern crate differential_dataflow;
    ///
    /// use timely::Config;
    /// use timely::dataflow::ProbeHandle;
    /// use timely::dataflow::operators::Probe;
    /// use differential_dataflow::input::InputSession;
    /// use differential_dataflow::operators::arrange::ArrangeBySelf;
    /// use differential_dataflow::operators::reduce::Reduce;
    /// use differential_dataflow::trace::Trace;
    /// use differential_dataflow::trace::implementations::ord::OrdValSpine;
    ///
    /// fn main() {
    ///     ::timely::execute(Config::thread(), |worker| {
    ///
    ///         let mut input = InputSession::<_,(),isize>::new();
    ///         let mut probe = ProbeHandle::new();
    ///
    ///         // create a first dataflow
    ///         let mut trace = worker.dataflow::<u32,_,_>(|scope| {
    ///             // create input handle and collection.
    ///             input.to_collection(scope)
    ///                  .arrange_by_self()
    ///                  .trace
    ///         });
    ///
    ///         // do some work.
    ///         worker.step();
    ///         worker.step();
    ///
    ///         // create a second dataflow
    ///         let mut shutdown = worker.dataflow(|scope| {
    ///             let (arrange, button) = trace.import_core(scope, "Import");
    ///             arrange.stream.probe_with(&mut probe);
    ///             button
    ///         });
    ///
    ///         worker.step();
    ///         worker.step();
    ///         assert!(!probe.done());
    ///
    ///         shutdown.press();
    ///
    ///         worker.step();
    ///         worker.step();
    ///         assert!(probe.done());
    ///
    ///     }).unwrap();
    /// }
    /// ```
    pub fn import_core<G>(&mut self, scope: &G, name: &str) -> (Arranged<G, TraceAgent<Tr>>, ShutdownButton<CapabilitySet<Tr::Time>>)
    where
        G: Scope<Timestamp=Tr::Time>,
        Tr::Time: Timestamp,
    {
        let trace = self.clone();

        let mut shutdown_button = None;

        let stream = {

            let shutdown_button_ref = &mut shutdown_button;
            source(scope, name, move |capability, info| {

                let capabilities = Rc::new(RefCell::new(Some(CapabilitySet::new())));

                let activator = scope.activator_for(&info.address[..]);
                let queue = self.new_listener(activator);

                let activator = scope.activator_for(&info.address[..]);
                *shutdown_button_ref = Some(ShutdownButton::new(capabilities.clone(), activator));

                capabilities.borrow_mut().as_mut().unwrap().insert(capability);

                move |output| {

                    let mut capabilities = capabilities.borrow_mut();
                    if let Some(ref mut capabilities) = *capabilities {

                        let mut borrow = queue.1.borrow_mut();
                        for instruction in borrow.drain(..) {
                            match instruction {
                                TraceReplayInstruction::Frontier(frontier) => {
                                    capabilities.downgrade(&frontier.borrow()[..]);
                                },
                                TraceReplayInstruction::Batch(batch, hint) => {
                                    if let Some(time) = hint {
                                        if !batch.is_empty() {
                                            let delayed = capabilities.delayed(&time);
                                            output.session(&delayed).give(batch);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            })
        };

        (Arranged { stream, trace }, shutdown_button.unwrap())
    }

    /// Imports an arrangement into the supplied scope.
    ///
    /// This variant of import uses the `get_logical_compaction` to forcibly advance timestamps in updates.
    ///
    /// # Examples
    ///
    /// ```
    /// extern crate timely;
    /// extern crate differential_dataflow;
    ///
    /// use timely::Config;
    /// use timely::progress::frontier::AntichainRef;
    /// use timely::dataflow::ProbeHandle;
    /// use timely::dataflow::operators::Probe;
    /// use timely::dataflow::operators::Inspect;
    /// use differential_dataflow::input::InputSession;
    /// use differential_dataflow::operators::arrange::ArrangeBySelf;
    /// use differential_dataflow::operators::reduce::Reduce;
    /// use differential_dataflow::trace::Trace;
    /// use differential_dataflow::trace::TraceReader;
    /// use differential_dataflow::trace::implementations::ord::OrdValSpine;
    /// use differential_dataflow::input::Input;
    ///
    /// fn main() {
    ///     ::timely::execute(Config::thread(), |worker| {
    ///
    ///         let mut probe = ProbeHandle::new();
    ///
    ///         // create a first dataflow
    ///         let (mut handle, mut trace) = worker.dataflow::<u32,_,_>(|scope| {
    ///             // create input handle and collection.
    ///             let (handle, stream) = scope.new_collection();
    ///             let trace = stream.arrange_by_self().trace;
    ///             (handle, trace)
    ///         });
    ///
    ///         handle.insert(0); handle.advance_to(1); handle.flush(); worker.step();
    ///         handle.remove(0); handle.advance_to(2); handle.flush(); worker.step();
    ///         handle.insert(1); handle.advance_to(3); handle.flush(); worker.step();
    ///         handle.remove(1); handle.advance_to(4); handle.flush(); worker.step();
    ///         handle.insert(0); handle.advance_to(5); handle.flush(); worker.step();
    ///
    ///         trace.set_logical_compaction(AntichainRef::new(&[5]));
    ///
    ///         // create a second dataflow
    ///         let mut shutdown = worker.dataflow(|scope| {
    ///             let (arrange, button) = trace.import_frontier(scope, "Import");
    ///             arrange
    ///                 .as_collection(|k,v| (*k,*v))
    ///                 .inner
    ///                 .inspect(|(d,t,r)| {
    ///                     assert!(t >= &5);
    ///                 })
    ///                 .probe_with(&mut probe);
    ///
    ///             button
    ///         });
    ///
    ///         worker.step();
    ///         worker.step();
    ///         assert!(!probe.done());
    ///
    ///         shutdown.press();
    ///
    ///         worker.step();
    ///         worker.step();
    ///         assert!(probe.done());
    ///
    ///     }).unwrap();
    /// }
    /// ```
    pub fn import_frontier<G>(&mut self, scope: &G, name: &str) -> (Arranged<G, TraceFrontier<TraceAgent<Tr>>>, ShutdownButton<CapabilitySet<Tr::Time>>)
    where
        G: Scope<Timestamp=Tr::Time>,
        Tr::Time: Timestamp+ Lattice+Ord+Clone+'static,
        Tr: TraceReader,
    {
        // This frontier describes our only guarantee on the compaction frontier.
        let since = self.get_logical_compaction().to_owned();
        self.import_frontier_core(scope, name, since, Antichain::new())
    }

    /// Import a trace restricted to a specific time interval `[since, until)`.
    ///
    /// All updates present in the input trace will be first advanced to `since`, and then either emitted,
    /// or if greater or equal to `until`, suppressed. Once all times are certain to be greater or equal
    /// to `until` the operator capability will be dropped.
    ///
    /// Invoking this method with an `until` of `Antichain::new()` will perform no filtering, as the empty
    /// frontier indicates the end of times.
    pub fn import_frontier_core<G>(&mut self, scope: &G, name: &str, since: Antichain<Tr::Time>, until: Antichain<Tr::Time>) -> (Arranged<G, TraceFrontier<TraceAgent<Tr>>>, ShutdownButton<CapabilitySet<Tr::Time>>)
    where
        G: Scope<Timestamp=Tr::Time>,
        Tr::Time: Timestamp+ Lattice+Ord+Clone+'static,
        Tr: TraceReader,
    {
        let trace = self.clone();
        let trace = TraceFrontier::make_from(trace, since.borrow(), until.borrow());

        let mut shutdown_button = None;

        let stream = {

            let shutdown_button_ref = &mut shutdown_button;
            source(scope, name, move |capability, info| {

                let capabilities = Rc::new(RefCell::new(Some(CapabilitySet::new())));

                let activator = scope.activator_for(&info.address[..]);
                let queue = self.new_listener(activator);

                let activator = scope.activator_for(&info.address[..]);
                *shutdown_button_ref = Some(ShutdownButton::new(capabilities.clone(), activator));

                capabilities.borrow_mut().as_mut().unwrap().insert(capability);

                move |output| {

                    let mut capabilities = capabilities.borrow_mut();
                    if let Some(ref mut capabilities) = *capabilities {
                        let mut borrow = queue.1.borrow_mut();
                        for instruction in borrow.drain(..) {
                            // If we have dropped the capabilities due to `until`, attempt no further work.
                            // Without the capabilities, we should soon be shut down (once this loop ends).
                            if !capabilities.is_empty() {
                                match instruction {
                                    TraceReplayInstruction::Frontier(frontier) => {
                                        if timely::PartialOrder::less_equal(&until, &frontier) {
                                            // It might be nice to actively *drop* `capabilities`, but it seems
                                            // complicated logically (i.e. we'd have to break out of the loop).
                                            capabilities.downgrade(&[]);
                                        } else {
                                            capabilities.downgrade(&frontier.borrow()[..]);
                                        }
                                    },
                                    TraceReplayInstruction::Batch(batch, hint) => {
                                        if let Some(time) = hint {
                                            if !batch.is_empty() {
                                                let delayed = capabilities.delayed(&time);
                                                output.session(&delayed).give(BatchFrontier::make_from(batch, since.borrow(), until.borrow()));
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            })
        };

        (Arranged { stream, trace }, shutdown_button.unwrap())
    }
}



/// Wrapper than can drop shared references.
pub struct ShutdownButton<T> {
    reference: Rc<RefCell<Option<T>>>,
    activator: Activator,
}

impl<T> ShutdownButton<T> {
    /// Creates a new ShutdownButton.
    pub fn new(reference: Rc<RefCell<Option<T>>>, activator: Activator) -> Self {
        Self { reference, activator }
    }
    /// Push the shutdown button, dropping the shared objects.
    pub fn press(&mut self) {
        *self.reference.borrow_mut() = None;
        self.activator.activate();
    }
    /// Hotwires the button to one that is pressed if dropped.
    pub fn press_on_drop(self) -> ShutdownDeadmans<T> {
        ShutdownDeadmans {
            button: self
        }
    }
}

/// A deadman's switch version of a shutdown button.
///
/// This type hosts a shutdown button and will press it when dropped.
pub struct ShutdownDeadmans<T> {
    button: ShutdownButton<T>,
}

impl<T> Drop for ShutdownDeadmans<T> {
    fn drop(&mut self) {
        self.button.press();
    }
}

impl<Tr> Clone for TraceAgent<Tr>
where
    Tr: TraceReader,
    Tr::Time: Lattice+Ord+Clone+'static,
{
    fn clone(&self) -> Self {

        if let Some(logging) = &self.logging {
            logging.log(
                ::logging::TraceShare { operator: self.operator.global_id, diff: 1 }
            );
        }

        // increase counts for wrapped `TraceBox`.
        let empty_frontier = Antichain::new();
        self.trace.borrow_mut().adjust_logical_compaction(empty_frontier.borrow(), self.logical_compaction.borrow());
        self.trace.borrow_mut().adjust_physical_compaction(empty_frontier.borrow(), self.physical_compaction.borrow());

        TraceAgent {
            trace: self.trace.clone(),
            queues: self.queues.clone(),
            logical_compaction: self.logical_compaction.clone(),
            physical_compaction: self.physical_compaction.clone(),
            operator: self.operator.clone(),
            logging: self.logging.clone(),
            temp_antichain: Antichain::new(),
        }
    }
}

impl<Tr> Drop for TraceAgent<Tr>
where
    Tr: TraceReader,
    Tr::Time: Lattice+Ord+Clone+'static,
{
    fn drop(&mut self) {

        if let Some(logging) = &self.logging {
            logging.log(
                ::logging::TraceShare { operator: self.operator.global_id, diff: -1 }
            );
        }

        // decrement borrow counts to remove all holds
        let empty_frontier = Antichain::new();
        self.trace.borrow_mut().adjust_logical_compaction(self.logical_compaction.borrow(), empty_frontier.borrow());
        self.trace.borrow_mut().adjust_physical_compaction(self.physical_compaction.borrow(), empty_frontier.borrow());
    }
}
