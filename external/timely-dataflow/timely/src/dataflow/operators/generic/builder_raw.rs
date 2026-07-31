//! Types to build operators with general shapes.
//!
//! These types expose some raw timely interfaces, and while public so that others can build on them,
//! they require some sophistication to use correctly. I recommend checking out `builder_rc.rs` for
//! an interface that is intentionally harder to mis-use.

use std::cell::RefCell;
use std::default::Default;
use std::rc::Rc;

use crate::scheduling::{Activations, Schedule};

use crate::progress::{operate::SharedProgress, Antichain, Operate, Timestamp};
use crate::progress::{Source, Target};

use crate::dataflow::channels::pact::ParallelizationContractCore;
use crate::dataflow::channels::pushers::TeeCore;
use crate::dataflow::operators::generic::operator_info::OperatorInfo;
use crate::dataflow::{Scope, StreamCore};
use crate::Container;

/// Contains type-free information about the operator properties.
#[derive(Debug)]
pub struct OperatorShape {
    name: String,   // A meaningful name for the operator.
    notify: bool,   // Does the operator require progress notifications.
    peers: usize,   // The total number of workers in the computation.
    inputs: usize,  // The number of input ports.
    outputs: usize, // The number of output ports.
}

/// Core data for the structure of an operator, minus scope and logic.
impl OperatorShape {
    fn new(name: String, peers: usize) -> Self {
        OperatorShape {
            name,
            notify: true,
            peers,
            inputs: 0,
            outputs: 0,
        }
    }

    /// The number of inputs of this operator
    pub fn inputs(&self) -> usize {
        self.inputs
    }

    /// The number of outputs of this operator
    pub fn outputs(&self) -> usize {
        self.outputs
    }
}

/// Builds operators with generic shape.
#[derive(Debug)]
pub struct OperatorBuilder<G: Scope> {
    scope: G,
    index: usize,
    global: usize,
    address: Vec<usize>, // path to the operator (ending with index).
    shape: OperatorShape,
    summary: Vec<Vec<Antichain<<G::Timestamp as Timestamp>::Summary>>>,
}

impl<G: Scope> OperatorBuilder<G> {
    /// Allocates a new generic operator builder from its containing scope.
    pub fn new(name: String, mut scope: G) -> Self {
        let global = scope.new_identifier();
        let index = scope.allocate_operator_index();
        let mut address = scope.addr();
        address.push(index);
        let peers = scope.peers();

        OperatorBuilder {
            scope,
            index,
            global,
            address,
            shape: OperatorShape::new(name, peers),
            summary: vec![],
        }
    }

    /// The operator's scope-local index.
    pub fn index(&self) -> usize {
        self.index
    }

    /// The operator's worker-unique identifier.
    pub fn global(&self) -> usize {
        self.global
    }

    /// Return a reference to the operator's shape
    pub fn shape(&self) -> &OperatorShape {
        &self.shape
    }

    /// Indicates whether the operator requires frontier information.
    pub fn set_notify(&mut self, notify: bool) {
        self.shape.notify = notify;
    }

    /// Adds a new input to a generic operator builder, returning the `Pull` implementor to use.
    pub fn new_input<D: Container, P>(&mut self, stream: &StreamCore<G, D>, pact: P) -> P::Puller
    where
        P: ParallelizationContractCore<G::Timestamp, D>,
    {
        let connection = vec![Antichain::from_elem(Default::default()); self.shape.outputs];
        self.new_input_connection(stream, pact, connection)
    }

    /// Adds a new input to a generic operator builder, returning the `Pull` implementor to use.
    pub fn new_input_connection<D: Container, P>(
        &mut self,
        stream: &StreamCore<G, D>,
        pact: P,
        connection: Vec<Antichain<<G::Timestamp as Timestamp>::Summary>>,
    ) -> P::Puller
    where
        P: ParallelizationContractCore<G::Timestamp, D>,
    {
        let channel_id = self.scope.new_identifier();
        let logging = self.scope.logging();
        let (sender, receiver) =
            pact.connect(&mut self.scope, channel_id, &self.address[..], logging);
        let target = Target::new(self.index, self.shape.inputs);
        stream.connect_to(target, sender, channel_id);

        self.shape.inputs += 1;
        assert_eq!(self.shape.outputs, connection.len());
        self.summary.push(connection);

        receiver
    }

    /// Adds a new output to a generic operator builder, returning the `Push` implementor to use.
    pub fn new_output<D: Container>(&mut self) -> (TeeCore<G::Timestamp, D>, StreamCore<G, D>) {
        let connection = vec![Antichain::from_elem(Default::default()); self.shape.inputs];
        self.new_output_connection(connection)
    }

    /// Adds a new output to a generic operator builder, returning the `Push` implementor to use.
    pub fn new_output_connection<D: Container>(
        &mut self,
        connection: Vec<Antichain<<G::Timestamp as Timestamp>::Summary>>,
    ) -> (TeeCore<G::Timestamp, D>, StreamCore<G, D>) {
        let (targets, registrar) = TeeCore::<G::Timestamp, D>::new();
        let source = Source::new(self.index, self.shape.outputs);
        let stream = StreamCore::new(source, registrar, self.scope.clone());

        self.shape.outputs += 1;
        assert_eq!(self.shape.inputs, connection.len());
        for (summary, entry) in self.summary.iter_mut().zip(connection.into_iter()) {
            summary.push(entry);
        }

        (targets, stream)
    }

    /// Creates an operator implementation from supplied logic constructor.
    pub fn build<L>(mut self, logic: L)
    where
        L: FnMut(&mut SharedProgress<G::Timestamp>) -> bool + 'static,
    {
        let inputs = self.shape.inputs;
        let outputs = self.shape.outputs;

        let operator = OperatorCore {
            shape: self.shape,
            address: self.address,
            activations: self.scope.activations(),
            logic,
            shared_progress: Rc::new(RefCell::new(SharedProgress::new(inputs, outputs))),
            summary: self.summary,
        };

        self.scope
            .add_operator_with_indices(Box::new(operator), self.index, self.global);
    }

    /// Information describing the operator.
    pub fn operator_info(&self) -> OperatorInfo {
        OperatorInfo::new(self.index, self.global, &self.address[..])
    }
}

struct OperatorCore<T, L>
where
    T: Timestamp,
    L: FnMut(&mut SharedProgress<T>) -> bool + 'static,
{
    shape: OperatorShape,
    address: Vec<usize>,
    logic: L,
    shared_progress: Rc<RefCell<SharedProgress<T>>>,
    activations: Rc<RefCell<Activations>>,
    summary: Vec<Vec<Antichain<T::Summary>>>,
}

impl<T, L> Schedule for OperatorCore<T, L>
where
    T: Timestamp,
    L: FnMut(&mut SharedProgress<T>) -> bool + 'static,
{
    fn name(&self) -> &str {
        &self.shape.name
    }
    fn path(&self) -> &[usize] {
        &self.address[..]
    }
    fn schedule(&mut self) -> bool {
        let shared_progress = &mut *self.shared_progress.borrow_mut();
        (self.logic)(shared_progress)
    }
}

impl<T, L> Operate<T> for OperatorCore<T, L>
where
    T: Timestamp,
    L: FnMut(&mut SharedProgress<T>) -> bool + 'static,
{
    fn inputs(&self) -> usize {
        self.shape.inputs
    }
    fn outputs(&self) -> usize {
        self.shape.outputs
    }

    // announce internal topology as fully connected, and hold all default capabilities.
    fn get_internal_summary(
        &mut self,
    ) -> (
        Vec<Vec<Antichain<T::Summary>>>,
        Rc<RefCell<SharedProgress<T>>>,
    ) {
        // Request the operator to be scheduled at least once.
        self.activations.borrow_mut().activate(&self.address[..]);

        // by default, we reserve a capability for each output port at `Default::default()`.
        self.shared_progress
            .borrow_mut()
            .internals
            .iter_mut()
            .for_each(|output| output.update(T::minimum(), self.shape.peers as i64));

        (self.summary.clone(), self.shared_progress.clone())
    }

    // initialize self.frontier antichains as indicated by hosting scope.
    fn set_external_summary(&mut self) {
        // should we schedule the operator here, or just await the first invocation?
        self.schedule();
    }

    fn notify_me(&self) -> bool {
        self.shape.notify
    }
}
