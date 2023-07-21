//! Types to build operators with general shapes.

use std::rc::Rc;
use std::cell::RefCell;
use std::default::Default;

use crate::progress::{ChangeBatch, Timestamp};
use crate::progress::operate::SharedProgress;
use crate::progress::frontier::{Antichain, MutableAntichain};

use crate::Container;
use crate::dataflow::{Scope, StreamCore};
use crate::dataflow::channels::pushers::TeeCore;
use crate::dataflow::channels::pushers::CounterCore as PushCounter;
use crate::dataflow::channels::pushers::buffer::BufferCore as PushBuffer;
use crate::dataflow::channels::pact::ParallelizationContractCore;
use crate::dataflow::channels::pullers::Counter as PullCounter;
use crate::dataflow::operators::capability::Capability;
use crate::dataflow::operators::generic::handles::{InputHandleCore, new_input_handle, OutputWrapper};
use crate::dataflow::operators::generic::operator_info::OperatorInfo;
use crate::dataflow::operators::generic::builder_raw::OperatorShape;

use crate::logging::TimelyLogger as Logger;

use super::builder_raw::OperatorBuilder as OperatorBuilderRaw;

/// Builds operators with generic shape.
#[derive(Debug)]
pub struct OperatorBuilder<G: Scope> {
    builder: OperatorBuilderRaw<G>,
    frontier: Vec<MutableAntichain<G::Timestamp>>,
    consumed: Vec<Rc<RefCell<ChangeBatch<G::Timestamp>>>>,
    internal: Rc<RefCell<Vec<Rc<RefCell<ChangeBatch<G::Timestamp>>>>>>,
    produced: Vec<Rc<RefCell<ChangeBatch<G::Timestamp>>>>,
    logging: Option<Logger>,
}

impl<G: Scope> OperatorBuilder<G> {

    /// Allocates a new generic operator builder from its containing scope.
    pub fn new(name: String, scope: G) -> Self {
        let logging = scope.logging();
        OperatorBuilder {
            builder: OperatorBuilderRaw::new(name, scope),
            frontier: Vec::new(),
            consumed: Vec::new(),
            internal: Rc::new(RefCell::new(Vec::new())),
            produced: Vec::new(),
            logging,
        }
    }

    /// Indicates whether the operator requires frontier information.
    pub fn set_notify(&mut self, notify: bool) {
        self.builder.set_notify(notify);
    }

    /// Adds a new input to a generic operator builder, returning the `Pull` implementor to use.
    pub fn new_input<D: Container, P>(&mut self, stream: &StreamCore<G, D>, pact: P) -> InputHandleCore<G::Timestamp, D, P::Puller>
    where
        P: ParallelizationContractCore<G::Timestamp, D> {

        let connection = vec![Antichain::from_elem(Default::default()); self.builder.shape().outputs()];
        self.new_input_connection(stream, pact, connection)
    }

    /// Adds a new input with connection information to a generic operator builder, returning the `Pull` implementor to use.
    ///
    /// The `connection` parameter contains promises made by the operator for each of the existing *outputs*, that any timestamp
    /// appearing at the input, any output timestamp will be greater than or equal to the input timestamp subjected to a `Summary`
    /// greater or equal to some element of the corresponding antichain in `connection`.
    ///
    /// Commonly the connections are either the unit summary, indicating the same timestamp might be produced as output, or an empty
    /// antichain indicating that there is no connection from the input to the output.
    pub fn new_input_connection<D: Container, P>(&mut self, stream: &StreamCore<G, D>, pact: P, connection: Vec<Antichain<<G::Timestamp as Timestamp>::Summary>>) -> InputHandleCore<G::Timestamp, D, P::Puller>
        where
            P: ParallelizationContractCore<G::Timestamp, D> {

        let puller = self.builder.new_input_connection(stream, pact, connection);

        let input = PullCounter::new(puller);
        self.frontier.push(MutableAntichain::new());
        self.consumed.push(input.consumed().clone());

        new_input_handle(input, self.internal.clone(), self.logging.clone())
    }

    /// Adds a new output to a generic operator builder, returning the `Push` implementor to use.
    pub fn new_output<D: Container>(&mut self) -> (OutputWrapper<G::Timestamp, D, TeeCore<G::Timestamp, D>>, StreamCore<G, D>) {
        let connection = vec![Antichain::from_elem(Default::default()); self.builder.shape().inputs()];
        self.new_output_connection(connection)
    }

    /// Adds a new output with connection information to a generic operator builder, returning the `Push` implementor to use.
    ///
    /// The `connection` parameter contains promises made by the operator for each of the existing *inputs*, that any timestamp
    /// appearing at the input, any output timestamp will be greater than or equal to the input timestamp subjected to a `Summary`
    /// greater or equal to some element of the corresponding antichain in `connection`.
    ///
    /// Commonly the connections are either the unit summary, indicating the same timestamp might be produced as output, or an empty
    /// antichain indicating that there is no connection from the input to the output.
    pub fn new_output_connection<D: Container>(&mut self, connection: Vec<Antichain<<G::Timestamp as Timestamp>::Summary>>) -> (OutputWrapper<G::Timestamp, D, TeeCore<G::Timestamp, D>>, StreamCore<G, D>) {

        let (tee, stream) = self.builder.new_output_connection(connection);

        let internal = Rc::new(RefCell::new(ChangeBatch::new()));
        self.internal.borrow_mut().push(internal.clone());

        let mut buffer = PushBuffer::new(PushCounter::new(tee));
        self.produced.push(buffer.inner().produced().clone());

        (OutputWrapper::new(buffer, internal), stream)
    }

    /// Creates an operator implementation from supplied logic constructor.
    pub fn build<B, L>(self, constructor: B)
    where
        B: FnOnce(Vec<Capability<G::Timestamp>>) -> L,
        L: FnMut(&[MutableAntichain<G::Timestamp>])+'static
    {
        self.build_reschedule(|caps| {
            let mut logic = constructor(caps);
            move |frontier| { logic(frontier); false }
        })
    }

    /// Creates an operator implementation from supplied logic constructor.
    ///
    /// Unlike `build`, the supplied closure can indicate if the operator
    /// should be considered incomplete. The `build` method indicates that
    /// the operator is never incomplete and can be shut down at the system's
    /// discretion.
    pub fn build_reschedule<B, L>(self, constructor: B)
    where
        B: FnOnce(Vec<Capability<G::Timestamp>>) -> L,
        L: FnMut(&[MutableAntichain<G::Timestamp>])->bool+'static
    {
        // create capabilities, discard references to their creation.
        let mut capabilities = Vec::with_capacity(self.internal.borrow().len());
        for batch in self.internal.borrow().iter() {
            capabilities.push(Capability::new(G::Timestamp::minimum(), batch.clone()));
            // Discard evidence of creation, as we are assumed to start with one.
            batch.borrow_mut().clear();
        }

        let mut logic = constructor(capabilities);

        let mut self_frontier = self.frontier;
        let self_consumed = self.consumed;
        let self_internal = self.internal;
        let self_produced = self.produced;

        let raw_logic =
        move |progress: &mut SharedProgress<G::Timestamp>| {

            // drain frontier changes
            for (progress, frontier) in progress.frontiers.iter_mut().zip(self_frontier.iter_mut()) {
                frontier.update_iter(progress.drain());
            }

            // invoke supplied logic
            let result = logic(&self_frontier[..]);

            // move batches of consumed changes.
            for (progress, consumed) in progress.consumeds.iter_mut().zip(self_consumed.iter()) {
                consumed.borrow_mut().drain_into(progress);
            }

            // move batches of internal changes.
            let self_internal_borrow = self_internal.borrow_mut();
            for index in 0 .. self_internal_borrow.len() {
                let mut borrow = self_internal_borrow[index].borrow_mut();
                progress.internals[index].extend(borrow.drain());
            }

            // move batches of produced changes.
            for (progress, produced) in progress.produceds.iter_mut().zip(self_produced.iter()) {
                produced.borrow_mut().drain_into(progress);
            }

            result
        };

        self.builder.build(raw_logic);
    }

    /// Get the identifier assigned to the operator being constructed
    pub fn index(&self) -> usize {
        self.builder.index()
    }

    /// The operator's worker-unique identifier.
    pub fn global(&self) -> usize {
        self.builder.global()
    }

    /// Return a reference to the operator's shape
    pub fn shape(&self) -> &OperatorShape {
        self.builder.shape()
    }

    /// Creates operator info for the operator.
    pub fn operator_info(&self) -> OperatorInfo {
        self.builder.operator_info()
    }
}


#[cfg(test)]
mod tests {

    #[test]
    #[should_panic]
    fn incorrect_capabilities() {

        // This tests that if we attempt to use a capability associated with the
        // wrong output, there is a run-time assertion.

        use crate::dataflow::operators::generic::builder_rc::OperatorBuilder;

        crate::example(|scope| {

            let mut builder = OperatorBuilder::new("Failure".to_owned(), scope.clone());

            // let mut input = builder.new_input(stream, Pipeline);
            let (mut output1, _stream1) = builder.new_output::<Vec<()>>();
            let (mut output2, _stream2) = builder.new_output::<Vec<()>>();

            builder.build(move |capabilities| {
                move |_frontiers| {

                    let mut output_handle1 = output1.activate();
                    let mut output_handle2 = output2.activate();

                    // NOTE: Using incorrect capabilities here.
                    output_handle2.session(&capabilities[0]);
                    output_handle1.session(&capabilities[1]);
                }
            });
        })
    }

    #[test]
    fn correct_capabilities() {

        // This tests that if we attempt to use capabilities with the correct outputs
        // there is no runtime assertion

        use crate::dataflow::operators::generic::builder_rc::OperatorBuilder;

        crate::example(|scope| {

            let mut builder = OperatorBuilder::new("Failure".to_owned(), scope.clone());

            // let mut input = builder.new_input(stream, Pipeline);
            let (mut output1, _stream1) = builder.new_output::<Vec<()>>();
            let (mut output2, _stream2) = builder.new_output::<Vec<()>>();

            builder.build(move |mut capabilities| {
                move |_frontiers| {

                    let mut output_handle1 = output1.activate();
                    let mut output_handle2 = output2.activate();

                    // Avoid second call.
                    if !capabilities.is_empty() {

                        // NOTE: Using correct capabilities here.
                        output_handle1.session(&capabilities[0]);
                        output_handle2.session(&capabilities[1]);

                        capabilities.clear();
                    }
                }
            });

            "Hello".to_owned()
        });
    }
}
