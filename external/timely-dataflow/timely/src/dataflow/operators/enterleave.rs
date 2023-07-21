//! Extension traits to move a `Stream` between an outer `Scope` and inner `Scope`.
//!
//! Each `Stream` indicates its containing `Scope` as part of its type signature. To create a new
//! stream with the same contents in another scope, one must explicit use the methods `enter` and
//! `leave`, to clearly indicate the transition to the timely dataflow progress tracking logic.
//!
//! # Examples
//! ```
//! use timely::dataflow::scopes::Scope;
//! use timely::dataflow::operators::{Enter, Leave, ToStream, Inspect};
//!
//! timely::example(|outer| {
//!     let stream = (0..9).to_stream(outer);
//!     let output = outer.region(|inner| {
//!         stream.enter(inner)
//!               .inspect(|x| println!("in nested scope: {:?}", x))
//!               .leave()
//!     });
//! });
//! ```

use std::marker::PhantomData;

use crate::progress::Timestamp;
use crate::progress::timestamp::Refines;
use crate::progress::{Source, Target};
use crate::order::Product;
use crate::{Container, Data};
use crate::communication::Push;
use crate::dataflow::channels::pushers::{CounterCore, TeeCore};
use crate::dataflow::channels::{BundleCore, Message};

use crate::worker::AsWorker;
use crate::dataflow::{StreamCore, Scope, Stream};
use crate::dataflow::scopes::{Child, ScopeParent};
use crate::dataflow::operators::delay::Delay;

/// Extension trait to move a `Stream` into a child of its current `Scope`.
pub trait Enter<G: Scope, T: Timestamp+Refines<G::Timestamp>, C: Container> {
    /// Moves the `Stream` argument into a child of its current `Scope`.
    ///
    /// # Examples
    /// ```
    /// use timely::dataflow::scopes::Scope;
    /// use timely::dataflow::operators::{Enter, Leave, ToStream};
    ///
    /// timely::example(|outer| {
    ///     let stream = (0..9).to_stream(outer);
    ///     let output = outer.region(|inner| {
    ///         stream.enter(inner).leave()
    ///     });
    /// });
    /// ```
    fn enter<'a>(&self, _: &Child<'a, G, T>) -> StreamCore<Child<'a, G, T>, C>;
}

use crate::dataflow::scopes::child::Iterative;

/// Extension trait to move a `Stream` into a child of its current `Scope` setting the timestamp for each element.
pub trait EnterAt<G: Scope, T: Timestamp, D: Data> {
    /// Moves the `Stream` argument into a child of its current `Scope` setting the timestamp for each element by `initial`.
    ///
    /// # Examples
    /// ```
    /// use timely::dataflow::scopes::Scope;
    /// use timely::dataflow::operators::{EnterAt, Leave, ToStream};
    ///
    /// timely::example(|outer| {
    ///     let stream = (0..9u64).to_stream(outer);
    ///     let output = outer.iterative(|inner| {
    ///         stream.enter_at(inner, |x| *x).leave()
    ///     });
    /// });
    /// ```
    fn enter_at<'a, F:FnMut(&D)->T+'static>(&self, scope: &Iterative<'a, G, T>, initial: F) -> Stream<Iterative<'a, G, T>, D> ;
}

impl<G: Scope, T: Timestamp, D: Data, E: Enter<G, Product<<G as ScopeParent>::Timestamp, T>, Vec<D>>> EnterAt<G, T, D> for E {
    fn enter_at<'a, F:FnMut(&D)->T+'static>(&self, scope: &Iterative<'a, G, T>, mut initial: F) ->
        Stream<Iterative<'a, G, T>, D> {
            self.enter(scope).delay(move |datum, time| Product::new(time.clone().to_outer(), initial(datum)))
    }
}

impl<G: Scope, T: Timestamp+Refines<G::Timestamp>, C: Data+Container> Enter<G, T, C> for StreamCore<G, C> {
    fn enter<'a>(&self, scope: &Child<'a, G, T>) -> StreamCore<Child<'a, G, T>, C> {

        use crate::scheduling::Scheduler;

        let (targets, registrar) = TeeCore::<T, C>::new();
        let ingress = IngressNub {
            targets: CounterCore::new(targets),
            phantom: ::std::marker::PhantomData,
            activator: scope.activator_for(&scope.addr()),
            active: false,
        };
        let produced = ingress.targets.produced().clone();

        let input = scope.subgraph.borrow_mut().new_input(produced);

        let channel_id = scope.clone().new_identifier();
        self.connect_to(input, ingress, channel_id);
        StreamCore::new(Source::new(0, input.port), registrar, scope.clone())
    }
}

/// Extension trait to move a `Stream` to the parent of its current `Scope`.
pub trait Leave<G: Scope, D: Container> {
    /// Moves a `Stream` to the parent of its current `Scope`.
    ///
    /// # Examples
    /// ```
    /// use timely::dataflow::scopes::Scope;
    /// use timely::dataflow::operators::{Enter, Leave, ToStream};
    ///
    /// timely::example(|outer| {
    ///     let stream = (0..9).to_stream(outer);
    ///     let output = outer.region(|inner| {
    ///         stream.enter(inner).leave()
    ///     });
    /// });
    /// ```
    fn leave(&self) -> StreamCore<G, D>;
}

impl<'a, G: Scope, D: Clone+Container, T: Timestamp+Refines<G::Timestamp>> Leave<G, D> for StreamCore<Child<'a, G, T>, D> {
    fn leave(&self) -> StreamCore<G, D> {

        let scope = self.scope();

        let output = scope.subgraph.borrow_mut().new_output();
        let (targets, registrar) = TeeCore::<G::Timestamp, D>::new();
        let channel_id = scope.clone().new_identifier();
        self.connect_to(Target::new(0, output.port), EgressNub { targets, phantom: PhantomData }, channel_id);

        StreamCore::new(
            output,
            registrar,
            scope.parent,
        )
    }
}


struct IngressNub<TOuter: Timestamp, TInner: Timestamp+Refines<TOuter>, TData: Container> {
    targets: CounterCore<TInner, TData, TeeCore<TInner, TData>>,
    phantom: ::std::marker::PhantomData<TOuter>,
    activator: crate::scheduling::Activator,
    active: bool,
}

impl<TOuter: Timestamp, TInner: Timestamp+Refines<TOuter>, TData: Container> Push<BundleCore<TOuter, TData>> for IngressNub<TOuter, TInner, TData> {
    fn push(&mut self, element: &mut Option<BundleCore<TOuter, TData>>) {
        if let Some(message) = element {
            let outer_message = message.as_mut();
            let data = ::std::mem::take(&mut outer_message.data);
            let mut inner_message = Some(BundleCore::from_typed(Message::new(TInner::to_inner(outer_message.time.clone()), data, 0, 0)));
            self.targets.push(&mut inner_message);
            if let Some(inner_message) = inner_message {
                if let Some(inner_message) = inner_message.if_typed() {
                    outer_message.data = inner_message.data;
                }
            }
            self.active = true;
        }
        else {
            if self.active {
                self.activator.activate();
                self.active = false;
            }
            self.targets.done();
        }
    }
}


struct EgressNub<TOuter: Timestamp, TInner: Timestamp+Refines<TOuter>, TData: Data> {
    targets: TeeCore<TOuter, TData>,
    phantom: PhantomData<TInner>,
}

impl<TOuter, TInner, TData: Container> Push<BundleCore<TInner, TData>> for EgressNub<TOuter, TInner, TData>
where TOuter: Timestamp, TInner: Timestamp+Refines<TOuter>, TData: Data {
    fn push(&mut self, message: &mut Option<BundleCore<TInner, TData>>) {
        if let Some(message) = message {
            let inner_message = message.as_mut();
            let data = ::std::mem::take(&mut inner_message.data);
            let mut outer_message = Some(BundleCore::from_typed(Message::new(inner_message.time.clone().to_outer(), data, 0, 0)));
            self.targets.push(&mut outer_message);
            if let Some(outer_message) = outer_message {
                if let Some(outer_message) = outer_message.if_typed() {
                    inner_message.data = outer_message.data;
                }
            }
        }
        else { self.targets.done(); }
    }
}

#[cfg(test)]
mod test {
    /// Test that nested scopes with pass-through edges (no operators) correctly communicate progress.
    ///
    /// This is for issue: https://github.com/TimelyDataflow/timely-dataflow/issues/377
    #[test]
    fn test_nested() {

        use crate::dataflow::{InputHandle, ProbeHandle};
        use crate::dataflow::operators::{Input, Inspect, Probe};

        use crate::dataflow::Scope;
        use crate::dataflow::operators::{Enter, Leave};

        // initializes and runs a timely dataflow.
        crate::execute(crate::Config::process(3), |worker| {

            let index = worker.index();
            let mut input = InputHandle::new();
            let mut probe = ProbeHandle::new();

            // create a new input, exchange data, and inspect its output
            worker.dataflow(|scope| {
                let data = scope.input_from(&mut input);

                scope.region(|inner| {

                    let data = data.enter(inner);
                    inner.region(|inner2| data.enter(inner2).leave()).leave()
                })
                    .inspect(move |x| println!("worker {}:\thello {}", index, x))
                    .probe_with(&mut probe);
            });

            // introduce data and watch!
            input.advance_to(0);
            for round in 0..10 {
                if index == 0 {
                    input.send(round);
                }
                input.advance_to(round + 1);
                while probe.less_than(input.time()) {
                    worker.step_or_park(None);
                }
            }
        }).unwrap();
    }

}
