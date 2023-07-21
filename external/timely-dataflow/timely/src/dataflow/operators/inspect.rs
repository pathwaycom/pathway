//! Extension trait and implementation for observing and action on streamed data.

use std::rc::Rc;
#[cfg(feature = "columnation")]
use timely_container::columnation::{Columnation, TimelyStack};
use crate::Container;
use crate::Data;
use crate::dataflow::channels::pact::Pipeline;
use crate::dataflow::{Scope, StreamCore};
use crate::dataflow::operators::generic::Operator;

/// Methods to inspect records and batches of records on a stream.
pub trait Inspect<G: Scope, C: Container>: InspectCore<G, C> + Sized {
    /// Runs a supplied closure on each observed data element.
    ///
    /// # Examples
    /// ```
    /// use timely::dataflow::operators::{ToStream, Map, Inspect};
    ///
    /// timely::example(|scope| {
    ///     (0..10).to_stream(scope)
    ///            .inspect(|x| println!("seen: {:?}", x));
    /// });
    /// ```
    fn inspect(&self, mut func: impl FnMut(&C::Item)+'static) -> Self {
        self.inspect_batch(move |_, data| {
            for datum in data.iter() { func(datum); }
        })
    }

    /// Runs a supplied closure on each observed data element and associated time.
    ///
    /// # Examples
    /// ```
    /// use timely::dataflow::operators::{ToStream, Map, Inspect};
    ///
    /// timely::example(|scope| {
    ///     (0..10).to_stream(scope)
    ///            .inspect_time(|t, x| println!("seen at: {:?}\t{:?}", t, x));
    /// });
    /// ```
    fn inspect_time(&self, mut func: impl FnMut(&G::Timestamp, &C::Item)+'static) -> Self {
        self.inspect_batch(move |time, data| {
            for datum in data.iter() {
                func(&time, &datum);
            }
        })
    }

    /// Runs a supplied closure on each observed data batch (time and data slice).
    ///
    /// # Examples
    /// ```
    /// use timely::dataflow::operators::{ToStream, Map, Inspect};
    ///
    /// timely::example(|scope| {
    ///     (0..10).to_stream(scope)
    ///            .inspect_batch(|t,xs| println!("seen at: {:?}\t{:?} records", t, xs.len()));
    /// });
    /// ```
    fn inspect_batch(&self, mut func: impl FnMut(&G::Timestamp, &[C::Item])+'static) -> Self {
        self.inspect_core(move |event| {
            if let Ok((time, data)) = event {
                func(time, data);
            }
        })
    }

    /// Runs a supplied closure on each observed data batch, and each frontier advancement.
    ///
    /// Rust's `Result` type is used to distinguish the events, with `Ok` for time and data,
    /// and `Err` for frontiers. Frontiers are only presented when they change.
    ///
    /// # Examples
    /// ```
    /// use timely::dataflow::operators::{ToStream, Map, Inspect};
    ///
    /// timely::example(|scope| {
    ///     (0..10).to_stream(scope)
    ///            .inspect_core(|event| {
    ///                match event {
    ///                    Ok((time, data)) => println!("seen at: {:?}\t{:?} records", time, data.len()),
    ///                    Err(frontier) => println!("frontier advanced to {:?}", frontier),
    ///                }
    ///             });
    /// });
    /// ```
    fn inspect_core<F>(&self, func: F) -> Self where F: FnMut(Result<(&G::Timestamp, &[C::Item]), &[G::Timestamp]>)+'static;
}

impl<G: Scope, D: Data> Inspect<G, Vec<D>> for StreamCore<G, Vec<D>> {
    fn inspect_core<F>(&self, mut func: F) -> Self where F: FnMut(Result<(&G::Timestamp, &[D]), &[G::Timestamp]>) + 'static {
        self.inspect_container(move |r| func(r.map(|(t, c)| (t, &c[..]))))
    }
}

#[cfg(feature = "columnation")]
impl<G: Scope, D: Data+Columnation> Inspect<G, TimelyStack<D>> for StreamCore<G, TimelyStack<D>> {
    fn inspect_core<F>(&self, mut func: F) -> Self where F: FnMut(Result<(&G::Timestamp, &[D]), &[G::Timestamp]>) + 'static {
        self.inspect_container(move |r| func(r.map(|(t, c)| (t, &c[..]))))
    }
}

impl<G: Scope, C: Container> Inspect<G, Rc<C>> for StreamCore<G, Rc<C>>
    where C: AsRef<[C::Item]>
{
    fn inspect_core<F>(&self, mut func: F) -> Self where F: FnMut(Result<(&G::Timestamp, &[C::Item]), &[G::Timestamp]>) + 'static {
        self.inspect_container(move |r| func(r.map(|(t, c)| (t, c.as_ref().as_ref()))))
    }
}

/// Inspect containers
pub trait InspectCore<G: Scope, C: Container> {
    /// Runs a supplied closure on each observed container, and each frontier advancement.
    ///
    /// Rust's `Result` type is used to distinguish the events, with `Ok` for time and data,
    /// and `Err` for frontiers. Frontiers are only presented when they change.
    ///
    /// # Examples
    /// ```
    /// use timely::dataflow::operators::{ToStream, Map, InspectCore};
    ///
    /// timely::example(|scope| {
    ///     (0..10).to_stream(scope)
    ///            .inspect_container(|event| {
    ///                match event {
    ///                    Ok((time, data)) => println!("seen at: {:?}\t{:?} records", time, data.len()),
    ///                    Err(frontier) => println!("frontier advanced to {:?}", frontier),
    ///                }
    ///             });
    /// });
    /// ```
    fn inspect_container<F>(&self, func: F) -> StreamCore<G, C> where F: FnMut(Result<(&G::Timestamp, &C), &[G::Timestamp]>)+'static;
}

impl<G: Scope, C: Container> InspectCore<G, C> for StreamCore<G, C> {

    fn inspect_container<F>(&self, mut func: F) -> StreamCore<G, C>
        where F: FnMut(Result<(&G::Timestamp, &C), &[G::Timestamp]>)+'static
    {
        use crate::progress::timestamp::Timestamp;
        let mut frontier = crate::progress::Antichain::from_elem(G::Timestamp::minimum());
        let mut vector = Default::default();
        self.unary_frontier(Pipeline, "InspectBatch", move |_,_| move |input, output| {
            if input.frontier.frontier() != frontier.borrow() {
                frontier.clear();
                frontier.extend(input.frontier.frontier().iter().cloned());
                func(Err(frontier.elements()));
            }
            input.for_each(|time, data| {
                data.swap(&mut vector);
                func(Ok((&time, &vector)));
                output.session(&time).give_container(&mut vector);
            });
        })
    }
}
