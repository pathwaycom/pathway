//! Aggregates the weights of equal records into at most one record.
//!
//! As differential dataflow streams are unordered and taken to be the accumulation of all records,
//! no semantic change happens via `consolidate`. However, there is a practical difference between
//! a collection that aggregates down to zero records, and one that actually has no records. The
//! underlying system can more clearly see that no work must be done in the later case, and we can
//! drop out of, e.g. iterative computations.

use timely::dataflow::Scope;

use ::{Collection, ExchangeData, Hashable};
use ::difference::Semigroup;

use Data;
use lattice::Lattice;

/// Methods which require data be arrangeable.
impl<G, D, R> Collection<G, D, R>
where
    G: Scope,
    G::Timestamp: Data+Lattice,
    D: ExchangeData+Hashable,
    R: Semigroup+ExchangeData,
{
    /// Aggregates the weights of equal records into at most one record.
    ///
    /// This method uses the type `D`'s `hashed()` method to partition the data. The data are
    /// accumulated in place, each held back until their timestamp has completed.
    ///
    /// # Examples
    ///
    /// ```
    /// extern crate timely;
    /// extern crate differential_dataflow;
    ///
    /// use differential_dataflow::input::Input;
    ///
    /// fn main() {
    ///     ::timely::example(|scope| {
    ///
    ///         let x = scope.new_collection_from(1 .. 10u32).1;
    ///
    ///         x.negate()
    ///          .concat(&x)
    ///          .consolidate() // <-- ensures cancellation occurs
    ///          .assert_empty();
    ///     });
    /// }
    /// ```
    pub fn consolidate(&self) -> Self {
        use trace::implementations::ord::OrdKeySpine as DefaultKeyTrace;
        self.consolidate_named::<DefaultKeyTrace<_,_,_>>("Consolidate")
    }

    /// As `consolidate` but with the ability to name the operator and specify the trace type.
    pub fn consolidate_named<Tr>(&self, name: &str) -> Self
    where
        Tr: crate::trace::Trace+crate::trace::TraceReader<Key=D,Val=(),Time=G::Timestamp,R=R>+'static,
        Tr::Batch: crate::trace::Batch,
    {
        use operators::arrange::arrangement::Arrange;
        self.map(|k| (k, ()))
            .arrange_named::<Tr>(name)
            .as_collection(|d: &D, _| d.clone())
    }

    /// Aggregates the weights of equal records.
    ///
    /// Unlike `consolidate`, this method does not exchange data and does not
    /// ensure that at most one copy of each `(data, time)` pair exists in the
    /// results. Instead, it acts on each batch of data and collapses equivalent
    /// `(data, time)` pairs found therein, suppressing any that accumulate to
    /// zero.
    ///
    /// # Examples
    ///
    /// ```
    /// extern crate timely;
    /// extern crate differential_dataflow;
    ///
    /// use differential_dataflow::input::Input;
    ///
    /// fn main() {
    ///     ::timely::example(|scope| {
    ///
    ///         let x = scope.new_collection_from(1 .. 10u32).1;
    ///
    ///         // nothing to assert, as no particular guarantees.
    ///         x.negate()
    ///          .concat(&x)
    ///          .consolidate_stream();
    ///     });
    /// }
    /// ```
    pub fn consolidate_stream(&self) -> Self {

        use timely::dataflow::channels::pact::Pipeline;
        use timely::dataflow::operators::Operator;
        use collection::AsCollection;

        self.inner
            .unary(Pipeline, "ConsolidateStream", |_cap, _info| {

                let mut vector = Vec::new();
                move |input, output| {
                    input.for_each(|time, data| {
                        data.swap(&mut vector);
                        crate::consolidation::consolidate_updates(&mut vector);
                        output.session(&time).give_vec(&mut vector);
                    })
                }
            })
            .as_collection()
    }
}
