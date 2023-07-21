#[macro_use]
extern crate abomonation_derive;
extern crate abomonation;
extern crate timely;
extern crate differential_dataflow;
#[macro_use]
extern crate serde_derive;
extern crate serde;

use std::hash::Hash;

use timely::dataflow::Scope;
use timely::progress::Timestamp;
use timely::dataflow::operators::Partition;
use timely::dataflow::operators::Concatenate;

use differential_dataflow::{ExchangeData, Collection, AsCollection};
use differential_dataflow::operators::Threshold;
use differential_dataflow::difference::{Monoid, Multiply};
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::TraceAgent;
use differential_dataflow::operators::arrange::{ArrangeBySelf, ArrangeByKey};

pub mod altneu;
pub mod calculus;
pub mod operators;

/// A type capable of extending a stream of prefixes.
///
/**
    Implementors of `PrefixExtension` provide types and methods for extending a differential dataflow collection,
    via the three methods `count`, `propose`, and `validate`.
**/
pub trait PrefixExtender<G: Scope, R: Monoid+Multiply<Output = R>> {
    /// The required type of prefix to extend.
    type Prefix;
    /// The type to be produced as extension.
    type Extension;
    /// Annotates prefixes with the number of extensions the relation would propose.
    fn count(&mut self, prefixes: &Collection<G, (Self::Prefix, usize, usize), R>, index: usize) -> Collection<G, (Self::Prefix, usize, usize), R>;
    /// Extends each prefix with corresponding extensions.
    fn propose(&mut self, prefixes: &Collection<G, Self::Prefix, R>) -> Collection<G, (Self::Prefix, Self::Extension), R>;
    /// Restricts proposed extensions by those the extender would have proposed.
    fn validate(&mut self, extensions: &Collection<G, (Self::Prefix, Self::Extension), R>) -> Collection<G, (Self::Prefix, Self::Extension), R>;
}

pub trait ProposeExtensionMethod<G: Scope, P: ExchangeData+Ord, R: Monoid+Multiply<Output = R>> {
    fn propose_using<PE: PrefixExtender<G, R, Prefix=P>>(&self, extender: &mut PE) -> Collection<G, (P, PE::Extension), R>;
    fn extend<E: ExchangeData+Ord>(&self, extenders: &mut [&mut dyn PrefixExtender<G,R,Prefix=P,Extension=E>]) -> Collection<G, (P, E), R>;
}

impl<G, P, R> ProposeExtensionMethod<G, P, R> for Collection<G, P, R>
where
    G: Scope,
    P: ExchangeData+Ord,
    R: Monoid+Multiply<Output = R>,
{
    fn propose_using<PE>(&self, extender: &mut PE) -> Collection<G, (P, PE::Extension), R>
    where
        PE: PrefixExtender<G, R, Prefix=P>
    {
        extender.propose(self)
    }
    fn extend<E>(&self, extenders: &mut [&mut dyn PrefixExtender<G,R,Prefix=P,Extension=E>]) -> Collection<G, (P, E), R>
    where
        E: ExchangeData+Ord
    {

        if extenders.len() == 1 {
            extenders[0].propose(&self.clone())
        }
        else {
            let mut counts = self.map(|p| (p, 1 << 31, 0));
            for (index,extender) in extenders.iter_mut().enumerate() {
                counts = extender.count(&counts, index);
            }

            let parts = counts.inner.partition(extenders.len() as u64, |((p, _, i),t,d)| (i as u64, (p,t,d)));

            let mut results = Vec::new();
            for (index, nominations) in parts.into_iter().enumerate() {
                let mut extensions = extenders[index].propose(&nominations.as_collection());
                for other in (0..extenders.len()).filter(|&x| x != index) {
                    extensions = extenders[other].validate(&extensions);
                }

                results.push(extensions.inner);    // save extensions
            }

            self.scope().concatenate(results).as_collection()
        }
    }
}

pub trait ValidateExtensionMethod<G: Scope, R: Monoid+Multiply<Output = R>, P, E> {
    fn validate_using<PE: PrefixExtender<G, R, Prefix=P, Extension=E>>(&self, extender: &mut PE) -> Collection<G, (P, E), R>;
}

impl<G: Scope, R: Monoid+Multiply<Output = R>, P, E> ValidateExtensionMethod<G, R, P, E> for Collection<G, (P, E), R> {
    fn validate_using<PE: PrefixExtender<G, R, Prefix=P, Extension=E>>(&self, extender: &mut PE) -> Collection<G, (P, E), R> {
        extender.validate(self)
    }
}

// These are all defined here so that users can be assured a common layout.
use differential_dataflow::trace::implementations::ord::{OrdKeySpine, OrdValSpine};
type TraceValHandle<K,V,T,R> = TraceAgent<OrdValSpine<K,V,T,R>>;
type TraceKeyHandle<K,T,R> = TraceAgent<OrdKeySpine<K,T,R>>;

pub struct CollectionIndex<K, V, T, R>
where
    K: ExchangeData,
    V: ExchangeData,
    T: Lattice+ExchangeData+Timestamp,
    R: Monoid+Multiply<Output = R>+ExchangeData,
{
    /// A trace of type (K, ()), used to count extensions for each prefix.
    count_trace: TraceKeyHandle<K, T, isize>,

    /// A trace of type (K, V), used to propose extensions for each prefix.
    propose_trace: TraceValHandle<K, V, T, R>,

    /// A trace of type ((K, V), ()), used to validate proposed extensions.
    validate_trace: TraceKeyHandle<(K, V), T, R>,
}

impl<K, V, T, R> Clone for CollectionIndex<K, V, T, R>
where
    K: ExchangeData+Hash,
    V: ExchangeData+Hash,
    T: Lattice+ExchangeData+Timestamp,
    R: Monoid+Multiply<Output = R>+ExchangeData,
{
    fn clone(&self) -> Self {
        CollectionIndex {
            count_trace: self.count_trace.clone(),
            propose_trace: self.propose_trace.clone(),
            validate_trace: self.validate_trace.clone(),
        }
    }
}

impl<K, V, T, R> CollectionIndex<K, V, T, R>
where
    K: ExchangeData+Hash,
    V: ExchangeData+Hash,
    T: Lattice+ExchangeData+Timestamp,
    R: Monoid+Multiply<Output = R>+ExchangeData,
{

    pub fn index<G: Scope<Timestamp = T>>(collection: &Collection<G, (K, V), R>) -> Self {
        // We need to count the number of (k, v) pairs and not rely on the given Monoid R and its binary addition operation.
        // counts and validate can share the base arrangement
        let arranged = collection.arrange_by_self();
        let counts = arranged
            .distinct()
            .map(|(k, _v)| k)
            .arrange_by_self()
            .trace;
        let propose = collection.arrange_by_key().trace;
        let validate = arranged.trace;

        CollectionIndex {
            count_trace: counts,
            propose_trace: propose,
            validate_trace: validate,
        }
    }
    pub fn extend_using<P, F: Fn(&P)->K+Clone>(&self, logic: F) -> CollectionExtender<K, V, T, R, P, F> {
        CollectionExtender {
            phantom: std::marker::PhantomData,
            indices: self.clone(),
            key_selector: logic,
        }
    }
}

pub struct CollectionExtender<K, V, T, R, P, F>
where
    K: ExchangeData,
    V: ExchangeData,
    T: Lattice+ExchangeData+Timestamp,
    R: Monoid+Multiply<Output = R>+ExchangeData,
    F: Fn(&P)->K+Clone,
{
    phantom: std::marker::PhantomData<P>,
    indices: CollectionIndex<K, V, T, R>,
    key_selector: F,
}

impl<G, K, V, R, P, F> PrefixExtender<G, R> for CollectionExtender<K, V, G::Timestamp, R, P, F>
where
    G: Scope,
    K: ExchangeData+Hash+Default,
    V: ExchangeData+Hash+Default,
    P: ExchangeData,
    G::Timestamp: Lattice+ExchangeData,
    R: Monoid+Multiply<Output = R>+ExchangeData,
    F: Fn(&P)->K+Clone+'static,
{
    type Prefix = P;
    type Extension = V;

    fn count(&mut self, prefixes: &Collection<G, (P, usize, usize), R>, index: usize) -> Collection<G, (P, usize, usize), R> {
        let counts = self.indices.count_trace.import(&prefixes.scope());
        operators::count::count(prefixes, counts, self.key_selector.clone(), index)
    }

    fn propose(&mut self, prefixes: &Collection<G, P, R>) -> Collection<G, (P, V), R> {
        let propose = self.indices.propose_trace.import(&prefixes.scope());
        operators::propose::propose(prefixes, propose, self.key_selector.clone())
    }

    fn validate(&mut self, extensions: &Collection<G, (P, V), R>) -> Collection<G, (P, V), R> {
        let validate = self.indices.validate_trace.import(&extensions.scope());
        operators::validate::validate(extensions, validate, self.key_selector.clone())
    }
}
