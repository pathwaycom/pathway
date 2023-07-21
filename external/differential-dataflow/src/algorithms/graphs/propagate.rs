//! Directed label reachability.

use std::hash::Hash;

use timely::dataflow::*;

use ::{Collection, ExchangeData};
use ::operators::*;
use ::lattice::Lattice;
use ::difference::{Abelian, Multiply};
use ::operators::arrange::arrangement::ArrangeByKey;

/// Propagates labels forward, retaining the minimum label.
///
/// This algorithm naively propagates all labels at once, much like standard label propagation.
/// To more carefully control the label propagation, consider `propagate_core` which supports a
/// method to limit the introduction of labels.
pub fn propagate<G, N, L, R>(edges: &Collection<G, (N,N), R>, nodes: &Collection<G,(N,L),R>) -> Collection<G,(N,L),R>
where
    G: Scope,
    G::Timestamp: Lattice+Ord,
    N: ExchangeData+Hash,
    R: ExchangeData+Abelian,
    R: Multiply<R, Output=R>,
    R: From<i8>,
    L: ExchangeData,
{
    propagate_core(&edges.arrange_by_key(), nodes, |_label| 0)
}

/// Propagates labels forward, retaining the minimum label.
///
/// This algorithm naively propagates all labels at once, much like standard label propagation.
/// To more carefully control the label propagation, consider `propagate_core` which supports a
/// method to limit the introduction of labels.
pub fn propagate_at<G, N, L, F, R>(edges: &Collection<G, (N,N), R>, nodes: &Collection<G,(N,L),R>, logic: F) -> Collection<G,(N,L),R>
where
    G: Scope,
    G::Timestamp: Lattice+Ord,
    N: ExchangeData+Hash,
    R: ExchangeData+Abelian,
    R: Multiply<R, Output=R>,
    R: From<i8>,
    L: ExchangeData,
    F: Fn(&L)->u64+Clone+'static,
{
    propagate_core(&edges.arrange_by_key(), nodes, logic)
}

use trace::TraceReader;
use operators::arrange::arrangement::Arranged;

/// Propagates labels forward, retaining the minimum label.
///
/// This variant takes a pre-arranged edge collection, to facilitate re-use, and allows
/// a method `logic` to specify the rounds in which we introduce various labels. The output
/// of `logic should be a number in the interval [0,64],
pub fn propagate_core<G, N, L, Tr, F, R>(edges: &Arranged<G,Tr>, nodes: &Collection<G,(N,L),R>, logic: F) -> Collection<G,(N,L),R>
where
    G: Scope,
    G::Timestamp: Lattice+Ord,
    N: ExchangeData+Hash,
    R: ExchangeData+Abelian,
    R: Multiply<R, Output=R>,
    R: From<i8>,
    L: ExchangeData,
    Tr: TraceReader<Key=N, Val=N, Time=G::Timestamp, R=R>+Clone+'static,
    F: Fn(&L)->u64+Clone+'static,
{
    // Morally the code performs the following iterative computation. However, in the interest of a simplified
    // dataflow graph and reduced memory footprint we instead have a wordier version below. The core differences
    // between the two are that 1. the former filters its input and pretends to perform non-monotonic computation,
    // whereas the latter creates an initially empty monotonic iteration variable, and 2. the latter rotates the
    // iterative computation so that the arrangement produced by `reduce` can be re-used.

    // nodes.filter(|_| false)
    //      .iterate(|inner| {
    //          let edges = edges.enter(&inner.scope());
    //          let nodes = nodes.enter_at(&inner.scope(), move |r| 256 * (64 - (logic(&r.1)).leading_zeros() as u64));
    //          inner.join_map(&edges, |_k,l,d| (d.clone(),l.clone()))
    //               .concat(&nodes)
    //               .reduce(|_, s, t| t.push((s[0].0.clone(), 1)))
    //      })

    nodes.scope().iterative::<usize,_,_>(|scope| {

        use crate::operators::reduce::ReduceCore;
        use crate::operators::iterate::SemigroupVariable;
        use crate::trace::implementations::ord::OrdValSpine as DefaultValTrace;

        use timely::order::Product;

        let edges = edges.enter(scope);
        let nodes = nodes.enter_at(scope, move |r| 256 * (64 - (logic(&r.1)).leading_zeros() as usize));

        let proposals = SemigroupVariable::new(scope, Product::new(Default::default(), 1usize));

        let labels =
        proposals
            .concat(&nodes)
            .reduce_abelian::<_,DefaultValTrace<_,_,_,_>>("Propagate", |_, s, t| t.push((s[0].0.clone(), R::from(1 as i8))));

        let propagate: Collection<_, (N, L), R> =
        labels
            .join_core(&edges, |_k, l: &L, d| Some((d.clone(), l.clone())));

        proposals.set(&propagate);

        labels
            .as_collection(|k,v| (k.clone(), v.clone()))
            .leave()
    })
}
