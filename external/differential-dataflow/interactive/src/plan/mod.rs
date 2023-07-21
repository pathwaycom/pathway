//! Types and traits for implementing query plans.

use std::hash::Hash;

use timely::dataflow::Scope;
use differential_dataflow::{Collection, ExchangeData};

use {TraceManager, Time, Diff};

// pub mod count;
pub mod filter;
pub mod join;
pub mod map;
pub mod sfw;

use crate::Datum;

// pub use self::count::Count;
pub use self::filter::{Filter, Predicate};
pub use self::join::Join;
pub use self::sfw::MultiwayJoin;
pub use self::map::Map;

/// A type that can be rendered as a collection.
pub trait Render : Sized {

    /// Value type produced.
    type Value: ExchangeData+Datum;

    /// Renders the instance as a collection in the supplied scope.
    ///
    /// This method has access to arranged data, and may rely on and update the set
    /// of arrangements based on the needs and offerings of the rendering process.
    fn render<S: Scope<Timestamp = Time>>(
        &self,
        scope: &mut S,
        collections: &mut std::collections::HashMap<Plan<Self::Value>, Collection<S, Vec<Self::Value>, Diff>>,
        arrangements: &mut TraceManager<Self::Value>,
    ) -> Collection<S, Vec<Self::Value>, Diff>;
}

/// Possible query plan types.
#[derive(Serialize, Deserialize, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum Plan<V: Datum> {
    /// Map
    Map(Map<V>),
    /// Distinct
    Distinct(Box<Plan<V>>),
    /// Concat
    Concat(Vec<Plan<V>>),
    /// Consolidate
    Consolidate(Box<Plan<V>>),
    /// Equijoin
    Join(Join<V>),
    /// MultiwayJoin
    MultiwayJoin(MultiwayJoin<V>),
    /// Negation
    Negate(Box<Plan<V>>),
    /// Filters bindings by one of the built-in predicates
    Filter(Filter<V>),
    /// Sources data from another relation.
    Source(String),
    /// Prints resulting updates.
    Inspect(String, Box<Plan<V>>),
}

impl<V: ExchangeData+Hash+Datum> Plan<V> {
    /// Retains only the values at the indicated indices.
    pub fn project(self, indices: Vec<usize>) -> Self {
        Plan::Map(Map {
            expressions: indices.into_iter().map(|i| V::projection(i)).collect(),
            plan: Box::new(self),
        })
    }
    /// Reduces a collection to distinct tuples.
    pub fn distinct(self) -> Self {
        Plan::Distinct(Box::new(self))
    }
    /// Merges two collections.
    pub fn concat(self, other: Self) -> Self {
        Plan::Concat(vec![self, other])
    }
    /// Merges multiple collections.
    pub fn concatenate(plans: Vec<Self>) -> Self {
        Plan::Concat(plans)
    }
    /// Merges multiple collections.
    pub fn consolidate(self) -> Self {
        Plan::Consolidate(Box::new(self))
    }
    /// Equi-joins two collections using the specified pairs of keys.
    pub fn join(self, other: Plan<V>, keys: Vec<(usize, usize)>) -> Self {
        Plan::Join(Join {
            keys,
            plan1: Box::new(self),
            plan2: Box::new(other),
        })
    }
    /// Equi-joins multiple collections using lists of equality constraints.
    ///
    /// The list `equalities` should contain equivalence classes of pairs of
    /// attribute index and source index, and the `multiway_join` method will
    /// ensure that each equivalence class has equal values in each attribute.
    pub fn multiway_join(
        sources: Vec<Self>,
        equalities: Vec<Vec<(usize, usize)>>,
        results: Vec<(usize, usize)>
    ) -> Self {
        Plan::MultiwayJoin(MultiwayJoin {
            results,
            sources,
            equalities,
        })
    }
    /// Negates a collection (negating multiplicities).
    pub fn negate(self) -> Self {
        Plan::Negate(Box::new(self))
    }
    /// Restricts collection to tuples satisfying the predicate.
    pub fn filter(self, predicate: Predicate<V>) -> Self {
        Plan::Filter(Filter { predicate, plan: Box::new(self) } )
    }
    /// Loads a source of data by name.
    pub fn source(name: &str) -> Self {
        Plan::Source(name.to_string())
    }
    /// Prints each tuple prefixed by `text`.
    pub fn inspect(self, text: &str) -> Self {
        Plan::Inspect(text.to_string(), Box::new(self))
    }
    /// Convert the plan into a named rule.
    pub fn into_rule(self, name: &str) -> crate::Rule<V> {
        crate::Rule {
            name: name.to_string(),
            plan: self,
        }
    }
}

impl<V: ExchangeData+Hash+Datum> Render for Plan<V> {

    type Value = V;

    fn render<S: Scope<Timestamp = Time>>(
        &self,
        scope: &mut S,
        collections: &mut std::collections::HashMap<Plan<Self::Value>, Collection<S, Vec<Self::Value>, Diff>>,
        arrangements: &mut TraceManager<Self::Value>,
    ) -> Collection<S, Vec<Self::Value>, Diff>
    {
        if collections.get(self).is_none() {

            let collection =
            match self {
                // Plan::Project(projection) => projection.render(scope, collections, arrangements),
                Plan::Map(expressions) => expressions.render(scope, collections, arrangements),
                Plan::Distinct(distinct) => {

                    use differential_dataflow::operators::reduce::ReduceCore;
                    use differential_dataflow::operators::arrange::ArrangeBySelf;
                    use differential_dataflow::trace::implementations::ord::OrdKeySpine;

                    let input =
                    if let Some(mut trace) = arrangements.get_unkeyed(&self) {
                        trace.import(scope)
                    }
                    else {
                        let input_arrangement = distinct.render(scope, collections, arrangements).arrange_by_self();
                        arrangements.set_unkeyed(&distinct, &input_arrangement.trace);
                        input_arrangement
                    };

                    let output = input.reduce_abelian::<_,OrdKeySpine<_,_,_>>("Distinct", move |_,_,t| t.push(((), 1)));

                    arrangements.set_unkeyed(&self, &output.trace);
                    output.as_collection(|k,&()| k.clone())

                },
                Plan::Concat(concat) => {

                    use timely::dataflow::operators::Concatenate;
                    use differential_dataflow::AsCollection;

                    let plans =
                    concat
                        .iter()
                        .map(|plan| plan.render(scope, collections, arrangements).inner)
                        .collect::<Vec<_>>();

                    scope
                        .concatenate(plans)
                        .as_collection()
                }
                Plan::Consolidate(consolidate) => {
                    if let Some(mut trace) = arrangements.get_unkeyed(&self) {
                        trace.import(scope).as_collection(|k,&()| k.clone())
                    }
                    else {
                        use differential_dataflow::operators::Consolidate;
                        consolidate.render(scope, collections, arrangements).consolidate()
                    }
                },
                Plan::Join(join) => join.render(scope, collections, arrangements),
                Plan::MultiwayJoin(join) => join.render(scope, collections, arrangements),
                Plan::Negate(negate) => {
                    negate.render(scope, collections, arrangements).negate()
                },
                Plan::Filter(filter) => filter.render(scope, collections, arrangements),
                Plan::Source(source) => {
                    arrangements
                        .get_unkeyed(self)
                        .expect(&format!("Failed to find source collection: {:?}", source))
                        .import(scope)
                        .as_collection(|k,()| k.to_vec())
                },
                Plan::Inspect(text, plan) => {
                    let text = text.clone();
                    plan.render(scope, collections, arrangements)
                        .inspect(move |x| println!("{}\t{:?}", text, x))
                },
            };

            collections.insert(self.clone(), collection);
        }

        collections.get(self).expect("We just installed this").clone()
    }
}
