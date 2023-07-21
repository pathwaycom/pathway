//! Predicate expression plan.

use std::hash::Hash;

use timely::dataflow::Scope;

use differential_dataflow::{Collection, ExchangeData};
use plan::{Plan, Render};
use {TraceManager, Time, Diff, Datum};

/// What to compare against.
///
/// A second argument is either a constant or the index of another value.
#[derive(Serialize, Deserialize, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum SecondArgument<Value> {
    /// A constant value.
    Constant(Value),
    /// An index of another value.
    Position(usize),
}

impl<Value> SecondArgument<Value> {
    /// Produces the indicated value.
    pub fn value<'a>(&'a self, values: &'a [Value]) -> &'a Value {
        match self {
            SecondArgument::Constant(value) => value,
            SecondArgument::Position(index) => &values[*index],
        }
    }
}

/// Possible predicates to apply.
#[derive(Serialize, Deserialize, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum Predicate<Value> {
    /// Strictly less than.
    LessThan(usize, SecondArgument<Value>),
    /// Less than or equal.
    LessEqual(usize, SecondArgument<Value>),
    /// Strictly greater than.
    GreaterThan(usize, SecondArgument<Value>),
    /// Greater than or equal.
    GreaterEqual(usize, SecondArgument<Value>),
    /// Equal.
    Equal(usize, SecondArgument<Value>),
    /// Not equal.
    NotEqual(usize, SecondArgument<Value>),
    /// Any of a list of predicates.
    Any(Vec<Predicate<Value>>),
    /// All of a list of predicates.
    All(Vec<Predicate<Value>>),
    /// The complement of a predicate.
    Not(Box<Predicate<Value>>),
}

impl<Value: Ord> Predicate<Value> {
    /// Indicates if the predicate is satisfied.
    pub fn satisfied(&self, values: &[Value]) -> bool {
        match self {
            Predicate::LessThan(index, other) => values[*index].lt(other.value(values)),
            Predicate::LessEqual(index, other) => values[*index].le(other.value(values)),
            Predicate::GreaterThan(index, other) => values[*index].gt(other.value(values)),
            Predicate::GreaterEqual(index, other) => values[*index].ge(other.value(values)),
            Predicate::Equal(index, other) => values[*index].eq(other.value(values)),
            Predicate::NotEqual(index, other) => values[*index].ne(other.value(values)),
            Predicate::Any(predicates) => predicates.iter().any(|p| p.satisfied(values)),
            Predicate::All(predicates) => predicates.iter().all(|p| p.satisfied(values)),
            Predicate::Not(predicate) => !predicate.satisfied(values),
        }
    }
}

/// A plan stage filtering source tuples by the specified
/// predicate. Frontends are responsible for ensuring that the source
/// binds the argument symbols.
#[derive(Serialize, Deserialize, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct Filter<V: Datum> {
    /// Logical predicate to apply.
    pub predicate: Predicate<V>,
    /// Plan for the data source.
    pub plan: Box<Plan<V>>,
}

impl<V: ExchangeData+Hash+Datum> Render for Filter<V> {

    type Value = V;

    fn render<S: Scope<Timestamp = Time>>(
        &self,
        scope: &mut S,
        collections: &mut std::collections::HashMap<Plan<Self::Value>, Collection<S, Vec<Self::Value>, Diff>>,
        arrangements: &mut TraceManager<Self::Value>,
    ) -> Collection<S, Vec<Self::Value>, Diff>
    {
        let predicate = self.predicate.clone();
        self.plan
            .render(scope, collections, arrangements)
            .filter(move |tuple| predicate.satisfied(tuple))
    }
}