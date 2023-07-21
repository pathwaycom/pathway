//! Equijoin expression plan.

use std::hash::Hash;

use timely::dataflow::Scope;

use differential_dataflow::operators::JoinCore;

use differential_dataflow::{Collection, ExchangeData};
use plan::{Plan, Render};
use {TraceManager, Time, Diff, Datum};

/// A plan stage joining two source relations on the specified
/// symbols. Throws if any of the join symbols isn't bound by both
/// sources.
#[derive(Serialize, Deserialize, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct Join<Value: Datum> {
    /// Pairs of indices whose values must be equal.
    pub keys: Vec<(usize, usize)>,
    /// Plan for the left input.
    pub plan1: Box<Plan<Value>>,
    /// Plan for the right input.
    pub plan2: Box<Plan<Value>>,
}

impl<V: ExchangeData+Hash+Datum> Render for Join<V> {

    type Value = V;

    fn render<S: Scope<Timestamp = Time>>(
        &self,
        scope: &mut S,
        collections: &mut std::collections::HashMap<Plan<Self::Value>, Collection<S, Vec<Self::Value>, Diff>>,
        arrangements: &mut TraceManager<Self::Value>,
    ) -> Collection<S, Vec<Self::Value>, Diff>
    {
        use differential_dataflow::operators::arrange::ArrangeByKey;

        // acquire arrangements for each input.
        let keys1 = self.keys.iter().map(|key| key.0).collect::<Vec<_>>();
        let mut trace1 =
        if let Some(arrangement) = arrangements.get_keyed(&self.plan1, &keys1[..]) {
            arrangement
        }
        else {
            let keys = keys1.clone();
            let arrangement =
            self.plan1
                .render(scope, collections, arrangements)
                .map(move |tuple|
                    (
                        // TODO: Re-use `tuple` for values.
                        keys.iter().map(|index| tuple[*index].clone()).collect::<Vec<_>>(),
                        tuple
                            .into_iter()
                            .enumerate()
                            .filter(|(index,_value)| !keys.contains(index))
                            .map(|(_index,value)| value)
                            .collect::<Vec<_>>(),
                    )
                )
                .arrange_by_key();

            arrangements.set_keyed(&self.plan1, &keys1[..], &arrangement.trace);
            arrangement.trace
        };

        // extract relevant fields for each index.
        let keys2 = self.keys.iter().map(|key| key.1).collect::<Vec<_>>();
        let mut trace2 =
        if let Some(arrangement) = arrangements.get_keyed(&self.plan2, &keys2[..]) {
            arrangement
        }
        else {
            let keys = keys2.clone();
            let arrangement =
            self.plan2
                .render(scope, collections, arrangements)
                .map(move |tuple|
                    (
                        // TODO: Re-use `tuple` for values.
                        keys.iter().map(|index| tuple[*index].clone()).collect::<Vec<_>>(),
                        tuple
                            .into_iter()
                            .enumerate()
                            .filter(|(index,_value)| !keys.contains(index))
                            .map(|(_index,value)| value)
                            .collect::<Vec<_>>(),
                    )
                )
                .arrange_by_key();

            arrangements.set_keyed(&self.plan2, &keys2[..], &arrangement.trace);
            arrangement.trace
        };

        let arrange1 = trace1.import(scope);
        let arrange2 = trace2.import(scope);

        arrange1
            .join_core(&arrange2, |keys, vals1, vals2| {
                Some(
                    keys.iter().cloned()
                        .chain(vals1.iter().cloned())
                        .chain(vals2.iter().cloned())
                        .collect()
                )
            })
    }
}
