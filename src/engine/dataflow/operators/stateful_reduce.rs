// Copyright © 2024 Pathway

use std::collections::{BTreeMap, HashMap};
use std::hash::Hash;
use std::panic::Location;

use differential_dataflow::difference::Semigroup;
use differential_dataflow::operators::arrange::Arranged;
use differential_dataflow::trace::{BatchReader, Cursor, TraceReader};
use differential_dataflow::{AsCollection, Collection, Data, ExchangeData};
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::Operator;
use timely::order::TotalOrder;

use super::ArrangeWithTypes;
use crate::engine::dataflow::maybe_total::MaybeTotalScope;
use crate::engine::dataflow::shard::Shard;
use crate::engine::dataflow::ArrangedByKey;

pub trait StatefulReduce<S, K, V, R>
where
    S: MaybeTotalScope,
    S::Timestamp: TotalOrder,
    R: Semigroup,
{
    #[track_caller]
    fn stateful_reduce<V2: Data>(
        &self,
        logic: impl FnMut(Option<&V2>, Vec<(V, R)>) -> Option<V2> + 'static,
    ) -> Collection<S, (K, V2), R> {
        self.stateful_reduce_named("StatefulReduce", logic)
    }

    fn stateful_reduce_named<V2: Data>(
        &self,
        name: &str,
        logic: impl FnMut(Option<&V2>, Vec<(V, R)>) -> Option<V2> + 'static,
    ) -> Collection<S, (K, V2), R>;
}

impl<S, K, V, R> StatefulReduce<S, K, V, R> for Collection<S, (K, V), R>
where
    S: MaybeTotalScope,
    S::Timestamp: TotalOrder,
    K: ExchangeData + Shard + Hash,
    V: ExchangeData,
    R: ExchangeData + Semigroup + From<i8>,
{
    #[track_caller]
    fn stateful_reduce_named<V2: Data>(
        &self,
        name: &str,
        logic: impl FnMut(Option<&V2>, Vec<(V, R)>) -> Option<V2> + 'static,
    ) -> Collection<S, (K, V2), R> {
        let arranged: ArrangedByKey<S, K, V, R> = self.arrange_named(&format!("Arrange: {name}"));
        arranged.stateful_reduce_named(name, logic)
    }
}

impl<S, Tr> StatefulReduce<S, Tr::Key, Tr::Val, Tr::R> for Arranged<S, Tr>
where
    S: MaybeTotalScope,
    S::Timestamp: TotalOrder,
    Tr: TraceReader<Time = S::Timestamp> + Clone,
    Tr::Key: Data + Hash,
    Tr::Val: Data,
    Tr::R: Semigroup + From<i8>,
{
    #[track_caller]
    fn stateful_reduce_named<V2: Data>(
        &self,
        name: &str,
        mut logic: impl FnMut(Option<&V2>, Vec<(Tr::Val, Tr::R)>) -> Option<V2> + 'static,
    ) -> Collection<S, (Tr::Key, V2), Tr::R> {
        let caller = Location::caller();
        let name = format!("{name} at {caller}");

        let mut state_by_key: HashMap<Tr::Key, V2> = HashMap::new();
        self.stream
            .unary(Pipeline, &name, move |_, _| {
                move |input, output| {
                    input.for_each(|cap, data| {
                        let mut session = output.session(&cap);
                        for batch in data.iter() {
                            let mut cursor = batch.cursor();
                            while let Some(key) = cursor.get_key(batch) {
                                let mut data_by_time = BTreeMap::new();
                                while let Some(val) = cursor.get_val(batch) {
                                    cursor.map_times(batch, |time, diff| {
                                        if !data_by_time.contains_key(time) {
                                            data_by_time.insert(time.clone(), Vec::new());
                                        }
                                        data_by_time
                                            .get_mut(time)
                                            .unwrap()
                                            .push((val.clone(), diff.clone()));
                                    });
                                    cursor.step_val(batch);
                                }
                                let mut state = state_by_key.remove(key);
                                for (time, data) in data_by_time {
                                    let new_state = logic(state.as_ref(), data);
                                    if new_state == state {
                                        continue;
                                    }
                                    if let Some(state) = state {
                                        session.give((
                                            (key.clone(), state),
                                            time.clone(),
                                            Tr::R::from(-1),
                                        ));
                                    }
                                    if let Some(new_state) = new_state.clone() {
                                        session.give((
                                            (key.clone(), new_state),
                                            time.clone(),
                                            Tr::R::from(1),
                                        ));
                                    }
                                    state = new_state;
                                }
                                if let Some(state) = state {
                                    state_by_key.insert(key.clone(), state);
                                }
                                cursor.step_key(batch);
                            }
                        }
                    });
                }
            })
            .as_collection()
    }
}
