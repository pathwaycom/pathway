// Copyright © 2024 Pathway

use std::panic::Location;

use differential_dataflow::difference::{Monoid, Semigroup};
use differential_dataflow::operators::arrange::Arranged;
use differential_dataflow::trace::TraceReader;
use differential_dataflow::{Collection, Data, ExchangeData};
use itertools::partition;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::Operator;
use timely::dataflow::Stream;

use crate::engine::dataflow::maybe_total::MaybeTotalScope;
use crate::engine::dataflow::shard::Shard;
use crate::engine::dataflow::ArrangedBySelf;

use super::utils::batch_by_time;
use super::{ArrangeWithTypes, ArrangeWithTypesSharded};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct OutputBatch<T, D, R> {
    pub time: T,
    pub data: Vec<(D, R)>,
}

pub trait ConsolidateForOutput<S, D, R>
where
    S: MaybeTotalScope,
    R: Monoid,
{
    fn consolidate_for_output_named(
        &self,
        name: &str,
        single_threaded: bool,
    ) -> Stream<S, OutputBatch<S::Timestamp, D, R>>;

    #[track_caller]
    fn consolidate_for_output(
        &self,
        single_threaded: bool,
    ) -> Stream<S, OutputBatch<S::Timestamp, D, R>> {
        self.consolidate_for_output_named("ConsolidateForOutput", single_threaded)
    }
}

impl<S, D, R> ConsolidateForOutput<S, D, R> for Collection<S, D, R>
where
    S: MaybeTotalScope,
    D: ExchangeData + Shard,
    R: Monoid + ExchangeData,
{
    #[track_caller]
    fn consolidate_for_output_named(
        &self,
        name: &str,
        single_threaded: bool,
    ) -> Stream<S, OutputBatch<S::Timestamp, D, R>> {
        let arranged: ArrangedBySelf<S, D, R> = if single_threaded {
            self.arrange_sharded_named(&format!("Arrange [single-threaded]: {name}"), |_| 0)
        } else {
            self.arrange_named(&format!("Arrange: {name}"))
        };
        arranged.consolidate_for_output_map_named(name, |k, ()| k.clone())
    }
}

pub trait ConsolidateForOutputMap<S, K, V, R>
where
    S: MaybeTotalScope,
    R: Semigroup,
{
    fn consolidate_for_output_map_named<D: Data>(
        &self,
        name: &str,
        logic: impl FnMut(&K, &V) -> D + 'static,
    ) -> Stream<S, OutputBatch<S::Timestamp, D, R>>;

    #[track_caller]
    fn consolidate_for_output_map<D: Data>(
        &self,
        logic: impl FnMut(&K, &V) -> D + 'static,
    ) -> Stream<S, OutputBatch<S::Timestamp, D, R>> {
        self.consolidate_for_output_map_named("ConsolidateForOutput", logic)
    }
}

impl<S, Tr> ConsolidateForOutputMap<S, Tr::Key, Tr::Val, Tr::R> for Arranged<S, Tr>
where
    S: MaybeTotalScope,
    Tr: TraceReader<Time = S::Timestamp> + Clone,
    Tr::Key: Data,
    Tr::Val: Data,
    Tr::R: Monoid,
{
    #[track_caller]
    fn consolidate_for_output_map_named<D: Data>(
        &self,
        name: &str,
        mut logic: impl FnMut(&Tr::Key, &Tr::Val) -> D + 'static,
    ) -> Stream<S, OutputBatch<S::Timestamp, D, Tr::R>> {
        let caller = Location::caller();
        let name = format!("{name} at {caller}");
        self.stream.unary(Pipeline, &name, move |_cap, _info| {
            move |input, output| {
                input.for_each(|cap, data| {
                    let batched = batch_by_time(&data, |key, val, _time, diff| {
                        (logic(key, val), diff.clone())
                    });
                    for (time, mut data) in batched {
                        partition(&mut data, |(_data, diff)| diff < &Monoid::zero());
                        output
                            .session(&cap.delayed(&time))
                            .give(OutputBatch { time, data });
                    }
                });
            }
        })
    }
}
