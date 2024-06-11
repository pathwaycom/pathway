// Copyright © 2024 Pathway
use itertools::Itertools;

use differential_dataflow::operators::arrange::{Arranged, TraceAgent};
use std::panic::Location;

use differential_dataflow::difference::Abelian;
use differential_dataflow::operators::arrange::Arrange;
use differential_dataflow::trace::implementations::ord::OrdValSpine;
use differential_dataflow::{AsCollection, Collection, ExchangeData};
use itertools::Either;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::Broadcast;
use timely::dataflow::operators::Operator;
use timely::dataflow::Scope;
type KeyValArr<G, K, V, R> =
    Arranged<G, TraceAgent<OrdValSpine<K, V, <G as MaybeTotalScope>::MaybeTotalTimestamp, R>>>;

use crate::engine::dataflow::maybe_total::MaybeTotalScope;

use super::utils::batch_by_time;
use super::MapWrapped;

pub trait Index<K, V, R, K2, V2, Ret> {
    fn take_updates(&mut self, batch: Vec<(K, V, R)>);
    fn search(&self, batch: Vec<(K2, V2, R)>) -> Vec<(K2, Ret, R)>;
}

/**
    Trait denoting that given collection can accept:
    - a query stream,
    - an implementation of Index providing indices:
        -- accepting self to modify the index (`take_update`, handling adding / removing index entries)
        -- accepting elements of query stream as queries (`search`)
    and produces a stream of queries extended by tuples of matching IDs (according to current state (as-of-now) `ExternalIndex`)
*/
pub trait UseExternalIndexAsOfNow<G: Scope, K: ExchangeData, V: ExchangeData, R: Abelian> {
    fn use_external_index_as_of_now<K2, V2, Ret>(
        &self,
        query_stream: &Collection<G, (K2, V2), R>,
        index: Box<dyn Index<K, V, R, K2, V2, Ret>>,
    ) -> Collection<G, (K2, Ret), R>
    where
        K2: ExchangeData,
        V2: ExchangeData,
        Ret: ExchangeData;
}

impl<G, K, V, R> UseExternalIndexAsOfNow<G, K, V, R> for Collection<G, (K, V), R>
where
    G: MaybeTotalScope,
    K: ExchangeData,
    R: ExchangeData + Abelian,
    V: ExchangeData,
{
    fn use_external_index_as_of_now<K2, V2, Ret>(
        &self,
        query_stream: &Collection<G, (K2, V2), R>,
        index: Box<dyn Index<K, V, R, K2, V2, Ret>>,
    ) -> Collection<G, (K2, Ret), R>
    where
        K2: ExchangeData,
        V2: ExchangeData,
        Ret: ExchangeData,
    {
        use_external_index_as_of_now_core(self, query_stream, index)
    }
}

/**
    Implementation of `use_external_index_as_of_now`.
    - it duplicates the index stream, to make it available for all workers
    - it synchronizes index and query streams via concatenation, so that we work on data with the same timestamp
    The index stream only changes the state of the external index (according to its implementation), each query
    in the query stream generates one entry in the output stream (so it's a map-like operator from the point of view
    of query stream)
*/

fn use_external_index_as_of_now_core<G, K, K2, V, V2, R, Ret>(
    index_stream: &Collection<G, (K, V), R>,
    query_stream: &Collection<G, (K2, V2), R>,
    index: Box<dyn Index<K, V, R, K2, V2, Ret>>,
) -> Collection<G, (K2, Ret), R>
where
    G: MaybeTotalScope,
    K: ExchangeData,
    K2: ExchangeData,
    V: ExchangeData,
    V2: ExchangeData,
    R: ExchangeData + Abelian,
    Ret: ExchangeData,
{
    let merged_stream = index_stream
        .inner
        .broadcast() //duplicate stream
        .as_collection()
        .map_named("wrap index stream in Either", |(k, v)| {
            (Either::Left(k), Either::Left(v))
        })
        .concat(
            &query_stream.map_named("wrap query stream in Either", |(k, v)| {
                (Either::Right(k), Either::Right(v))
            }),
        );
    // arrangement that is used to split stream into chunks with guarantee that
    // the maximum time from some chunk X is not present in all chunks after X
    #[allow(clippy::disallowed_methods)]
    let merged_stream_batched: KeyValArr<G, Either<K, K2>, Either<V, V2>, R> =
        merged_stream.arrange_core(Pipeline, "slice_stream");

    let caller = Location::caller();
    merged_stream_batched
        .stream
        .unary(
            Pipeline,
            &format!("use external index as of now at {caller}"),
            move |_capability, _info| {
                // Swappable buffer for input extraction.
                let mut input_buffer = Vec::new();

                let mut index = index;
                move |input, output| {
                    input.for_each(|capability, batch| {
                        batch.swap(&mut input_buffer);
                        let grouped =
                            batch_by_time(&input_buffer, |key, val, _time, diff| {
                                match (key, val) {
                                    (Either::Left(key), Either::Left(val)) => {
                                        Either::Left((key.clone(), val.clone(), diff.clone()))
                                    }
                                    (Either::Right(key), Either::Right(val)) => {
                                        Either::Right((key.clone(), val.clone(), diff.clone()))
                                    }
                                    _ => unreachable!(),
                                }
                            });

                        for (time, data) in grouped {
                            // update index
                            let (updates, queries): (_, Vec<_>) =
                                data.into_iter().partition_map(|x| x);

                            index.take_updates(updates);
                            //ask queries, deposit answers
                            let delayed = &capability.delayed(&time);
                            let mut session = output.session(delayed);

                            let mut ret: Vec<((K2, Ret), G::Timestamp, R)> = index
                                .search(queries)
                                .into_iter()
                                .map(|(k, v, diff)| ((k, v), time.clone(), diff))
                                .collect();

                            session.give_vec(&mut ret);
                        }
                    });
                }
            },
        )
        .as_collection()
}
