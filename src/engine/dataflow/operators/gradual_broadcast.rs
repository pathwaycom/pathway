// Copyright © 2024 Pathway

use crate::engine::dataflow::maybe_total::MaybeTotalScope;
use crate::engine::dataflow::shard::Shard;
use ordered_float::OrderedFloat;
use std::cmp::{max, min};
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::ops::{Div, Sub};
use std::rc::Rc;

use super::ArrangeWithTypes;
use differential_dataflow::trace::implementations::ord::OrdValBatch;
use timely::communication::Push;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::generic::{Operator, OutputHandle};
use timely::dataflow::operators::{Broadcast, Capability};
use timely::dataflow::{Scope, ScopeParent};

use crate::engine::value::Key;
use differential_dataflow::difference::Abelian;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::{Arranged, TraceAgent};
use differential_dataflow::trace::{BatchReader, Cursor};
use differential_dataflow::{AsCollection, Collection, ExchangeData};

use crate::engine::value::KeyImpl;
use differential_dataflow::trace::TraceReader;
use timely::progress::frontier::Antichain;
use timely::progress::Timestamp;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ApxToBroadcast<Sc> {
    lower: Sc,
    value: Sc,
    upper: Sc,
}

impl<Sc: Clone> ApxToBroadcast<Sc> {
    fn from_triplet(triplet: (Sc, Sc, Sc)) -> ApxToBroadcast<Sc> {
        ApxToBroadcast {
            lower: triplet.0,
            value: triplet.1,
            upper: triplet.2,
        }
    }

    fn to_upper_bound(&self) -> ApxToBroadcast<Sc> {
        ApxToBroadcast {
            lower: self.lower.clone(),
            value: self.upper.clone(),
            upper: self.upper.clone(),
        }
    }

    fn to_lower_bound(&self) -> ApxToBroadcast<Sc> {
        ApxToBroadcast {
            lower: self.lower.clone(),
            value: self.lower.clone(),
            upper: self.upper.clone(),
        }
    }
}

pub trait GradualBroadcast<G: Scope, K: ExchangeData + Shard, V: ExchangeData, R: Abelian> {
    fn gradual_broadcast<K2, V2, R2>(
        &self,
        value_stream: &Collection<G, (K2, (V2, V2, V2)), R2>,
    ) -> Collection<G, (K, (V, V2)), R>
    where
        K2: ExchangeData,
        V2: ExchangeData + Sub<Output = V2> + Div,
        <V2 as Div>::Output: Scale<K>,
        R2: ExchangeData + Abelian;
}

impl<G, K, V, R> GradualBroadcast<G, K, V, R> for Collection<G, (K, V), R>
where
    G: Scope + MaybeTotalScope,
    G::Timestamp: Timestamp + Lattice,
    K: ExchangeData + Shard + HasMaxValue,
    V: ExchangeData,
    R: ExchangeData + Abelian,
{
    fn gradual_broadcast<K2, V2, R2>(
        &self,
        value_stream: &Collection<G, (K2, (V2, V2, V2)), R2>,
    ) -> Collection<G, (K, (V, V2)), R>
    where
        K2: ExchangeData,
        V2: ExchangeData + Sub<Output = V2> + Div,
        <V2 as Div>::Output: Scale<K>,
        R2: ExchangeData + Abelian,
    {
        gradual_broadcast_core(
            &ArrangeWithTypes::arrange(self),
            &value_stream.inner.broadcast().as_collection(),
        )
    }
}

pub trait HasMaxValue {
    fn max_value() -> Self;
}

impl HasMaxValue for Key {
    fn max_value() -> Key {
        Key(KeyImpl::MAX)
    }
}

pub trait Scale<N> {
    fn scale(factor: Self, number: N) -> N;
}

impl Scale<Key> for OrderedFloat<f64> {
    fn scale(factor: OrderedFloat<f64>, number: Key) -> Key {
        //scaling does not need to pe precise
        #[allow(
            clippy::cast_sign_loss,
            clippy::cast_possible_truncation,
            clippy::cast_precision_loss
        )]
        Key((factor.0 * number.0 as f64) as KeyImpl)
    }
}

fn get_threshold<V, N>(triplet: &ApxToBroadcast<V>, range: &N) -> N
where
    V: Sub<Output = V> + Div + Clone + Debug,
    <V as Div>::Output: Scale<N>,
    N: Clone + Debug,
{
    Scale::<N>::scale(
        (triplet.value.clone() - triplet.lower.clone())
            / (triplet.upper.clone() - triplet.lower.clone()),
        range.clone(),
    )
}

#[allow(clippy::too_many_arguments)]
fn apply_to_fragment<K, V, T, R, St, Sc, C, P>(
    input_cursor: &mut C,
    input_storage: &St,
    triplet: &ApxToBroadcast<Sc>,
    from: Option<&K>,
    to: Option<&K>,
    negate: bool,
    output: &mut OutputHandle<'_, T, ((K, (V, Sc)), T, R), P>,
    time: &Option<T>,
    capability: &Capability<T>,
) where
    K: ExchangeData + HasMaxValue,
    V: ExchangeData,
    T: ExchangeData + Timestamp + Lattice,
    R: ExchangeData + Abelian,
    Sc: 'static + Debug + Sub<Output = Sc> + Div + Clone,
    <Sc as Div>::Output: Scale<K>,
    C: Cursor<Key = K, Val = V, Time = T, R = R, Storage = St>,
    P: Push<
        timely::communication::Message<
            timely::dataflow::channels::Message<C::Time, std::vec::Vec<((K, (V, Sc)), T, R)>>,
        >,
    >,
{
    let threshold = get_threshold(triplet, &K::max_value());
    input_cursor.rewind_keys(input_storage);

    if let Some(from) = from {
        input_cursor.seek_key(input_storage, from);
    }

    while input_cursor.key_valid(input_storage) {
        let key = input_cursor.key(input_storage);
        if to.is_some_and(|to| key >= to) {
            break;
        }
        while input_cursor.val_valid(input_storage) {
            let val = input_cursor.val(input_storage);
            let data = (
                val.clone(),
                if key < &threshold {
                    triplet.upper.clone()
                } else {
                    triplet.lower.clone()
                },
            );
            input_cursor.map_times(input_storage, |ts, r| {
                let t = if let Some(time) = time { time } else { ts };
                let r = if negate {
                    r.clone().negate()
                } else {
                    r.clone()
                };

                output
                    .session(&capability)
                    .give(((key.clone(), data.clone()), t.clone(), r));
            });

            input_cursor.step_val(input_storage);
        }
        input_cursor.step_key(input_storage);
    }
}

#[allow(clippy::too_many_arguments)]
fn replace_in_fragment<K, V, T, R, St, Sc, C, P>(
    input_cursor: &mut C,
    input_storage: &St,
    old_triplet: &ApxToBroadcast<Sc>,
    new_triplet: &ApxToBroadcast<Sc>,
    from: Option<&K>,
    to: Option<&K>,
    output: &mut OutputHandle<'_, T, ((K, (V, Sc)), T, R), P>,
    time: &Option<T>,
    capability: &Capability<T>,
) where
    K: ExchangeData + HasMaxValue,
    V: ExchangeData,
    T: ExchangeData + Timestamp + Lattice,
    R: ExchangeData + Abelian,
    Sc: 'static + Debug + Sub<Output = Sc> + Div + Clone,
    <Sc as Div>::Output: Scale<K>,
    C: Cursor<Key = K, Val = V, Time = T, R = R, Storage = St>,
    P: Push<
        timely::communication::Message<
            timely::dataflow::channels::Message<C::Time, std::vec::Vec<((K, (V, Sc)), T, R)>>,
        >,
    >,
{
    apply_to_fragment(
        input_cursor,
        input_storage,
        old_triplet,
        from,
        to,
        true,
        output,
        time,
        capability,
    );

    apply_to_fragment(
        input_cursor,
        input_storage,
        new_triplet,
        from,
        to,
        false,
        output,
        time,
        capability,
    );
}

fn get_new_triplet_from_input_vec<K, V, T, R>(
    current: &Option<ApxToBroadcast<V>>,
    input: &Vec<((K, (V, V, V)), T, R)>,
) -> Option<(T, ApxToBroadcast<V>)>
where
    K: ExchangeData,
    V: ExchangeData,
    T: Timestamp,
    R: ExchangeData + Abelian,
{
    let mut tmap: BTreeMap<(&K, &(V, V, V)), (&T, R)> = BTreeMap::new();

    for ((key, val), t, r) in input {
        if tmap.contains_key(&(key, val)) {
            let (mut time, mut aggregate) = tmap.get(&(key, val)).unwrap().clone();
            aggregate.plus_equals(r);
            time = max(t, time);

            tmap.insert((key, val), (time, aggregate));
        } else {
            tmap.insert((key, val), (t, r.clone()));
        }
    }

    let non_zero: Vec<_> = tmap.iter().filter(|&(_kv, (_t, r))| !r.is_zero()).collect();
    assert!(
        non_zero.len() <= 2,
        "Gradual broadcast sees inconsistent stream"
    );

    if non_zero.is_empty() {
        return None;
    }

    let (&(_, received_triplet), _) = non_zero[0];

    let (&(_key, received_triplet), (retrieved_time, _r)) = if non_zero.len() == 2
        && current == &Some(ApxToBroadcast::from_triplet(received_triplet.clone()))
    {
        non_zero[1]
    } else {
        non_zero[0]
    };
    Some((
        (*retrieved_time).clone(),
        ApxToBroadcast::from_triplet(received_triplet.clone()),
    ))
}

#[allow(clippy::too_many_lines)]
fn gradual_broadcast_core<G, K, V, R, K2, V2, R2>(
    input_arrangement: &Arranged<
        G,
        TraceAgent<
            differential_dataflow::trace::implementations::spine_fueled::Spine<
                Rc<OrdValBatch<K, V, <G as ScopeParent>::Timestamp, R>>,
            >,
        >,
    >,
    value_stream: &Collection<G, (K2, (V2, V2, V2)), R2>,
) -> Collection<G, (K, (V, V2)), R>
where
    G: Scope,
    G::Timestamp: Timestamp + Lattice,
    K: ExchangeData + HasMaxValue,
    V: ExchangeData,
    R: ExchangeData + Abelian,
    K2: ExchangeData,
    V2: ExchangeData + Sub<Output = V2> + Div,
    <V2 as Div>::Output: Scale<K>,
    R2: ExchangeData + Abelian,
{
    input_arrangement
        .stream
        .binary_frontier(
            &value_stream.inner,
            Pipeline,
            Pipeline,
            "Gradual Broadcast",
            move |_capability, _info| {
                let trace_option = Some(input_arrangement.trace.clone());
                let mut old_triplet: Option<ApxToBroadcast<V2>> = None;
                let mut triplet: Option<ApxToBroadcast<V2>> = None;

                // Swappable buffers for input extraction.
                let mut input1_buffer = Vec::new();
                let mut input2_buffer: Vec<(
                    (K2, (V2, V2, V2)),
                    <G as ScopeParent>::Timestamp,
                    R2,
                )> = Vec::new();

                move |input1, input2, output| {
                    let mut upper_limit = Antichain::from_elem(<G::Timestamp>::minimum());

                    // old threshold need to be adjusted if lower/upper changed
                    // if they went down, we need to put it to max_value, otherwise to min_value
                    // if new bucket is not adjacent, we need to delete old values based on old triplet
                    // and add brand new based on new triplet for whole arrangement

                    input1.for_each(|cap1, data| {
                        let cap1 = cap1.retain();
                        data.swap(&mut input1_buffer);
                        for batch in input1_buffer.drain(..) {
                            upper_limit.clone_from(batch.upper());
                            if !batch.is_empty() {
                                let mut batch_cursor = batch.cursor();
                                if let Some(unwrapped_triplet) = triplet.clone() {
                                    apply_to_fragment(
                                        &mut batch_cursor,
                                        &batch,
                                        &unwrapped_triplet,
                                        None,
                                        None,
                                        false,
                                        output,
                                        &None,
                                        &cap1,
                                    );
                                }
                            }
                        }
                    });

                    if trace_option.is_none() {
                        return;
                    }
                    let mut input_trace = trace_option.clone().unwrap();
                    let (mut input_cursor, input_storage) = input_trace.cursor();
                    input_trace.set_logical_compaction(upper_limit.borrow());
                    input_trace.set_physical_compaction(upper_limit.borrow());
                    input2.for_each(|cap2, data| {
                        let cap2 = cap2.retain();
                        data.swap(&mut input2_buffer);

                        old_triplet.clone_from(&triplet);

                        let processed = get_new_triplet_from_input_vec(&triplet, &input2_buffer);
                        if processed.is_none() {
                            return;
                        }
                        let (time, unwrapped_triplet) = processed.unwrap();
                        triplet = Some(unwrapped_triplet);
                        let mut redo = false;
                        if old_triplet.is_some() {
                            let otr = old_triplet.clone().unwrap();
                            let tr = triplet.clone().unwrap();
                            //if we move bucket, we need adjust things

                            if otr.lower != tr.lower || otr.upper != tr.upper {
                                if otr.lower != tr.upper && tr.lower != otr.upper {
                                    redo = true;
                                } else {
                                    // if -> new bucket covers lower values
                                    // else -> new bucket covers larger values
                                    let (fake_triplet, old_triplet_replacement) =
                                        if otr.lower == tr.upper {
                                            (otr.to_lower_bound(), tr.to_upper_bound())
                                        } else {
                                            (otr.to_upper_bound(), tr.to_lower_bound())
                                        };
                                    // prepare new 'old_triplet', old 'old_triplet' is still in otr
                                    old_triplet = Some(old_triplet_replacement.clone());

                                    let threshold1 = get_threshold(&fake_triplet, &K::max_value());
                                    let threshold2 = get_threshold(&otr, &K::max_value());

                                    let from = min(&threshold1, &threshold2);
                                    let to = max(&threshold1, &threshold2);
                                    replace_in_fragment(
                                        &mut input_cursor,
                                        &input_storage,
                                        &otr,
                                        &fake_triplet,
                                        Some(from),
                                        Some(to),
                                        output,
                                        &Some(time.clone()),
                                        &cap2,
                                    );
                                }
                            }
                        }
                        if redo {
                            replace_in_fragment(
                                &mut input_cursor,
                                &input_storage,
                                old_triplet.as_ref().unwrap(),
                                triplet.as_ref().unwrap(),
                                None,
                                None,
                                output,
                                &Some(time.clone()),
                                &cap2,
                            );
                        } else if let Some(unwrapped_triplet) = triplet.clone() {
                            if let Some(old_triplet) = old_triplet.clone() {
                                let threshold = get_threshold(&unwrapped_triplet, &K::max_value());
                                let old_threshold = get_threshold(&old_triplet, &K::max_value());

                                let from = min(&old_threshold, &threshold);
                                let to = max(&old_threshold, &threshold);

                                replace_in_fragment(
                                    &mut input_cursor,
                                    &input_storage,
                                    &old_triplet,
                                    &unwrapped_triplet,
                                    Some(from),
                                    Some(to),
                                    output,
                                    &Some(time.clone()),
                                    &cap2,
                                );
                            } else {
                                apply_to_fragment(
                                    &mut input_cursor,
                                    &input_storage,
                                    &unwrapped_triplet,
                                    None,
                                    None,
                                    false,
                                    output,
                                    &Some(time.clone()),
                                    &cap2,
                                );
                            }
                        }
                    });
                }
            },
        )
        .as_collection()
}
