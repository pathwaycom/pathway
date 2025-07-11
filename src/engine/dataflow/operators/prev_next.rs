// Copyright © 2024 Pathway

use std::cmp::Ord;
use std::fmt::Debug;
use std::iter::Peekable;
use std::rc::Rc;

use differential_dataflow::difference::{Abelian, Semigroup};
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::{Arranged, TraceAgent};
use differential_dataflow::pathway::cursor::BidirectionalCursor;
use differential_dataflow::pathway::trace::BidirectionalTraceReader;
use differential_dataflow::trace::cursor::Cursor;
use differential_dataflow::trace::implementations::ord::{OrdKeyBatch, OrdValBatch};
use differential_dataflow::trace::{Trace, TraceReader};
use differential_dataflow::{Data, ExchangeData};
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::Operator;
use timely::dataflow::scopes::{Scope, ScopeParent};
use timely::order::TotalOrder;
use timely::progress::{Antichain, Timestamp};

use super::utils::{get_upper_antichain_by_time, key_val_weight_up_to_time};
use crate::engine::dataflow::operators::utils::{
    batch_by_time, CursorStorageWrapper, SortingBatchBuilder,
};

type OutputBatchBuilder<K, V, T, R> = SortingBatchBuilder<K, V, T, R>;

/// This struct is a wrapper for an entry that:
/// - needs to be inserted into the output batch
/// - does not have all fields
///
/// The need for such entry arises, when we process the input data. Whenever we delete entries,
/// the previous-non deleted entry may need to adjust its next. Then, if we delete a block of entries
/// we don't really know what is the proper value of next. As such, we store half build replacement entry
/// in `CarryEntry`, and insert it into the batch only when we are sure it has 'next' that won't change.
#[derive(Debug)]
struct CarryEntry<K, T> {
    key: Option<K>,
    prev: Option<K>,
    next: Option<K>,
    time: Option<T>,
}

impl<K, T> CarryEntry<K, T> {
    /// Returns empty (None, None, None, None) carry entry.
    fn make_empty() -> CarryEntry<K, T> {
        CarryEntry {
            key: None,
            prev: None,
            next: None,
            time: None,
        }
    }

    /// Returns true if carry entry is (None, None, None, None)
    fn is_empty(&self) -> bool {
        self.key.is_none() && self.prev.is_none() && self.next.is_none() && self.time.is_none()
    }
    /// Replaces self.next with next.
    fn replace_next(mut self, next: Option<K>) -> CarryEntry<K, T> {
        self.next = next;
        self
    }
    /// Replaces self.time with time.
    fn replace_time(mut self, time: Option<T>) -> CarryEntry<K, T> {
        self.time = time;
        self
    }
}

/// Returns true, if total weight associated with the current key
/// of the cursor passed in wrapper is zero.
fn key_has_zero_weight<C: Cursor>(wrapper: &mut CursorStorageWrapper<C>, time: &C::Time) -> bool
where
    C::R: Semigroup,
    C::Key: Debug,
    C::Time: Timestamp,
{
    let mut sum = None;

    while wrapper.cursor.val_valid(wrapper.storage) {
        let curr = key_val_weight_up_to_time(wrapper, time);

        match (sum.clone(), curr) {
            (None | Some(_), None) => (),
            (None, Some(val)) => sum = Some(val),
            (Some(mut val1), Some(val2)) => {
                val1.plus_equals(&val2);
                sum = Some(val1);
            }
        }
        wrapper.cursor.step_val(wrapper.storage);
    }

    sum.is_none() || sum.unwrap().is_zero()
}

/// Returns true, if total weight associated with the current position up to `time` (including `time`) [i.e. a fixed (key, val) pair in storage]
/// of the cursor passed in wrapper is zero.
fn key_val_has_zero_weight<C: Cursor>(wrapper: &mut CursorStorageWrapper<C>, time: &C::Time) -> bool
where
    C::R: Semigroup,
    C::Key: Debug,
    C::Time: Timestamp,
{
    let sum: Option<C::R> = key_val_weight_up_to_time(wrapper, time);
    sum.is_none() || sum.unwrap().is_zero()
}

/*cursor utils*/

// wrapper for rewind_keys(...) + seek(key)
#[inline]
fn move_to_key_or_upper_bound<C: Cursor>(wrapper: &mut CursorStorageWrapper<C>, key: &C::Key)
where
    C::Key: Debug,
{
    wrapper.cursor.rewind_keys(wrapper.storage);
    wrapper.cursor.seek_key(wrapper.storage, key);
}

// Takes an option key an an instance filter. If the key is present,
// and belongs to the instance, returns Some(key), otherwise returns None.
fn other_instance_to_none<K>(
    instance_filter: (impl Fn(&K) -> bool + Sized),
    maybe_key: Option<K>,
) -> Option<K> {
    if maybe_key.is_some() && instance_filter(maybe_key.as_ref().unwrap()) {
        maybe_key
    } else {
        None
    }
}

// a helper function that moves cursor to a position preceding the result of cursor.seek_key(&storage,key)
// starting form a rewound cursor
#[inline]
fn move_to_key_or_lower_bound<C: BidirectionalCursor>(
    wrapper: &mut CursorStorageWrapper<C>,
    key: &C::Key,
) where
    C::R: Semigroup,
    C::Key: Debug,
{
    wrapper.cursor.rewind_keys(wrapper.storage);
    wrapper.cursor.seek_smaller_eq_key(wrapper.storage, key);
}

// checks whether key is present in storage
fn key_present<C: Cursor>(wrapper: &mut CursorStorageWrapper<C>, key: &C::Key) -> bool
where
    C::Key: PartialEq + Debug,
{
    move_to_key_or_upper_bound(wrapper, key);
    wrapper.cursor.key_valid(wrapper.storage) && wrapper.cursor.key(wrapper.storage).eq(key)
}

fn rewind_zero_weight_val_forward<C: Cursor>(wrapper: &mut CursorStorageWrapper<C>, time: &C::Time)
where
    C::R: Semigroup,
    C::Key: Debug,
    C::Time: Timestamp,
{
    while wrapper.cursor.val_valid(wrapper.storage) && key_val_has_zero_weight(wrapper, time) {
        wrapper.cursor.step_val(wrapper.storage);
    }
}

/*batch building utils*/

// this function inserts (key, val, time, 1) to the batch builder
// if storage already holds a value for a given key,
// the function also inserts key, old_val, time, -1 to the batch builder

fn push_insert_replace<C: Cursor>(
    wrapper: &mut CursorStorageWrapper<C>,
    key: &C::Key,
    val: &C::Val,
    time: &C::Time,
    diff: &C::R,
    batch_builder: &mut OutputBatchBuilder<C::Key, C::Val, C::Time, C::R>,
) where
    C::Time: Clone + Lattice + Timestamp,
    C::Key: Ord + Clone + 'static + Debug,
    C::Val: Ord + Clone + 'static + Debug,
    C::R: Abelian + 'static + Debug,
{
    move_to_key_or_upper_bound(wrapper, key);

    if key_present(wrapper, key) && !key_has_zero_weight(wrapper, time) {
        wrapper.cursor.rewind_vals(wrapper.storage);
        rewind_zero_weight_val_forward(wrapper, time);
        let old_val = wrapper.cursor.val(wrapper.storage);
        let old_weight = key_val_weight_up_to_time(wrapper, time);
        log::debug!("bb.p deleting {old_val:?} for {key:?} with time {time:?} and weight {old_weight:?} in push-replace");
        batch_builder.push((
            key.clone(),
            old_val.clone(),
            time.clone(),
            old_weight.unwrap().negate(),
        ));
    }
    log::debug!(
        "bb.p inserting {val:?} for {key:?} with time {time:?} and weight {diff:?} in push-replace",
    );

    batch_builder.push((key.clone(), val.clone(), time.clone(), diff.clone()));
}

// this function inserts (key, val, time, 1) to the batch builder
// if storage already holds a value for a given key,
// the function also inserts key, old_val, time, -1 to the batch builder

fn push_prev_replace<K, C: Cursor<Key = K, Val = (Option<K>, Option<K>)>>(
    wrapper: &mut CursorStorageWrapper<C>,
    key: &K,
    new_prev: Option<&K>,
    time: &C::Time,
    batch_builder: &mut OutputBatchBuilder<C::Key, C::Val, C::Time, C::R>,
) where
    C::Time: Clone + Lattice + Timestamp,
    C::Key: Ord + Clone + 'static + Debug,
    C::Val: Ord + Clone + 'static + Debug,
    C::R: Abelian + 'static + Debug,
{
    move_to_key_or_upper_bound(wrapper, key);

    rewind_zero_weight_val_forward(wrapper, time);
    let val = wrapper.cursor.val(wrapper.storage);
    let weight = key_val_weight_up_to_time(wrapper, time);
    log::debug!("bb.p deleting {val:?} for {key:?} in push-prev-replace");
    batch_builder.push((
        key.clone(),
        val.clone(),
        time.clone(),
        weight.as_ref().unwrap().clone().negate(),
    ));
    log::debug!(
        "bb.p inserting {:?} for {key:?} in push-prev-replace",
        (new_prev, val.1.clone())
    );
    batch_builder.push((
        key.clone(),
        (new_prev.cloned(), val.1.clone()),
        time.clone(),
        weight.unwrap(),
    ));
}

/*batch processing utils*/

// this function iterates over the input-trace entries;
// finds the first non zero predecessor and returns it
// due to invariants, all previous zero weight entries are already processed,
// hence no need to do any extra work for them
fn find_non_zero_prev<K, T, R, C: BidirectionalCursor<Key = K, Val = (), Time = T, R = R>>(
    wrapper: &mut CursorStorageWrapper<C>,
    key: &K,
    time: &T,
) -> Option<K>
where
    K: Data,
    T: Timestamp + Lattice + Clone,
    R: Semigroup,
{
    move_to_key_or_lower_bound(wrapper, key);
    wrapper.cursor.step_back_key(wrapper.storage);
    while wrapper.cursor.key_valid(wrapper.storage) && key_val_has_zero_weight(wrapper, time) {
        wrapper.cursor.step_back_key(wrapper.storage);
    }

    let prev: Option<K> = if wrapper.cursor.key_valid(wrapper.storage) {
        Some(wrapper.cursor.key(wrapper.storage).clone())
    } else {
        None
    };
    prev
}

// this function iterates over the input-trace entries;
// finds the first non zero successor and returns it
// along the way, pushes delete entries for each
// zero weight key that was also present in the input batch
fn process_to_non_zero_next<
    'a,
    K,
    T,
    R,
    C1: Cursor<Key = K, Val = (), Time = T, R = R>,
    C2: Cursor<Key = K, Val = (Option<K>, Option<K>), Time = T, R = R>,
>(
    input_wrapper: &mut CursorStorageWrapper<C1>,
    output_wrapper: &mut CursorStorageWrapper<C2>,
    batch_iter: &mut Peekable<impl Iterator<Item = &'a (K, R)>>,
    batch_builder: &mut OutputBatchBuilder<K, (Option<K>, Option<K>), T, R>,
    key: &K,
    time: &T,
    instance_filter: (impl Fn(&K) -> bool + Sized),
) -> Option<K>
where
    K: Data,
    T: Timestamp + Lattice + Clone,
    R: Abelian,
{
    move_to_key_or_upper_bound(input_wrapper, key);
    // In most cases the condition is met.
    // However, calling on delete while having aggressive compaction policy may put us in a situation
    // in which input trace compacted entry to be deleted, before we get to process the batch
    let input_trace_key = input_wrapper.cursor.get_key(input_wrapper.storage);
    if input_trace_key.is_some() && key.eq(input_trace_key.unwrap()) {
        input_wrapper.cursor.step_key(input_wrapper.storage);
    }
    batch_iter.next();

    // here we find the first non-zero weight successor in the input trace
    // if along the way, we find some batch entries, we process them (generate
    // proper entries and push them to batch_builder)
    while input_wrapper.cursor.key_valid(input_wrapper.storage)
        && instance_filter(input_wrapper.cursor.key(input_wrapper.storage))
        && key_val_has_zero_weight(input_wrapper, time)
    {
        if key_valid(batch_iter) && instance_filter(peek_key(batch_iter)) {
            let (batch_key, batch_weight) = batch_iter.peek().unwrap();

            if batch_key.eq(input_wrapper.cursor.key(input_wrapper.storage)) {
                assert!(
                    key_present(output_wrapper, batch_key),
                    "internal sort error - deleting non existing key"
                );

                rewind_zero_weight_val_forward(output_wrapper, time);
                let val = output_wrapper.cursor.val(output_wrapper.storage);
                log::debug!(
                    "bb.p deleting {val:?} for {batch_key:?} while process to non-zero-next"
                );

                batch_builder.push((
                    batch_key.clone(),
                    val.clone(),
                    time.clone(),
                    batch_weight.clone(),
                ));

                batch_iter.next();
            }
        }

        input_wrapper.cursor.step_key(input_wrapper.storage);
    }

    let next: Option<K> = if input_wrapper.cursor.key_valid(input_wrapper.storage) {
        Some(input_wrapper.cursor.key(input_wrapper.storage).clone())
    } else {
        None
    };
    next
}

fn get_non_zero_prev_next<
    'a,
    K,
    T,
    R,
    C1: BidirectionalCursor<Key = K, Val = (), Time = T, R = R>,
    C2: Cursor<Key = K, Val = (Option<K>, Option<K>), Time = T, R = R>,
>(
    input_wrapper: &mut CursorStorageWrapper<C1>,
    output_wrapper: &mut CursorStorageWrapper<C2>,
    batch_iter: &mut Peekable<impl Iterator<Item = &'a (K, R)>>,
    batch_builder: &mut OutputBatchBuilder<K, (Option<K>, Option<K>), T, R>,
    key: &K,
    time: &T,
    instance_filter: (impl Fn(&K) -> bool + Sized),
) -> (Option<K>, Option<K>, Option<K>)
where
    K: Data,
    T: Timestamp + Lattice + Clone,
    R: Abelian,
{
    let prev = find_non_zero_prev(input_wrapper, key, time);
    let mut prev_prev = None;

    if prev.is_some() {
        prev_prev = find_non_zero_prev(input_wrapper, prev.as_ref().unwrap(), time);
    }

    let next = process_to_non_zero_next(
        input_wrapper,
        output_wrapper,
        batch_iter,
        batch_builder,
        key,
        time,
        &instance_filter,
    );
    (
        other_instance_to_none(&instance_filter, prev_prev),
        other_instance_to_none(&instance_filter, prev),
        other_instance_to_none(&instance_filter, next),
    )
}

fn key_valid<I: Iterator>(batch_iter: &mut Peekable<I>) -> bool {
    batch_iter.peek().is_some()
}

fn peek_key<'a, K, D, I: Iterator<Item = &'a (K, D)>>(batch_iter: &mut Peekable<I>) -> &'a K
where
    K: 'a + Clone,
    D: 'a,
{
    &batch_iter.peek().unwrap().0
}

#[allow(clippy::too_many_lines)]
fn handle_delete<
    'a,
    K,
    T,
    R,
    C1: BidirectionalCursor<Key = K, Val = (), Time = T, R = R>,
    C2: Cursor<Key = K, Val = (Option<K>, Option<K>), Time = T, R = R>,
>(
    input_wrapper: &mut CursorStorageWrapper<C1>,
    output_wrapper: &mut CursorStorageWrapper<C2>,
    batch_iter: &mut Peekable<impl Iterator<Item = &'a (K, R)>>,
    batch_builder: &mut OutputBatchBuilder<K, (Option<K>, Option<K>), T, R>,
    time: &T,
    mut carry_entry: CarryEntry<K, T>,
    instance_filter: (impl Fn(&K) -> bool + Sized),
) -> CarryEntry<K, T>
where
    K: Data,
    T: Timestamp + Lattice + Clone,
    R: Abelian,
{
    log::debug!("delete, with carried entry {carry_entry:?}");

    debug_assert!(key_valid(batch_iter));
    let key = peek_key(batch_iter);

    let (prev_prev, prev, next) = get_non_zero_prev_next(
        input_wrapper,
        output_wrapper,
        batch_iter,
        batch_builder,
        key,
        time,
        &instance_filter,
    );

    if prev.is_some() {
        //delete entry associated with key
        move_to_key_or_upper_bound(output_wrapper, key);
        rewind_zero_weight_val_forward(output_wrapper, time);
        let val = output_wrapper.cursor.val(output_wrapper.storage);
        let weight = key_val_weight_up_to_time(output_wrapper, time);
        log::debug!("bb.p deleting {val:?} for {key:?} in regular item-delete");

        batch_builder.push((
            key.clone(),
            val.clone(),
            time.clone(),
            weight.unwrap().negate(),
        ));

        //normalize carry_entry, to get rid of some cases to consider
        if carry_entry.is_empty() {
            carry_entry = CarryEntry {
                key: prev.clone(),
                prev: prev_prev.clone(),
                next: Some(key.clone()),
                time: Some(time.clone()),
            };
        }

        // if carried entry has the same key as prev,
        // the next in carried entry pointed to the key that is being deleted
        // as such, we need to switch pointer in this entry to next
        // otherwise, the carried entry is ready to add
        if prev.eq(&carry_entry.key) {
            //update carried entry
            /*
                (K, (P,N)),   carry = (P, (_PP, K))
                            ||
                            \/
                    carry = (P, (_PP, N))
            */
            carry_entry = carry_entry
                .replace_next(next)
                .replace_time(Some(time.clone()));
        } else {
            //push carried entry to batch builder

            /*
                (K, (P,N)), (P, (PP, K))   carry = (P', (_PP', K'))
                                    ||
                                    \/
                carry = (P, (PP,N)),  push (P', (_PP', K'))
            */
            if !carry_entry.is_empty() {
                if carry_entry.key.is_some() {
                    move_to_key_or_upper_bound(input_wrapper, carry_entry.key.as_ref().unwrap());
                    let weight = key_val_weight_up_to_time(input_wrapper, time);
                    log::debug!("pushing carry entry {carry_entry:?}",);
                    push_insert_replace(
                        output_wrapper,
                        &carry_entry.key.clone().unwrap(),
                        &(carry_entry.prev, carry_entry.next.clone()),
                        &carry_entry.time.unwrap(),
                        &weight.unwrap(),
                        batch_builder,
                    );
                }
                if carry_entry.next.as_ref().unwrap() < prev.as_ref().unwrap() {
                    push_prev_replace(
                        output_wrapper,
                        carry_entry.next.as_ref().unwrap(),
                        carry_entry.key.clone().as_ref(),
                        time,
                        batch_builder,
                    );
                }
            }
            // make new carried entry, out of the prev of the current key
            carry_entry = CarryEntry {
                key: prev,
                prev: prev_prev,
                next,
                time: Some(time.clone()),
            };
        }
    } else {
        //prev is none
        assert!(carry_entry.key.is_none(),
                "sort internal error: predecessor of entry to delete is none, but carry is {carry_entry:?}");
        move_to_key_or_upper_bound(output_wrapper, key);
        rewind_zero_weight_val_forward(output_wrapper, time);
        let val = output_wrapper.cursor.val(output_wrapper.storage);
        let weight = key_val_weight_up_to_time(output_wrapper, time);
        log::debug!("bb.p deleting {val:?} for {key:?} in first-item-delete");
        batch_builder.push((
            key.clone(),
            val.clone(),
            time.clone(),
            weight.unwrap().negate(),
        ));

        carry_entry = CarryEntry {
            key: None,
            prev: None,
            next,
            time: Some(time.clone()),
        };
        log::debug!("end delete with carry {carry_entry:?}");
    }

    // remark: no need to do anything about next just yet; it is stored in carry_entry
    // and will be processed (pushed to batch_builder or removed) later

    //return carried half-built entry
    carry_entry
}

fn handle_insert<
    'a,
    K,
    T,
    R,
    C1: BidirectionalCursor<Key = K, Val = (), Time = T, R = R>,
    C2: Cursor<Key = K, Val = (Option<K>, Option<K>), Time = T, R = R>,
>(
    input_wrapper: &mut CursorStorageWrapper<C1>,
    output_wrapper: &mut CursorStorageWrapper<C2>,
    batch_iter: &mut Peekable<impl Iterator<Item = &'a (K, R)>>,
    batch_builder: &mut OutputBatchBuilder<K, (Option<K>, Option<K>), T, R>,
    time: &T,
    carry_entry: CarryEntry<K, T>,
    instance_filter: (impl Fn(&K) -> bool + Sized),
) -> CarryEntry<K, T>
where
    K: Data,
    T: Timestamp + Lattice + Clone,
    R: Abelian,
{
    log::debug!(
        "insert {:?}, with carried entry {carry_entry:?}",
        &batch_iter.peek().unwrap()
    );
    debug_assert!(key_valid(batch_iter));
    let key = peek_key(batch_iter);

    let (prev_prev, prev, next) = get_non_zero_prev_next(
        input_wrapper,
        output_wrapper,
        batch_iter,
        batch_builder,
        key,
        time,
        &instance_filter,
    );

    if prev.is_some() {
        // push old, set new carried entry
        /*
            // P may be the same as P', but does not have to
            // case P=/=P' covered by the first if-statement
            (K, (P,N)),   carry = (P', (P'P, P'N))
                            ||
                            \/
            carry = (K, (P, N)), push(P, (PP, K)) push (P', (P'P, P'N))

        */

        //normalize carry_entry, to get rid of some cases to consider

        if carry_entry.key.is_some() && !carry_entry.key.eq(&prev) {
            move_to_key_or_upper_bound(input_wrapper, carry_entry.key.as_ref().unwrap());
            let weight = key_val_weight_up_to_time(input_wrapper, time);
            push_insert_replace(
                output_wrapper,
                carry_entry.key.as_ref().unwrap(),
                &(carry_entry.prev, carry_entry.next.clone()),
                carry_entry.time.as_ref().unwrap(),
                &weight.unwrap(),
                batch_builder,
            );
        }
        //if next of carried entry is less than prev, we need fix prev of key from carry-entry-next
        if carry_entry.next.is_some() && carry_entry.next.lt(&prev) {
            push_prev_replace(
                output_wrapper,
                &carry_entry.next.unwrap(),
                carry_entry.key.as_ref(),
                &carry_entry.time.unwrap(),
                batch_builder,
            );
        }

        move_to_key_or_upper_bound(input_wrapper, prev.as_ref().unwrap());
        let weight = key_val_weight_up_to_time(input_wrapper, time);
        log::debug!(
            "bb.push_insert_replace key {:?}, val {:?}",
            prev,
            &(prev_prev.clone(), Some(key.clone()))
        );

        push_insert_replace(
            output_wrapper,
            prev.as_ref().unwrap(),
            &(prev_prev, Some(key.clone())),
            time,
            &weight.unwrap(),
            batch_builder,
        );
    } else {
        assert!(
            carry_entry.key.is_none(),
            "sort internal error: predecessor of entry to insert is none, but carry is some"
        );
    }
    // remark: no need to do anything about next just yet; it is stored in carry_entry
    // and will be processed (pushed to batch_builder or removed) later

    //return carried half-built entry
    CarryEntry {
        key: Some(key.clone()),
        prev,
        next,
        time: Some(time.clone()),
    }
}

fn handle_one_instance<
    'a,
    K,
    T,
    R,
    C1: BidirectionalCursor<Key = K, Val = (), Time = T, R = R>,
    C2: Cursor<Key = K, Val = (Option<K>, Option<K>), Time = T, R = R>,
>(
    input_wrapper: &mut CursorStorageWrapper<C1>,
    output_wrapper: &mut CursorStorageWrapper<C2>,
    batch_iter: &mut Peekable<impl Iterator<Item = &'a (K, R)>>,
    batch_builder: &mut OutputBatchBuilder<K, (Option<K>, Option<K>), T, R>,
    instance_filter: impl Fn(&K, &K) -> bool,
    time: &T,
) where
    K: Data,
    T: Timestamp + Lattice + Clone,
    R: Abelian,
{
    let instance_representative = peek_key(batch_iter);

    let unary_instance_filter: &dyn Fn(&K) -> bool =
        &|element| instance_filter(instance_representative, element);

    let mut carry_entry = CarryEntry::<K, T>::make_empty();
    while key_valid(batch_iter) && unary_instance_filter(peek_key(batch_iter)) {
        log::debug!(
            "repr {:?} current {:?} ",
            &instance_representative,
            peek_key(batch_iter)
        );

        let (key, _diff) = &batch_iter.peek().unwrap();

        // find the total weight of given key in the input trace
        // 0 total weight indicates that the key just got deleted
        move_to_key_or_upper_bound(input_wrapper, key);

        let is_weight_zero = !(input_wrapper.cursor.key_valid(input_wrapper.storage)
            && input_wrapper.cursor.key(input_wrapper.storage) == key)
            || key_val_has_zero_weight(input_wrapper, time);

        if is_weight_zero {
            carry_entry = handle_delete(
                input_wrapper,
                output_wrapper,
                batch_iter,
                batch_builder,
                time,
                carry_entry,
                unary_instance_filter,
            );
        } else {
            carry_entry = handle_insert(
                input_wrapper,
                output_wrapper,
                batch_iter,
                batch_builder,
                time,
                carry_entry,
                unary_instance_filter,
            );
        }
    }
    log::debug!("final carry {carry_entry:?}");
    if !carry_entry.is_empty() {
        if carry_entry.key.is_some() {
            move_to_key_or_upper_bound(input_wrapper, carry_entry.key.as_ref().unwrap());
            let weight = key_val_weight_up_to_time(input_wrapper, time);
            push_insert_replace(
                output_wrapper,
                carry_entry.key.as_ref().unwrap(),
                &(carry_entry.prev, carry_entry.next.clone()),
                carry_entry.time.as_ref().unwrap(),
                &weight.unwrap(),
                batch_builder,
            );
        }

        if carry_entry.next.is_some() {
            let key_to_fix = carry_entry.next.unwrap();
            move_to_key_or_upper_bound(output_wrapper, &key_to_fix);
            rewind_zero_weight_val_forward(output_wrapper, time);
            push_prev_replace(
                output_wrapper,
                &key_to_fix,
                carry_entry.key.as_ref(),
                carry_entry.time.as_ref().unwrap(),
                batch_builder,
            );
        }
    }
}

pub fn add_prev_next_pointers<G: Scope, K: ExchangeData, R: ExchangeData + Abelian>(
    mut input_arrangement: Arranged<
        G,
        TraceAgent<
            differential_dataflow::trace::implementations::spine_fueled::Spine<
                Rc<OrdKeyBatch<K, <G as ScopeParent>::Timestamp, R>>,
            >,
        >,
    >,
    instance_filter: &'static impl Fn(&K, &K) -> bool,
) -> Arranged<
    G,
    TraceAgent<
        differential_dataflow::trace::implementations::spine_fueled::Spine<
            Rc<OrdValBatch<K, (Option<K>, Option<K>), <G as ScopeParent>::Timestamp, R>>,
        >,
    >,
>
where
    G::Timestamp: Lattice + Ord + TotalOrder,
{
    let mut result_trace = None;
    let stream = {
        let result_trace = &mut result_trace;
        input_arrangement
            .stream
            .unary(Pipeline, "sorter", move |_capability, operator_info| {
                let mut input_buffer = Vec::new();

                let empty: differential_dataflow::trace::implementations::spine_fueled::Spine<
                    Rc<OrdValBatch<K, (Option<K>, Option<K>), <G as ScopeParent>::Timestamp, R>>,
                > = Trace::new(operator_info.clone(), None, None);

                let (mut output_reader, mut output_writer) =
                    TraceAgent::new(empty, operator_info, None);

                *result_trace = Some(output_reader.clone());

                move |input, output| {
                    input.for_each(|capability, batches| {
                        batches.swap(&mut input_buffer);
                        let grouped = batch_by_time(&input_buffer, |key, _val, _time, diff| {
                            (key.clone(), diff.clone())
                        });

                        let upper_antichains_for_times = get_upper_antichain_by_time(&input_buffer);

                        for (time, entries) in grouped {
                            let (mut output_cursor, output_storage) = output_reader.cursor();
                            let upper_limit = &upper_antichains_for_times[&time];

                            let (mut cursor, storage) =
                                input_arrangement.trace.bidirectional_cursor();

                            log::debug!("pushing batch {entries:?}");
                            let mut output_wrapper = CursorStorageWrapper {
                                cursor: &mut output_cursor,
                                storage: &output_storage,
                            };
                            let mut input_wrapper = CursorStorageWrapper {
                                cursor: &mut cursor,
                                storage: &storage,
                            };

                            let mut batch_builder = <OutputBatchBuilder<
                                K,
                                (Option<K>, Option<K>),
                                G::Timestamp,
                                R,
                            >>::with_capacity(
                                3 * entries.len()
                            );

                            let mut batch_iter = entries.iter().peekable();
                            while key_valid(&mut batch_iter) {
                                handle_one_instance(
                                    &mut input_wrapper,
                                    &mut output_wrapper,
                                    &mut batch_iter,
                                    &mut batch_builder,
                                    instance_filter,
                                    &time,
                                );
                            }

                            let mut output_upper_antichain = Antichain::default();
                            output_reader.read_upper(&mut output_upper_antichain);
                            let res_batch = Rc::new(batch_builder.done(
                                output_upper_antichain.clone(),
                                upper_antichains_for_times[&time].clone(),
                                Antichain::from_elem(
                                    <G::Timestamp as timely::progress::Timestamp>::minimum(),
                                ),
                            ));

                            log::debug!("pushing batch with upper{:?}", upper_limit.clone());
                            log::debug!("pushing batch with content {res_batch:?}");

                            output.session(&capability).give(res_batch.clone());
                            output_writer.insert(res_batch, Some(time));
                            output_writer.seal(upper_limit.clone());

                            input_arrangement
                                .trace
                                .set_logical_compaction(upper_limit.borrow());
                            output_reader.set_logical_compaction(upper_limit.borrow());

                            input_arrangement
                                .trace
                                .set_physical_compaction(upper_limit.borrow());
                            output_reader.set_physical_compaction(upper_limit.borrow());
                        }
                    });
                }
            })
    };

    Arranged {
        stream,
        trace: result_trace.unwrap(),
    }
}
