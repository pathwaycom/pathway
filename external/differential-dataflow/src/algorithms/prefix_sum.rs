//! Implementation of Parallel Prefix Sum

use timely::dataflow::Scope;

use ::{Collection, ExchangeData};
use ::lattice::Lattice;
use ::operators::*;

/// Extension trait for the prefix_sum method.
pub trait PrefixSum<G: Scope, K, D> {
    /// Computes the prefix sum for each element in the collection.
    ///
    /// The prefix sum is data-parallel, in the sense that the sums are computed independently for
    /// each key of type `K`. For a single prefix sum this type can be `()`, but this permits the
    /// more general accumulation of multiple independent sequences.
    fn prefix_sum<F>(&self, zero: D, combine: F) -> Self where F: Fn(&K,&D,&D)->D + 'static;

    /// Determine the prefix sum at each element of `location`.
    fn prefix_sum_at<F>(&self, locations: Collection<G, (usize, K)>, zero: D, combine: F) -> Self where F: Fn(&K,&D,&D)->D + 'static;
}

impl<G, K, D> PrefixSum<G, K, D> for Collection<G, ((usize, K), D)>
where
    G: Scope,
    G::Timestamp: Lattice,
    K: ExchangeData+::std::hash::Hash,
    D: ExchangeData+::std::hash::Hash,
{
    fn prefix_sum<F>(&self, zero: D, combine: F) -> Self where F: Fn(&K,&D,&D)->D + 'static {
        self.prefix_sum_at(self.map(|(x,_)| x), zero, combine)
    }

    fn prefix_sum_at<F>(&self, locations: Collection<G, (usize, K)>, zero: D, combine: F) -> Self where F: Fn(&K,&D,&D)->D + 'static {

        let combine1 = ::std::rc::Rc::new(combine);
        let combine2 = combine1.clone();

        let ranges = aggregate(self.clone(), move |k,x,y| (*combine1)(k,x,y));
        let values = broadcast(ranges, locations, zero, move |k,x,y| (*combine2)(k,x,y));

        values
    }
}

/// Accumulate data in `collection` into all powers-of-two intervals containing them.
pub fn aggregate<G, K, D, F>(collection: Collection<G, ((usize, K), D)>, combine: F) -> Collection<G, ((usize, usize, K), D)>
where
    G: Scope,
    G::Timestamp: Lattice,
    K: ExchangeData+::std::hash::Hash,
    D: ExchangeData+::std::hash::Hash,
    F: Fn(&K,&D,&D)->D + 'static,
{
    // initial ranges are at each index, and with width 2^0.
    let unit_ranges = collection.map(|((index, key), data)| ((index, 0, key), data));

    unit_ranges
        .iterate(|ranges|

            // Each available range, of size less than usize::max_value(), advertises itself as the range
            // twice as large, aligned to integer multiples of its size. Each range, which may contain at
            // most two elements, then summarizes itself using the `combine` function. Finally, we re-add
            // the initial `unit_ranges` intervals, so that the set of ranges grows monotonically.

            ranges
                .filter(|&((_pos, log, _), _)| log < 64)
                .map(|((pos, log, key), data)| ((pos >> 1, log + 1, key), (pos, data)))
                .reduce(move |&(_pos, _log, ref key), input, output| {
                    let mut result = (input[0].0).1.clone();
                    if input.len() > 1 { result = combine(key, &result, &(input[1].0).1); }
                    output.push((result, 1));
                })
                .concat(&unit_ranges.enter(&ranges.scope()))
        )
}

/// Produces the accumulated values at each of the `usize` locations in `queries`.
pub fn broadcast<G, K, D, F>(
    ranges: Collection<G, ((usize, usize, K), D)>,
    queries: Collection<G, (usize, K)>,
    zero: D,
    combine: F) -> Collection<G, ((usize, K), D)>
where
    G: Scope,
    G::Timestamp: Lattice+Ord+::std::fmt::Debug,
    K: ExchangeData+::std::hash::Hash,
    D: ExchangeData+::std::hash::Hash,
    F: Fn(&K,&D,&D)->D + 'static,
{

    let zero0 = zero.clone();
    let zero1 = zero.clone();
    let zero2 = zero.clone();

    // The `queries` collection may not line up with an existing element of `ranges`, and so we must
    // track down the first range that matches. If it doesn't exist, we will need to produce a zero
    // value. We could produce the full path from (0, key) to (idx, key), and aggregate any and all
    // matches. This has the defect of being n log n rather than linear, as the root ranges will be
    // replicated for each query.
    //
    // I think it works to have each (idx, key) propose each of the intervals it knows should be used
    // to assemble its input. We then `distinct` these and intersect them with the offered `ranges`,
    // essentially performing a semijoin. We then perform the unfolding, where we might need to use
    // empty ranges if none exist in `ranges`.

    // We extract desired ranges for each `idx` from its binary representation: each set bit requires
    // the contribution of a range, and we call out each of these. This could produce a super-linear
    // amount of data (multiple requests for the roots), but it will be compacted down in `distinct`.
    // We could reduce the amount of data by producing the requests iteratively, with a distinct in
    // the loop to pre-suppress duplicate requests. This comes at a complexity cost, though.
    let requests =
        queries
            .flat_map(|(idx, key)|
                (0 .. 64)
                    .filter(move |i| (idx & (1usize << i)) != 0)    // set bits require help.
                    .map(move |i| ((idx >> i) - 1, i, key.clone())) // width 2^i interval.
            )
            .distinct();

    // Acquire each requested range.
    let full_ranges =
        ranges
            .semijoin(&requests);

    // Each requested range should exist, even if as a zero range, for correct reconstruction.
    let zero_ranges =
        full_ranges
            .map(move |((idx, log, key), _)| ((idx, log, key), zero0.clone()))
            .negate()
            .concat(&requests.map(move |(idx, log, key)| ((idx, log, key), zero1.clone())));

    // Merge occupied and empty ranges.
    let used_ranges = full_ranges.concat(&zero_ranges);

    // Each key should initiate a value of `zero` at position `0`.
    let init_states =
        queries
            .map(move |(_, key)| ((0, key), zero2.clone()))
            .distinct();

    // Iteratively expand assigned values by joining existing ranges with current assignments.
    init_states
        .iterate(|states| {
            used_ranges
                .enter(&states.scope())
                .map(|((pos, log, key), data)| ((pos << log, key), (log, data)))
                .join_map(states, move |&(pos, ref key), &(log, ref data), state|
                    ((pos + (1 << log), key.clone()), combine(key, state, data)))
                .concat(&init_states.enter(&states.scope()))
                .distinct()
        })
        .semijoin(&queries)
}