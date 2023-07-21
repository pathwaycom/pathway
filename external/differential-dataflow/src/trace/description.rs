//! Descriptions of intervals of partially ordered times.
//!
//! A description provides what intends to be an unambiguous characterization of a batch of
//! updates. We do assume that these updates are in the context of a known computation and
//! known input, so there is a well-defined "correct answer" for the full set of updates.
//!
//! ```ignore
//!      full = { (data, time, diff) }
//! ```
//!
//! Our aim with a description is to specify a subset of these updates unambiguously.
//!
//! Each description contains three frontiers, sets of mutually incomparable partially ordered
//! times. The first two frontiers are `lower` and `upper`, and they indicate the subset of
//! `full` represented in the batch: those updates whose times are greater or equal to some
//! element of `lower` but not greater or equal to any element of `upper`.
//!
//! ```ignore
//!     subset = { (data, time, diff) in full | lower.any(|t| t.le(time)) &&
//!                                            !upper.any(|t| t.le(time)) }
//! ```
//!
//! The third frontier `since` is used to indicate that the times presented by the batch may
//! no longer reflect the values in `subset` above. Although the updates are precisely those
//! bound by `lower` and `upper`, we may have *advanced* some of the times.
//!
//! The guarantee provided by a batch is that for any time greater or equal to some element of
//! `since`, the accumulated weight of batch updates before that time is identical to the accumulated
//! weights of updates from `full` at times greater or equal to an element of `lower`, greater
//! or equal to no element of `upper`, and less or equal to the query time.
//!
//! ```ignore
//!     for all times t1:
//!
//!        if since.any(|t2| t2.less_equal(t1)) then:
//!
//!            for all data:
//!
//!                sum x where (data, t2, x) in batch and t2.less_equal(t1)
//!            ==
//!                sum x where (data, t2, x) in full and  t2.less_equal(t1)
//!                                                  and  lower.any(|t3| t3.less_equal(t2))
//!                                                  and !upper.any(|t3| t3.less_equal(t2))
//! ```
//!
//! Very importantly, this equality does not make any other guarantees about the contents of
//! the batch when one iterates through it. There are some consequences of the math that can
//! be relied upon, though.
//!
//! The most important consequence is that when `since <= lower` the contents of the batch
//! must be identical to the updates in `subset`. If it is ever the case that `since` is
//! in advance of `lower`, consumers of the batch must take care that they not use the times
//! observed in the batch without ensuring that they are appropriately advanced (typically by
//! `since`). Failing to do so may produce updates that are not in advance of `since`, which
//! will often be a logic bug, as `since` does not advance without a corresponding advance in
//! times at which data may possibly be sent.

use timely::{PartialOrder, progress::Antichain};
use serde::{Serialize, Deserialize};

/// Describes an interval of partially ordered times.
///
/// A `Description` indicates a set of partially ordered times, and a moment at which they are
/// observed. The `lower` and `upper` frontiers bound the times contained within, and the `since`
/// frontier indicates a moment at which the times were observed. If `since` is strictly in
/// advance of `lower`, the contained times may be "advanced" to times which appear equivalent to
/// any time after `since`.
#[derive(Clone, Debug, Abomonation, Serialize, Deserialize)]
pub struct Description<Time> {
    /// lower frontier of contained updates.
    lower: Antichain<Time>,
    /// upper frontier of contained updates.
    upper: Antichain<Time>,
    /// frontier used for update compaction.
    since: Antichain<Time>,
}

impl<Time: PartialOrder+Clone> Description<Time> {
    /// Returns a new description from its component parts.
    pub fn new(lower: Antichain<Time>, upper: Antichain<Time>, since: Antichain<Time>) -> Self {
        assert!(lower.elements().len() > 0);    // this should always be true.
        // assert!(upper.len() > 0);            // this may not always be true.
        Description {
            lower,
            upper,
            since,
        }
    }
}

impl<Time> Description<Time> {
    /// The lower envelope for times in the interval.
    pub fn lower(&self) -> &Antichain<Time> { &self.lower }
    /// The upper envelope for times in the interval.
    pub fn upper(&self) -> &Antichain<Time> { &self.upper }
    /// Times from whose future the interval may be observed.
    pub fn since(&self) -> &Antichain<Time> { &self.since }
}

impl<Time: PartialEq> PartialEq for Description<Time> {
    fn eq(&self, other: &Self) -> bool {
        self.lower.eq(other.lower())
            && self.upper.eq(other.upper())
            && self.since.eq(other.since())
    }
}

impl<Time: Eq> Eq for Description<Time> {}
