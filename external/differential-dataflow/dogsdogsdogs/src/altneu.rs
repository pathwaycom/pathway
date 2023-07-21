//! A lexicographically ordered pair of timestamps.
//!
//! Two timestamps (s1, t1) and (s2, t2) are ordered either if
//! s1 and s2 are ordered, or if s1 equals s2 and t1 and t2 are
//! ordered.
//!
//! The join of two timestamps should have as its first coordinate
//! the join of the first coordinates, and for its second coordinate
//! the join of the second coordinates for elements whose first
//! coordinate equals the computed join. That may be the minimum
//! element of the second lattice, if neither first element equals
//! the join.



/// A pair of timestamps, partially ordered by the product order.
#[derive(Debug, Hash, Default, Clone, Eq, PartialEq, Ord, PartialOrd, Abomonation, Serialize, Deserialize)]
pub struct AltNeu<T> {
    pub time: T,
    pub neu: bool,  // alt < neu in timestamp comparisons.
}

impl<T> AltNeu<T> {
    pub fn alt(time: T) -> Self { AltNeu { time, neu: false } }
    pub fn neu(time: T) -> Self { AltNeu { time, neu: true } }
}

// Implement timely dataflow's `PartialOrder` trait.
use timely::order::PartialOrder;
impl<T: PartialOrder> PartialOrder for AltNeu<T> {
    fn less_equal(&self, other: &Self) -> bool {
        if self.time.eq(&other.time) {
            self.neu <= other.neu
        }
        else {
            self.time.less_equal(&other.time)
        }
    }
}

// Implement timely dataflow's `PathSummary` trait.
// This is preparation for the `Timestamp` implementation below.
use timely::progress::PathSummary;
impl<T: Timestamp> PathSummary<AltNeu<T>> for () {
    fn results_in(&self, timestamp: &AltNeu<T>) -> Option<AltNeu<T>> {
        Some(timestamp.clone())
    }
    fn followed_by(&self, other: &Self) -> Option<Self> {
        Some(other.clone())
    }
}

// Implement timely dataflow's `Timestamp` trait.
use timely::progress::Timestamp;
impl<T: Timestamp> Timestamp for AltNeu<T> {
    type Summary = ();
    fn minimum() -> Self { AltNeu::alt(T::minimum()) }
}

use timely::progress::timestamp::Refines;

impl<T: Timestamp> Refines<T> for AltNeu<T> {
    fn to_inner(other: T) -> Self {
        AltNeu::alt(other)
    }
    fn to_outer(self: AltNeu<T>) -> T {
        self.time
    }
    fn summarize(_path: ()) -> <T as Timestamp>::Summary {
        Default::default()
    }
}

// Implement differential dataflow's `Lattice` trait.
// This extends the `PartialOrder` implementation with additional structure.
use differential_dataflow::lattice::Lattice;
impl<T: Lattice> Lattice for AltNeu<T> {
    fn join(&self, other: &Self) -> Self {
        let time = self.time.join(&other.time);
        let mut neu = false;
        if time == self.time {
            neu = neu || self.neu;
        }
        if time == other.time {
            neu = neu || other.neu;
        }
        AltNeu { time, neu }
    }
    fn meet(&self, other: &Self) -> Self {
        let time = self.time.meet(&other.time);
        let mut neu = true;
        if time == self.time {
            neu = neu && self.neu;
        }
        if time == other.time {
            neu = neu && other.neu;
        }
        AltNeu { time, neu }
    }
}
