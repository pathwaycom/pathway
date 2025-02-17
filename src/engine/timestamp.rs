use std::fmt::{self, Display};
use std::str::FromStr;

use differential_dataflow::lattice::Lattice;
use serde::Deserialize;
use serde::Serialize;
use timely::order::TotalOrder;
use timely::progress::timestamp::Refines;
use timely::progress::PathSummary;
use timely::progress::Timestamp as TimestampTrait;
use timely::PartialOrder;

use super::dataflow::maybe_total::MaybeTotalTimestamp;
use super::dataflow::maybe_total::Total;
use super::dataflow::operators::time_column::Epsilon;
use super::dataflow::operators::time_column::MaxTimestamp;
use crate::timestamp::current_unix_timestamp_ms;

#[derive(Debug, Clone, Copy, PartialOrd, Ord, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Timestamp(pub u64);

impl Timestamp {
    pub fn new_from_current_time() -> Self {
        let new_timestamp = u64::try_from(current_unix_timestamp_ms())
            .expect("number of milliseconds should fit in 64 bits");
        let new_timestamp = (new_timestamp / 2) * 2; //use only even times (required by alt-neu)
        Timestamp(new_timestamp)
    }

    pub fn original_from(value: u64) -> Self {
        Timestamp(value * 2)
    }

    pub fn retraction_from(value: u64) -> Self {
        Timestamp(value * 2 + 1)
    }
}

impl PartialOrder for Timestamp {
    fn less_equal(&self, other: &Self) -> bool {
        self.0 <= other.0
    }
}

impl TotalOrder for Timestamp {}

impl TimestampTrait for Timestamp {
    type Summary = Summary;

    fn minimum() -> Self {
        Self(0)
    }
}

impl Lattice for Timestamp {
    fn join(&self, other: &Self) -> Self {
        Self(self.0.join(&other.0))
    }

    fn meet(&self, other: &Self) -> Self {
        Self(self.0.meet(&other.0))
    }
}

#[allow(clippy::unused_unit, clippy::semicolon_if_nothing_returned)]
impl Refines<()> for Timestamp {
    fn to_inner(_other: ()) -> Self {
        Self::minimum()
    }

    fn to_outer(self) -> () {
        ()
    }

    fn summarize(_path: Self::Summary) -> () {
        ()
    }
}

impl Epsilon for Timestamp {
    fn epsilon() -> Self::Summary {
        Summary(1)
    }
}

impl MaybeTotalTimestamp for Timestamp {
    type IsTotal = Total;
}

impl Display for Timestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl FromStr for Timestamp {
    type Err = <u64 as FromStr>::Err;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(s.parse()?))
    }
}

impl MaxTimestamp for Timestamp {
    fn get_max_timestamp() -> Self {
        Self(u64::get_max_timestamp()) // XXX
    }
}

#[derive(
    Default, Debug, Clone, Copy, PartialOrd, Ord, PartialEq, Eq, Hash, Serialize, Deserialize,
)]
pub struct Summary(pub u64);

impl PartialOrder for Summary {
    fn less_equal(&self, other: &Self) -> bool {
        self.0 <= other.0
    }
}

impl PathSummary<Timestamp> for Summary {
    fn results_in(&self, src: &Timestamp) -> Option<Timestamp> {
        self.0.results_in(&src.0).map(Timestamp)
    }

    fn followed_by(&self, other: &Self) -> Option<Self> {
        self.0.followed_by(&other.0).map(Self)
    }
}

// XXX
mod python_conversions {
    use pyo3::prelude::*;

    use super::Timestamp;

    impl IntoPy<PyObject> for Timestamp {
        fn into_py(self, py: Python<'_>) -> PyObject {
            self.0.into_py(py)
        }
    }

    impl<'py> FromPyObject<'py> for Timestamp {
        fn extract_bound(ob: &Bound<'py, PyAny>) -> PyResult<Self> {
            ob.extract().map(Self)
        }
    }
}

pub trait OriginalOrRetraction {
    fn is_original(&self) -> bool;
    fn is_retraction(&self) -> bool {
        !self.is_original()
    }
    #[must_use]
    fn next_retraction_time(&self) -> Self;
}

impl OriginalOrRetraction for Timestamp {
    fn is_original(&self) -> bool {
        self.0 % 2 == 0
    }

    fn next_retraction_time(&self) -> Self {
        Self(self.0 + 1)
    }
}
