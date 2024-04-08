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

#[derive(Debug, Clone, Copy, PartialOrd, Ord, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Timestamp(pub u64);

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
    use pyo3::{FromPyObject, IntoPy, PyAny, PyObject, PyResult, Python};

    use super::Timestamp;

    impl IntoPy<PyObject> for Timestamp {
        fn into_py(self, py: Python<'_>) -> PyObject {
            self.0.into_py(py)
        }
    }

    impl<'source> FromPyObject<'source> for Timestamp {
        fn extract(ob: &'source PyAny) -> PyResult<Self> {
            ob.extract().map(Self)
        }
    }
}
