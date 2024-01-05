// Copyright © 2024 Pathway

use differential_dataflow::difference::Semigroup;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::Arranged;
use differential_dataflow::trace::TraceReader;
use differential_dataflow::{operators, Collection, Data};
use timely::dataflow::Scope;
use timely::order::{Product, TotalOrder};
use timely::progress::Timestamp;

pub trait MaybeTotalTimestamp: Timestamp + Lattice {
    type IsTotal: MaybeTotalSwitch<Self>;
}

pub trait MaybeTotalSwitch<Time>
where
    Time: Timestamp + Lattice,
{
    fn count<S, T>(arranged: &Arranged<S, T>) -> Collection<S, (T::Key, T::R), isize>
    where
        S: MaybeTotalScope<MaybeTotalTimestamp = Time>,
        T: TraceReader<Val = (), Time = Time> + Clone + 'static,
        T::Key: Data,
        T::R: Semigroup;

    fn distinct<S, T>(arranged: &Arranged<S, T>) -> Collection<S, T::Key, isize>
    where
        S: MaybeTotalScope<MaybeTotalTimestamp = Time>,
        T: TraceReader<Val = (), Time = Time> + Clone + 'static,
        T::Key: Data,
        T::R: Semigroup;
}

pub struct Total;
pub struct NotTotal;

impl<Time> MaybeTotalSwitch<Time> for Total
where
    Time: Timestamp + Lattice + TotalOrder,
{
    fn count<S, T>(arranged: &Arranged<S, T>) -> Collection<S, (T::Key, T::R), isize>
    where
        S: MaybeTotalScope<MaybeTotalTimestamp = Time>,
        T: TraceReader<Val = (), Time = Time> + Clone + 'static,
        T::Key: Data,
        T::R: Semigroup,
    {
        operators::CountTotal::count_total(arranged)
    }

    fn distinct<S, T>(arranged: &Arranged<S, T>) -> Collection<S, T::Key, isize>
    where
        S: MaybeTotalScope<MaybeTotalTimestamp = Time>,
        T: TraceReader<Val = (), Time = Time> + Clone + 'static,
        T::Key: Data,
        T::R: Semigroup,
    {
        operators::ThresholdTotal::distinct_total(arranged)
    }
}

impl<Time> MaybeTotalSwitch<Time> for NotTotal
where
    Time: Timestamp + Lattice,
{
    fn count<S, T>(arranged: &Arranged<S, T>) -> Collection<S, (T::Key, T::R), isize>
    where
        S: MaybeTotalScope<MaybeTotalTimestamp = Time>,
        T: TraceReader<Val = (), Time = Time> + Clone + 'static,
        T::Key: Data,
        T::R: Semigroup,
    {
        #[allow(clippy::disallowed_methods)]
        operators::Count::count(arranged)
    }

    fn distinct<S, T>(arranged: &Arranged<S, T>) -> Collection<S, T::Key, isize>
    where
        S: MaybeTotalScope<MaybeTotalTimestamp = Time>,
        T: TraceReader<Val = (), Time = Time> + Clone + 'static,
        T::Key: Data,
        T::R: Semigroup,
    {
        #[allow(clippy::disallowed_methods)]
        operators::Threshold::distinct(arranged)
    }
}

impl MaybeTotalTimestamp for u64 {
    type IsTotal = Total;
}

impl MaybeTotalTimestamp for i32 {
    type IsTotal = Total;
}

impl<T1, T2> MaybeTotalTimestamp for Product<T1, T2>
where
    T1: Timestamp + Lattice,
    T2: Timestamp + Lattice,
{
    type IsTotal = NotTotal;
}

pub trait MaybeTotalScope: Scope<Timestamp = Self::MaybeTotalTimestamp> {
    type MaybeTotalTimestamp: MaybeTotalTimestamp;

    type IsTotal: MaybeTotalSwitch<Self::MaybeTotalTimestamp>;
}

impl<S> MaybeTotalScope for S
where
    S: Scope,
    S::Timestamp: MaybeTotalTimestamp,
{
    type MaybeTotalTimestamp = S::Timestamp;

    type IsTotal = <S::Timestamp as MaybeTotalTimestamp>::IsTotal;
}
