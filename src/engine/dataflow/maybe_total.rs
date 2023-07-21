use differential_dataflow::difference::Semigroup;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::Arranged;
use differential_dataflow::trace::TraceReader;
use differential_dataflow::{operators, Collection, Data};
use timely::dataflow::scopes::Child;
use timely::dataflow::{Scope, ScopeParent};
use timely::order::Product;
use timely::progress::Timestamp;

pub trait MaybeTotalScope: Scope<Timestamp = Self::MaybeTotalTimestamp> {
    type MaybeTotalTimestamp: Timestamp + Lattice;

    fn count<T>(arranged: &Arranged<Self, T>) -> Collection<Self, (T::Key, T::R), isize>
    where
        T: TraceReader<Val = (), Time = Self::Timestamp> + Clone + 'static,
        T::Key: Data,
        T::R: Semigroup;

    fn distinct<T>(arranged: &Arranged<Self, T>) -> Collection<Self, T::Key, isize>
    where
        T: TraceReader<Val = (), Time = Self::Timestamp> + Clone + 'static,
        T::Key: Data,
        T::R: Semigroup;
}

impl<'a, S> MaybeTotalScope for Child<'a, S, u64>
where
    S: ScopeParent<Timestamp = ()>,
{
    type MaybeTotalTimestamp = Self::Timestamp;

    fn count<T>(arranged: &Arranged<Self, T>) -> Collection<Self, (T::Key, T::R), isize>
    where
        T: TraceReader<Val = (), Time = <Self>::Timestamp> + Clone + 'static,
        T::Key: Data,
        T::R: Semigroup,
    {
        operators::CountTotal::count_total(arranged)
    }

    fn distinct<T>(arranged: &Arranged<Self, T>) -> Collection<Self, T::Key, isize>
    where
        T: TraceReader<Val = (), Time = <Self>::Timestamp> + Clone + 'static,
        T::Key: Data,
        T::R: Semigroup,
    {
        operators::ThresholdTotal::distinct_total(arranged)
    }
}

impl<'a, S, T1, T2> MaybeTotalScope for Child<'a, S, Product<T1, T2>>
where
    S: ScopeParent<Timestamp = T1>,
    T1: Timestamp + Lattice,
    T2: Timestamp + Lattice,
{
    type MaybeTotalTimestamp = Self::Timestamp;

    fn count<T>(arranged: &Arranged<Self, T>) -> Collection<Self, (T::Key, T::R), isize>
    where
        T: TraceReader<Val = (), Time = <Self>::Timestamp> + Clone + 'static,
        T::Key: Data,
        T::R: Semigroup,
    {
        #[allow(clippy::disallowed_methods)]
        operators::Count::count(arranged)
    }

    fn distinct<T>(arranged: &Arranged<Self, T>) -> Collection<Self, T::Key, isize>
    where
        T: TraceReader<Val = (), Time = <Self>::Timestamp> + Clone + 'static,
        T::Key: Data,
        T::R: Semigroup,
    {
        #[allow(clippy::disallowed_methods)]
        operators::Threshold::distinct(arranged)
    }
}
