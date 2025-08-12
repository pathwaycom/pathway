use timely::order::Product;
use timely::progress::Timestamp as TimestampTrait;

pub trait Epsilon: TimestampTrait {
    fn epsilon() -> Self::Summary;
}

impl Epsilon for i32 {
    fn epsilon() -> i32 {
        1
    }
}

impl Epsilon for u64 {
    fn epsilon() -> u64 {
        1
    }
}

pub trait MaybeEpsilon: TimestampTrait {
    fn maybe_epsilon() -> Option<Self::Summary>;
}

impl MaybeEpsilon for i32 {
    fn maybe_epsilon() -> Option<Self::Summary> {
        Some(Self::epsilon())
    }
}

impl MaybeEpsilon for u64 {
    fn maybe_epsilon() -> Option<Self::Summary> {
        Some(Self::epsilon())
    }
}

impl<T1, T2> MaybeEpsilon for Product<T1, T2>
where
    T1: TimestampTrait,
    T2: TimestampTrait,
{
    fn maybe_epsilon() -> Option<Self::Summary> {
        None
    }
}

pub trait OriginalOrRetraction {
    fn is_original(&self) -> bool;
    fn is_retraction(&self) -> bool {
        !self.is_original()
    }
}

impl OriginalOrRetraction for i32 {
    fn is_original(&self) -> bool {
        true
    }
}

impl OriginalOrRetraction for u64 {
    fn is_original(&self) -> bool {
        true
    }
}

impl OriginalOrRetraction for u32 {
    fn is_original(&self) -> bool {
        true
    }
}

impl<T1, T2> OriginalOrRetraction for Product<T1, T2>
where
    T1: OriginalOrRetraction,
    T2: OriginalOrRetraction,
{
    fn is_original(&self) -> bool {
        // Currently we can only have retractions from the outer scope
        // when product of times is used. is_original shouldn't be used
        // in scopes with product times anyway. The implementation is present
        // to allow for a generic implementation of some functions using
        // is_original conditionally.
        self.outer.is_original() && self.inner.is_original()
    }
}

pub trait NextRetractionTime {
    #[must_use]
    fn next_retraction_time(&self) -> Self;
}
