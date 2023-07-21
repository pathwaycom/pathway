//! A type that can be treated as a difference.
//!
//! Differential dataflow most commonly tracks the counts associated with records in a multiset, but it
//! generalizes to tracking any map from the records to an Abelian group. The most common generalization
//! is when we maintain both a count and another accumulation, for example height. The differential
//! dataflow collections would then track for each record the total of counts and heights, which allows
//! us to track something like the average.

use ::Data;

#[deprecated]
pub use self::Abelian as Diff;

/// A type with addition and a test for zero.
///
/// These traits are currently the minimal requirements for a type to be a "difference" in differential
/// dataflow. Addition allows differential dataflow to compact multiple updates to the same data, and
/// the test for zero allows differential dataflow to retire updates that have no effect. There is no
/// requirement that the test for zero ever return true, and the zero value does not need to inhabit the
/// type.
///
/// There is a light presumption of commutativity here, in that while we will largely perform addition
/// in order of timestamps, for many types of timestamps there is no total order and consequently no
/// obvious order to respect. Non-commutative semigroups should be used with care.
pub trait Semigroup : ::std::marker::Sized + Data + Clone {
    /// The method of `std::ops::AddAssign`, for types that do not implement `AddAssign`.
    fn plus_equals(&mut self, rhs: &Self);
    /// Returns true if the element is the additive identity.
    ///
    /// This is primarily used by differential dataflow to know when it is safe to delete an update.
    /// When a difference accumulates to zero, the difference has no effect on any accumulation and can
    /// be removed.
    ///
    /// A semigroup is not obligated to have a zero element, and this method could always return
    /// false in such a setting.
    fn is_zero(&self) -> bool;
}

/// A semigroup with an explicit zero element.
pub trait Monoid : Semigroup {
    /// A zero element under the semigroup addition operator.
    fn zero() -> Self;
}

/// A `Monoid` with negation.
///
/// This trait extends the requirements of `Semigroup` to include a negation operator.
/// Several differential dataflow operators require negation in order to retract prior outputs, but
/// not quite as many as you might imagine.
pub trait Abelian : Monoid {
    /// The method of `std::ops::Neg`, for types that do not implement `Neg`.
    fn negate(self) -> Self;
}

/// A replacement for `std::ops::Mul` for types that do not implement it.
pub trait Multiply<Rhs = Self> {
    /// Output type per the `Mul` trait.
    type Output;
    /// Core method per the `Mul` trait.
    fn multiply(self, rhs: &Rhs) -> Self::Output;
}

/// Implementation for built-in signed integers.
macro_rules! builtin_implementation {
    ($t:ty) => {
        impl Semigroup for $t {
            #[inline] fn plus_equals(&mut self, rhs: &Self) { *self += rhs; }
            #[inline] fn is_zero(&self) -> bool { self == &0 }
        }

        impl Monoid for $t {
            #[inline] fn zero() -> Self { 0 }
        }

        impl Multiply<Self> for $t {
            type Output = Self;
            fn multiply(self, rhs: &Self) -> Self { self * rhs}
        }
    };
}

macro_rules! builtin_abelian_implementation {
    ($t:ty) => {
        impl Abelian for $t {
            #[inline] fn negate(self) -> Self { -self }
        }
    };
}

builtin_implementation!(i8);
builtin_implementation!(i16);
builtin_implementation!(i32);
builtin_implementation!(i64);
builtin_implementation!(i128);
builtin_implementation!(isize);
builtin_implementation!(u8);
builtin_implementation!(u16);
builtin_implementation!(u32);
builtin_implementation!(u64);
builtin_implementation!(u128);
builtin_implementation!(usize);

builtin_abelian_implementation!(i8);
builtin_abelian_implementation!(i16);
builtin_abelian_implementation!(i32);
builtin_abelian_implementation!(i64);
builtin_abelian_implementation!(i128);
builtin_abelian_implementation!(isize);

/// Implementations for wrapping signed integers, which have a different zero.
macro_rules! wrapping_implementation {
    ($t:ty) => {
        impl Semigroup for $t {
            #[inline] fn plus_equals(&mut self, rhs: &Self) { *self += rhs; }
            #[inline] fn is_zero(&self) -> bool { self == &std::num::Wrapping(0) }
        }

        impl Monoid for $t {
            #[inline] fn zero() -> Self { std::num::Wrapping(0) }
        }

        impl Abelian for $t {
            #[inline] fn negate(self) -> Self { -self }
        }

        impl Multiply<Self> for $t {
            type Output = Self;
            fn multiply(self, rhs: &Self) -> Self { self * rhs}
        }
    };
}

wrapping_implementation!(std::num::Wrapping<i8>);
wrapping_implementation!(std::num::Wrapping<i16>);
wrapping_implementation!(std::num::Wrapping<i32>);
wrapping_implementation!(std::num::Wrapping<i64>);
wrapping_implementation!(std::num::Wrapping<i128>);
wrapping_implementation!(std::num::Wrapping<isize>);


pub use self::present::Present;
mod present {

    /// A zero-sized difference that indicates the presence of a record.
    ///
    /// This difference type has no negation, and present records cannot be retracted.
    /// Addition and multiplication maintain presence, and zero does not inhabit the type.
    ///
    /// The primary feature of this type is that it has zero size, which reduces the overhead
    /// of differential dataflow's representations for settings where collections either do
    /// not change, or for which records are only added (for example, derived facts in Datalog).
    #[derive(Abomonation, Copy, Ord, PartialOrd, Eq, PartialEq, Debug, Clone, Serialize, Deserialize, Hash)]
    pub struct Present;

    impl<T: Clone> super::Multiply<T> for Present {
        type Output = T;
        fn multiply(self, rhs: &T) -> T {
            rhs.clone()
        }
    }

    impl super::Semigroup for Present {
        fn plus_equals(&mut self, _rhs: &Self) { }
        fn is_zero(&self) -> bool { false }
    }
}

// Pair implementations.
mod tuples {

    use super::{Semigroup, Monoid, Abelian, Multiply};

    /// Implementations for tuples. The two arguments must have the same length.
    macro_rules! tuple_implementation {
        ( ($($name:ident)*), ($($name2:ident)*) ) => (
            impl<$($name: Semigroup),*> Semigroup for ($($name,)*) {
                #[allow(non_snake_case)]
                #[inline] fn plus_equals(&mut self, rhs: &Self) {
                    let ($(ref mut $name,)*) = *self;
                    let ($(ref $name2,)*) = *rhs;
                    $($name.plus_equals($name2);)*
                }
                #[allow(unused_mut)]
                #[allow(non_snake_case)]
                #[inline] fn is_zero(&self) -> bool {
                    let mut zero = true;
                    let ($(ref $name,)*) = *self;
                    $( zero &= $name.is_zero(); )*
                    zero
                }
            }

            impl<$($name: Monoid),*> Monoid for ($($name,)*) {
                #[allow(non_snake_case)]
                #[inline] fn zero() -> Self {
                    ( $($name::zero(), )* )
                }
            }

            impl<$($name: Abelian),*> Abelian for ($($name,)*) {
                #[allow(non_snake_case)]
                #[inline] fn negate(self) -> Self {
                    let ($($name,)*) = self;
                    ( $($name.negate(), )* )
                }
            }

            impl<T, $($name: Multiply<T>),*> Multiply<T> for ($($name,)*) {
                type Output = ($(<$name as Multiply<T>>::Output,)*);
                #[allow(unused_variables)]
                #[allow(non_snake_case)]
                #[inline] fn multiply(self, rhs: &T) -> Self::Output {
                    let ($($name,)*) = self;
                    ( $($name.multiply(rhs), )* )
                }
            }
        )
    }

    tuple_implementation!((), ());
    tuple_implementation!((A1), (A2));
    tuple_implementation!((A1 B1), (A2 B2));
    tuple_implementation!((A1 B1 C1), (A2 B2 C2));
    tuple_implementation!((A1 B1 C1 D1), (A2 B2 C2 D2));
}

// Vector implementations
mod vector {

    use super::{Semigroup, Monoid, Abelian, Multiply};

    impl<R: Semigroup> Semigroup for Vec<R> {
        fn plus_equals(&mut self, rhs: &Self) {
            // Ensure sufficient length to receive addition.
            while self.len() < rhs.len() {
                let element = &rhs[self.len()];
                self.push(element.clone());
            }

            // As other is not longer, apply updates without tests.
            for (index, update) in rhs.iter().enumerate() {
                self[index].plus_equals(update);
            }
        }
        fn is_zero(&self) -> bool {
            self.iter().all(|x| x.is_zero())
        }
    }

    impl<R: Monoid> Monoid for Vec<R> {
        fn zero() -> Self {
            Self::new()
        }
    }

    impl<R: Abelian> Abelian for Vec<R> {
        fn negate(mut self) -> Self {
            for update in self.iter_mut() {
                *update = update.clone().negate();
            }
            self
        }
    }

    impl<T, R: Multiply<T>> Multiply<T> for Vec<R> {
        type Output = Vec<<R as Multiply<T>>::Output>;
        fn multiply(self, rhs: &T) -> Self::Output {
            self.into_iter()
                .map(|x| x.multiply(rhs))
                .collect()
        }
    }
}
