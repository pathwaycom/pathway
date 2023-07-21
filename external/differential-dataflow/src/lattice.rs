//! Partially ordered elements with a least upper bound.
//!
//! Lattices form the basis of differential dataflow's efficient execution in the presence of
//! iterative sub-computations. All logical times in differential dataflow must implement the
//! `Lattice` trait, and all reasoning in operators are done it terms of `Lattice` methods.

use timely::order::PartialOrder;
use timely::progress::{Antichain, frontier::AntichainRef};

/// A bounded partially ordered type supporting joins and meets.
pub trait Lattice : PartialOrder {

    /// The smallest element greater than or equal to both arguments.
    ///
    /// # Examples
    ///
    /// ```
    /// # extern crate timely;
    /// # extern crate differential_dataflow;
    /// # use timely::PartialOrder;
    /// # use timely::order::Product;
    /// # use differential_dataflow::lattice::Lattice;
    /// # fn main() {
    ///
    /// let time1 = Product::new(3, 7);
    /// let time2 = Product::new(4, 6);
    /// let join = time1.join(&time2);
    ///
    /// assert_eq!(join, Product::new(4, 7));
    /// # }
    /// ```
    fn join(&self, other: &Self) -> Self;

    /// Updates `self` to the smallest element greater than or equal to both arguments.
    ///
    /// # Examples
    ///
    /// ```
    /// # extern crate timely;
    /// # extern crate differential_dataflow;
    /// # use timely::PartialOrder;
    /// # use timely::order::Product;
    /// # use differential_dataflow::lattice::Lattice;
    /// # fn main() {
    ///
    /// let mut time1 = Product::new(3, 7);
    /// let time2 = Product::new(4, 6);
    /// time1.join_assign(&time2);
    ///
    /// assert_eq!(time1, Product::new(4, 7));
    /// # }
    /// ```
    fn join_assign(&mut self, other: &Self) where Self: Sized {
        *self = self.join(other);
    }

    /// The largest element less than or equal to both arguments.
    ///
    /// # Examples
    ///
    /// ```
    /// # extern crate timely;
    /// # extern crate differential_dataflow;
    /// # use timely::PartialOrder;
    /// # use timely::order::Product;
    /// # use differential_dataflow::lattice::Lattice;
    /// # fn main() {
    ///
    /// let time1 = Product::new(3, 7);
    /// let time2 = Product::new(4, 6);
    /// let meet = time1.meet(&time2);
    ///
    /// assert_eq!(meet, Product::new(3, 6));
    /// # }
    /// ```
    fn meet(&self, other: &Self) -> Self;

    /// Updates `self` to the largest element less than or equal to both arguments.
    ///
    /// # Examples
    ///
    /// ```
    /// # extern crate timely;
    /// # extern crate differential_dataflow;
    /// # use timely::PartialOrder;
    /// # use timely::order::Product;
    /// # use differential_dataflow::lattice::Lattice;
    /// # fn main() {
    ///
    /// let mut time1 = Product::new(3, 7);
    /// let time2 = Product::new(4, 6);
    /// time1.meet_assign(&time2);
    ///
    /// assert_eq!(time1, Product::new(3, 6));
    /// # }
    /// ```
    fn meet_assign(&mut self, other: &Self) where Self: Sized  {
        *self = self.meet(other);
    }

    /// Advances self to the largest time indistinguishable under `frontier`.
    ///
    /// This method produces the "largest" lattice element with the property that for every
    /// lattice element greater than some element of `frontier`, both the result and `self`
    /// compare identically to the lattice element. The result is the "largest" element in
    /// the sense that any other element with the same property (compares identically to times
    /// greater or equal to `frontier`) must be less or equal to the result.
    ///
    /// When provided an empty frontier `self` is not modified.
    ///
    /// # Examples
    ///
    /// ```
    /// # extern crate timely;
    /// # extern crate differential_dataflow;
    /// # use timely::PartialOrder;
    /// # use timely::order::Product;
    /// # use differential_dataflow::lattice::Lattice;
    /// # fn main() {
    ///
    /// use timely::progress::frontier::{Antichain, AntichainRef};
    ///
    /// let time = Product::new(3, 7);
    /// let mut advanced = Product::new(3, 7);
    /// let frontier = Antichain::from(vec![Product::new(4, 8), Product::new(5, 3)]);
    /// advanced.advance_by(frontier.borrow());
    ///
    /// // `time` and `advanced` are indistinguishable to elements >= an element of `frontier`
    /// for i in 0 .. 10 {
    ///     for j in 0 .. 10 {
    ///         let test = Product::new(i, j);
    ///         // for `test` in the future of `frontier` ..
    ///         if frontier.less_equal(&test) {
    ///             assert_eq!(time.less_equal(&test), advanced.less_equal(&test));
    ///         }
    ///     }
    /// }
    ///
    /// assert_eq!(advanced, Product::new(4, 7));
    /// # }
    /// ```
    #[inline]
    fn advance_by(&mut self, frontier: AntichainRef<Self>) where Self: Sized {
        let mut iter = frontier.iter();
        if let Some(first) = iter.next() {
            let mut result = self.join(first);
            for f in iter {
                result.meet_assign(&self.join(f));
            }
            *self = result;
        }
    }
}

use timely::order::Product;

impl<T1: Lattice, T2: Lattice> Lattice for Product<T1, T2> {
    #[inline]
    fn join(&self, other: &Product<T1, T2>) -> Product<T1, T2> {
        Product {
            outer: self.outer.join(&other.outer),
            inner: self.inner.join(&other.inner),
        }
    }
    #[inline]
    fn meet(&self, other: &Product<T1, T2>) -> Product<T1, T2> {
        Product {
            outer: self.outer.meet(&other.outer),
            inner: self.inner.meet(&other.inner),
        }
    }
}

/// A type that has a unique maximum element.
pub trait Maximum {
    /// The unique maximal element of the set.
    fn maximum() -> Self;
}

/// Implements `Maximum` for elements with a `MAX` associated constant.
macro_rules! implement_maximum {
    ($($index_type:ty,)*) => (
        $(
            impl Maximum for $index_type {
                fn maximum() -> Self { Self::MAX }
            }
        )*
    )
}

implement_maximum!(usize, u128, u64, u32, u16, u8, isize, i128, i64, i32, i16, i8, Duration,);
impl Maximum for () { fn maximum() -> () { () }}

use timely::progress::Timestamp;

// Tuples have the annoyance that they are only a lattice for `T2` with maximal elements,
// as the `meet` operator on `(x, _)` and `(y, _)` would be `(x meet y, maximum())`.
impl<T1: Lattice+Clone, T2: Lattice+Clone+Maximum+Timestamp> Lattice for (T1, T2) {
    #[inline]
    fn join(&self, other: &(T1, T2)) -> (T1, T2) {
        if self.0.eq(&other.0) {
            (self.0.clone(), self.1.join(&other.1))
        } else if self.0.less_than(&other.0) {
            other.clone()
        } else if other.0.less_than(&self.0) {
            self.clone()
        } else {
            (self.0.join(&other.0), T2::minimum())
        }
    }
    #[inline]
    fn meet(&self, other: &(T1, T2)) -> (T1, T2) {
        if self.0.eq(&other.0) {
            (self.0.clone(), self.1.meet(&other.1))
        } else if self.0.less_than(&other.0) {
            self.clone()
        } else if other.0.less_than(&self.0) {
            other.clone()
        } else {
            (self.0.meet(&other.0), T2::maximum())
        }
    }
}

macro_rules! implement_lattice {
    ($index_type:ty, $minimum:expr) => (
        impl Lattice for $index_type {
            #[inline] fn join(&self, other: &Self) -> Self { ::std::cmp::max(*self, *other) }
            #[inline] fn meet(&self, other: &Self) -> Self { ::std::cmp::min(*self, *other) }
        }
    )
}

use std::time::Duration;

implement_lattice!(Duration, Duration::new(0, 0));
implement_lattice!(usize, 0);
implement_lattice!(u128, 0);
implement_lattice!(u64, 0);
implement_lattice!(u32, 0);
implement_lattice!(u16, 0);
implement_lattice!(u8, 0);
implement_lattice!(isize, 0);
implement_lattice!(i128, 0);
implement_lattice!(i64, 0);
implement_lattice!(i32, 0);
implement_lattice!(i16, 0);
implement_lattice!(i8, 0);
implement_lattice!((), ());

/// Returns the "smallest" minimal antichain "greater or equal" to both inputs.
///
/// This method is primarily meant for cases where one cannot use the methods
/// of `Antichain`'s `PartialOrder` implementation, such as when one has only
/// references rather than owned antichains.
///
/// # Examples
///
/// ```
/// # extern crate timely;
/// # extern crate differential_dataflow;
/// # use timely::PartialOrder;
/// # use timely::order::Product;
/// # use differential_dataflow::lattice::Lattice;
/// # use differential_dataflow::lattice::antichain_join;
/// # fn main() {
///
/// let f1 = &[Product::new(3, 7), Product::new(5, 6)];
/// let f2 = &[Product::new(4, 6)];
/// let join = antichain_join(f1, f2);
/// assert_eq!(&*join.elements(), &[Product::new(4, 7), Product::new(5, 6)]);
/// # }
/// ```
pub fn antichain_join<T: Lattice>(one: &[T], other: &[T]) -> Antichain<T> {
    let mut upper = Antichain::new();
    antichain_join_into(one, other, &mut upper);
    upper
}

/// Returns the "smallest" minimal antichain "greater or equal" to both inputs.
///
/// This method is primarily meant for cases where one cannot use the methods
/// of `Antichain`'s `PartialOrder` implementation, such as when one has only
/// references rather than owned antichains.
///
/// This function is similar to [antichain_join] but reuses an existing allocation.
/// The provided antichain is cleared before inserting elements.
///
/// # Examples
///
/// ```
/// # extern crate timely;
/// # extern crate differential_dataflow;
/// # use timely::PartialOrder;
/// # use timely::order::Product;
/// # use timely::progress::Antichain;
/// # use differential_dataflow::lattice::Lattice;
/// # use differential_dataflow::lattice::antichain_join_into;
/// # fn main() {
///
/// let mut join = Antichain::new();
/// let f1 = &[Product::new(3, 7), Product::new(5, 6)];
/// let f2 = &[Product::new(4, 6)];
/// antichain_join_into(f1, f2, &mut join);
/// assert_eq!(&*join.elements(), &[Product::new(4, 7), Product::new(5, 6)]);
/// # }
/// ```
pub fn antichain_join_into<T: Lattice>(one: &[T], other: &[T], upper: &mut Antichain<T>) {
    upper.clear();
    for time1 in one {
        for time2 in other {
            upper.insert(time1.join(time2));
        }
    }
}

/// Returns the "greatest" minimal antichain "less or equal" to both inputs.
///
/// This method is primarily meant for cases where one cannot use the methods
/// of `Antichain`'s `PartialOrder` implementation, such as when one has only
/// references rather than owned antichains.
///
/// # Examples
///
/// ```
/// # extern crate timely;
/// # extern crate differential_dataflow;
/// # use timely::PartialOrder;
/// # use timely::order::Product;
/// # use differential_dataflow::lattice::Lattice;
/// # use differential_dataflow::lattice::antichain_meet;
/// # fn main() {
///
/// let f1 = &[Product::new(3, 7), Product::new(5, 6)];
/// let f2 = &[Product::new(4, 6)];
/// let meet = antichain_meet(f1, f2);
/// assert_eq!(&*meet.elements(), &[Product::new(3, 7), Product::new(4, 6)]);
/// # }
/// ```
pub fn antichain_meet<T: Lattice+Clone>(one: &[T], other: &[T]) -> Antichain<T> {
    let mut upper = Antichain::new();
    for time1 in one {
        upper.insert(time1.clone());
    }
    for time2 in other {
        upper.insert(time2.clone());
    }
    upper
}

impl<T: Lattice+Clone> Lattice for Antichain<T> {
    fn join(&self, other: &Self) -> Self {
        let mut upper = Antichain::new();
        for time1 in self.elements().iter() {
            for time2 in other.elements().iter() {
                upper.insert(time1.join(time2));
            }
        }
        upper
    }
    fn meet(&self, other: &Self) -> Self {
        let mut upper = Antichain::new();
        for time1 in self.elements().iter() {
            upper.insert(time1.clone());
        }
        for time2 in other.elements().iter() {
            upper.insert(time2.clone());
        }
        upper
    }
}
