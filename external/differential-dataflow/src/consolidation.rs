//! Common logic for the consolidation of vectors of Semigroups.
//!
//! Often we find ourselves with collections of records with associated weights (often
//! integers) where we want to reduce the collection to the point that each record occurs
//! at most once, with the accumulated weights. These methods supply that functionality.
//!
//! Importantly, these methods are used internally by differential dataflow, but are made
//! public for the convenience of others. Their precise behavior is driven by the needs of
//! differential dataflow (chiefly: canonicalizing sequences of non-zero updates); should
//! you need specific behavior, it may be best to defensively copy, paste, and maintain the
//! specific behavior you require.

use crate::difference::Semigroup;

/// Sorts and consolidates `vec`.
///
/// This method will sort `vec` and then consolidate runs of more than one entry with
/// identical first elements by accumulating the second elements of the pairs. Should the final
/// accumulation be zero, the element is discarded.
pub fn consolidate<T: Ord, R: Semigroup>(vec: &mut Vec<(T, R)>) {
    consolidate_from(vec, 0);
}

/// Sorts and consolidate `vec[offset..]`.
///
/// This method will sort `vec[offset..]` and then consolidate runs of more than one entry with
/// identical first elements by accumulating the second elements of the pairs. Should the final
/// accumulation be zero, the element is discarded.
pub fn consolidate_from<T: Ord, R: Semigroup>(vec: &mut Vec<(T, R)>, offset: usize) {
    let length = consolidate_slice(&mut vec[offset..]);
    vec.truncate(offset + length);
}

/// Sorts and consolidates a slice, returning the valid prefix length.
pub fn consolidate_slice<T: Ord, R: Semigroup>(slice: &mut [(T, R)]) -> usize {

    // We could do an insertion-sort like initial scan which builds up sorted, consolidated runs.
    // In a world where there are not many results, we may never even need to call in to merge sort.
    slice.sort_by(|x,y| x.0.cmp(&y.0));

    let slice_ptr = slice.as_mut_ptr();

    // Counts the number of distinct known-non-zero accumulations. Indexes the write location.
    let mut offset = 0;
    for index in 1 .. slice.len() {

        // The following unsafe block elides various bounds checks, using the reasoning that `offset`
        // is always strictly less than `index` at the beginning of each iteration. This is initially
        // true, and in each iteration `offset` can increase by at most one (whereas `index` always
        // increases by one). As `index` is always in bounds, and `offset` starts at zero, it too is
        // always in bounds.
        //
        // LLVM appears to struggle to optimize out Rust's split_at_mut, which would prove disjointness
        // using run-time tests.
        unsafe {

            assert!(offset < index);

            // LOOP INVARIANT: offset < index
            let ptr1 = slice_ptr.add(offset);
            let ptr2 = slice_ptr.add(index);

            if (*ptr1).0 == (*ptr2).0 {
                (*ptr1).1.plus_equals(&(*ptr2).1);
            }
            else {
                if !(*ptr1).1.is_zero() {
                    offset += 1;
                }
                let ptr1 = slice_ptr.add(offset);
                std::ptr::swap(ptr1, ptr2);
            }
        }
    }
    if offset < slice.len() && !slice[offset].1.is_zero() {
        offset += 1;
    }

    offset
}

/// Sorts and consolidates `vec`.
///
/// This method will sort `vec` and then consolidate runs of more than one entry with
/// identical first two elements by accumulating the third elements of the triples. Should the final
/// accumulation be zero, the element is discarded.
pub fn consolidate_updates<D: Ord, T: Ord, R: Semigroup>(vec: &mut Vec<(D, T, R)>) {
    consolidate_updates_from(vec, 0);
}

/// Sorts and consolidate `vec[offset..]`.
///
/// This method will sort `vec[offset..]` and then consolidate runs of more than one entry with
/// identical first two elements by accumulating the third elements of the triples. Should the final
/// accumulation be zero, the element is discarded.
pub fn consolidate_updates_from<D: Ord, T: Ord, R: Semigroup>(vec: &mut Vec<(D, T, R)>, offset: usize) {
    let length = consolidate_updates_slice(&mut vec[offset..]);
    vec.truncate(offset + length);
}

/// Sorts and consolidates a slice, returning the valid prefix length.
pub fn consolidate_updates_slice<D: Ord, T: Ord, R: Semigroup>(slice: &mut [(D, T, R)]) -> usize {

    // We could do an insertion-sort like initial scan which builds up sorted, consolidated runs.
    // In a world where there are not many results, we may never even need to call in to merge sort.
    slice.sort_unstable_by(|x,y| (&x.0, &x.1).cmp(&(&y.0, &y.1)));

    let slice_ptr = slice.as_mut_ptr();

    // Counts the number of distinct known-non-zero accumulations. Indexes the write location.
    let mut offset = 0;
    for index in 1 .. slice.len() {

        // The following unsafe block elides various bounds checks, using the reasoning that `offset`
        // is always strictly less than `index` at the beginning of each iteration. This is initially
        // true, and in each iteration `offset` can increase by at most one (whereas `index` always
        // increases by one). As `index` is always in bounds, and `offset` starts at zero, it too is
        // always in bounds.
        //
        // LLVM appears to struggle to optimize out Rust's split_at_mut, which would prove disjointness
        // using run-time tests.
        unsafe {

            // LOOP INVARIANT: offset < index
            let ptr1 = slice_ptr.add(offset);
            let ptr2 = slice_ptr.add(index);

            if (*ptr1).0 == (*ptr2).0 && (*ptr1).1 == (*ptr2).1 {
                (*ptr1).2.plus_equals(&(*ptr2).2);
            }
            else {
                if !(*ptr1).2.is_zero() {
                    offset += 1;
                }
                let ptr1 = slice_ptr.add(offset);
                std::ptr::swap(ptr1, ptr2);
            }

        }
    }
    if offset < slice.len() && !slice[offset].2.is_zero() {
        offset += 1;
    }

    offset
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_consolidate() {
        let test_cases = vec![
            (
                vec![("a", -1), ("b", -2), ("a", 1)],
                vec![("b", -2)],
            ),
            (
                vec![("a", -1), ("b", 0), ("a", 1)],
                vec![],
            ),
            (
                vec![("a", 0)],
                vec![],
            ),
            (
                vec![("a", 0), ("b", 0)],
                vec![],
            ),
            (
                vec![("a", 1), ("b", 1)],
                vec![("a", 1), ("b", 1)],
            ),
        ];

        for (mut input, output) in test_cases {
            consolidate(&mut input);
            assert_eq!(input, output);
        }
    }


    #[test]
    fn test_consolidate_updates() {
        let test_cases = vec![
            (
                vec![("a", 1, -1), ("b", 1, -2), ("a", 1, 1)],
                vec![("b", 1, -2)],
            ),
            (
                vec![("a", 1, -1), ("b", 1, 0), ("a", 1, 1)],
                vec![],
            ),
            (
                vec![("a", 1, 0)],
                vec![],
            ),
            (
                vec![("a", 1, 0), ("b", 1, 0)],
                vec![],
            ),
            (
                vec![("a", 1, 1), ("b", 2, 1)],
                vec![("a", 1, 1), ("b", 2, 1)],
            ),
        ];

        for (mut input, output) in test_cases {
            consolidate_updates(&mut input);
            assert_eq!(input, output);
        }
    }
}
