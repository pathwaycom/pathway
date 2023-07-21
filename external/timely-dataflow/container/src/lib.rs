//! Specifications for containers

#![forbid(missing_docs)]

#[cfg(feature = "columnation")]
pub mod columnation;

/// A container transferring data through dataflow edges
///
/// A container stores a number of elements and thus is able to describe it length (`len()`) and
/// whether it is empty (`is_empty()`). It supports removing all elements (`clear`).
///
/// A container must implement default. The default implementation is not required to allocate
/// memory for variable-length components.
///
/// We require the container to be cloneable to enable efficient copies when providing references
/// of containers to operators. Care must be taken that the type's `clone_from` implementation
/// is efficient (which is not necessarily the case when deriving `Clone`.)
/// TODO: Don't require `Container: Clone`
pub trait Container: Default + Clone + 'static {
    /// The type of elements this container holds.
    type Item;

    /// The number of elements in this container
    ///
    /// The length of a container must be consistent between sending and receiving it.
    /// When exchanging a container and partitioning it into pieces, the sum of the length
    /// of all pieces must be equal to the length of the original container.
    fn len(&self) -> usize;

    /// Determine if the container contains any elements, corresponding to `len() == 0`.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The capacity of the underlying container
    fn capacity(&self) -> usize;

    /// Remove all contents from `self` while retaining allocated memory.
    /// After calling `clear`, `is_empty` must return `true` and `len` 0.
    fn clear(&mut self);
}

impl<T: Clone + 'static> Container for Vec<T> {
    type Item = T;

    fn len(&self) -> usize {
        Vec::len(self)
    }

    fn is_empty(&self) -> bool {
        Vec::is_empty(self)
    }

    fn capacity(&self) -> usize {
        Vec::capacity(self)
    }

    fn clear(&mut self) { Vec::clear(self) }
}

mod rc {
    use std::rc::Rc;

    use crate::Container;

    impl<T: Container> Container for Rc<T> {
        type Item = T::Item;

        fn len(&self) -> usize {
            std::ops::Deref::deref(self).len()
        }

        fn is_empty(&self) -> bool {
            std::ops::Deref::deref(self).is_empty()
        }

        fn capacity(&self) -> usize {
            std::ops::Deref::deref(self).capacity()
        }

        fn clear(&mut self) { }
    }
}

mod arc {
    use std::sync::Arc;

    use crate::Container;

    impl<T: Container> Container for Arc<T> {
        type Item = T::Item;

        fn len(&self) -> usize {
            std::ops::Deref::deref(self).len()
        }

        fn is_empty(&self) -> bool {
            std::ops::Deref::deref(self).is_empty()
        }

        fn capacity(&self) -> usize {
            std::ops::Deref::deref(self).capacity()
        }

        fn clear(&mut self) { }
    }
}

/// A container that can partition itself into pieces.
pub trait PushPartitioned: Container {
    /// Partition and push this container.
    ///
    /// Drain all elements from `self`, and use the function `index` to determine which `buffer` to
    /// append an element to. Call `flush` with an index and a buffer to send the data downstream.
    fn push_partitioned<I, F>(&mut self, buffers: &mut [Self], index: I, flush: F)
    where
        I: FnMut(&Self::Item) -> usize,
        F: FnMut(usize, &mut Self);
}

impl<T: Clone + 'static> PushPartitioned for Vec<T> {
    fn push_partitioned<I, F>(&mut self, buffers: &mut [Self], mut index: I, mut flush: F)
    where
        I: FnMut(&Self::Item) -> usize,
        F: FnMut(usize, &mut Self),
    {
        fn ensure_capacity<E>(this: &mut Vec<E>) {
            let capacity = this.capacity();
            let desired_capacity = buffer::default_capacity::<E>();
            if capacity < desired_capacity {
                this.reserve(desired_capacity - capacity);
            }
        }

        for datum in self.drain(..) {
            let index = index(&datum);
            ensure_capacity(&mut buffers[index]);
            buffers[index].push(datum);
            if buffers[index].len() == buffers[index].capacity() {
                flush(index, &mut buffers[index]);
            }
        }
    }
}

pub mod buffer {
    //! Functionality related to calculating default buffer sizes

    /// The upper limit for buffers to allocate, size in bytes. [default_capacity] converts
    /// this to size in elements.
    pub const BUFFER_SIZE_BYTES: usize = 1 << 13;

    /// The maximum buffer capacity in elements. Returns a number between [BUFFER_SIZE_BYTES]
    /// and 1, inclusively.
    pub const fn default_capacity<T>() -> usize {
        let size = ::std::mem::size_of::<T>();
        if size == 0 {
            BUFFER_SIZE_BYTES
        } else if size <= BUFFER_SIZE_BYTES {
            BUFFER_SIZE_BYTES / size
        } else {
            1
        }
    }
}
