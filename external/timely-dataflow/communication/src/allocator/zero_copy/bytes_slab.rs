//! A large binary allocation for writing and sharing.

use bytes::arc::Bytes;

/// A large binary allocation for writing and sharing.
///
/// A bytes slab wraps a `Bytes` and maintains a valid (written) length, and supports writing after
/// this valid length, and extracting `Bytes` up to this valid length. Extracted bytes are enqueued
/// and checked for uniqueness in order to recycle them (once all shared references are dropped).
pub struct BytesSlab {
    buffer:         Bytes,                      // current working buffer.
    in_progress:    Vec<Option<Bytes>>,         // buffers shared with workers.
    stash:          Vec<Bytes>,                 // reclaimed and resuable buffers.
    shift:          usize,                      // current buffer allocation size.
    valid:          usize,                      // buffer[..valid] are valid bytes.
}

impl BytesSlab {
    /// Allocates a new `BytesSlab` with an initial size determined by a shift.
    pub fn new(shift: usize) -> Self {
        BytesSlab {
            buffer: Bytes::from(vec![0u8; 1 << shift].into_boxed_slice()),
            in_progress: Vec::new(),
            stash: Vec::new(),
            shift,
            valid: 0,
        }
    }
    /// The empty region of the slab.
    pub fn empty(&mut self) -> &mut [u8] {
        &mut self.buffer[self.valid..]
    }
    /// The valid region of the slab.
    pub fn valid(&mut self) -> &mut [u8] {
        &mut self.buffer[..self.valid]
    }
    /// Marks the next `bytes` bytes as valid.
    pub fn make_valid(&mut self, bytes: usize) {
        self.valid += bytes;
    }
    /// Extracts the first `bytes` valid bytes.
    pub fn extract(&mut self, bytes: usize) -> Bytes {
        debug_assert!(bytes <= self.valid);
        self.valid -= bytes;
        self.buffer.extract_to(bytes)
    }

    /// Ensures that `self.empty().len()` is at least `capacity`.
    ///
    /// This method may retire the current buffer if it does not have enough space, in which case
    /// it will copy any remaining contents into a new buffer. If this would not create enough free
    /// space, the shift is increased until it is sufficient.
    pub fn ensure_capacity(&mut self, capacity: usize) {

        if self.empty().len() < capacity {

            let mut increased_shift = false;

            // Increase allocation if copy would be insufficient.
            while self.valid + capacity > (1 << self.shift) {
                self.shift += 1;
                self.stash.clear();         // clear wrongly sized buffers.
                self.in_progress.clear();   // clear wrongly sized buffers.
                increased_shift = true;
            }

            // Attempt to reclaim shared slices.
            if self.stash.is_empty() {
                for shared in self.in_progress.iter_mut() {
                    if let Some(mut bytes) = shared.take() {
                        if bytes.try_regenerate::<Box<[u8]>>() {
                            // NOTE: Test should be redundant, but better safe...
                            if bytes.len() == (1 << self.shift) {
                                self.stash.push(bytes);
                            }
                        }
                        else {
                            *shared = Some(bytes);
                        }
                    }
                }
                self.in_progress.retain(|x| x.is_some());
            }

            let new_buffer = self.stash.pop().unwrap_or_else(|| Bytes::from(vec![0; 1 << self.shift].into_boxed_slice()));
            let old_buffer = ::std::mem::replace(&mut self.buffer, new_buffer);

            self.buffer[.. self.valid].copy_from_slice(&old_buffer[.. self.valid]);
            if !increased_shift {
                self.in_progress.push(Some(old_buffer));
            }
        }
    }
}