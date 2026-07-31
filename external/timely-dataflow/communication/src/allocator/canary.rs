//! A helper struct to report when something has been dropped.

use std::cell::RefCell;
use std::rc::Rc;

/// An opaque type that reports when it is dropped.
pub struct Canary {
    index: usize,
    queue: Rc<RefCell<Vec<usize>>>,
}

impl Canary {
    /// Allocates a new drop canary.
    pub fn new(index: usize, queue: Rc<RefCell<Vec<usize>>>) -> Self {
        Canary { index, queue }
    }
}

impl Drop for Canary {
    fn drop(&mut self) {
        self.queue.borrow_mut().push(self.index);
    }
}
