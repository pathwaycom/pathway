//! A type that can unpark specific threads.

use std::thread::Thread;

/// Can unpark a specific thread.
#[derive(Clone)]
pub struct Buzzer {
    thread: Thread,
}

impl Buzzer {
    /// Creates a new buzzer for the current thread.
    pub fn new() -> Self {
        Self {
            thread: std::thread::current(),
        }
    }
    /// Unparks the target thread.
    pub fn buzz(&self) {
        self.thread.unpark()
    }
}
