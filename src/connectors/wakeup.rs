// Copyright © 2026 Pathway

//! Wake-up protocol between the reader threads of the connectors and the
//! timely worker thread that consumes their entries.
//!
//! Waking the worker for every entry is wasteful once entries arrive faster
//! than the worker gets scheduled: on a shared core every wake-up becomes a
//! context switch that preempts the reader, and the worker finds a single
//! entry each time. Instead the worker polls its inputs on a short timer while
//! entries keep arriving, and readers wake it only after it has announced a
//! long park with nothing to do.

use std::cell::RefCell;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread::{self, Thread};
use std::time::Duration;

/// How long the worker sleeps between polls while entries keep arriving.
pub const ACTIVE_POLL_INTERVAL: Duration = Duration::from_millis(1);

pub struct WorkerWakeup {
    thread: Thread,
    /// Raised by the worker for the duration of a long park; readers unpark it only then.
    parked: AtomicBool,
    /// Entries queued by all readers; the worker compares it between iterations.
    sent: AtomicU64,
    /// Wake on every entry, for worker threads that never run the adaptive loop.
    always_wake: bool,
}

impl WorkerWakeup {
    fn new(thread: Thread, always_wake: bool) -> Self {
        Self {
            thread,
            parked: AtomicBool::new(false),
            sent: AtomicU64::new(0),
            always_wake,
        }
    }

    /// Reader side: an entry has been queued for the worker.
    pub fn notify(&self) {
        // `SeqCst` on both sides makes this a Dekker handshake with `enter_parked`:
        // either the worker sees this entry before parking, or the reader sees the
        // worker parked and unparks it. An extra unpark only costs a spurious wake-up.
        self.sent.fetch_add(1, Ordering::SeqCst);
        if self.always_wake || self.parked.load(Ordering::SeqCst) {
            self.thread.unpark();
        }
    }

    /// Reader side: wakes the worker unconditionally, e.g. when the reader is done.
    pub fn wake(&self) {
        self.thread.unpark();
    }

    /// Worker side: entries queued so far.
    pub fn sent(&self) -> u64 {
        self.sent.load(Ordering::SeqCst)
    }

    /// Worker side: announces a long park. Returns `false` when an entry was queued
    /// after `seen`, in which case the worker must poll instead of parking.
    pub fn enter_parked(&self, seen: u64) -> bool {
        self.parked.store(true, Ordering::SeqCst);
        if self.sent.load(Ordering::SeqCst) != seen {
            self.parked.store(false, Ordering::SeqCst);
            return false;
        }
        true
    }

    /// Worker side: the park is over, readers stop unparking.
    pub fn leave_parked(&self) {
        self.parked.store(false, Ordering::SeqCst);
    }
}

thread_local! {
    static CURRENT: RefCell<Option<Arc<WorkerWakeup>>> = const { RefCell::new(None) };
}

/// Installs the adaptive wake-up state for the current thread, which must be the
/// one running the worker loop that honors `enter_parked` / `leave_parked`.
pub fn install_for_current_thread() -> Arc<WorkerWakeup> {
    let wakeup = Arc::new(WorkerWakeup::new(thread::current(), false));
    CURRENT.with(|current| *current.borrow_mut() = Some(wakeup.clone()));
    wakeup
}

/// The wake-up state for readers started from the current thread. A thread that
/// never installed one gets a state that wakes it on every entry.
pub fn for_current_thread() -> Arc<WorkerWakeup> {
    CURRENT.with(|current| {
        current
            .borrow()
            .clone()
            .unwrap_or_else(|| Arc::new(WorkerWakeup::new(thread::current(), true)))
    })
}
