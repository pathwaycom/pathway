//! Types and traits to activate and schedule fibers.

use std::cell::RefCell;
use std::rc::Rc;

pub mod activate;

pub use self::activate::{ActivateOnDrop, Activations, Activator, SyncActivator};

/// A type that can be scheduled.
pub trait Schedule {
    /// A descriptive name for the operator
    fn name(&self) -> &str;
    /// An address identifying the operator.
    fn path(&self) -> &[usize];
    /// Schedules the operator, receives "cannot terminate" boolean.
    ///
    /// The return value indicates whether `self` has outstanding
    /// work and would be upset if the computation terminated.
    fn schedule(&mut self) -> bool;
}

/// Methods for types which schedule fibers.
pub trait Scheduler {
    /// Provides a shared handle to the activation scheduler.
    fn activations(&self) -> Rc<RefCell<Activations>>;

    /// Constructs an `Activator` tied to the specified operator address.
    fn activator_for(&self, path: &[usize]) -> Activator {
        Activator::new(path, self.activations())
    }

    /// Constructs a `SyncActivator` tied to the specified operator address.
    fn sync_activator_for(&self, path: &[usize]) -> SyncActivator {
        let sync_activations = self.activations().borrow().sync();
        SyncActivator::new(path, sync_activations)
    }
}
