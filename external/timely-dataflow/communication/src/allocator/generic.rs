//! A generic allocator, wrapping known implementors of `Allocate`.
//!
//! This type is useful in settings where it is difficult to write code generic in `A: Allocate`,
//! for example closures whose type arguments must be specified.

use std::rc::Rc;
use std::cell::RefCell;

use crate::allocator::thread::ThreadBuilder;
use crate::allocator::process::ProcessBuilder as TypedProcessBuilder;
use crate::allocator::{Allocate, AllocateBuilder, Thread, Process};
use crate::allocator::zero_copy::allocator_process::{ProcessBuilder, ProcessAllocator};
use crate::allocator::zero_copy::allocator::{TcpBuilder, TcpAllocator};

use crate::{Push, Pull, Data, Message};

/// Enumerates known implementors of `Allocate`.
/// Passes trait method calls on to members.
pub enum Generic {
    /// Intra-thread allocator.
    Thread(Thread),
    /// Inter-thread, intra-process allocator.
    Process(Process),
    /// Inter-thread, intra-process serializing allocator.
    ProcessBinary(ProcessAllocator),
    /// Inter-process allocator.
    ZeroCopy(TcpAllocator<Process>),
}

impl Generic {
    /// The index of the worker out of `(0..self.peers())`.
    pub fn index(&self) -> usize {
        match self {
            Generic::Thread(t) => t.index(),
            Generic::Process(p) => p.index(),
            Generic::ProcessBinary(pb) => pb.index(),
            Generic::ZeroCopy(z) => z.index(),
        }
    }
    /// The number of workers.
    pub fn peers(&self) -> usize {
        match self {
            Generic::Thread(t) => t.peers(),
            Generic::Process(p) => p.peers(),
            Generic::ProcessBinary(pb) => pb.peers(),
            Generic::ZeroCopy(z) => z.peers(),
        }
    }
    /// Constructs several send endpoints and one receive endpoint.
    fn allocate<T: Data>(&mut self, identifier: usize) -> (Vec<Box<dyn Push<Message<T>>>>, Box<dyn Pull<Message<T>>>) {
        match self {
            Generic::Thread(t) => t.allocate(identifier),
            Generic::Process(p) => p.allocate(identifier),
            Generic::ProcessBinary(pb) => pb.allocate(identifier),
            Generic::ZeroCopy(z) => z.allocate(identifier),
        }
    }
    /// Perform work before scheduling operators.
    fn receive(&mut self) {
        match self {
            Generic::Thread(t) => t.receive(),
            Generic::Process(p) => p.receive(),
            Generic::ProcessBinary(pb) => pb.receive(),
            Generic::ZeroCopy(z) => z.receive(),
        }
    }
    /// Perform work after scheduling operators.
    pub fn release(&mut self) {
        match self {
            Generic::Thread(t) => t.release(),
            Generic::Process(p) => p.release(),
            Generic::ProcessBinary(pb) => pb.release(),
            Generic::ZeroCopy(z) => z.release(),
        }
    }
    fn events(&self) -> &Rc<RefCell<Vec<usize>>> {
        match self {
            Generic::Thread(ref t) => t.events(),
            Generic::Process(ref p) => p.events(),
            Generic::ProcessBinary(ref pb) => pb.events(),
            Generic::ZeroCopy(ref z) => z.events(),
        }
    }
}

impl Allocate for Generic {
    fn index(&self) -> usize { self.index() }
    fn peers(&self) -> usize { self.peers() }
    fn allocate<T: Data>(&mut self, identifier: usize) -> (Vec<Box<dyn Push<Message<T>>>>, Box<dyn Pull<Message<T>>>) {
        self.allocate(identifier)
    }

    fn receive(&mut self) { self.receive(); }
    fn release(&mut self) { self.release(); }
    fn events(&self) -> &Rc<RefCell<Vec<usize>>> { self.events() }
    fn await_events(&self, _duration: Option<std::time::Duration>) {
        match self {
            Generic::Thread(t) => t.await_events(_duration),
            Generic::Process(p) => p.await_events(_duration),
            Generic::ProcessBinary(pb) => pb.await_events(_duration),
            Generic::ZeroCopy(z) => z.await_events(_duration),
        }
    }
}


/// Enumerations of constructable implementors of `Allocate`.
///
/// The builder variants are meant to be `Send`, so that they can be moved across threads,
/// whereas the allocator they construct may not. As an example, the `ProcessBinary` type
/// contains `Rc` wrapped state, and so cannot itself be moved across threads.
pub enum GenericBuilder {
    /// Builder for `Thread` allocator.
    Thread(ThreadBuilder),
    /// Builder for `Process` allocator.
    Process(TypedProcessBuilder),
    /// Builder for `ProcessBinary` allocator.
    ProcessBinary(ProcessBuilder),
    /// Builder for `ZeroCopy` allocator.
    ZeroCopy(TcpBuilder<TypedProcessBuilder>),
}

impl AllocateBuilder for GenericBuilder {
    type Allocator = Generic;
    fn build(self) -> Generic {
        match self {
            GenericBuilder::Thread(t) => Generic::Thread(t.build()),
            GenericBuilder::Process(p) => Generic::Process(p.build()),
            GenericBuilder::ProcessBinary(pb) => Generic::ProcessBinary(pb.build()),
            GenericBuilder::ZeroCopy(z) => Generic::ZeroCopy(z.build()),
        }
    }
}
