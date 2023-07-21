extern crate libloading;
extern crate timely;
extern crate differential_dataflow;

use std::any::Any;
use std::rc::Rc;
use std::collections::HashMap;
use std::time::Instant;

use libloading::Library;

use timely::communication::Allocator;
use timely::worker::Worker;
use timely::dataflow::scopes::Child;
use timely::dataflow::operators::probe::Handle as ProbeHandle;

// stuff for talking about shared trace types ...
use differential_dataflow::operators::arrange::TraceAgent;
use differential_dataflow::trace::implementations::spine_fueled::Spine;
use differential_dataflow::trace::implementations::ord::OrdValBatch;

// These are all defined here so that users can be assured a common layout.
pub type RootTime = usize;
type TraceBatch = OrdValBatch<usize, usize, RootTime, isize>;
type TraceSpine = Spine<usize, usize, RootTime, isize, Rc<TraceBatch>>;
pub type TraceHandle = TraceAgent<usize, usize, RootTime, isize, TraceSpine>;

/// Arguments provided to each shared library to help build their dataflows and register their results.
pub type Environment<'a, 'b> = (
    &'a mut Child<'b, Worker<Allocator>,usize>,
    &'a mut TraceHandler,
    &'a mut ProbeHandle<RootTime>,
    &'a Instant,
    &'a [String]
);

/// A wrapper around types that keep their source libraries alive.
///
/// This type is meant to be a smart pointer for a type `T` that needs to keep
/// a `Library` alive, perhaps because its methods would call in to the library.
/// The type should have a specified drop order (viz RFC 1857) which guarentees
/// that the shared library reference drops only after the element itself is
/// dropped. It also implements `Deref` and `DerefMut` to provide the experience
/// of a `T` itself.
///
/// Nothing about this type actually *guarantees* that the code for type `T` is
/// found in the wrapped library, and it is up to the user to wrap the correct
/// libraries here.
pub struct LibraryWrapper<T> {
    /// The wrapped element.
    element: T,
    /// An unused field used to keep the reference count alive.
    _library: Rc<Library>,
}

impl<T> LibraryWrapper<T> {
    /// Creates a new instance of LibraryWrapper.
    pub fn new(element: T, library: &Rc<Library>) -> Self {
        LibraryWrapper {
            element: element,
            _library: library.clone(),
        }
    }
}

impl<T> ::std::ops::Deref for LibraryWrapper<T> {
    type Target = T;
    fn deref(&self) -> &T { &self.element }
}

impl<T> ::std::ops::DerefMut for LibraryWrapper<T> {
    fn deref_mut(&mut self) -> &mut T { &mut self.element }
}

/// A wrapper around a `HashMap<String, Box<Any>>` that handles downcasting.
pub struct TraceHandler {
    handles: HashMap<String, Box<Any>>,
}

impl TraceHandler {
    /// Create a new trace handler.
    pub fn new() -> Self { TraceHandler { handles: HashMap::new() } }
    /// Acquire a mutable borrow of the value for `name`, if it is of type `T`.
    pub fn get_mut<'a, T: Any>(&'a mut self, name: &str) -> Result<&'a mut T, String> {
        let boxed = self.handles.get_mut(name).ok_or(format!("failed to find handle: {:?}", name))?;
        boxed.downcast_mut::<T>().ok_or(format!("failed to downcast: {}", name))
    }
    /// Enumerates the keys maintained in storage (for the `list` operation).
    pub fn keys(&self) -> ::std::collections::hash_map::Keys<String, Box<Any>> {
        self.handles.keys()
    }
    /// Assign a thing to key `name`, boxed as `Box<Any>`.
    pub fn set<T: Any>(&mut self, name: String, thing: T) {
        let boxed: Box<Any> = Box::new(thing);
        assert!(boxed.downcast_ref::<T>().is_some());
        self.handles.insert(name, boxed);
    }
    /// Removes the resource associated with `name`.
    pub fn remove(&mut self, name: &str) -> Option<Box<Any>> {
        self.handles.remove(name)
    }
}