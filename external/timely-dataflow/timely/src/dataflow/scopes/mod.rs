//! Hierarchical organization of timely dataflow graphs.

use crate::communication::Allocate;
use crate::order::Product;
use crate::progress::timestamp::Refines;
use crate::progress::{Operate, Source, Target, Timestamp};
use crate::worker::AsWorker;

pub mod child;

pub use self::child::Child;

/// The information a child scope needs from its parent.
pub trait ScopeParent: AsWorker + Clone {
    /// The timestamp associated with data in this scope.
    type Timestamp: Timestamp;
}

impl<A: Allocate> ScopeParent for crate::worker::Worker<A> {
    type Timestamp = ();
}

/// The fundamental operations required to add and connect operators in a timely dataflow graph.
///
/// Importantly, this is often a *shared* object, backed by a `Rc<RefCell<>>` wrapper. Each method
/// takes a shared reference, but can be thought of as first calling .clone() and then calling the
/// method. Each method does not hold the `RefCell`'s borrow, and should prevent accidental panics.
pub trait Scope: ScopeParent {
    /// A useful name describing the scope.
    fn name(&self) -> String;

    /// A sequence of scope identifiers describing the path from the worker root to this scope.
    fn addr(&self) -> Vec<usize>;

    /// Connects a source of data with a target of the data. This only links the two for
    /// the purposes of tracking progress, rather than effect any data movement itself.
    fn add_edge(&self, source: Source, target: Target);

    /// Adds a child `Operate` to the builder's scope. Returns the new child's index.
    fn add_operator(&mut self, operator: Box<dyn Operate<Self::Timestamp>>) -> usize {
        let index = self.allocate_operator_index();
        let global = self.new_identifier();
        self.add_operator_with_indices(operator, index, global);
        index
    }

    /// Allocates a new scope-local operator index.
    ///
    /// This method is meant for use with `add_operator_with_index`, which accepts a scope-local
    /// operator index allocated with this method. This method does cause the scope to expect that
    /// an operator will be added, and it is an error not to eventually add such an operator.
    fn allocate_operator_index(&mut self) -> usize;

    /// Adds a child `Operate` to the builder's scope using a supplied index.
    ///
    /// This is used internally when there is a gap between allocate a child identifier and adding the
    /// child, as happens in subgraph creation.
    fn add_operator_with_index(
        &mut self,
        operator: Box<dyn Operate<Self::Timestamp>>,
        index: usize,
    ) {
        let global = self.new_identifier();
        self.add_operator_with_indices(operator, index, global);
    }

    /// Adds a child `Operate` to the builder's scope using supplied indices.
    ///
    /// The two indices are the scope-local operator index, and a worker-unique index used for e.g. logging.
    fn add_operator_with_indices(
        &mut self,
        operator: Box<dyn Operate<Self::Timestamp>>,
        local: usize,
        global: usize,
    );

    /// Creates a dataflow subgraph.
    ///
    /// This method allows the user to create a nested scope with any timestamp that
    /// "refines" the enclosing timestamp (informally: extends it in a reversible way).
    ///
    /// This is most commonly used to create new iterative contexts, and the provided
    /// method `iterative` for this task demonstrates the use of this method.
    ///
    /// # Examples
    /// ```
    /// use timely::dataflow::Scope;
    /// use timely::dataflow::operators::{Input, Enter, Leave};
    /// use timely::order::Product;
    ///
    /// timely::execute_from_args(std::env::args(), |worker| {
    ///     // must specify types as nothing else drives inference.
    ///     let input = worker.dataflow::<u64,_,_>(|child1| {
    ///         let (input, stream) = child1.new_input::<String>();
    ///         let output = child1.scoped::<Product<u64,u32>,_,_>("ScopeName", |child2| {
    ///             stream.enter(child2).leave()
    ///         });
    ///         input
    ///     });
    /// });
    /// ```
    fn scoped<T, R, F>(&mut self, name: &str, func: F) -> R
    where
        T: Timestamp + Refines<<Self as ScopeParent>::Timestamp>,
        F: FnOnce(&mut Child<Self, T>) -> R;

    /// Creates a iterative dataflow subgraph.
    ///
    /// This method is a specialization of `scoped` which uses the `Product` timestamp
    /// combinator, suitable for iterative computations in which iterative development
    /// at some time cannot influence prior iterations at a future time.
    ///
    /// # Examples
    /// ```
    /// use timely::dataflow::Scope;
    /// use timely::dataflow::operators::{Input, Enter, Leave};
    ///
    /// timely::execute_from_args(std::env::args(), |worker| {
    ///     // must specify types as nothing else drives inference.
    ///     let input = worker.dataflow::<u64,_,_>(|child1| {
    ///         let (input, stream) = child1.new_input::<String>();
    ///         let output = child1.iterative::<u32,_,_>(|child2| {
    ///             stream.enter(child2).leave()
    ///         });
    ///         input
    ///     });
    /// });
    /// ```
    fn iterative<T, R, F>(&mut self, func: F) -> R
    where
        T: Timestamp,
        F: FnOnce(&mut Child<Self, Product<<Self as ScopeParent>::Timestamp, T>>) -> R,
    {
        self.scoped::<Product<<Self as ScopeParent>::Timestamp, T>, R, F>("Iterative", func)
    }

    /// Creates a dataflow region with the same timestamp.
    ///
    /// This method is a specialization of `scoped` which uses the same timestamp as the
    /// containing scope. It is used mainly to group regions of a dataflow computation, and
    /// provides some computational benefits by abstracting the specifics of the region.
    ///
    /// # Examples
    /// ```
    /// use timely::dataflow::Scope;
    /// use timely::dataflow::operators::{Input, Enter, Leave};
    ///
    /// timely::execute_from_args(std::env::args(), |worker| {
    ///     // must specify types as nothing else drives inference.
    ///     let input = worker.dataflow::<u64,_,_>(|child1| {
    ///         let (input, stream) = child1.new_input::<String>();
    ///         let output = child1.region(|child2| {
    ///             stream.enter(child2).leave()
    ///         });
    ///         input
    ///     });
    /// });
    /// ```
    fn region<R, F>(&mut self, func: F) -> R
    where
        F: FnOnce(&mut Child<Self, <Self as ScopeParent>::Timestamp>) -> R,
    {
        self.region_named("Region", func)
    }

    /// Creates a dataflow region with the same timestamp.
    ///
    /// This method is a specialization of `scoped` which uses the same timestamp as the
    /// containing scope. It is used mainly to group regions of a dataflow computation, and
    /// provides some computational benefits by abstracting the specifics of the region.
    ///
    /// This variant allows you to specify a name for the region, which can be read out in
    /// the timely logging streams.
    ///
    /// # Examples
    /// ```
    /// use timely::dataflow::Scope;
    /// use timely::dataflow::operators::{Input, Enter, Leave};
    ///
    /// timely::execute_from_args(std::env::args(), |worker| {
    ///     // must specify types as nothing else drives inference.
    ///     let input = worker.dataflow::<u64,_,_>(|child1| {
    ///         let (input, stream) = child1.new_input::<String>();
    ///         let output = child1.region_named("region", |child2| {
    ///             stream.enter(child2).leave()
    ///         });
    ///         input
    ///     });
    /// });
    /// ```
    fn region_named<R, F>(&mut self, name: &str, func: F) -> R
    where
        F: FnOnce(&mut Child<Self, <Self as ScopeParent>::Timestamp>) -> R,
    {
        self.scoped::<<Self as ScopeParent>::Timestamp, R, F>(name, func)
    }
}
