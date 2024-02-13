//! Manages pointstamp reachability within a timely dataflow graph.
//!
//! Timely dataflow is concerned with understanding and communicating the potential
//! for capabilities to reach nodes in a directed graph, by following paths through
//! the graph (along edges and through nodes). This module contains one abstraction
//! for managing this information.
//!
//! # Examples
//!
//! ```rust
//! use timely::progress::{Location, Port};
//! use timely::progress::frontier::Antichain;
//! use timely::progress::{Source, Target};
//! use timely::progress::reachability::{Builder, Tracker};
//!
//! // allocate a new empty topology builder.
//! let mut builder = Builder::<usize>::new();
//!
//! // Each node with one input connected to one output.
//! builder.add_node(0, 1, 1, vec![vec![Antichain::from_elem(0)]]);
//! builder.add_node(1, 1, 1, vec![vec![Antichain::from_elem(0)]]);
//! builder.add_node(2, 1, 1, vec![vec![Antichain::from_elem(1)]]);
//!
//! // Connect nodes in sequence, looping around to the first from the last.
//! builder.add_edge(Source::new(0, 0), Target::new(1, 0));
//! builder.add_edge(Source::new(1, 0), Target::new(2, 0));
//! builder.add_edge(Source::new(2, 0), Target::new(0, 0));
//!
//! // Construct a reachability tracker.
//! let (mut tracker, _) = builder.build(None);
//!
//! // Introduce a pointstamp at the output of the first node.
//! tracker.update_source(Source::new(0, 0), 17, 1);
//!
//! // Propagate changes; until this call updates are simply buffered.
//! tracker.propagate_all();
//!
//! let mut results =
//! tracker
//!     .pushed()
//!     .drain()
//!     .filter(|((location, time), delta)| location.is_target())
//!     .collect::<Vec<_>>();
//!
//! results.sort();
//!
//! println!("{:?}", results);
//!
//! assert_eq!(results.len(), 3);
//! assert_eq!(results[0], ((Location::new_target(0, 0), 18), 1));
//! assert_eq!(results[1], ((Location::new_target(1, 0), 17), 1));
//! assert_eq!(results[2], ((Location::new_target(2, 0), 17), 1));
//!
//! // Introduce a pointstamp at the output of the first node.
//! tracker.update_source(Source::new(0, 0), 17, -1);
//!
//! // Propagate changes; until this call updates are simply buffered.
//! tracker.propagate_all();
//!
//! let mut results =
//! tracker
//!     .pushed()
//!     .drain()
//!     .filter(|((location, time), delta)| location.is_target())
//!     .collect::<Vec<_>>();
//!
//! results.sort();
//!
//! assert_eq!(results.len(), 3);
//! assert_eq!(results[0], ((Location::new_target(0, 0), 18), -1));
//! assert_eq!(results[1], ((Location::new_target(1, 0), 17), -1));
//! assert_eq!(results[2], ((Location::new_target(2, 0), 17), -1));
//! ```

use std::collections::{BinaryHeap, HashMap, VecDeque};
use std::cmp::Reverse;

use crate::progress::Timestamp;
use crate::progress::{Source, Target};
use crate::progress::ChangeBatch;
use crate::progress::{Location, Port};

use crate::progress::frontier::{Antichain, MutableAntichain};
use crate::progress::timestamp::PathSummary;


/// A topology builder, which can summarize reachability along paths.
///
/// A `Builder` takes descriptions of the nodes and edges in a graph, and compiles
/// a static summary of the minimal actions a timestamp must endure going from any
/// input or output port to a destination input port.
///
/// A graph is provides as (i) several indexed nodes, each with some number of input
/// and output ports, and each with a summary of the internal paths connecting each
/// input to each output, and (ii) a set of edges connecting output ports to input
/// ports. Edges do not adjust timestamps; only nodes do this.
///
/// The resulting summary describes, for each origin port in the graph and destination
/// input port, a set of incomparable path summaries, each describing what happens to
/// a timestamp as it moves along the path. There may be multiple summaries for each
/// part of origin and destination due to the fact that the actions on timestamps may
/// not be totally ordered (e.g., "increment the timestamp" and "take the maximum of
/// the timestamp and seven").
///
/// # Examples
///
/// ```rust
/// use timely::progress::frontier::Antichain;
/// use timely::progress::{Source, Target};
/// use timely::progress::reachability::Builder;
///
/// // allocate a new empty topology builder.
/// let mut builder = Builder::<usize>::new();
///
/// // Each node with one input connected to one output.
/// builder.add_node(0, 1, 1, vec![vec![Antichain::from_elem(0)]]);
/// builder.add_node(1, 1, 1, vec![vec![Antichain::from_elem(0)]]);
/// builder.add_node(2, 1, 1, vec![vec![Antichain::from_elem(1)]]);
///
/// // Connect nodes in sequence, looping around to the first from the last.
/// builder.add_edge(Source::new(0, 0), Target::new(1, 0));
/// builder.add_edge(Source::new(1, 0), Target::new(2, 0));
/// builder.add_edge(Source::new(2, 0), Target::new(0, 0));
///
/// // Summarize reachability information.
/// let (tracker, _) = builder.build(None);
/// ```
#[derive(Clone, Debug)]
pub struct Builder<T: Timestamp> {
    /// Internal connections within hosted operators.
    ///
    /// Indexed by operator index, then input port, then output port. This is the
    /// same format returned by `get_internal_summary`, as if we simply appended
    /// all of the summaries for the hosted nodes.
    pub nodes: Vec<Vec<Vec<Antichain<T::Summary>>>>,
    /// Direct connections from sources to targets.
    ///
    /// Edges do not affect timestamps, so we only need to know the connectivity.
    /// Indexed by operator index then output port.
    pub edges: Vec<Vec<Vec<Target>>>,
    /// Numbers of inputs and outputs for each node.
    pub shape: Vec<(usize, usize)>,
}

impl<T: Timestamp> Builder<T> {

    /// Create a new empty topology builder.
    pub fn new() -> Self {
        Builder {
            nodes: Vec::new(),
            edges: Vec::new(),
            shape: Vec::new(),
        }
    }

    /// Add links internal to operators.
    ///
    /// This method overwrites any existing summary, instead of anything more sophisticated.
    pub fn add_node(&mut self, index: usize, inputs: usize, outputs: usize, summary: Vec<Vec<Antichain<T::Summary>>>) {

        // Assert that all summaries exist.
        debug_assert_eq!(inputs, summary.len());
        for x in summary.iter() { debug_assert_eq!(outputs, x.len()); }

        while self.nodes.len() <= index {
            self.nodes.push(Vec::new());
            self.edges.push(Vec::new());
            self.shape.push((0, 0));
        }

        self.nodes[index] = summary;
        if self.edges[index].len() != outputs {
            self.edges[index] = vec![Vec::new(); outputs];
        }
        self.shape[index] = (inputs, outputs);
    }

    /// Add links between operators.
    ///
    /// This method does not check that the associated nodes and ports exist. References to
    /// missing nodes or ports are discovered in `build`.
    pub fn add_edge(&mut self, source: Source, target: Target) {

        // Assert that the edge is between existing ports.
        debug_assert!(source.port < self.shape[source.node].1);
        debug_assert!(target.port < self.shape[target.node].0);

        self.edges[source.node][source.port].push(target);
    }

    /// Compiles the current nodes and edges into immutable path summaries.
    ///
    /// This method has the opportunity to perform some error checking that the path summaries
    /// are valid, including references to undefined nodes and ports, as well as self-loops with
    /// default summaries (a serious liveness issue).
    ///
    /// The optional logger information is baked into the resulting tracker.
    pub fn build(self, logger: Option<logging::TrackerLogger>) -> (Tracker<T>, Vec<Vec<Antichain<T::Summary>>>) {

        if !self.is_acyclic() {
            println!("Cycle detected without timestamp increment");
            println!("{:?}", self);
        }

        Tracker::allocate_from(self, logger)
    }

    /// Tests whether the graph a cycle of default path summaries.
    ///
    /// Graphs containing cycles of default path summaries will most likely
    /// not work well with progress tracking, as a timestamp can result in
    /// itself. Such computations can still *run*, but one should not block
    /// on frontier information before yielding results, as you many never
    /// unblock.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use timely::progress::frontier::Antichain;
    /// use timely::progress::{Source, Target};
    /// use timely::progress::reachability::Builder;
    ///
    /// // allocate a new empty topology builder.
    /// let mut builder = Builder::<usize>::new();
    ///
    /// // Each node with one input connected to one output.
    /// builder.add_node(0, 1, 1, vec![vec![Antichain::from_elem(0)]]);
    /// builder.add_node(1, 1, 1, vec![vec![Antichain::from_elem(0)]]);
    /// builder.add_node(2, 1, 1, vec![vec![Antichain::from_elem(0)]]);
    ///
    /// // Connect nodes in sequence, looping around to the first from the last.
    /// builder.add_edge(Source::new(0, 0), Target::new(1, 0));
    /// builder.add_edge(Source::new(1, 0), Target::new(2, 0));
    ///
    /// assert!(builder.is_acyclic());
    ///
    /// builder.add_edge(Source::new(2, 0), Target::new(0, 0));
    ///
    /// assert!(!builder.is_acyclic());
    /// ```
    ///
    /// This test exists because it is possible to describe dataflow graphs that
    /// do not contain non-incrementing cycles, but without feedback nodes that
    /// strictly increment timestamps. For example,
    ///
    /// ```rust
    /// use timely::progress::frontier::Antichain;
    /// use timely::progress::{Source, Target};
    /// use timely::progress::reachability::Builder;
    ///
    /// // allocate a new empty topology builder.
    /// let mut builder = Builder::<usize>::new();
    ///
    /// // Two inputs and outputs, only one of which advances.
    /// builder.add_node(0, 2, 2, vec![
    ///     vec![Antichain::from_elem(0),Antichain::new(),],
    ///     vec![Antichain::new(),Antichain::from_elem(1),],
    /// ]);
    ///
    /// // Connect each output to the opposite input.
    /// builder.add_edge(Source::new(0, 0), Target::new(0, 1));
    /// builder.add_edge(Source::new(0, 1), Target::new(0, 0));
    ///
    /// assert!(builder.is_acyclic());
    /// ```
    pub fn is_acyclic(&self) -> bool {

        let locations = self.shape.iter().map(|(targets, sources)| targets + sources).sum();
        let mut in_degree = HashMap::with_capacity(locations);

        // Load edges as default summaries.
        for (index, ports) in self.edges.iter().enumerate() {
            for (output, targets) in ports.iter().enumerate() {
                let source = Location::new_source(index, output);
                in_degree.entry(source).or_insert(0);
                for &target in targets.iter() {
                    let target = Location::from(target);
                    *in_degree.entry(target).or_insert(0) += 1;
                }
            }
        }

        // Load default intra-node summaries.
        for (index, summary) in self.nodes.iter().enumerate() {
            for (input, outputs) in summary.iter().enumerate() {
                let target = Location::new_target(index, input);
                in_degree.entry(target).or_insert(0);
                for (output, summaries) in outputs.iter().enumerate() {
                    let source = Location::new_source(index, output);
                    for summary in summaries.elements().iter() {
                        if summary == &Default::default() {
                            *in_degree.entry(source).or_insert(0) += 1;
                        }
                    }
                }
            }
        }

        // A worklist of nodes that cannot be reached from the whole graph.
        // Initially this list contains observed locations with no incoming
        // edges, but as the algorithm develops we add to it any locations
        // that can only be reached by nodes that have been on this list.
        let mut worklist = Vec::with_capacity(in_degree.len());
        for (key, val) in in_degree.iter() {
            if *val == 0 {
                worklist.push(*key);
            }
        }
        in_degree.retain(|_key, val| val != &0);

        // Repeatedly remove nodes and update adjacent in-edges.
        while let Some(Location { node, port }) = worklist.pop() {
            match port {
                Port::Source(port) => {
                    for target in self.edges[node][port].iter() {
                        let target = Location::from(*target);
                        *in_degree.get_mut(&target).unwrap() -= 1;
                        if in_degree[&target] == 0 {
                            in_degree.remove(&target);
                            worklist.push(target);
                        }
                    }
                },
                Port::Target(port) => {
                    for (output, summaries) in self.nodes[node][port].iter().enumerate() {
                        let source = Location::new_source(node, output);
                        for summary in summaries.elements().iter() {
                            if summary == &Default::default() {
                                *in_degree.get_mut(&source).unwrap() -= 1;
                                if in_degree[&source] == 0 {
                                    in_degree.remove(&source);
                                    worklist.push(source);
                                }
                            }
                        }
                    }
                },
            }
        }

        // Acyclic graphs should reduce to empty collections.
        in_degree.is_empty()
    }
}

impl<T: Timestamp> Default for Builder<T> {
    fn default() -> Self {
        Self::new()
    }
}

/// An interactive tracker of propagated reachability information.
///
/// A `Tracker` tracks, for a fixed graph topology, the implications of
/// pointstamp changes at various node input and output ports. These changes may
/// alter the potential pointstamps that could arrive at downstream input ports.
pub struct Tracker<T:Timestamp> {

    /// Internal connections within hosted operators.
    ///
    /// Indexed by operator index, then input port, then output port. This is the
    /// same format returned by `get_internal_summary`, as if we simply appended
    /// all of the summaries for the hosted nodes.
    nodes: Vec<Vec<Vec<Antichain<T::Summary>>>>,
    /// Direct connections from sources to targets.
    ///
    /// Edges do not affect timestamps, so we only need to know the connectivity.
    /// Indexed by operator index then output port.
    edges: Vec<Vec<Vec<Target>>>,

    // TODO: All of the sizes of these allocations are static (except internal to `ChangeBatch`).
    //       It seems we should be able to flatten most of these so that there are a few allocations
    //       independent of the numbers of nodes and ports and such.
    //
    // TODO: We could also change the internal representation to be a graph of targets, using usize
    //       identifiers for each, so that internally we needn't use multiple levels of indirection.
    //       This may make more sense once we commit to topologically ordering the targets.

    /// Each source and target has a mutable antichain to ensure that we track their discrete frontiers,
    /// rather than their multiplicities. We separately track the frontiers resulting from propagated
    /// frontiers, to protect them from transient negativity in inbound target updates.
    per_operator: Vec<PerOperator<T>>,

    /// Source and target changes are buffered, which allows us to delay processing until propagation,
    /// and so consolidate updates, but to leap directly to those frontiers that may have changed.
    target_changes: ChangeBatch<(Target, T)>,
    source_changes: ChangeBatch<(Source, T)>,

    /// Worklist of updates to perform, ordered by increasing timestamp and target.
    worklist: BinaryHeap<Reverse<(T, Location, i64)>>,

    /// Buffer of consequent changes.
    pushed_changes: ChangeBatch<(Location, T)>,

    /// Compiled summaries from each internal location (not scope inputs) to each scope output.
    output_changes: Vec<ChangeBatch<T>>,

    /// A non-negative sum of post-filtration input changes.
    ///
    /// This sum should be zero exactly when the accumulated input changes are zero,
    /// indicating that the progress tracker is currently tracking nothing. It should
    /// always be exactly equal to the sum across all operators of the frontier sizes
    /// of the target and source `pointstamps` member.
    total_counts: i64,

    /// Optionally, a unique logging identifier and logging for tracking events.
    logger: Option<logging::TrackerLogger>,
}

/// Target and source information for each operator.
pub struct PerOperator<T: Timestamp> {
    /// Port information for each target.
    pub targets: Vec<PortInformation<T>>,
    /// Port information for each source.
    pub sources: Vec<PortInformation<T>>,
}

impl<T: Timestamp> PerOperator<T> {
    /// A new PerOperator bundle from numbers of input and output ports.
    pub fn new(inputs: usize, outputs: usize) -> Self {
        PerOperator {
            targets: vec![PortInformation::new(); inputs],
            sources: vec![PortInformation::new(); outputs],
        }
    }
}

/// Per-port progress-tracking information.
#[derive(Clone)]
pub struct PortInformation<T: Timestamp> {
    /// Current counts of active pointstamps.
    pub pointstamps: MutableAntichain<T>,
    /// Current implications of active pointstamps across the dataflow.
    pub implications: MutableAntichain<T>,
    /// Path summaries to each of the scope outputs.
    pub output_summaries: Vec<Antichain<T::Summary>>,
}

impl<T: Timestamp> PortInformation<T> {
    /// Creates empty port information.
    pub fn new() -> Self {
        PortInformation {
            pointstamps: MutableAntichain::new(),
            implications: MutableAntichain::new(),
            output_summaries: Vec::new(),
        }
    }

    /// True if updates at this pointstamp uniquely block progress.
    ///
    /// This method returns true if the currently maintained pointstamp
    /// counts are such that zeroing out outstanding updates at *this*
    /// pointstamp would change the frontiers at this operator. When the
    /// method returns false it means that, temporarily at least, there
    /// are outstanding pointstamp updates that are strictly less than
    /// this pointstamp.
    #[inline]
    pub fn is_global(&self, time: &T) -> bool {
        let dominated = self.implications.frontier().iter().any(|t| t.less_than(time));
        let redundant = self.implications.count_for(time) > 1;
        !dominated && !redundant
    }
}

impl<T: Timestamp> Default for PortInformation<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T:Timestamp> Tracker<T> {

    /// Updates the count for a time at a location.
    #[inline]
    pub fn update(&mut self, location: Location, time: T, value: i64) {
        match location.port {
            Port::Target(port) => self.update_target(Target::new(location.node, port), time, value),
            Port::Source(port) => self.update_source(Source::new(location.node, port), time, value),
        };
    }

    /// Updates the count for a time at a target (operator input, scope output).
    #[inline]
    pub fn update_target(&mut self, target: Target, time: T, value: i64) {
        self.target_changes.update((target, time), value);
    }
    /// Updates the count for a time at a source (operator output, scope input).
    #[inline]
    pub fn update_source(&mut self, source: Source, time: T, value: i64) {
        self.source_changes.update((source, time), value);
    }

    /// Indicates if any pointstamps have positive count.
    pub fn tracking_anything(&mut self) -> bool {
        !self.source_changes.is_empty() ||
        !self.target_changes.is_empty() ||
        self.total_counts > 0
    }

    /// Allocate a new `Tracker` using the shape from `summaries`.
    ///
    /// The result is a pair of tracker, and the summaries from each input port to each
    /// output port.
    ///
    /// If the optional logger is provided, it will be used to log various tracker events.
    pub fn allocate_from(builder: Builder<T>, logger: Option<logging::TrackerLogger>) -> (Self, Vec<Vec<Antichain<T::Summary>>>) {

        // Allocate buffer space for each input and input port.
        let mut per_operator =
        builder
            .shape
            .iter()
            .map(|&(inputs, outputs)| PerOperator::new(inputs, outputs))
            .collect::<Vec<_>>();

        // Summary of scope inputs to scope outputs.
        let mut builder_summary = vec![vec![]; builder.shape[0].1];

        // Compile summaries from each location to each scope output.
        let output_summaries = summarize_outputs::<T>(&builder.nodes, &builder.edges);
        for (location, summaries) in output_summaries.into_iter() {
            // Summaries from scope inputs are useful in summarizing the scope.
            if location.node == 0 {
                if let Port::Source(port) = location.port {
                    builder_summary[port] = summaries;
                }
                else {
                    // Ignore (ideally trivial) output to output summaries.
                }
            }
            // Summaries from internal nodes are important for projecting capabilities.
            else {
                match location.port {
                    Port::Target(port) => {
                        per_operator[location.node].targets[port].output_summaries = summaries;
                    },
                    Port::Source(port) => {
                        per_operator[location.node].sources[port].output_summaries = summaries;
                    },
                }
            }
        }

        let scope_outputs = builder.shape[0].0;
        let output_changes = vec![ChangeBatch::new(); scope_outputs];

        let tracker =
        Tracker {
            nodes: builder.nodes,
            edges: builder.edges,
            per_operator,
            target_changes: ChangeBatch::new(),
            source_changes: ChangeBatch::new(),
            worklist: BinaryHeap::new(),
            pushed_changes: ChangeBatch::new(),
            output_changes,
            total_counts: 0,
            logger,
        };

        (tracker, builder_summary)
    }

    /// Propagates all pending updates.
    ///
    /// The method drains `self.input_changes` and circulates their implications
    /// until we cease deriving new implications.
    pub fn propagate_all(&mut self) {

        // Step 0: If logging is enabled, construct and log inbound changes.
        if let Some(logger) = &mut self.logger {

            let target_changes =
            self.target_changes
                .iter()
                .map(|((target, time), diff)| (target.node, target.port, time.clone(), *diff))
                .collect::<Vec<_>>();

            if !target_changes.is_empty() {
                logger.log_target_updates(Box::new(target_changes));
            }

            let source_changes =
            self.source_changes
                .iter()
                .map(|((source, time), diff)| (source.node, source.port, time.clone(), *diff))
                .collect::<Vec<_>>();

            if !source_changes.is_empty() {
                logger.log_source_updates(Box::new(source_changes));
            }
        }

        // Step 1: Drain `self.input_changes` and determine actual frontier changes.
        //
        // Not all changes in `self.input_changes` may alter the frontier at a location.
        // By filtering the changes through `self.pointstamps` we react only to discrete
        // changes in the frontier, rather than changes in the pointstamp counts that
        // witness that frontier.
        for ((target, time), diff) in self.target_changes.drain() {

            let operator = &mut self.per_operator[target.node].targets[target.port];
            let changes = operator.pointstamps.update_iter(Some((time, diff)));

            for (time, diff) in changes {
                self.total_counts += diff;
                for (output, summaries) in operator.output_summaries.iter().enumerate() {
                    let output_changes = &mut self.output_changes[output];
                    summaries
                        .elements()
                        .iter()
                        .flat_map(|summary| summary.results_in(&time))
                        .for_each(|out_time| output_changes.update(out_time, diff));
                }
                self.worklist.push(Reverse((time, Location::from(target), diff)));
            }
        }

        for ((source, time), diff) in self.source_changes.drain() {

            let operator = &mut self.per_operator[source.node].sources[source.port];
            let changes = operator.pointstamps.update_iter(Some((time, diff)));

            for (time, diff) in changes {
                self.total_counts += diff;
                for (output, summaries) in operator.output_summaries.iter().enumerate() {
                    let output_changes = &mut self.output_changes[output];
                    summaries
                        .elements()
                        .iter()
                        .flat_map(|summary| summary.results_in(&time))
                        .for_each(|out_time| output_changes.update(out_time, diff));
                }
                self.worklist.push(Reverse((time, Location::from(source), diff)));
            }
        }

        // Step 2: Circulate implications of changes to `self.pointstamps`.
        //
        // TODO: The argument that this always terminates is subtle, and should be made.
        //       The intent is that that by moving forward in layers through `time`, we
        //       will discover zero-change times when we first visit them, as no further
        //       changes can be made to them once we complete them.
        while let Some(Reverse((time, location, mut diff))) = self.worklist.pop() {

            // Drain and accumulate all updates that have the same time and location.
            while self.worklist.peek().map(|x| ((x.0).0 == time) && ((x.0).1 == location)).unwrap_or(false) {
                diff += (self.worklist.pop().unwrap().0).2;
            }

            // Only act if there is a net change, positive or negative.
            if diff != 0 {

                match location.port {
                    // Update to an operator input.
                    // Propagate any changes forward across the operator.
                    Port::Target(port_index) => {

                        let changes =
                        self.per_operator[location.node]
                            .targets[port_index]
                            .implications
                            .update_iter(Some((time, diff)));

                        for (time, diff) in changes {
                            let nodes = &self.nodes[location.node][port_index];
                            for (output_port, summaries) in nodes.iter().enumerate() {
                                let source = Location { node: location.node, port: Port::Source(output_port) };
                                for summary in summaries.elements().iter() {
                                    if let Some(new_time) = summary.results_in(&time) {
                                        self.worklist.push(Reverse((new_time, source, diff)));
                                    }
                                }
                            }
                            self.pushed_changes.update((location, time), diff);
                        }
                    }
                    // Update to an operator output.
                    // Propagate any changes forward along outgoing edges.
                    Port::Source(port_index) => {

                        let changes =
                        self.per_operator[location.node]
                            .sources[port_index]
                            .implications
                            .update_iter(Some((time, diff)));

                        for (time, diff) in changes {
                            for new_target in self.edges[location.node][port_index].iter() {
                                self.worklist.push(Reverse((
                                    time.clone(),
                                    Location::from(*new_target),
                                    diff,
                                )));
                            }
                            self.pushed_changes.update((location, time), diff);
                        }
                    },
                };
            }
        }
    }

    /// Implications of maintained capabilities projected to each output.
    pub fn pushed_output(&mut self) -> &mut [ChangeBatch<T>] {
        &mut self.output_changes[..]
    }

    /// A mutable reference to the pushed results of changes.
    pub fn pushed(&mut self) -> &mut ChangeBatch<(Location, T)> {
        &mut self.pushed_changes
    }

    /// Reveals per-operator frontier state.
    pub fn node_state(&self, index: usize) -> &PerOperator<T> {
        &self.per_operator[index]
    }

    /// Indicates if pointstamp is in the scope-wide frontier.
    ///
    /// Such a pointstamp would, if removed from `self.pointstamps`, cause a change
    /// to `self.implications`, which is what we track for per operator input frontiers.
    /// If the above do not hold, then its removal either 1. shouldn't be possible,
    /// or 2. will not affect the output of `self.implications`.
    pub fn is_global(&self, location: Location, time: &T) -> bool {
        match location.port {
            Port::Target(port) => self.per_operator[location.node].targets[port].is_global(time),
            Port::Source(port) => self.per_operator[location.node].sources[port].is_global(time),
        }
    }
}

/// Determines summaries from locations to scope outputs.
///
/// Specifically, for each location whose node identifier is non-zero, we compile
/// the summaries along which they can reach each output.
///
/// Graph locations may be missing from the output, in which case they have no
/// paths to scope outputs.
fn summarize_outputs<T: Timestamp>(
    nodes: &Vec<Vec<Vec<Antichain<T::Summary>>>>,
    edges: &Vec<Vec<Vec<Target>>>,
    ) -> HashMap<Location, Vec<Antichain<T::Summary>>>
{
    // A reverse edge map, to allow us to walk back up the dataflow graph.
    let mut reverse = HashMap::new();
    for (node, outputs) in edges.iter().enumerate() {
        for (output, targets) in outputs.iter().enumerate() {
            for target in targets.iter() {
                reverse.insert(
                    Location::from(*target),
                    Location { node, port: Port::Source(output) }
                );
            }
        }
    }

    let mut results: HashMap<Location, Vec<Antichain<T::Summary>>> = HashMap::new();
    let mut worklist = VecDeque::<(Location, usize, T::Summary)>::new();

    let outputs =
    edges
        .iter()
        .flat_map(|x| x.iter())
        .flat_map(|x| x.iter())
        .filter(|target| target.node == 0);

    // The scope may have no outputs, in which case we can do no work.
    for output_target in outputs {
        worklist.push_back((Location::from(*output_target), output_target.port, Default::default()));
    }

    // Loop until we stop discovering novel reachability paths.
    while let Some((location, output, summary)) = worklist.pop_front() {

        match location.port {

            // This is an output port of an operator, or a scope input.
            // We want to crawl up the operator, to its inputs.
            Port::Source(output_port) => {

                // Consider each input port of the associated operator.
                for (input_port, summaries) in nodes[location.node].iter().enumerate() {

                    // Determine the current path summaries from the input port.
                    let location = Location { node: location.node, port: Port::Target(input_port) };
                    let antichains = results
                        .entry(location)
                        .and_modify(|antichains| antichains.reserve(output))
                        .or_insert_with(|| Vec::with_capacity(output));

                    while antichains.len() <= output { antichains.push(Antichain::new()); }

                    // Combine each operator-internal summary to the output with `summary`.
                    for operator_summary in summaries[output_port].elements().iter() {
                        if let Some(combined) = operator_summary.followed_by(&summary) {
                            if antichains[output].insert(combined.clone()) {
                                worklist.push_back((location, output, combined));
                            }
                        }
                    }
                }

            },

            // This is an input port of an operator, or a scope output.
            // We want to walk back the edges leading to it.
            Port::Target(_port) => {

                // Each target should have (at most) one source.
                if let Some(&source) = reverse.get(&location) {
                    let antichains = results
                        .entry(source)
                        .and_modify(|antichains| antichains.reserve(output))
                        .or_insert_with(|| Vec::with_capacity(output));

                    while antichains.len() <= output { antichains.push(Antichain::new()); }

                    if antichains[output].insert(summary.clone()) {
                        worklist.push_back((source, output, summary.clone()));
                    }
                }

            },
        }

    }

    results
}

/// Logging types for reachability tracking events.
pub mod logging {

    use crate::logging::{Logger, ProgressEventTimestampVec};

    /// A logger with additional identifying information about the tracker.
    pub struct TrackerLogger {
        path: Vec<usize>,
        logger: Logger<TrackerEvent>,
    }

    impl TrackerLogger {
        /// Create a new tracker logger from its fields.
        pub fn new(path: Vec<usize>, logger: Logger<TrackerEvent>) -> Self {
            Self { path, logger }
        }

        /// Log source update events with additional identifying information.
        pub fn log_source_updates(&mut self, updates: Box<dyn ProgressEventTimestampVec>) {
            self.logger.log({
                SourceUpdate {
                    tracker_id: self.path.clone(),
                    updates,
                }
            })
        }
        /// Log target update events with additional identifying information.
        pub fn log_target_updates(&mut self, updates: Box<dyn ProgressEventTimestampVec>) {
            self.logger.log({
                TargetUpdate {
                    tracker_id: self.path.clone(),
                    updates,
                }
            })
        }
    }

    /// Events that the tracker may record.
    pub enum TrackerEvent {
        /// Updates made at a source of data.
        SourceUpdate(SourceUpdate),
        /// Updates made at a target of data.
        TargetUpdate(TargetUpdate),
    }

    /// An update made at a source of data.
    pub struct SourceUpdate {
        /// An identifier for the tracker.
        pub tracker_id: Vec<usize>,
        /// Updates themselves, as `(node, port, time, diff)`.
        pub updates: Box<dyn ProgressEventTimestampVec>,
    }

    /// An update made at a target of data.
    pub struct TargetUpdate {
        /// An identifier for the tracker.
        pub tracker_id: Vec<usize>,
        /// Updates themselves, as `(node, port, time, diff)`.
        pub updates: Box<dyn ProgressEventTimestampVec>,
    }

    impl From<SourceUpdate> for TrackerEvent {
        fn from(v: SourceUpdate) -> TrackerEvent { TrackerEvent::SourceUpdate(v) }
    }

    impl From<TargetUpdate> for TrackerEvent {
        fn from(v: TargetUpdate) -> TrackerEvent { TrackerEvent::TargetUpdate(v) }
    }
}

// The Drop implementation for `Tracker` makes sure that reachability logging is correct for
// prematurely dropped dataflows. At the moment, this is only possible through `drop_dataflow`,
// because in all other cases the tracker stays alive while it has outstanding work, leaving no
// remaining work for this Drop implementation.
impl<T: Timestamp> Drop for Tracker<T> {
    fn drop(&mut self) {
        let logger = if let Some(logger) = &mut self.logger {
            logger
        } else {
            // No cleanup necessary when there is no logger.
            return;
        };

        // Retract pending data that `propagate_all` would normally log.
        for (index, per_operator) in self.per_operator.iter_mut().enumerate() {
            let target_changes = per_operator.targets
                .iter_mut()
                .enumerate()
                .flat_map(|(port, target)| {
                    target.pointstamps
                        .updates()
                        .map(move |(time, diff)| (index, port, time.clone(), -diff))
                })
                .collect::<Vec<_>>();
            if !target_changes.is_empty() {
                logger.log_target_updates(Box::new(target_changes));
            }

            let source_changes = per_operator.sources
                .iter_mut()
                .enumerate()
                .flat_map(|(port, source)| {
                    source.pointstamps
                        .updates()
                        .map(move |(time, diff)| (index, port, time.clone(), -diff))
                })
                .collect::<Vec<_>>();
            if !source_changes.is_empty() {
                logger.log_source_updates(Box::new(source_changes));
            }
        }
    }
}
