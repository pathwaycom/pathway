//! The root of each single-threaded worker.

use std::rc::Rc;
use std::cell::{RefCell, RefMut};
use std::any::Any;
use std::str::FromStr;
use std::time::{Instant, Duration};
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::Arc;

use crate::communication::{Allocate, Data, Push, Pull};
use crate::communication::allocator::thread::{ThreadPusher, ThreadPuller};
use crate::scheduling::{Schedule, Scheduler, Activations};
use crate::progress::timestamp::{Refines};
use crate::progress::SubgraphBuilder;
use crate::progress::operate::Operate;
use crate::dataflow::scopes::Child;
use crate::logging::TimelyLogger;

/// Different ways in which timely's progress tracking can work.
///
/// These options drive some buffering and accumulation that timely
/// can do to try and trade volume of progress traffic against latency.
/// By accumulating updates longer, a smaller total volume of messages
/// are sent.
///
/// The `ProgressMode::Demand` variant is the most robust, and least
/// likely to lead to catastrophic performance. The `Eager` variant
/// is useful for getting the smallest latencies on systems with few
/// workers, but does risk saturating the system with progress messages
/// and should be used with care, or not at all.
///
/// If you are not certain which option to use, prefer `Demand`, and
/// perhaps monitor the progress messages through timely's logging
/// infrastructure to see if their volume is surprisingly high.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum ProgressMode {
    /// Eagerly transmit all progress updates produced by a worker.
    ///
    /// Progress messages are transmitted without consideration for the
    /// possibility that they may unblock other workers. This can result
    /// in a substantial volume of messages that do not result in a
    /// change to the lower bound of outstanding work.
    Eager,
    /// Delay transmission of progress updates until any could advance
    /// the global frontier of timestamps.
    ///
    /// As timely executes, the progress messages inform each worker of
    /// the outstanding work remaining in the system. As workers work,
    /// they produce changes to this outstanding work. This option
    /// delays the communication of those changes until they might
    /// possibly cause a change in the lower bound of all outstanding
    /// work.
    ///
    /// The most common case this remedies is when one worker transmits
    /// messages to other workers, that worker holds a capability for the
    /// operator and timestamp. Other workers will receive messages, and
    /// with this option will not immediately acknowledge receiving the
    /// messages, because the held capability is strictly prior to what
    /// the messages can affect. Once the capability is released, the
    /// progress messages are unblocked and transmitted, in accumulated
    /// form.
    Demand,
}

impl Default for ProgressMode {
    fn default() -> ProgressMode {
        ProgressMode::Demand
    }
}

impl FromStr for ProgressMode {
    type Err = String;

    fn from_str(s: &str) -> Result<ProgressMode, String> {
        match s {
            "eager" => Ok(ProgressMode::Eager),
            "demand" => Ok(ProgressMode::Demand),
            _ => Err(format!("unknown progress mode: {}", s)),
        }
    }
}

/// Worker configuration.
#[derive(Debug, Default, Clone)]
pub struct Config {
    /// The progress mode to use.
    pub(crate) progress_mode: ProgressMode,
    /// A map from parameter name to typed parameter values.
    registry: HashMap<String, Arc<dyn Any + Send + Sync>>,
}

impl Config {
    /// Installs options into a [getopts_dep::Options] struct that correspond
    /// to the parameters in the configuration.
    ///
    /// It is the caller's responsibility to ensure that the installed options
    /// do not conflict with any other options that may exist in `opts`, or
    /// that may be installed into `opts` in the future.
    ///
    /// This method is only available if the `getopts` feature is enabled, which
    /// it is by default.
    #[cfg(feature = "getopts")]
    pub fn install_options(opts: &mut getopts_dep::Options) {
        opts.optopt("", "progress-mode", "progress tracking mode (eager or demand)", "MODE");
    }

    /// Instantiates a configuration based upon the parsed options in `matches`.
    ///
    /// The `matches` object must have been constructed from a
    /// [getopts_dep::Options] which contained at least the options installed by
    /// [Self::install_options].
    ///
    /// This method is only available if the `getopts` feature is enabled, which
    /// it is by default.
    #[cfg(feature = "getopts")]
    pub fn from_matches(matches: &getopts_dep::Matches) -> Result<Config, String> {
        let progress_mode = matches
            .opt_get_default("progress-mode", ProgressMode::Eager)?;
        Ok(Config::default().progress_mode(progress_mode))
    }

    /// Sets the progress mode to `progress_mode`.
    pub fn progress_mode(mut self, progress_mode: ProgressMode) -> Self {
        self.progress_mode = progress_mode;
        self
    }

    /// Sets a typed configuration parameter for the given `key`.
    ///
    /// It is recommended to install a single configuration struct using a key
    /// that uniquely identifies your project, to avoid clashes. For example,
    /// differential dataflow registers a configuration struct under the key
    /// "differential".
    ///
    /// # Examples
    /// ```rust
    /// let mut config = timely::Config::process(3);
    /// config.worker.set("example".to_string(), 7u64);
    /// timely::execute(config, |worker| {
    ///    use crate::timely::worker::AsWorker;
    ///    assert_eq!(worker.config().get::<u64>("example"), Some(&7));
    /// }).unwrap();
    /// ```
    pub fn set<T>(&mut self, key: String, val: T) -> &mut Self
    where
        T: Send + Sync + 'static,
    {
        self.registry.insert(key, Arc::new(val));
        self
    }

    /// Gets the value for configured parameter `key`.
    ///
    /// Returns `None` if `key` has not previously been set with
    /// [Config::set], or if the specified `T` does not match the `T`
    /// from the call to `set`.
    ///
    /// # Examples
    /// ```rust
    /// let mut config = timely::Config::process(3);
    /// config.worker.set("example".to_string(), 7u64);
    /// timely::execute(config, |worker| {
    ///    use crate::timely::worker::AsWorker;
    ///    assert_eq!(worker.config().get::<u64>("example"), Some(&7));
    /// }).unwrap();
    /// ```
    pub fn get<T: 'static>(&self, key: &str) -> Option<&T> {
        self.registry.get(key).and_then(|val| val.downcast_ref())
    }
}

/// Methods provided by the root Worker.
///
/// These methods are often proxied by child scopes, and this trait provides access.
pub trait AsWorker : Scheduler {
    /// Returns the worker configuration parameters.
    fn config(&self) -> &Config;
    /// Index of the worker among its peers.
    fn index(&self) -> usize;
    /// Number of peer workers.
    fn peers(&self) -> usize;
    /// Allocates a new channel from a supplied identifier and address.
    ///
    /// The identifier is used to identify the underlying channel and route
    /// its data. It should be distinct from other identifiers passed used
    /// for allocation, but can otherwise be arbitrary.
    ///
    /// The address should specify a path to an operator that should be
    /// scheduled in response to the receipt of records on the channel.
    /// Most commonly, this would be the address of the *target* of the
    /// channel.
    fn allocate<T: Data>(&mut self, identifier: usize, address: &[usize]) -> (Vec<Box<dyn Push<Message<T>>>>, Box<dyn Pull<Message<T>>>);
    /// Constructs a pipeline channel from the worker to itself.
    ///
    /// By default this method uses the native channel allocation mechanism, but the expectation is
    /// that this behavior will be overriden to be more efficient.
    fn pipeline<T: 'static>(&mut self, identifier: usize, address: &[usize]) -> (ThreadPusher<Message<T>>, ThreadPuller<Message<T>>);

    /// Allocates a new worker-unique identifier.
    fn new_identifier(&mut self) -> usize;
    /// Provides access to named logging streams.
    fn log_register(&self) -> ::std::cell::RefMut<crate::logging_core::Registry<crate::logging::WorkerIdentifier>>;
    /// Provides access to the timely logging stream.
    fn logging(&self) -> Option<crate::logging::TimelyLogger> { self.log_register().get("timely") }
}

/// A `Worker` is the entry point to a timely dataflow computation. It wraps a `Allocate`,
/// and has a list of dataflows that it manages.
pub struct Worker<A: Allocate> {
    config: Config,
    timer: Instant,
    paths: Rc<RefCell<HashMap<usize, Vec<usize>>>>,
    allocator: Rc<RefCell<A>>,
    identifiers: Rc<RefCell<usize>>,
    // dataflows: Rc<RefCell<Vec<Wrapper>>>,
    dataflows: Rc<RefCell<HashMap<usize, Wrapper>>>,
    dataflow_counter: Rc<RefCell<usize>>,
    logging: Rc<RefCell<crate::logging_core::Registry<crate::logging::WorkerIdentifier>>>,

    activations: Rc<RefCell<Activations>>,
    active_dataflows: Vec<usize>,

    // Temporary storage for channel identifiers during dataflow construction.
    // These are then associated with a dataflow once constructed.
    temp_channel_ids: Rc<RefCell<Vec<usize>>>,
}

impl<A: Allocate> AsWorker for Worker<A> {
    fn config(&self) -> &Config { &self.config }
    fn index(&self) -> usize { self.allocator.borrow().index() }
    fn peers(&self) -> usize { self.allocator.borrow().peers() }
    fn allocate<D: Data>(&mut self, identifier: usize, address: &[usize]) -> (Vec<Box<dyn Push<Message<D>>>>, Box<dyn Pull<Message<D>>>) {
        if address.is_empty() { panic!("Unacceptable address: Length zero"); }
        let mut paths = self.paths.borrow_mut();
        paths.insert(identifier, address.to_vec());
        self.temp_channel_ids.borrow_mut().push(identifier);
        self.allocator.borrow_mut().allocate(identifier)
    }
    fn pipeline<T: 'static>(&mut self, identifier: usize, address: &[usize]) -> (ThreadPusher<Message<T>>, ThreadPuller<Message<T>>) {
        if address.is_empty() { panic!("Unacceptable address: Length zero"); }
        let mut paths = self.paths.borrow_mut();
        paths.insert(identifier, address.to_vec());
        self.temp_channel_ids.borrow_mut().push(identifier);
        self.allocator.borrow_mut().pipeline(identifier)
    }

    fn new_identifier(&mut self) -> usize { self.new_identifier() }
    fn log_register(&self) -> RefMut<crate::logging_core::Registry<crate::logging::WorkerIdentifier>> {
        self.log_register()
    }
}

impl<A: Allocate> Scheduler for Worker<A> {
    fn activations(&self) -> Rc<RefCell<Activations>> {
        self.activations.clone()
    }
}

impl<A: Allocate> Worker<A> {
    /// Allocates a new `Worker` bound to a channel allocator.
    pub fn new(config: Config, c: A) -> Worker<A> {
        let now = Instant::now();
        let index = c.index();
        Worker {
            config,
            timer: now,
            paths:  Default::default(),
            allocator: Rc::new(RefCell::new(c)),
            identifiers:  Default::default(),
            dataflows: Default::default(),
            dataflow_counter:  Default::default(),
            logging: Rc::new(RefCell::new(crate::logging_core::Registry::new(now, index))),
            activations: Rc::new(RefCell::new(Activations::new(now))),
            active_dataflows: Default::default(),
            temp_channel_ids:  Default::default(),
        }
    }

    /// Performs one step of the computation.
    ///
    /// A step gives each dataflow operator a chance to run, and is the
    /// main way to ensure that a computation proceeds.
    ///
    /// # Examples
    ///
    /// ```
    /// timely::execute_from_args(::std::env::args(), |worker| {
    ///
    ///     use timely::dataflow::operators::{ToStream, Inspect};
    ///
    ///     worker.dataflow::<usize,_,_>(|scope| {
    ///         (0 .. 10)
    ///             .to_stream(scope)
    ///             .inspect(|x| println!("{:?}", x));
    ///     });
    ///
    ///     worker.step();
    /// });
    /// ```
    pub fn step(&mut self) -> bool {
        self.step_or_park(Some(Duration::from_secs(0)))
    }

    /// Performs one step of the computation.
    ///
    /// A step gives each dataflow operator a chance to run, and is the
    /// main way to ensure that a computation proceeds.
    ///
    /// This method takes an optional timeout and may park the thread until
    /// there is work to perform or until this timeout expires. A value of
    /// `None` allows the worker to park indefinitely, whereas a value of
    /// `Some(Duration::new(0, 0))` will return without parking the thread.
    ///
    /// # Examples
    ///
    /// ```
    /// timely::execute_from_args(::std::env::args(), |worker| {
    ///
    ///     use std::time::Duration;
    ///     use timely::dataflow::operators::{ToStream, Inspect};
    ///
    ///     worker.dataflow::<usize,_,_>(|scope| {
    ///         (0 .. 10)
    ///             .to_stream(scope)
    ///             .inspect(|x| println!("{:?}", x));
    ///     });
    ///
    ///     worker.step_or_park(Some(Duration::from_secs(1)));
    /// });
    /// ```
    pub fn step_or_park(&mut self, duration: Option<Duration>) -> bool {

        {   // Process channel events. Activate responders.
            let mut allocator = self.allocator.borrow_mut();
            allocator.receive();
            let events = allocator.events().clone();
            let mut borrow = events.borrow_mut();
            let paths = self.paths.borrow();
            borrow.sort_unstable();
            borrow.dedup();
            for channel in borrow.drain(..) {
                // Consider tracking whether a channel
                // in non-empty, and only activating
                // on the basis of non-empty channels.
                // TODO: This is a sloppy way to deal
                // with channels that may not be alloc'd.
                if let Some(path) = paths.get(&channel) {
                    self.activations
                        .borrow_mut()
                        .activate(&path[..]);
                }
            }
        }

        // Organize activations.
        self.activations
            .borrow_mut()
            .advance();

        // Consider parking only if we have no pending events, some dataflows, and a non-zero duration.
        let empty_for = self.activations.borrow().empty_for();
        // Determine the minimum park duration, where `None` are an absence of a constraint.
        let delay = match (duration, empty_for) {
            (Some(x), Some(y)) => Some(std::cmp::min(x,y)),
            (x, y) => x.or(y),
        };

        if delay != Some(Duration::new(0,0)) {

            // Log parking and flush log.
            if let Some(l) = self.logging().as_mut() {
                l.log(crate::logging::ParkEvent::park(delay));
                l.flush();
            }

            self.allocator
                .borrow()
                .await_events(delay);

            // Log return from unpark.
            self.logging().as_mut().map(|l| l.log(crate::logging::ParkEvent::unpark()));
        }
        else {   // Schedule active dataflows.

            let active_dataflows = &mut self.active_dataflows;
            self.activations
                .borrow_mut()
                .for_extensions(&[], |index| active_dataflows.push(index));

            let mut dataflows = self.dataflows.borrow_mut();
            for index in active_dataflows.drain(..) {
                // Step dataflow if it exists, remove if not incomplete.
                if let Entry::Occupied(mut entry) = dataflows.entry(index) {
                    // TODO: This is a moment at which a scheduling decision is being made.
                    let incomplete = entry.get_mut().step();
                    if !incomplete {
                        let mut paths = self.paths.borrow_mut();
                        for channel in entry.get_mut().channel_ids.drain(..) {
                            paths.remove(&channel);
                        }
                        entry.remove_entry();
                    }
                }
            }
        }

        // Clean up, indicate if dataflows remain.
        self.logging.borrow_mut().flush();
        self.allocator.borrow_mut().release();
        !self.dataflows.borrow().is_empty()
    }

    /// Calls `self.step()` as long as `func` evaluates to true.
    ///
    /// This method will continually execute even if there is not work
    /// for the worker to perform. Consider using the similar method
    /// `Self::step_or_park_while(duration)` to allow the worker to yield
    /// control if that is appropriate.
    ///
    /// # Examples
    ///
    /// ```
    /// timely::execute_from_args(::std::env::args(), |worker| {
    ///
    ///     use timely::dataflow::operators::{ToStream, Inspect, Probe};
    ///
    ///     let probe =
    ///     worker.dataflow::<usize,_,_>(|scope| {
    ///         (0 .. 10)
    ///             .to_stream(scope)
    ///             .inspect(|x| println!("{:?}", x))
    ///             .probe()
    ///     });
    ///
    ///     worker.step_while(|| probe.less_than(&0));
    /// });
    /// ```
    pub fn step_while<F: FnMut()->bool>(&mut self, func: F) {
        self.step_or_park_while(Some(Duration::from_secs(0)), func)
    }

    /// Calls `self.step_or_park(duration)` as long as `func` evaluates to true.
    ///
    /// This method may yield whenever there is no work to perform, as performed
    /// by `Self::step_or_park()`. Please consult the documentation for further
    /// information about that method and its behavior. In particular, the method
    /// can park the worker indefinitely, if no new work re-awakens the worker.
    ///
    /// # Examples
    ///
    /// ```
    /// timely::execute_from_args(::std::env::args(), |worker| {
    ///
    ///     use timely::dataflow::operators::{ToStream, Inspect, Probe};
    ///
    ///     let probe =
    ///     worker.dataflow::<usize,_,_>(|scope| {
    ///         (0 .. 10)
    ///             .to_stream(scope)
    ///             .inspect(|x| println!("{:?}", x))
    ///             .probe()
    ///     });
    ///
    ///     worker.step_or_park_while(None, || probe.less_than(&0));
    /// });
    /// ```
    pub fn step_or_park_while<F: FnMut()->bool>(&mut self, duration: Option<Duration>, mut func: F) {
        while func() { self.step_or_park(duration); }
    }

    /// The index of the worker out of its peers.
    ///
    /// # Examples
    /// ```
    /// timely::execute_from_args(::std::env::args(), |worker| {
    ///
    ///     let index = worker.index();
    ///     let peers = worker.peers();
    ///     let timer = worker.timer();
    ///
    ///     println!("{:?}\tWorker {} of {}", timer.elapsed(), index, peers);
    ///
    /// });
    /// ```
    pub fn index(&self) -> usize { self.allocator.borrow().index() }
    /// The total number of peer workers.
    ///
    /// # Examples
    /// ```
    /// timely::execute_from_args(::std::env::args(), |worker| {
    ///
    ///     let index = worker.index();
    ///     let peers = worker.peers();
    ///     let timer = worker.timer();
    ///
    ///     println!("{:?}\tWorker {} of {}", timer.elapsed(), index, peers);
    ///
    /// });
    /// ```
    pub fn peers(&self) -> usize { self.allocator.borrow().peers() }

    /// A timer started at the initiation of the timely computation.
    ///
    /// # Examples
    /// ```
    /// timely::execute_from_args(::std::env::args(), |worker| {
    ///
    ///     let index = worker.index();
    ///     let peers = worker.peers();
    ///     let timer = worker.timer();
    ///
    ///     println!("{:?}\tWorker {} of {}", timer.elapsed(), index, peers);
    ///
    /// });
    /// ```
    pub fn timer(&self) -> Instant { self.timer }

    /// Allocate a new worker-unique identifier.
    ///
    /// This method is public, though it is not expected to be widely used outside
    /// of the timely dataflow system.
    pub fn new_identifier(&mut self) -> usize {
        *self.identifiers.borrow_mut() += 1;
        *self.identifiers.borrow() - 1
    }

    /// Access to named loggers.
    ///
    /// # Examples
    ///
    /// ```
    /// timely::execute_from_args(::std::env::args(), |worker| {
    ///
    ///     worker.log_register()
    ///           .insert::<timely::logging::TimelyEvent,_>("timely", |time, data|
    ///               println!("{:?}\t{:?}", time, data)
    ///           );
    /// });
    /// ```
    pub fn log_register(&self) -> ::std::cell::RefMut<crate::logging_core::Registry<crate::logging::WorkerIdentifier>> {
        self.logging.borrow_mut()
    }

    /// Construct a new dataflow.
    ///
    /// # Examples
    /// ```
    /// timely::execute_from_args(::std::env::args(), |worker| {
    ///
    ///     // We must supply the timestamp type here, although
    ///     // it would generally be determined by type inference.
    ///     worker.dataflow::<usize,_,_>(|scope| {
    ///
    ///         // uses of `scope` to build dataflow
    ///
    ///     });
    /// });
    /// ```
    pub fn dataflow<T, R, F>(&mut self, func: F) -> R
    where
        T: Refines<()>,
        F: FnOnce(&mut Child<Self, T>)->R,
    {
        let logging = self.logging.borrow_mut().get("timely");
        self.dataflow_core("Dataflow", logging, Box::new(()), |_, child| func(child))
    }

    /// Construct a new dataflow with a (purely cosmetic) name.
    ///
    /// # Examples
    /// ```
    /// timely::execute_from_args(::std::env::args(), |worker| {
    ///
    ///     // We must supply the timestamp type here, although
    ///     // it would generally be determined by type inference.
    ///     worker.dataflow_named::<usize,_,_>("Some Dataflow", |scope| {
    ///
    ///         // uses of `scope` to build dataflow
    ///
    ///     });
    /// });
    /// ```
    pub fn dataflow_named<T, R, F>(&mut self, name: &str, func: F) -> R
    where
        T: Refines<()>,
        F: FnOnce(&mut Child<Self, T>)->R,
    {
        let logging = self.logging.borrow_mut().get("timely");
        self.dataflow_core(name, logging, Box::new(()), |_, child| func(child))
    }

    /// Construct a new dataflow with specific configurations.
    ///
    /// This method constructs a new dataflow, using a name, logger, and additional
    /// resources specified as argument. The name is cosmetic, the logger is used to
    /// handle events generated by the dataflow, and the additional resources are kept
    /// alive for as long as the dataflow is alive (use case: shared library bindings).
    ///
    /// # Examples
    /// ```
    /// timely::execute_from_args(::std::env::args(), |worker| {
    ///
    ///     // We must supply the timestamp type here, although
    ///     // it would generally be determined by type inference.
    ///     worker.dataflow_core::<usize,_,_,_>(
    ///         "dataflow X",           // Dataflow name
    ///         None,                   // Optional logger
    ///         37,                     // Any resources
    ///         |resources, scope| {    // Closure
    ///
    ///             // uses of `resources`, `scope`to build dataflow
    ///
    ///         }
    ///     );
    /// });
    /// ```
    pub fn dataflow_core<T, R, F, V>(&mut self, name: &str, mut logging: Option<TimelyLogger>, mut resources: V, func: F) -> R
    where
        T: Refines<()>,
        F: FnOnce(&mut V, &mut Child<Self, T>)->R,
        V: Any+'static,
    {
        let addr = vec![];
        let dataflow_index = self.allocate_dataflow_index();
        let identifier = self.new_identifier();

        let progress_logging = self.logging.borrow_mut().get("timely/progress");
        let subscope = SubgraphBuilder::new_from(dataflow_index, addr, logging.clone(), progress_logging.clone(), name);
        let subscope = RefCell::new(subscope);

        let result = {
            let mut builder = Child {
                subgraph: &subscope,
                parent: self.clone(),
                logging: logging.clone(),
                progress_logging,
            };
            func(&mut resources, &mut builder)
        };

        let mut operator = subscope.into_inner().build(self);

        if let Some(l) = logging.as_mut() {
            l.log(crate::logging::OperatesEvent {
                id: identifier,
                addr: operator.path().to_vec(),
                name: operator.name().to_string(),
            });
            l.flush();
        }

        operator.get_internal_summary();
        operator.set_external_summary();

        let mut temp_channel_ids = self.temp_channel_ids.borrow_mut();
        let channel_ids = temp_channel_ids.drain(..).collect::<Vec<_>>();

        let wrapper = Wrapper {
            logging,
            identifier,
            operate: Some(Box::new(operator)),
            resources: Some(Box::new(resources)),
            channel_ids,
        };
        self.dataflows.borrow_mut().insert(dataflow_index, wrapper);

        result

    }

    /// Drops an identified dataflow.
    ///
    /// This method removes the identified dataflow, which will no longer be scheduled.
    /// Various other resources will be cleaned up, though the method is currently in
    /// public beta rather than expected to work. Please report all crashes and unmet
    /// expectations!
    pub fn drop_dataflow(&mut self, dataflow_identifier: usize) {
        if let Some(mut entry) = self.dataflows.borrow_mut().remove(&dataflow_identifier) {
            // Garbage collect channel_id to path information.
            let mut paths = self.paths.borrow_mut();
            for channel in entry.channel_ids.drain(..) {
                paths.remove(&channel);
            }
        }
    }

    /// Returns the next index to be used for dataflow construction.
    ///
    /// This identifier will appear in the address of contained operators, and can
    /// be used to drop the dataflow using `self.drop_dataflow()`.
    pub fn next_dataflow_index(&self) -> usize {
        *self.dataflow_counter.borrow()
    }

    /// List the current dataflow indices.
    pub fn installed_dataflows(&self) -> Vec<usize> {
        self.dataflows.borrow().keys().cloned().collect()
    }

    /// True if there is at least one dataflow under management.
    pub fn has_dataflows(&self) -> bool {
        !self.dataflows.borrow().is_empty()
    }

    // Acquire a new distinct dataflow identifier.
    fn allocate_dataflow_index(&mut self) -> usize {
        *self.dataflow_counter.borrow_mut() += 1;
        *self.dataflow_counter.borrow() - 1
    }
}

use crate::communication::Message;

impl<A: Allocate> Clone for Worker<A> {
    fn clone(&self) -> Self {
        Worker {
            config: self.config.clone(),
            timer: self.timer,
            paths: self.paths.clone(),
            allocator: self.allocator.clone(),
            identifiers: self.identifiers.clone(),
            dataflows: self.dataflows.clone(),
            dataflow_counter: self.dataflow_counter.clone(),
            logging: self.logging.clone(),
            activations: self.activations.clone(),
            active_dataflows: Vec::new(),
            temp_channel_ids: self.temp_channel_ids.clone(),
        }
    }
}

struct Wrapper {
    logging: Option<TimelyLogger>,
    identifier: usize,
    operate: Option<Box<dyn Schedule>>,
    resources: Option<Box<dyn Any>>,
    channel_ids: Vec<usize>,
}

impl Wrapper {
    /// Steps the dataflow, indicates if it remains incomplete.
    ///
    /// If the dataflow is incomplete, this call will drop it and its resources,
    /// dropping the dataflow first and then the resources (so that, e.g., shared
    /// library bindings will outlive the dataflow).
    fn step(&mut self) -> bool {

        // Perhaps log information about the start of the schedule call.
        if let Some(l) = self.logging.as_mut() {
            l.log(crate::logging::ScheduleEvent::start(self.identifier));
        }

        let incomplete = self.operate.as_mut().map(|op| op.schedule()).unwrap_or(false);
        if !incomplete {
            self.operate = None;
            self.resources = None;
        }

        // Perhaps log information about the stop of the schedule call.
        if let Some(l) = self.logging.as_mut() {
            l.log(crate::logging::ScheduleEvent::stop(self.identifier));
        }

        incomplete
    }
}

impl Drop for Wrapper {
    fn drop(&mut self) {
        if let Some(l) = self.logging.as_mut() {
            l.log(crate::logging::ShutdownEvent { id: self.identifier });
        }
        // ensure drop order
        self.operate = None;
        self.resources = None;
    }
}
