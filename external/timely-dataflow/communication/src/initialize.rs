//! Initialization logic for a generic instance of the `Allocate` channel allocation trait.

use std::thread;
#[cfg(feature = "getopts")]
use std::io::BufRead;
#[cfg(feature = "getopts")]
use getopts;
use std::sync::Arc;

use std::any::Any;

use crate::allocator::thread::ThreadBuilder;
use crate::allocator::{AllocateBuilder, Process, Generic, GenericBuilder};
use crate::allocator::zero_copy::allocator_process::ProcessBuilder;
use crate::allocator::zero_copy::initialize::initialize_networking;

use crate::logging::{CommunicationSetup, CommunicationEvent};
use logging_core::Logger;
use std::fmt::{Debug, Formatter};


/// Possible configurations for the communication infrastructure.
pub enum Config {
    /// Use one thread.
    Thread,
    /// Use one process with an indicated number of threads.
    Process(usize),
    /// Use one process with an indicated number of threads. Use zero-copy exchange channels.
    ProcessBinary(usize),
    /// Expect multiple processes.
    Cluster {
        /// Number of per-process worker threads
        threads: usize,
        /// Identity of this process
        process: usize,
        /// Addresses of all processes
        addresses: Vec<String>,
        /// Verbosely report connection process
        report: bool,
        /// Closure to create a new logger for a communication thread
        log_fn: Box<dyn Fn(CommunicationSetup) -> Option<Logger<CommunicationEvent, CommunicationSetup>> + Send + Sync>,
    }
}

impl Debug for Config {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Config::Thread => write!(f, "Config::Thread()"),
            Config::Process(n) => write!(f, "Config::Process({})", n),
            Config::ProcessBinary(n) => write!(f, "Config::ProcessBinary({})", n),
            Config::Cluster { threads, process, addresses, report, .. } => f
                .debug_struct("Config::Cluster")
                .field("threads", threads)
                .field("process", process)
                .field("addresses", addresses)
                .field("report", report)
                // TODO: Use `.finish_non_exhaustive()` after rust/#67364 lands
                .finish()
        }
    }
}

impl Config {
    /// Installs options into a [`getopts::Options`] struct that corresponds
    /// to the parameters in the configuration.
    ///
    /// It is the caller's responsibility to ensure that the installed options
    /// do not conflict with any other options that may exist in `opts`, or
    /// that may be installed into `opts` in the future.
    ///
    /// This method is only available if the `getopts` feature is enabled, which
    /// it is by default.
    #[cfg(feature = "getopts")]
    pub fn install_options(opts: &mut getopts::Options) {
        opts.optopt("w", "threads", "number of per-process worker threads", "NUM");
        opts.optopt("p", "process", "identity of this process", "IDX");
        opts.optopt("n", "processes", "number of processes", "NUM");
        opts.optopt("h", "hostfile", "text file whose lines are process addresses", "FILE");
        opts.optflag("r", "report", "reports connection progress");
        opts.optflag("z", "zerocopy", "enable zero-copy for intra-process communication");
    }

    /// Instantiates a configuration based upon the parsed options in `matches`.
    ///
    /// The `matches` object must have been constructed from a
    /// [`getopts::Options`] which contained at least the options installed by
    /// [`Self::install_options`].
    ///
    /// This method is only available if the `getopts` feature is enabled, which
    /// it is by default.
    #[cfg(feature = "getopts")]
    pub fn from_matches(matches: &getopts::Matches) -> Result<Config, String> {
        let threads = matches.opt_get_default("w", 1_usize).map_err(|e| e.to_string())?;
        let process = matches.opt_get_default("p", 0_usize).map_err(|e| e.to_string())?;
        let processes = matches.opt_get_default("n", 1_usize).map_err(|e| e.to_string())?;
        let report = matches.opt_present("report");
        let zerocopy = matches.opt_present("zerocopy");

        if processes > 1 {
            let mut addresses = Vec::new();
            if let Some(hosts) = matches.opt_str("h") {
                let file = ::std::fs::File::open(hosts.clone()).map_err(|e| e.to_string())?;
                let reader = ::std::io::BufReader::new(file);
                for line in reader.lines().take(processes) {
                    addresses.push(line.map_err(|e| e.to_string())?);
                }
                if addresses.len() < processes {
                    return Err(format!("could only read {} addresses from {}, but -n: {}", addresses.len(), hosts, processes));
                }
            }
            else {
                for index in 0..processes {
                    addresses.push(format!("localhost:{}", 2101 + index));
                }
            }

            assert!(processes == addresses.len());
            Ok(Config::Cluster {
                threads,
                process,
                addresses,
                report,
                log_fn: Box::new( | _ | None),
            })
        } else if threads > 1 {
            if zerocopy {
                Ok(Config::ProcessBinary(threads))
            } else {
                Ok(Config::Process(threads))
            }
        } else {
            Ok(Config::Thread)
        }
    }

    /// Constructs a new configuration by parsing the supplied text arguments.
    ///
    /// Most commonly, callers supply `std::env::args()` as the iterator.
    ///
    /// This method is only available if the `getopts` feature is enabled, which
    /// it is by default.
    #[cfg(feature = "getopts")]
    pub fn from_args<I: Iterator<Item=String>>(args: I) -> Result<Config, String> {
        let mut opts = getopts::Options::new();
        Config::install_options(&mut opts);
        let matches = opts.parse(args).map_err(|e| e.to_string())?;
        Config::from_matches(&matches)
    }

    /// Attempts to assemble the described communication infrastructure.
    pub fn try_build(self) -> Result<(Vec<GenericBuilder>, Box<dyn Any+Send>), String> {
        match self {
            Config::Thread => {
                Ok((vec![GenericBuilder::Thread(ThreadBuilder)], Box::new(())))
            },
            Config::Process(threads) => {
                Ok((Process::new_vector(threads).into_iter().map(|x| GenericBuilder::Process(x)).collect(), Box::new(())))
            },
            Config::ProcessBinary(threads) => {
                Ok((ProcessBuilder::new_vector(threads).into_iter().map(|x| GenericBuilder::ProcessBinary(x)).collect(), Box::new(())))
            },
            Config::Cluster { threads, process, addresses, report, log_fn } => {
                match initialize_networking(addresses, process, threads, report, log_fn) {
                    Ok((stuff, guard)) => {
                        Ok((stuff.into_iter().map(|x| GenericBuilder::ZeroCopy(x)).collect(), Box::new(guard)))
                    },
                    Err(err) => Err(format!("failed to initialize networking: {}", err))
                }
            },
        }
    }
}

/// Initializes communication and executes a distributed computation.
///
/// This method allocates an `allocator::Generic` for each thread, spawns local worker threads,
/// and invokes the supplied function with the allocator.
/// The method returns a `WorkerGuards<T>` which can be `join`ed to retrieve the return values
/// (or errors) of the workers.
///
///
/// # Examples
/// ```
/// use timely_communication::Allocate;
///
/// // configure for two threads, just one process.
/// let config = timely_communication::Config::Process(2);
///
/// // initializes communication, spawns workers
/// let guards = timely_communication::initialize(config, |mut allocator| {
///     println!("worker {} started", allocator.index());
///
///     // allocates a pair of senders list and one receiver.
///     let (mut senders, mut receiver) = allocator.allocate(0);
///
///     // send typed data along each channel
///     use timely_communication::Message;
///     senders[0].send(Message::from_typed(format!("hello, {}", 0)));
///     senders[1].send(Message::from_typed(format!("hello, {}", 1)));
///
///     // no support for termination notification,
///     // we have to count down ourselves.
///     let mut expecting = 2;
///     while expecting > 0 {
///         allocator.receive();
///         if let Some(message) = receiver.recv() {
///             use std::ops::Deref;
///             println!("worker {}: received: <{}>", allocator.index(), message.deref());
///             expecting -= 1;
///         }
///         allocator.release();
///     }
///
///     // optionally, return something
///     allocator.index()
/// });
///
/// // computation runs until guards are joined or dropped.
/// if let Ok(guards) = guards {
///     for guard in guards.join() {
///         println!("result: {:?}", guard);
///     }
/// }
/// else { println!("error in computation"); }
/// ```
///
/// The should produce output like:
///
/// ```ignore
/// worker 0 started
/// worker 1 started
/// worker 0: received: <hello, 0>
/// worker 1: received: <hello, 1>
/// worker 0: received: <hello, 0>
/// worker 1: received: <hello, 1>
/// result: Ok(0)
/// result: Ok(1)
/// ```
pub fn initialize<T:Send+'static, F: Fn(Generic)->T+Send+Sync+'static>(
    config: Config,
    func: F,
) -> Result<WorkerGuards<T>,String> {
    let (allocators, others) = config.try_build()?;
    initialize_from(allocators, others, func)
}

/// Initializes computation and runs a distributed computation.
///
/// This version of `initialize` allows you to explicitly specify the allocators that
/// you want to use, by providing an explicit list of allocator builders. Additionally,
/// you provide `others`, a `Box<Any>` which will be held by the resulting worker guard
/// and dropped when it is dropped, which allows you to join communication threads.
///
/// # Examples
/// ```
/// use timely_communication::Allocate;
///
/// // configure for two threads, just one process.
/// let builders = timely_communication::allocator::process::Process::new_vector(2);
///
/// // initializes communication, spawns workers
/// let guards = timely_communication::initialize_from(builders, Box::new(()), |mut allocator| {
///     println!("worker {} started", allocator.index());
///
///     // allocates a pair of senders list and one receiver.
///     let (mut senders, mut receiver) = allocator.allocate(0);
///
///     // send typed data along each channel
///     use timely_communication::Message;
///     senders[0].send(Message::from_typed(format!("hello, {}", 0)));
///     senders[1].send(Message::from_typed(format!("hello, {}", 1)));
///
///     // no support for termination notification,
///     // we have to count down ourselves.
///     let mut expecting = 2;
///     while expecting > 0 {
///         allocator.receive();
///         if let Some(message) = receiver.recv() {
///             use std::ops::Deref;
///             println!("worker {}: received: <{}>", allocator.index(), message.deref());
///             expecting -= 1;
///         }
///         allocator.release();
///     }
///
///     // optionally, return something
///     allocator.index()
/// });
///
/// // computation runs until guards are joined or dropped.
/// if let Ok(guards) = guards {
///     for guard in guards.join() {
///         println!("result: {:?}", guard);
///     }
/// }
/// else { println!("error in computation"); }
/// ```
pub fn initialize_from<A, T, F>(
    builders: Vec<A>,
    others: Box<dyn Any+Send>,
    func: F,
) -> Result<WorkerGuards<T>,String>
where
    A: AllocateBuilder+'static,
    T: Send+'static,
    F: Fn(<A as AllocateBuilder>::Allocator)->T+Send+Sync+'static
{
    let logic = Arc::new(func);
    let mut guards = Vec::new();
    for (index, builder) in builders.into_iter().enumerate() {
        let clone = logic.clone();
        guards.push(thread::Builder::new()
                            .name(format!("{}:work-{}", crate::THREAD_NAME_PREFIX, index))
                            .spawn(move || {
                                let communicator = builder.build();
                                (*clone)(communicator)
                            })
                            .map_err(|e| format!("{:?}", e))?);
    }

    Ok(WorkerGuards { guards, others })
}

/// Maintains `JoinHandle`s for worker threads.
pub struct WorkerGuards<T:Send+'static> {
    guards: Vec<::std::thread::JoinHandle<T>>,
    others: Box<dyn Any+Send>,
}

impl<T:Send+'static> WorkerGuards<T> {

    /// Returns a reference to the indexed guard.
    pub fn guards(&self) -> &[std::thread::JoinHandle<T>] {
        &self.guards[..]
    }

    /// Provides access to handles that are not worker threads.
    pub fn others(&self) -> &Box<dyn Any+Send> {
        &self.others
    }

    /// Waits on the worker threads and returns the results they produce.
    pub fn join(mut self) -> Vec<Result<T, String>> {
        self.guards
            .drain(..)
            .map(|guard| guard.join().map_err(|e| format!("{:?}", e)))
            .collect()
    }
}

impl<T:Send+'static> Drop for WorkerGuards<T> {
    fn drop(&mut self) {
        for guard in self.guards.drain(..) {
            guard.join().expect("Worker panic");
        }
        // println!("WORKER THREADS JOINED");
    }
}
