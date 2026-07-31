//! Create new `Streams` connected to external inputs.

use std::cell::RefCell;
use std::rc::Rc;

use crate::scheduling::{Activator, Schedule};

use crate::progress::frontier::Antichain;
use crate::progress::Source;
use crate::progress::{operate::SharedProgress, ChangeBatch, Operate, Timestamp};

use crate::communication::Push;
use crate::dataflow::channels::pushers::{CounterCore, TeeCore};
use crate::dataflow::channels::Message;
use crate::dataflow::{Scope, ScopeParent, Stream, StreamCore};
use crate::{Container, Data};

// TODO : This is an exogenous input, but it would be nice to wrap a Subgraph in something
// TODO : more like a harness, with direct access to its inputs.

// NOTE : This only takes a &self, not a &mut self, which works but is a bit weird.
// NOTE : Experiments with &mut indicate that the borrow of 'a lives for too long.
// NOTE : Might be able to fix with another lifetime parameter, say 'c: 'a.

/// Create a new `Stream` and `Handle` through which to supply input.
pub trait Input: Scope {
    /// Create a new `Stream` and `Handle` through which to supply input.
    ///
    /// The `new_input` method returns a pair `(Handle, Stream)` where the `Stream` can be used
    /// immediately for timely dataflow construction, and the `Handle` is later used to introduce
    /// data into the timely dataflow computation.
    ///
    /// The `Handle` also provides a means to indicate
    /// to timely dataflow that the input has advanced beyond certain timestamps, allowing timely
    /// to issue progress notifications.
    ///
    /// # Examples
    /// ```
    /// use timely::*;
    /// use timely::dataflow::operators::{Input, Inspect};
    ///
    /// // construct and execute a timely dataflow
    /// timely::execute(Config::thread(), |worker| {
    ///
    ///     // add an input and base computation off of it
    ///     let mut input = worker.dataflow(|scope| {
    ///         let (input, stream) = scope.new_input();
    ///         stream.inspect(|x| println!("hello {:?}", x));
    ///         input
    ///     });
    ///
    ///     // introduce input, advance computation
    ///     for round in 0..10 {
    ///         input.send(round);
    ///         input.advance_to(round + 1);
    ///         worker.step();
    ///     }
    /// });
    /// ```
    fn new_input<D: Data>(
        &mut self,
    ) -> (Handle<<Self as ScopeParent>::Timestamp, D>, Stream<Self, D>);

    /// Create a new [StreamCore] and [HandleCore] through which to supply input.
    ///
    /// The `new_input_core` method returns a pair `(HandleCore, StreamCore)` where the [StreamCore] can be used
    /// immediately for timely dataflow construction, and the `HandleCore` is later used to introduce
    /// data into the timely dataflow computation.
    ///
    /// The `HandleCore` also provides a means to indicate
    /// to timely dataflow that the input has advanced beyond certain timestamps, allowing timely
    /// to issue progress notifications.
    ///
    /// # Examples
    /// ```
    /// use timely::*;
    /// use timely::dataflow::operators::{Input, Inspect};
    ///
    /// // construct and execute a timely dataflow
    /// timely::execute(Config::thread(), |worker| {
    ///
    ///     // add an input and base computation off of it
    ///     let mut input = worker.dataflow(|scope| {
    ///         let (input, stream) = scope.new_input_core::<Vec<_>>();
    ///         stream.inspect(|x| println!("hello {:?}", x));
    ///         input
    ///     });
    ///
    ///     // introduce input, advance computation
    ///     for round in 0..10 {
    ///         input.send(round);
    ///         input.advance_to(round + 1);
    ///         worker.step();
    ///     }
    /// });
    /// ```
    fn new_input_core<D: Container>(
        &mut self,
    ) -> (
        HandleCore<<Self as ScopeParent>::Timestamp, D>,
        StreamCore<Self, D>,
    );

    /// Create a new stream from a supplied interactive handle.
    ///
    /// This method creates a new timely stream whose data are supplied interactively through the `handle`
    /// argument. Each handle may be used multiple times (or not at all), and will clone data as appropriate
    /// if it as attached to more than one stream.
    ///
    /// # Examples
    /// ```
    /// use timely::*;
    /// use timely::dataflow::operators::{Input, Inspect};
    /// use timely::dataflow::operators::input::Handle;
    ///
    /// // construct and execute a timely dataflow
    /// timely::execute(Config::thread(), |worker| {
    ///
    ///     // add an input and base computation off of it
    ///     let mut input = Handle::new();
    ///     worker.dataflow(|scope| {
    ///         scope.input_from(&mut input)
    ///              .inspect(|x| println!("hello {:?}", x));
    ///     });
    ///
    ///     // introduce input, advance computation
    ///     for round in 0..10 {
    ///         input.send(round);
    ///         input.advance_to(round + 1);
    ///         worker.step();
    ///     }
    /// });
    /// ```
    fn input_from<D: Data>(
        &mut self,
        handle: &mut Handle<<Self as ScopeParent>::Timestamp, D>,
    ) -> Stream<Self, D>;

    /// Create a new stream from a supplied interactive handle.
    ///
    /// This method creates a new timely stream whose data are supplied interactively through the `handle`
    /// argument. Each handle may be used multiple times (or not at all), and will clone data as appropriate
    /// if it as attached to more than one stream.
    ///
    /// # Examples
    /// ```
    /// use timely::*;
    /// use timely::dataflow::operators::{Input, Inspect};
    /// use timely::dataflow::operators::input::Handle;
    ///
    /// // construct and execute a timely dataflow
    /// timely::execute(Config::thread(), |worker| {
    ///
    ///     // add an input and base computation off of it
    ///     let mut input = Handle::new();
    ///     worker.dataflow(|scope| {
    ///         scope.input_from_core(&mut input)
    ///              .inspect(|x| println!("hello {:?}", x));
    ///     });
    ///
    ///     // introduce input, advance computation
    ///     for round in 0..10 {
    ///         input.send(round);
    ///         input.advance_to(round + 1);
    ///         worker.step();
    ///     }
    /// });
    /// ```
    fn input_from_core<D: Container>(
        &mut self,
        handle: &mut HandleCore<<Self as ScopeParent>::Timestamp, D>,
    ) -> StreamCore<Self, D>;
}

use crate::order::TotalOrder;
impl<G: Scope> Input for G
where
    <G as ScopeParent>::Timestamp: TotalOrder,
{
    fn new_input<D: Data>(&mut self) -> (Handle<<G as ScopeParent>::Timestamp, D>, Stream<G, D>) {
        self.new_input_core()
    }

    fn input_from<D: Data>(
        &mut self,
        handle: &mut Handle<<G as ScopeParent>::Timestamp, D>,
    ) -> Stream<G, D> {
        self.input_from_core(handle)
    }

    fn new_input_core<D: Container>(
        &mut self,
    ) -> (
        HandleCore<<G as ScopeParent>::Timestamp, D>,
        StreamCore<G, D>,
    ) {
        let mut handle = HandleCore::new();
        let stream = self.input_from_core(&mut handle);
        (handle, stream)
    }

    fn input_from_core<D: Container>(
        &mut self,
        handle: &mut HandleCore<<G as ScopeParent>::Timestamp, D>,
    ) -> StreamCore<G, D> {
        let (output, registrar) = TeeCore::<<G as ScopeParent>::Timestamp, D>::new();
        let counter = CounterCore::new(output);
        let produced = counter.produced().clone();

        let index = self.allocate_operator_index();
        let mut address = self.addr();
        address.push(index);

        handle.activate.push(self.activator_for(&address[..]));

        let progress = Rc::new(RefCell::new(ChangeBatch::new()));

        handle.register(counter, progress.clone());

        let copies = self.peers();

        self.add_operator_with_index(
            Box::new(Operator {
                name: "Input".to_owned(),
                address,
                shared_progress: Rc::new(RefCell::new(SharedProgress::new(0, 1))),
                progress,
                messages: produced,
                copies,
            }),
            index,
        );

        StreamCore::new(Source::new(index, 0), registrar, self.clone())
    }
}

#[derive(Debug)]
struct Operator<T: Timestamp> {
    name: String,
    address: Vec<usize>,
    shared_progress: Rc<RefCell<SharedProgress<T>>>,
    progress: Rc<RefCell<ChangeBatch<T>>>, // times closed since last asked
    messages: Rc<RefCell<ChangeBatch<T>>>, // messages sent since last asked
    copies: usize,
}

impl<T: Timestamp> Schedule for Operator<T> {
    fn name(&self) -> &str {
        &self.name
    }

    fn path(&self) -> &[usize] {
        &self.address[..]
    }

    fn schedule(&mut self) -> bool {
        let shared_progress = &mut *self.shared_progress.borrow_mut();
        self.progress
            .borrow_mut()
            .drain_into(&mut shared_progress.internals[0]);
        self.messages
            .borrow_mut()
            .drain_into(&mut shared_progress.produceds[0]);
        false
    }
}

impl<T: Timestamp> Operate<T> for Operator<T> {
    fn inputs(&self) -> usize {
        0
    }
    fn outputs(&self) -> usize {
        1
    }

    fn get_internal_summary(
        &mut self,
    ) -> (
        Vec<Vec<Antichain<<T as Timestamp>::Summary>>>,
        Rc<RefCell<SharedProgress<T>>>,
    ) {
        self.shared_progress.borrow_mut().internals[0].update(T::minimum(), self.copies as i64);
        (Vec::new(), self.shared_progress.clone())
    }

    fn notify_me(&self) -> bool {
        false
    }
}

/// A handle to an input `Stream`, used to introduce data to a timely dataflow computation.
#[derive(Debug)]
pub struct HandleCore<T: Timestamp, C: Container> {
    activate: Vec<Activator>,
    progress: Vec<Rc<RefCell<ChangeBatch<T>>>>,
    pushers: Vec<CounterCore<T, C, TeeCore<T, C>>>,
    buffer1: C,
    buffer2: C,
    now_at: T,
}

/// A handle specialized to vector-based containers.
pub type Handle<T, D> = HandleCore<T, Vec<D>>;

impl<T: Timestamp, D: Container> HandleCore<T, D> {
    /// Allocates a new input handle, from which one can create timely streams.
    ///
    /// # Examples
    /// ```
    /// use timely::*;
    /// use timely::dataflow::operators::{Input, Inspect};
    /// use timely::dataflow::operators::input::Handle;
    ///
    /// // construct and execute a timely dataflow
    /// timely::execute(Config::thread(), |worker| {
    ///
    ///     // add an input and base computation off of it
    ///     let mut input = Handle::new();
    ///     worker.dataflow(|scope| {
    ///         scope.input_from(&mut input)
    ///              .inspect(|x| println!("hello {:?}", x));
    ///     });
    ///
    ///     // introduce input, advance computation
    ///     for round in 0..10 {
    ///         input.send(round);
    ///         input.advance_to(round + 1);
    ///         worker.step();
    ///     }
    /// });
    /// ```
    pub fn new() -> Self {
        Self {
            activate: Vec::new(),
            progress: Vec::new(),
            pushers: Vec::new(),
            buffer1: Default::default(),
            buffer2: Default::default(),
            now_at: T::minimum(),
        }
    }

    /// Creates an input stream from the handle in the supplied scope.
    ///
    /// # Examples
    /// ```
    /// use timely::*;
    /// use timely::dataflow::operators::{Input, Inspect};
    /// use timely::dataflow::operators::input::Handle;
    ///
    /// // construct and execute a timely dataflow
    /// timely::execute(Config::thread(), |worker| {
    ///
    ///     // add an input and base computation off of it
    ///     let mut input = Handle::new();
    ///     worker.dataflow(|scope| {
    ///         input.to_stream(scope)
    ///              .inspect(|x| println!("hello {:?}", x));
    ///     });
    ///
    ///     // introduce input, advance computation
    ///     for round in 0..10 {
    ///         input.send(round);
    ///         input.advance_to(round + 1);
    ///         worker.step();
    ///     }
    /// });
    /// ```
    pub fn to_stream<G: Scope>(&mut self, scope: &mut G) -> StreamCore<G, D>
    where
        T: TotalOrder,
        G: ScopeParent<Timestamp = T>,
    {
        scope.input_from_core(self)
    }

    fn register(
        &mut self,
        pusher: CounterCore<T, D, TeeCore<T, D>>,
        progress: Rc<RefCell<ChangeBatch<T>>>,
    ) {
        // flush current contents, so new registrant does not see existing data.
        if !self.buffer1.is_empty() {
            self.flush();
        }

        // we need to produce an appropriate update to the capabilities for `progress`, in case a
        // user has decided to drive the handle around a bit before registering it.
        progress.borrow_mut().update(T::minimum(), -1);
        progress.borrow_mut().update(self.now_at.clone(), 1);

        self.progress.push(progress);
        self.pushers.push(pusher);
    }

    // flushes our buffer at each of the destinations. there can be more than one; clone if needed.
    #[inline(never)]
    fn flush(&mut self) {
        for index in 0..self.pushers.len() {
            if index < self.pushers.len() - 1 {
                self.buffer2.clone_from(&self.buffer1);
                Message::push_at(
                    &mut self.buffer2,
                    self.now_at.clone(),
                    &mut self.pushers[index],
                );
                debug_assert!(self.buffer2.is_empty());
            } else {
                Message::push_at(
                    &mut self.buffer1,
                    self.now_at.clone(),
                    &mut self.pushers[index],
                );
                debug_assert!(self.buffer1.is_empty());
            }
        }
        self.buffer1.clear();
    }

    // closes the current epoch, flushing if needed, shutting if needed, and updating the frontier.
    fn close_epoch(&mut self) {
        if !self.buffer1.is_empty() {
            self.flush();
        }
        for pusher in self.pushers.iter_mut() {
            pusher.done();
        }
        for progress in self.progress.iter() {
            progress.borrow_mut().update(self.now_at.clone(), -1);
        }
        // Alert worker of each active input operator.
        for activate in self.activate.iter() {
            activate.activate();
        }
    }

    /// Sends a batch of records into the corresponding timely dataflow [StreamCore], at the current epoch.
    ///
    /// This method flushes single elements previously sent with `send`, to keep the insertion order.
    ///
    /// # Examples
    /// ```
    /// use timely::*;
    /// use timely::dataflow::operators::{Input, InspectCore};
    /// use timely::dataflow::operators::input::HandleCore;
    ///
    /// // construct and execute a timely dataflow
    /// timely::execute(Config::thread(), |worker| {
    ///
    ///     // add an input and base computation off of it
    ///     let mut input = HandleCore::new();
    ///     worker.dataflow(|scope| {
    ///         scope.input_from_core(&mut input)
    ///              .inspect_container(|x| println!("hello {:?}", x));
    ///     });
    ///
    ///     // introduce input, advance computation
    ///     for round in 0..10 {
    ///         input.send_batch(&mut vec![format!("{}", round)]);
    ///         input.advance_to(round + 1);
    ///         worker.step();
    ///     }
    /// });
    /// ```
    pub fn send_batch(&mut self, buffer: &mut D) {
        if !buffer.is_empty() {
            // flush buffered elements to ensure local fifo.
            if !self.buffer1.is_empty() {
                self.flush();
            }

            // push buffer (or clone of buffer) at each destination.
            for index in 0..self.pushers.len() {
                if index < self.pushers.len() - 1 {
                    self.buffer2.clone_from(&buffer);
                    Message::push_at(
                        &mut self.buffer2,
                        self.now_at.clone(),
                        &mut self.pushers[index],
                    );
                    assert!(self.buffer2.is_empty());
                } else {
                    Message::push_at(buffer, self.now_at.clone(), &mut self.pushers[index]);
                    assert!(buffer.is_empty());
                }
            }
            buffer.clear();
        }
    }

    /// Advances the current epoch to `next`.
    ///
    /// This method allows timely dataflow to issue progress notifications as it can now determine
    /// that this input can no longer produce data at earlier timestamps.
    pub fn advance_to(&mut self, next: T) {
        // Assert that we do not rewind time.
        assert!(self.now_at.less_equal(&next));
        // Flush buffers if time has actually changed.
        if !self.now_at.eq(&next) {
            self.close_epoch();
            self.now_at = next;
            for progress in self.progress.iter() {
                progress.borrow_mut().update(self.now_at.clone(), 1);
            }
        }
    }

    /// Closes the input.
    ///
    /// This method allows timely dataflow to issue all progress notifications blocked by this input
    /// and to begin to shut down operators, as this input can no longer produce data.
    pub fn close(self) {}

    /// Reports the current epoch.
    pub fn epoch(&self) -> &T {
        &self.now_at
    }

    /// Reports the current timestamp.
    pub fn time(&self) -> &T {
        &self.now_at
    }
}

impl<T: Timestamp, D: Data> Handle<T, D> {
    #[inline]
    /// Sends one record into the corresponding timely dataflow `Stream`, at the current epoch.
    ///
    /// # Examples
    /// ```
    /// use timely::*;
    /// use timely::dataflow::operators::{Input, Inspect};
    /// use timely::dataflow::operators::input::Handle;
    ///
    /// // construct and execute a timely dataflow
    /// timely::execute(Config::thread(), |worker| {
    ///
    ///     // add an input and base computation off of it
    ///     let mut input = Handle::new();
    ///     worker.dataflow(|scope| {
    ///         scope.input_from(&mut input)
    ///              .inspect(|x| println!("hello {:?}", x));
    ///     });
    ///
    ///     // introduce input, advance computation
    ///     for round in 0..10 {
    ///         input.send(round);
    ///         input.advance_to(round + 1);
    ///         worker.step();
    ///     }
    /// });
    /// ```
    pub fn send(&mut self, data: D) {
        // assert!(self.buffer1.capacity() == Message::<T, D>::default_length());
        self.buffer1.push(data);
        if self.buffer1.len() == self.buffer1.capacity() {
            self.flush();
        }
    }
}

impl<T: Timestamp, D: Data> Default for Handle<T, D> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Timestamp, C: Container> Drop for HandleCore<T, C> {
    fn drop(&mut self) {
        self.close_epoch();
    }
}
