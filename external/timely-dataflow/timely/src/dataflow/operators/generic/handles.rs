//! Handles to an operator's input and output streams.
//!
//! These handles are used by the generic operator interfaces to allow user closures to interact as
//! the operator would with its input and output streams.

use std::rc::Rc;
use std::cell::RefCell;

use crate::progress::Timestamp;
use crate::progress::ChangeBatch;
use crate::progress::frontier::MutableAntichain;
use crate::dataflow::channels::pullers::Counter as PullCounter;
use crate::dataflow::channels::pushers::CounterCore as PushCounter;
use crate::dataflow::channels::pushers::buffer::{BufferCore, Session};
use crate::dataflow::channels::BundleCore;
use crate::communication::{Push, Pull, message::RefOrMut};
use crate::Container;
use crate::logging::TimelyLogger as Logger;

use crate::dataflow::operators::CapabilityRef;
use crate::dataflow::operators::capability::CapabilityTrait;

/// Handle to an operator's input stream.
pub struct InputHandleCore<T: Timestamp, D: Container, P: Pull<BundleCore<T, D>>> {
    pull_counter: PullCounter<T, D, P>,
    internal: Rc<RefCell<Vec<Rc<RefCell<ChangeBatch<T>>>>>>,
    logging: Option<Logger>,
}

/// Handle to an operator's input stream, specialized to vectors.
pub type InputHandle<T, D, P> = InputHandleCore<T, Vec<D>, P>;

/// Handle to an operator's input stream and frontier.
pub struct FrontieredInputHandleCore<'a, T: Timestamp, D: Container+'a, P: Pull<BundleCore<T, D>>+'a> {
    /// The underlying input handle.
    pub handle: &'a mut InputHandleCore<T, D, P>,
    /// The frontier as reported by timely progress tracking.
    pub frontier: &'a MutableAntichain<T>,
}

/// Handle to an operator's input stream and frontier, specialized to vectors.
pub type FrontieredInputHandle<'a, T, D, P> = FrontieredInputHandleCore<'a, T, Vec<D>, P>;

impl<'a, T: Timestamp, D: Container, P: Pull<BundleCore<T, D>>> InputHandleCore<T, D, P> {

    /// Reads the next input buffer (at some timestamp `t`) and a corresponding capability for `t`.
    /// The timestamp `t` of the input buffer can be retrieved by invoking `.time()` on the capability.
    /// Returns `None` when there's no more data available.
    #[inline]
    pub fn next(&mut self) -> Option<(CapabilityRef<T>, RefOrMut<D>)> {
        let internal = &self.internal;
        self.pull_counter.next_guarded().map(|(guard, bundle)| {
            match bundle.as_ref_or_mut() {
                RefOrMut::Ref(bundle) => {
                    (CapabilityRef::new(&bundle.time, internal.clone(), guard), RefOrMut::Ref(&bundle.data))
                },
                RefOrMut::Mut(bundle) => {
                    (CapabilityRef::new(&bundle.time, internal.clone(), guard), RefOrMut::Mut(&mut bundle.data))
                },
            }
        })
    }

    /// Repeatedly calls `logic` till exhaustion of the available input data.
    /// `logic` receives a capability and an input buffer.
    ///
    /// # Examples
    /// ```
    /// use timely::dataflow::operators::ToStream;
    /// use timely::dataflow::operators::generic::Operator;
    /// use timely::dataflow::channels::pact::Pipeline;
    ///
    /// timely::example(|scope| {
    ///     (0..10).to_stream(scope)
    ///            .unary(Pipeline, "example", |_cap, _info| |input, output| {
    ///                input.for_each(|cap, data| {
    ///                    output.session(&cap).give_vec(&mut data.replace(Vec::new()));
    ///                });
    ///            });
    /// });
    /// ```
    #[inline]
    pub fn for_each<F: FnMut(CapabilityRef<T>, RefOrMut<D>)>(&mut self, mut logic: F) {
        let mut logging = self.logging.take();
        while let Some((cap, data)) = self.next() {
            logging.as_mut().map(|l| l.log(crate::logging::GuardedMessageEvent { is_start: true }));
            logic(cap, data);
            logging.as_mut().map(|l| l.log(crate::logging::GuardedMessageEvent { is_start: false }));
        }
        self.logging = logging;
    }

}

impl<'a, T: Timestamp, D: Container, P: Pull<BundleCore<T, D>>+'a> FrontieredInputHandleCore<'a, T, D, P> {
    /// Allocate a new frontiered input handle.
    pub fn new(handle: &'a mut InputHandleCore<T, D, P>, frontier: &'a MutableAntichain<T>) -> Self {
        FrontieredInputHandleCore {
            handle,
            frontier,
        }
    }

    /// Reads the next input buffer (at some timestamp `t`) and a corresponding capability for `t`.
    /// The timestamp `t` of the input buffer can be retrieved by invoking `.time()` on the capability.
    /// Returns `None` when there's no more data available.
    #[inline]
    pub fn next(&mut self) -> Option<(CapabilityRef<T>, RefOrMut<D>)> {
        self.handle.next()
    }

    /// Repeatedly calls `logic` till exhaustion of the available input data.
    /// `logic` receives a capability and an input buffer.
    ///
    /// # Examples
    /// ```
    /// use timely::dataflow::operators::ToStream;
    /// use timely::dataflow::operators::generic::Operator;
    /// use timely::dataflow::channels::pact::Pipeline;
    ///
    /// timely::example(|scope| {
    ///     (0..10).to_stream(scope)
    ///            .unary(Pipeline, "example", |_cap,_info| |input, output| {
    ///                input.for_each(|cap, data| {
    ///                    output.session(&cap).give_vec(&mut data.replace(Vec::new()));
    ///                });
    ///            });
    /// });
    /// ```
    #[inline]
    pub fn for_each<F: FnMut(CapabilityRef<T>, RefOrMut<D>)>(&mut self, logic: F) {
        self.handle.for_each(logic)
    }

    /// Inspect the frontier associated with this input.
    #[inline]
    pub fn frontier(&self) -> &'a MutableAntichain<T> {
        self.frontier
    }
}

pub fn _access_pull_counter<T: Timestamp, D: Container, P: Pull<BundleCore<T, D>>>(input: &mut InputHandleCore<T, D, P>) -> &mut PullCounter<T, D, P> {
    &mut input.pull_counter
}

/// Constructs an input handle.
/// Declared separately so that it can be kept private when `InputHandle` is re-exported.
pub fn new_input_handle<T: Timestamp, D: Container, P: Pull<BundleCore<T, D>>>(pull_counter: PullCounter<T, D, P>, internal: Rc<RefCell<Vec<Rc<RefCell<ChangeBatch<T>>>>>>, logging: Option<Logger>) -> InputHandleCore<T, D, P> {
    InputHandleCore {
        pull_counter,
        internal,
        logging,
    }
}

/// An owned instance of an output buffer which ensures certain API use.
///
/// An `OutputWrapper` exists to prevent anyone from using the wrapped buffer in any way other
/// than with an `OutputHandle`, whose methods ensure that capabilities are used and that the
/// pusher is flushed (via the `cease` method) once it is no longer used.
#[derive(Debug)]
pub struct OutputWrapper<T: Timestamp, D: Container, P: Push<BundleCore<T, D>>> {
    push_buffer: BufferCore<T, D, PushCounter<T, D, P>>,
    internal_buffer: Rc<RefCell<ChangeBatch<T>>>,
}

impl<T: Timestamp, D: Container, P: Push<BundleCore<T, D>>> OutputWrapper<T, D, P> {
    /// Creates a new output wrapper from a push buffer.
    pub fn new(push_buffer: BufferCore<T, D, PushCounter<T, D, P>>, internal_buffer: Rc<RefCell<ChangeBatch<T>>>) -> Self {
        OutputWrapper {
            push_buffer,
            internal_buffer,
        }
    }
    /// Borrows the push buffer into a handle, which can be used to send records.
    ///
    /// This method ensures that the only access to the push buffer is through the `OutputHandle`
    /// type which ensures the use of capabilities, and which calls `cease` when it is dropped.
    pub fn activate(&mut self) -> OutputHandleCore<T, D, P> {
        OutputHandleCore {
            push_buffer: &mut self.push_buffer,
            internal_buffer: &self.internal_buffer,
        }
    }
}


/// Handle to an operator's output stream.
pub struct OutputHandleCore<'a, T: Timestamp, C: Container+'a, P: Push<BundleCore<T, C>>+'a> {
    push_buffer: &'a mut BufferCore<T, C, PushCounter<T, C, P>>,
    internal_buffer: &'a Rc<RefCell<ChangeBatch<T>>>,
}

/// Handle specialized to `Vec`-based container.
pub type OutputHandle<'a, T, D, P> = OutputHandleCore<'a, T, Vec<D>, P>;

impl<'a, T: Timestamp, C: Container, P: Push<BundleCore<T, C>>> OutputHandleCore<'a, T, C, P> {
    /// Obtains a session that can send data at the timestamp associated with capability `cap`.
    ///
    /// In order to send data at a future timestamp, obtain a capability for the new timestamp
    /// first, as show in the example.
    ///
    /// # Examples
    /// ```
    /// use timely::dataflow::operators::ToStream;
    /// use timely::dataflow::operators::generic::Operator;
    /// use timely::dataflow::channels::pact::Pipeline;
    ///
    /// timely::example(|scope| {
    ///     (0..10).to_stream(scope)
    ///            .unary(Pipeline, "example", |_cap, _info| |input, output| {
    ///                input.for_each(|cap, data| {
    ///                    let time = cap.time().clone() + 1;
    ///                    output.session(&cap.delayed(&time))
    ///                          .give_vec(&mut data.replace(Vec::new()));
    ///                });
    ///            });
    /// });
    /// ```
    pub fn session<'b, CT: CapabilityTrait<T>>(&'b mut self, cap: &'b CT) -> Session<'b, T, C, PushCounter<T, C, P>> where 'a: 'b {
        assert!(cap.valid_for_output(&self.internal_buffer), "Attempted to open output session with invalid capability");
        self.push_buffer.session(cap.time())
    }
}

impl<'a, T: Timestamp, C: Container, P: Push<BundleCore<T, C>>> Drop for OutputHandleCore<'a, T, C, P> {
    fn drop(&mut self) {
        self.push_buffer.cease();
    }
}
