//! Traits and types for extracting captured timely dataflow streams.

use super::EventCore;
use crate::Container;
use crate::Data;

/// Supports extracting a sequence of timestamp and data.
pub trait Extract<T: Ord, D: Ord> {
    /// Converts `self` into a sequence of timestamped data.
    ///
    /// Currently this is only implemented for `Receiver<Event<T, Vec<D>>>`, and is used only
    /// to easily pull data out of a timely dataflow computation once it has completed.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::rc::Rc;
    /// use std::sync::{Arc, Mutex};
    /// use timely::dataflow::Scope;
    /// use timely::dataflow::operators::{Capture, ToStream, Inspect};
    /// use timely::dataflow::operators::capture::{EventLinkCore, Replay, Extract};
    ///
    /// // get send and recv endpoints, wrap send to share
    /// let (send, recv) = ::std::sync::mpsc::channel();
    /// let send = Arc::new(Mutex::new(send));
    ///
    /// timely::execute(timely::Config::thread(), move |worker| {
    ///
    ///     // this is only to validate the output.
    ///     let send = send.lock().unwrap().clone();
    ///
    ///     // these are to capture/replay the stream.
    ///     let handle1 = Rc::new(EventLinkCore::new());
    ///     let handle2 = Some(handle1.clone());
    ///
    ///     worker.dataflow::<u64,_,_>(|scope1|
    ///         (0..10).to_stream(scope1)
    ///                .capture_into(handle1)
    ///     );
    ///
    ///     worker.dataflow(|scope2| {
    ///         handle2.replay_into(scope2)
    ///                .capture_into(send)
    ///     });
    /// }).unwrap();
    ///
    /// assert_eq!(recv.extract()[0].1, (0..10).collect::<Vec<_>>());
    /// ```
    fn extract(self) -> Vec<(T, Vec<D>)>;
}

impl<T: Ord, D: Ord + Data> Extract<T, D> for ::std::sync::mpsc::Receiver<EventCore<T, Vec<D>>> {
    fn extract(self) -> Vec<(T, Vec<D>)> {
        let mut result = self.extract_core();

        let mut current = 0;
        for i in 1..result.len() {
            if result[current].0 == result[i].0 {
                let dataz = ::std::mem::replace(&mut result[i].1, Vec::new());
                result[current].1.extend(dataz);
            } else {
                current = i;
            }
        }

        for &mut (_, ref mut data) in &mut result {
            data.sort();
        }
        result.retain(|x| !x.1.is_empty());
        result
    }
}

/// Supports extracting a sequence of timestamp and data.
pub trait ExtractCore<T, C> {
    /// Converts `self` into a sequence of timestamped data.
    ///
    /// Currently this is only implemented for `Receiver<Event<T, C>>`, and is used only
    /// to easily pull data out of a timely dataflow computation once it has completed.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::rc::Rc;
    /// use std::sync::{Arc, Mutex};
    /// use timely::dataflow::Scope;
    /// use timely::dataflow::operators::{Capture, ToStream, Inspect};
    /// use timely::dataflow::operators::capture::{EventLinkCore, Replay, ExtractCore};
    ///
    /// // get send and recv endpoints, wrap send to share
    /// let (send, recv) = ::std::sync::mpsc::channel();
    /// let send = Arc::new(Mutex::new(send));
    ///
    /// timely::execute(timely::Config::thread(), move |worker| {
    ///
    ///     // this is only to validate the output.
    ///     let send = send.lock().unwrap().clone();
    ///
    ///     // these are to capture/replay the stream.
    ///     let handle1 = Rc::new(EventLinkCore::new());
    ///     let handle2 = Some(handle1.clone());
    ///
    ///     worker.dataflow::<u64,_,_>(|scope1|
    ///         (0..10).to_stream(scope1)
    ///                .capture_into(handle1)
    ///     );
    ///
    ///     worker.dataflow(|scope2| {
    ///         handle2.replay_into(scope2)
    ///                .capture_into(send)
    ///     });
    /// }).unwrap();
    ///
    /// assert_eq!(recv.extract_core().into_iter().flat_map(|x| x.1).collect::<Vec<_>>(), (0..10).collect::<Vec<_>>());
    /// ```
    fn extract_core(self) -> Vec<(T, C)>;
}

impl<T, C: Container> ExtractCore<T, C> for ::std::sync::mpsc::Receiver<EventCore<T, C>> {
    fn extract_core(self) -> Vec<(T, C)> {
        let mut result = Vec::new();
        for event in self {
            if let EventCore::Messages(time, data) = event {
                result.push((time, data));
            }
        }
        result.retain(|x| !x.1.is_empty());
        result
    }
}
