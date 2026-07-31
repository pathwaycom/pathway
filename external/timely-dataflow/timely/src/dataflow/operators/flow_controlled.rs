//! Methods to construct flow-controlled sources.

use crate::dataflow::operators::generic::operator::source;
use crate::dataflow::operators::probe::Handle;
use crate::dataflow::{Scope, Stream};
use crate::order::{PartialOrder, TotalOrder};
use crate::progress::timestamp::Timestamp;
use crate::Data;

/// Output of the input reading function for iterator_source.
pub struct IteratorSourceInput<
    T: Clone,
    D: Data,
    DI: IntoIterator<Item = D>,
    I: IntoIterator<Item = (T, DI)>,
> {
    /// Lower bound on timestamps that can be emitted by this input in the future.
    pub lower_bound: T,
    /// Any `T: IntoIterator` of new input data in the form (time, data): time must be
    /// monotonically increasing.
    pub data: I,
    /// A timestamp that represents the frontier that the probe should have
    /// reached before the function is invoked again to ingest additional input.
    pub target: T,
}

/// Construct a source that repeatedly calls the provided function to ingest input.
/// - The function can return None to signal the end of the input;
/// - otherwise, it should return a `IteratorSourceInput`, where:
///   * `lower_bound` is a lower bound on timestamps that can be emitted by this input in the future,
///   `Default::default()` can be used if this isn't needed (the source will assume that
///   the timestamps in `data` are monotonically increasing and will release capabilities
///   accordingly);
///   * `data` is any `T: IntoIterator` of new input data in the form (time, data): time must be
///   monotonically increasing;
///   * `target` is a timestamp that represents the frontier that the probe should have
///   reached before the function is invoked again to ingest additional input.
/// The function will receive the current lower bound of timestamps that can be inserted,
/// `lower_bound`.
///
/// # Example
/// ```rust
/// extern crate timely;
///
/// use timely::dataflow::operators::flow_controlled::{iterator_source, IteratorSourceInput};
/// use timely::dataflow::operators::{probe, Probe, Inspect};
///
/// fn main() {
///     timely::execute_from_args(std::env::args(), |worker| {
///         let mut input = (0u64..100000).peekable();
///         worker.dataflow(|scope| {
///             let mut probe_handle = probe::Handle::new();
///             let probe_handle_2 = probe_handle.clone();
///
///             let mut next_t: u64 = 0;
///             iterator_source(
///                 scope,
///                 "Source",
///                 move |prev_t| {
///                     if let Some(first_x) = input.peek().cloned() {
///                         next_t = first_x / 100 * 100;
///                         Some(IteratorSourceInput {
///                             lower_bound: Default::default(),
///                             data: vec![
///                                 (next_t,
///                                  input.by_ref().take(10).map(|x| (/* "timestamp" */ x, x)).collect::<Vec<_>>())],
///                             target: *prev_t,
///                         })
///                     } else {
///                         None
///                     }
///                 },
///                 probe_handle_2)
///             .inspect_time(|t, d| eprintln!("@ {:?}: {:?}", t, d))
///             .probe_with(&mut probe_handle);
///         });
///     }).unwrap();
/// }
/// ```
pub fn iterator_source<
    G: Scope,
    D: Data,
    DI: IntoIterator<Item = D>,
    I: IntoIterator<Item = (G::Timestamp, DI)>,
    F: FnMut(&G::Timestamp) -> Option<IteratorSourceInput<G::Timestamp, D, DI, I>> + 'static,
>(
    scope: &G,
    name: &str,
    mut input_f: F,
    probe: Handle<G::Timestamp>,
) -> Stream<G, D>
where
    G::Timestamp: TotalOrder,
{
    let mut target = G::Timestamp::minimum();
    source(scope, name, |cap, info| {
        let mut cap = Some(cap);
        let activator = scope.activator_for(&info.address[..]);
        move |output| {
            cap = cap.take().and_then(|mut cap| loop {
                if !probe.less_than(&target) {
                    if let Some(IteratorSourceInput {
                        lower_bound,
                        data,
                        target: new_target,
                    }) = input_f(cap.time())
                    {
                        target = new_target;
                        let mut has_data = false;
                        for (t, ds) in data.into_iter() {
                            cap = if cap.time() != &t {
                                cap.delayed(&t)
                            } else {
                                cap
                            };
                            let mut session = output.session(&cap);
                            session.give_iterator(ds.into_iter());
                            has_data = true;
                        }

                        cap = if cap.time().less_than(&lower_bound) {
                            cap.delayed(&lower_bound)
                        } else {
                            cap
                        };
                        if !has_data {
                            break Some(cap);
                        }
                    } else {
                        break None;
                    }
                } else {
                    break Some(cap);
                }
            });

            if cap.is_some() {
                activator.activate();
            }
        }
    })
}
