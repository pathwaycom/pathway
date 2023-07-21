//! Methods for publishing logging arrangements.

use std::hash::Hash;
use std::time::Duration;

use timely::communication::Allocate;
use timely::worker::Worker;
use timely::logging::TimelyEvent;
use timely::dataflow::operators::capture::event::EventIterator;

use differential_dataflow::ExchangeData;
use differential_dataflow::logging::DifferentialEvent;

use crate::{Plan, VectorFrom, Datum};
use crate::manager::Manager;

/// A composite trait for values accommodating logging types.
pub trait LoggingValue : VectorFrom<TimelyEvent>+VectorFrom<DifferentialEvent> { }
impl<V: VectorFrom<TimelyEvent>+VectorFrom<DifferentialEvent>> LoggingValue for V { }

/// Timely logging capture and arrangement.
pub fn publish_timely_logging<V, A, I>(
    manager: &mut Manager<V>,
    worker: &mut Worker<A>,
    granularity_ns: u64,
    name: &str,
    events: I
)
where
    V: ExchangeData+Hash+LoggingValue+Datum,
    A: Allocate,
    I : IntoIterator,
    <I as IntoIterator>::Item: EventIterator<Duration, (Duration, usize, TimelyEvent)>+'static
{
    let (operates, channels, schedule, messages, shutdown, park, text) =
    worker.dataflow(move |scope| {

        // use timely::dataflow::operators::Map;
        // use timely::dataflow::operators::Operator;
        use timely::dataflow::operators::capture::Replay;
        use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;

        let input_stream = events.replay_into(scope);

        let mut demux = OperatorBuilder::new("Timely Logging Demux".to_string(), scope.clone());

        use timely::dataflow::channels::pact::Pipeline;
        let mut input = demux.new_input(&input_stream, Pipeline);

        let (mut operates_out, operates) = demux.new_output();
        let (mut channels_out, channels) = demux.new_output();
        let (mut schedule_out, schedule) = demux.new_output();
        let (mut messages_out, messages) = demux.new_output();
        let (mut shutdown_out, shutdown) = demux.new_output();
        let (mut park_out, park) = demux.new_output();
        let (mut text_out, text) = demux.new_output();

        let mut demux_buffer = Vec::new();

        demux.build(move |_capability| {

            move |_frontiers| {

                let mut operates = operates_out.activate();
                let mut channels = channels_out.activate();
                let mut schedule = schedule_out.activate();
                let mut messages = messages_out.activate();
                let mut shutdown = shutdown_out.activate();
                let mut park = park_out.activate();
                let mut text = text_out.activate();

                input.for_each(|time, data| {

                    data.swap(&mut demux_buffer);

                    let mut operates_session = operates.session(&time);
                    let mut channels_session = channels.session(&time);
                    let mut schedule_session = schedule.session(&time);
                    let mut messages_session = messages.session(&time);
                    let mut shutdown_session = shutdown.session(&time);
                    let mut park_session = park.session(&time);
                    let mut text_session = text.session(&time);

                    for (time, _worker, datum) in demux_buffer.drain(..) {

                        // Round time up to next multiple of `granularity_ns`.
                        let time_ns = (((time.as_nanos() as u64) / granularity_ns) + 1) * granularity_ns;
                        let time = Duration::from_nanos(time_ns);

                        match datum {
                            TimelyEvent::Operates(_) => {
                                operates_session.give((V::vector_from(datum), time, 1));
                            },
                            TimelyEvent::Channels(_) => {
                                channels_session.give((V::vector_from(datum), time, 1));
                            },
                            TimelyEvent::Schedule(_) => {
                                schedule_session.give((V::vector_from(datum), time, 1));
                            },
                            TimelyEvent::Messages(_) => {
                                messages_session.give((V::vector_from(datum), time, 1));
                            },
                            TimelyEvent::Shutdown(_) => {
                                shutdown_session.give((V::vector_from(datum), time, 1));
                            },
                            TimelyEvent::Park(_) => {
                                park_session.give((V::vector_from(datum), time, 1));
                            }
                            TimelyEvent::Text(_) => {
                                text_session.give((V::vector_from(datum), time, 1));
                            }
                            _ => { },
                        }
                    }
                });
            }
        });

        // // Pair up start and stop events, to capture scheduling durations.
        // let duration =
        //     input_stream
        //         .flat_map(move |(ts, worker, x)|
        //             if let TimelyEvent::Schedule(event) = x {
        //                 Some((ts, worker, event))
        //             } else { None }
        //         )
        //         .unary(timely::dataflow::channels::pact::Pipeline, "Schedules", |_,_| {

        //             let mut map = std::collections::HashMap::new();
        //             let mut vec = Vec::new();

        //             move |input, output| {

        //                 input.for_each(|time, data| {
        //                     data.swap(&mut vec);
        //                     let mut session = output.session(&time);
        //                     for (ts, worker, event) in vec.drain(..) {
        //                         let key = (worker, event.id);
        //                         match event.start_stop {
        //                             timely::logging::StartStop::Start => {
        //                                 assert!(!map.contains_key(&key));
        //                                 map.insert(key, ts);
        //                             },
        //                             timely::logging::StartStop::Stop => {
        //                                 assert!(map.contains_key(&key));
        //                                 let start = map.remove(&key).unwrap();
        //                                 let elapsed = ts - start;
        //                                 let elapsed_ns = (elapsed.as_secs() as isize) * 1_000_000_000 + (elapsed.subsec_nanos() as isize);
        //                                 session.give((key.1, ts, elapsed_ns));
        //                             }
        //                         }
        //                     }
        //                 });
        //             }
        //         });

        use differential_dataflow::collection::AsCollection;
        use differential_dataflow::operators::arrange::ArrangeBySelf;
        let operates = operates.as_collection().arrange_by_self().trace;
        let channels = channels.as_collection().arrange_by_self().trace;
        let schedule = schedule.as_collection().arrange_by_self().trace;
        let messages = messages.as_collection().arrange_by_self().trace;
        let shutdown = shutdown.as_collection().arrange_by_self().trace;
        let park = park.as_collection().arrange_by_self().trace;
        let text = text.as_collection().arrange_by_self().trace;

        // let elapsed =
        // duration
        //     .map(|(k,t,r)| (vec![Value::Usize(k)], t, r))
        //     .map(move |(k,time,r)| {
        //         // Round time up to next multiple of `granularity_ns`.
        //         let time_ns = (((time.as_nanos() as u64) / granularity_ns) + 1) * granularity_ns;
        //         let time = Duration::from_nanos(time_ns);
        //         (k,time,r)
        //     })
        //     .as_collection()
        //     .arrange_by_self()
        //     .trace;

        // let histogram =
        // duration
        //     .map(|(k,t,r)| (vec![Value::Usize(k), Value::Usize((r as usize).next_power_of_two())], t, 1))
        //     .map(move |(k,time,r)| {
        //         // Round time up to next multiple of `granularity_ns`.
        //         let time_ns = (((time.as_nanos() as u64) / granularity_ns) + 1) * granularity_ns;
        //         let time = Duration::from_nanos(time_ns);
        //         (k,time,r)
        //     })
        //     .as_collection()
        //     .arrange_by_self()
        //     .trace;

        (operates, channels, schedule, messages, shutdown, park, text)
    });

    manager.traces.set_unkeyed(&Plan::Source(format!("logs/{}/timely/operates", name)), &operates);
    manager.traces.set_unkeyed(&Plan::Source(format!("logs/{}/timely/channels", name)), &channels);
    manager.traces.set_unkeyed(&Plan::Source(format!("logs/{}/timely/schedule", name)), &schedule);
    manager.traces.set_unkeyed(&Plan::Source(format!("logs/{}/timely/messages", name)), &messages);
    manager.traces.set_unkeyed(&Plan::Source(format!("logs/{}/timely/shutdown", name)), &shutdown);
    manager.traces.set_unkeyed(&Plan::Source(format!("logs/{}/timely/park", name)), &park);
    manager.traces.set_unkeyed(&Plan::Source(format!("logs/{}/timely/text", name)), &text);

    // manager.traces.set_unkeyed(&Plan::Source(format!("logs/{}/timely/schedule/elapsed", name)), &elapsed);
    // manager.traces.set_unkeyed(&Plan::Source(format!("logs/{}/timely/schedule/histogram", name)), &histogram);
}

/// Timely logging capture and arrangement.
pub fn publish_differential_logging<V, A, I>(
    manager: &mut Manager<V>,
    worker: &mut Worker<A>,
    granularity_ns: u64,
    name: &str,
    events: I
)
where
    V: ExchangeData+Hash+LoggingValue+Datum,
    A: Allocate,
    I : IntoIterator,
    <I as IntoIterator>::Item: EventIterator<Duration, (Duration, usize, DifferentialEvent)>+'static
{
    let (merge,batch) =
    worker.dataflow(move |scope| {

        use timely::dataflow::operators::capture::Replay;
        use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;

        let input = events.replay_into(scope);

        let mut demux = OperatorBuilder::new("Differential Logging Demux".to_string(), scope.clone());

        use timely::dataflow::channels::pact::Pipeline;
        let mut input = demux.new_input(&input, Pipeline);

        let (mut batch_out, batch) = demux.new_output();
        let (mut merge_out, merge) = demux.new_output();

        let mut demux_buffer = Vec::new();

        demux.build(move |_capability| {

            move |_frontiers| {

                let mut batch = batch_out.activate();
                let mut merge = merge_out.activate();

                input.for_each(|time, data| {

                    data.swap(&mut demux_buffer);
                    let mut batch_session = batch.session(&time);
                    let mut merge_session = merge.session(&time);

                    for (time, _worker, datum) in demux_buffer.drain(..) {

                        // Round time up to next multiple of `granularity_ns`.
                        let time_ns = (((time.as_nanos() as u64) / granularity_ns) + 1) * granularity_ns;
                        let time = Duration::from_nanos(time_ns);

                        match datum {
                            DifferentialEvent::Batch(_) => {
                                batch_session.give((V::vector_from(datum), time, 1));
                            },
                            DifferentialEvent::Merge(_) => {
                                merge_session.give((V::vector_from(datum), time, 1));
                            },
                            _ => { },
                        }
                    }
                });
            }
        });

        use differential_dataflow::collection::AsCollection;
        use differential_dataflow::operators::arrange::ArrangeBySelf;
        let batch = batch.as_collection().arrange_by_self().trace;
        let merge = merge.as_collection().arrange_by_self().trace;

        (merge,batch)
    });

    manager.traces.set_unkeyed(&Plan::Source(format!("logs/{}/differential/arrange/batch", name)), &batch);
    manager.traces.set_unkeyed(&Plan::Source(format!("logs/{}/differential/arrange/merge", name)), &merge);
}