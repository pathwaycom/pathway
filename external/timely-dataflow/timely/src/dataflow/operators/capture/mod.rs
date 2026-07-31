//! Operators to capture and replay timely dataflow streams.
//!
//! The `capture_into` and `replay_into` operators respectively capture what a unary operator
//! sees as input (both data and progress information), and play this information back as a new
//! input.
//!
//! The `capture_into` method requires a `P: EventPusher<T, D>`, which is some type accepting
//! `Event<T, D>` inputs. This module provides several examples, including the linked list
//! `EventLink<T, D>`, and the binary `EventWriter<T, D, W>` wrapping any `W: Write`.
//!
//! Streams are captured at the worker granularity, and one can replay an arbitrary subset of
//! the captured streams on any number of workers (fewer, more, or as many as were captured).
//! There is a protocol the captured stream uses, and implementors of new event streams should
//! make sure to understand this (and complain if it is not clear).
//!
//! # Examples
//!
//! The type `Rc<EventLink<T,D>>` implements a typed linked list,
//! and can be captured into and replayed from.
//!
//! ```rust
//! use std::rc::Rc;
//! use timely::dataflow::Scope;
//! use timely::dataflow::operators::{Capture, ToStream, Inspect};
//! use timely::dataflow::operators::capture::{EventLinkCore, Replay};
//!
//! timely::execute(timely::Config::thread(), |worker| {
//!     let handle1 = Rc::new(EventLinkCore::new());
//!     let handle2 = Some(handle1.clone());
//!
//!     worker.dataflow::<u64,_,_>(|scope1|
//!         (0..10).to_stream(scope1)
//!                .capture_into(handle1)
//!     );
//!
//!     worker.dataflow(|scope2| {
//!         handle2.replay_into(scope2)
//!                .inspect(|x| println!("replayed: {:?}", x));
//!     })
//! }).unwrap();
//! ```
//!
//! The types `EventWriter<T, D, W>` and `EventReader<T, D, R>` can be
//! captured into and replayed from, respectively. The use binary writers
//! and readers respectively, and can be backed by files, network sockets,
//! etc.
//!
//! ```
//! use std::rc::Rc;
//! use std::net::{TcpListener, TcpStream};
//! use timely::dataflow::Scope;
//! use timely::dataflow::operators::{Capture, ToStream, Inspect};
//! use timely::dataflow::operators::capture::{EventReader, EventWriter, Replay};
//!
//! timely::execute(timely::Config::thread(), |worker| {
//!     let list = TcpListener::bind("127.0.0.1:8000").unwrap();
//!     let send = TcpStream::connect("127.0.0.1:8000").unwrap();
//!     let recv = list.incoming().next().unwrap().unwrap();
//!
//!     recv.set_nonblocking(true).unwrap();
//!
//!     worker.dataflow::<u64,_,_>(|scope1|
//!         (0..10u64)
//!             .to_stream(scope1)
//!             .capture_into(EventWriter::new(send))
//!     );
//!
//!     worker.dataflow::<u64,_,_>(|scope2| {
//!         Some(EventReader::<_,u64,_>::new(recv))
//!             .replay_into(scope2)
//!             .inspect(|x| println!("replayed: {:?}", x));
//!     })
//! }).unwrap();
//! ```

pub use self::capture::Capture;
pub use self::event::binary::{EventReader, EventReaderCore};
pub use self::event::binary::{EventWriter, EventWriterCore};
pub use self::event::link::{EventLink, EventLinkCore};
pub use self::event::{Event, EventCore, EventPusher, EventPusherCore};
pub use self::extract::{Extract, ExtractCore};
pub use self::replay::Replay;

pub mod capture;
pub mod event;
pub mod extract;
pub mod replay;
