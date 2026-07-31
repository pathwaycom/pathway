//! Abstractions for timely dataflow programming.
//!
//! Timely dataflow programs are constructed by manipulating [`Stream`](stream) objects,
//! most often using pre-defined [operators] that implement known patterns.
//!
//! # Examples
//! ```
//! use timely::dataflow::operators::{ToStream, Inspect};
//!
//! timely::example(|scope| {
//!     (0..10).to_stream(scope)
//!            .inspect(|x| println!("seen: {:?}", x));
//! });
//! ```

pub use self::scopes::{Scope, ScopeParent};
pub use self::stream::{Stream, StreamCore};

pub use self::operators::input::Handle as InputHandle;
pub use self::operators::input::HandleCore as InputHandleCore;
pub use self::operators::probe::Handle as ProbeHandle;

pub mod channels;
pub mod operators;
pub mod scopes;
pub mod stream;
