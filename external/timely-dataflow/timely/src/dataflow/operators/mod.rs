//! Extension traits for `Stream` implementing various operators.
//!
//! A collection of functions taking typed `Stream` objects as input and producing new `Stream`
//! objects as output. Many of the operators provide simple, composable functionality. Some of the
//! operators are more complicated, for use with advanced timely dataflow features.
//!
//! The [`Operator`](generic::operator) trait provides general
//! operators whose behavior can be supplied using closures accepting input and output handles.
//! Most of the operators in this module are defined using these two general operators.

pub use self::enterleave::{Enter, EnterAt, Leave};
pub use self::input::Input;
pub use self::unordered_input::{UnorderedInput, UnorderedInputCore};
pub use self::feedback::{Feedback, LoopVariable, ConnectLoop};
pub use self::concat::{Concat, Concatenate};
pub use self::partition::Partition;
pub use self::map::Map;
pub use self::inspect::{Inspect, InspectCore};
pub use self::filter::Filter;
pub use self::delay::Delay;
pub use self::exchange::Exchange;
pub use self::broadcast::Broadcast;
pub use self::probe::Probe;
pub use self::to_stream::{ToStream, ToStreamCore};
pub use self::capture::Capture;
pub use self::branch::{Branch, BranchWhen};
pub use self::ok_err::OkErr;
pub use self::result::ResultStream;

pub use self::generic::Operator;
pub use self::generic::{Notificator, FrontierNotificator};

pub use self::reclock::Reclock;
pub use self::count::Accumulate;

pub mod enterleave;
pub mod input;
pub mod flow_controlled;
pub mod unordered_input;
pub mod feedback;
pub mod concat;
pub mod partition;
pub mod map;
pub mod inspect;
pub mod filter;
pub mod delay;
pub mod exchange;
pub mod broadcast;
pub mod probe;
pub mod to_stream;
pub mod capture;
pub mod branch;
pub mod ok_err;
pub mod rc;
pub mod result;

pub mod aggregation;
pub mod generic;

pub mod reclock;
pub mod count;

// keep "mint" module-private
mod capability;
pub use self::capability::{ActivateCapability, Capability, InputCapability, CapabilitySet, DowngradeError};
