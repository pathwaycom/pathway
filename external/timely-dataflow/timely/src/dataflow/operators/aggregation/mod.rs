//! Aggregation operators of various flavors
//!
//! Two traits, `Aggregate` and `StateMachine`, which support the accumulation of streamed information.
//!
//! `Aggregate` accumulates records within times, and releases the accumulations once the time is complete.
//!
//! `StateMachine` responds to a sequence of keyed events, maintaining and updating a state for each key.
//! The user logic may produce output records for each transition, and optionally de-register the state to
//! clean up when appropriate.
//!
//! The two methods are often combined, using first `Aggregate` to reduce the volume of information, and then
//! `StateMachine` to track an accumulation across timestamps.

pub use self::aggregate::Aggregate;
pub use self::state_machine::StateMachine;

pub mod aggregate;
pub mod state_machine;
