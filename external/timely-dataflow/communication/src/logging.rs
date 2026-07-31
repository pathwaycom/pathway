//! Configuration and events for communication logging.

/// Configuration information about a communication thread.
#[derive(Abomonation, Debug, PartialEq, Eq, Hash, Clone, Copy)]
pub struct CommunicationSetup {
    /// True when this is a send thread (or the receive thread).
    pub sender: bool,
    /// The process id of the thread.
    pub process: usize,
    /// The remote process id.
    pub remote: Option<usize>,
}

/// Various communication events.
#[derive(Abomonation, Debug, PartialEq, Eq, Hash, Clone, Copy)]
pub enum CommunicationEvent {
    /// An observed message.
    Message(MessageEvent),
    /// A state transition.
    State(StateEvent),
}

/// An observed message.
#[derive(Abomonation, Debug, PartialEq, Eq, Hash, Clone, Copy)]
pub struct MessageEvent {
    /// true for send event, false for receive event
    pub is_send: bool,
    /// associated message header.
    pub header: crate::networking::MessageHeader,
}

/// Starting or stopping communication threads.
#[derive(Abomonation, Debug, PartialEq, Eq, Hash, Clone, Copy)]
pub struct StateEvent {
    /// Is the thread a send (vs a recv) thread.
    pub send: bool,
    /// The host process id.
    pub process: usize,
    /// The remote process id.
    pub remote: usize,
    /// Is the thread starting or stopping.
    pub start: bool,
}

impl From<MessageEvent> for CommunicationEvent {
    fn from(v: MessageEvent) -> CommunicationEvent {
        CommunicationEvent::Message(v)
    }
}
impl From<StateEvent> for CommunicationEvent {
    fn from(v: StateEvent) -> CommunicationEvent {
        CommunicationEvent::State(v)
    }
}
