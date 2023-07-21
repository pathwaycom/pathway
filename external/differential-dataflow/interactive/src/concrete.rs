//! An example value type.

use std::time::Duration;
use super::{Datum, VectorFrom, Command};

/// A session.
pub struct Session<W: std::io::Write> {
    write: W,
}

impl<W: std::io::Write> Session<W> {
    /// Create a new session.
    pub fn new(write: W) -> Self { Self { write } }
    /// Issue a command.
    pub fn issue<C: Into<Command<Value>>>(&mut self, command: C) {
        let command: Command<Value> = command.into();
        bincode::serialize_into(&mut self.write, &command)
            .expect("bincode: serialization failed");
    }
}

/// An example value type
#[derive(Serialize, Deserialize, Debug, Clone, Hash, Eq, PartialEq, Ord, PartialOrd)]
pub enum Value {
    /// boolean
    Bool(bool),
    /// integer
    Usize(usize),
    /// string
    String(String),
    /// vector
    Vector(Vec<Value>),
    /// duration
    Duration(Duration),
}

impl Datum for Value {
    type Expression = usize;
    fn subject_to(data: &[Self], expr: &Self::Expression) -> Self { data[*expr].clone() }
    fn projection(index: usize) -> Self::Expression { index }
}

impl From<usize> for Value { fn from(x: usize) -> Self { Value::Usize(x) } }
impl From<bool> for Value { fn from(x: bool) -> Self { Value::Bool(x) } }
impl From<String> for Value { fn from(x: String) -> Self { Value::String(x) } }
impl From<Duration> for Value { fn from(x: Duration) -> Self { Value::Duration(x) } }

impl<V> From<Vec<V>> for Value where Value: From<V> {
    fn from(x: Vec<V>) -> Self { Value::Vector(x.into_iter().map(|y| y.into()).collect()) }
}


use timely::logging::TimelyEvent;

impl VectorFrom<TimelyEvent> for Value {
    fn vector_from(item: TimelyEvent) -> Vec<Value> {
        match item {
            TimelyEvent::Operates(x) => {
                vec![
                    x.id.into(),
                    x.addr.into(),
                    x.name.into(),
                ]
            },
            TimelyEvent::Channels(x) => {
                vec![
                    x.id.into(),
                    x.scope_addr.into(),
                    x.source.0.into(),
                    x.source.1.into(),
                    x.target.0.into(),
                    x.target.1.into(),
                ]
            },
            TimelyEvent::Schedule(x) => {
                vec![
                    x.id.into(),
                    (x.start_stop == ::timely::logging::StartStop::Start).into(),
                ]
            },
            TimelyEvent::Messages(x) => {
                vec![
                    x.channel.into(),
                    x.is_send.into(),
                    x.source.into(),
                    x.target.into(),
                    x.seq_no.into(),
                    x.length.into(),
                ]
            },
            TimelyEvent::Shutdown(x) => { vec![x.id.into()] },
            // TimelyEvent::Park(x) => {
            //     match x {
            //         timely::logging::ParkEvent::ParkUnpark::Park(x) => { vec![true.into(), x.into()] },
            //         timely::logging::ParkEvent::ParkUnpark::Unpark => { vec![false.into(), 0.into()] },
            //     }
            // },
            TimelyEvent::Text(x) => { vec![Value::String(x)] }
            _ => { vec![] },
        }
    }
}

use differential_dataflow::logging::DifferentialEvent;

impl VectorFrom<DifferentialEvent> for Value {
    fn vector_from(item: DifferentialEvent) -> Vec<Value> {
        match item {
            DifferentialEvent::Batch(x) => {
                vec![
                    x.operator.into(),
                    x.length.into(),
                ]
            },
            DifferentialEvent::Merge(x) => {
                vec![
                    x.operator.into(),
                    x.scale.into(),
                    x.length1.into(),
                    x.length2.into(),
                    x.complete.unwrap_or(0).into(),
                    x.complete.is_some().into(),
                ]
            },
            _ => { vec![] },
        }
    }
}