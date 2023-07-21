//! Types and traits for arranging collections.
//!
//! Differential dataflow collections can be "arranged" into maintained, worker-local
//! indices that can be re-used by other dataflows at relatively low cost.
//!
//! The `arrange` operator, and its variants, takes a `Collection` and produces as an
//! output an instance of the `Arrangement` type. An arrangement is logically equivalent
//! to its input collection, but it is distributed across workers and maintained in a
//! way that makes it easy to re-use.
//!
//! The `arrange` operator receives update triples `(data, time, diff)` from its input,
//! and responds to changes in its input frontier, which as it advances signals further
//! times that will no longer be observed in input updates. For each frontier advance,
//! the operator creates a new "batch", containing exactly those updates whose times are
//! in advance of the previous frontier but not in advance of the new frontier. Updates
//! are partitioned among workers by a key, and each batch is indexed by this key.
//!
//! This sequence of batches defines a continually expanding view of committed updates
//! in the collection.
//! The sequence is presented by the `Arrangement` in two forms (its fields):
//!
//! 1.  A timely dataflow `Stream` of batch elements.
//!
//!     The stream is used by operators that want to exploit the arranged structure of
//!     batches, but want the push-based computational model of timely dataflow.
//!     Many differential dataflow operators can consume streams of batches, although
//!     they may also rely on access to the second representation of the sequence.
//!
//! 2.  A `Trace` type that provides a compact representation of the accumulated batches.
//!
//!     A trace is logically equivalent to a sequence of batches, but it is able to alter
//!     the representation for efficiency. In particular, the trace may merge batches so
//!     that the total number is kept small, and it may merge logical times if it able to
//!     determine that no trace users can distinguish between them.
//!
//! Importantly, the `Trace` type has no connection to the timely dataflow runtime.
//! This means a trace can be used in a variety of contexts where a `Stream` would not be
//! appropriate, for example outside of the dataflow in which the arragement is performed.
//! Traces may be directly inspected by any code with access to them, and they can even be
//! used to introduce the batches to other dataflows with the `import` method.

use std::rc::{Rc, Weak};
use std::cell::RefCell;
use std::collections::VecDeque;

use timely::scheduling::Activator;
use timely::progress::Antichain;
use trace::TraceReader;

/// Operating instructions on how to replay a trace.
pub enum TraceReplayInstruction<Tr>
where
    Tr: TraceReader,
{
    /// Describes a frontier advance.
    Frontier(Antichain<Tr::Time>),
    /// Describes a batch of data and a capability hint.
    Batch(Tr::Batch, Option<Tr::Time>),
}

// Short names for strongly and weakly owned activators and shared queues.
type BatchQueue<Tr> = VecDeque<TraceReplayInstruction<Tr>>;
type TraceAgentQueueReader<Tr> = Rc<(Activator, RefCell<BatchQueue<Tr>>)>;
type TraceAgentQueueWriter<Tr> = Weak<(Activator, RefCell<BatchQueue<Tr>>)>;

pub mod writer;
pub mod agent;
pub mod arrangement;

pub mod upsert;

pub use self::writer::TraceWriter;
pub use self::agent::{TraceAgent, ShutdownButton};

pub use self::arrangement::{Arranged, Arrange, ArrangeByKey, ArrangeBySelf};