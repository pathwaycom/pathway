#[macro_use]
extern crate abomonation_derive;
extern crate abomonation;

extern crate rand;
extern crate timely;
extern crate differential_dataflow;

use std::io::BufRead;

use timely::dataflow::ProbeHandle;
use timely::dataflow::operators::unordered_input::UnorderedInput;
use timely::dataflow::operators::Probe;
use timely::progress::frontier::AntichainRef;
use timely::PartialOrder;

use differential_dataflow::AsCollection;
use differential_dataflow::operators::{Count, arrange::ArrangeBySelf};
use differential_dataflow::trace::{Cursor, TraceReader};

use pair::Pair;

fn main() {

    timely::execute_from_args(std::env::args(), move |worker| {

        // Used to determine if our output has caught up to our input.
        let mut probe: ProbeHandle<Pair<isize, isize>> = ProbeHandle::new();

        let (mut input, mut capability, mut trace) =
        worker.dataflow(|scope| {

            // Create "unordered" inputs which provide their capabilities to users.
            // Here "capability" is a technical term, which is "permission to send
            // data or after a certain timestamp". When this capability is dropped
            // or downgraded, the input communicates that its possible timestamps
            // have advanced, and the system can start to make progress.
            let ((input, capability), data) = scope.new_unordered_input();

            let arrangement =
            data.as_collection()
                .count()
                .map(|(_value, count)| count)
                .arrange_by_self();

            arrangement.stream.probe_with(&mut probe);

            (input, capability, arrangement.trace)
        });

        // Do not hold back physical compaction.
        trace.set_physical_compaction(AntichainRef::new(&[]));

        println!("Multi-temporal histogram; valid commands are (integer arguments):");
        println!("  update   value time1 time2 change");
        println!("  advance-input  time1 time2");
        println!("  advance-output time1 time2");
        println!("  query          time1 time2");

        let std_input = std::io::stdin();
        for line in std_input.lock().lines().map(|x| x.unwrap()) {
            let mut elts = line[..].split_whitespace();
            if let Some(command) = elts.next() {
                if let Ok(arguments) = read_integers(elts) {
                    match (command, arguments.len()) {
                        ("update", 4) => {
                            let time = Pair::new(arguments[1], arguments[2]);
                            if capability.time().less_equal(&time) {
                                input
                                    .session(capability.clone())
                                    .give((arguments[0], time, arguments[3]));
                            } else {
                                println!("Requested time {:?} no longer open (input from {:?})", time, capability.time());
                            }
                        },
                        ("advance-input", 2) => {
                            let time = Pair::new(arguments[0], arguments[1]);
                            if capability.time().less_equal(&time) {
                                capability.downgrade(&time);
                                while probe.less_than(capability.time()) {
                                    worker.step();
                                }
                            } else {
                                println!("Requested time {:?} no longer open (input from {:?})", time, capability.time());
                            }
                        },
                        ("advance-output", 2) => {
                            let time = Pair::new(arguments[0], arguments[1]);
                            if trace.get_logical_compaction().less_equal(&time) {
                                trace.set_logical_compaction(AntichainRef::new(&[time]));
                                while probe.less_than(capability.time()) {
                                    worker.step();
                                }
                            } else {
                                println!("Requested time {:?} not readable (output from {:?})", time, trace.get_logical_compaction());
                            }
                        },
                        ("query", 2) => {
                            // Check that the query times are not beyond the current capabilities.
                            let query_time = Pair::new(arguments[0], arguments[1]);
                            if capability.time().less_equal(&query_time) {
                                println!("Query time ({:?}) is still open (input from {:?}).", query_time, capability.time());
                            } else if !trace.get_logical_compaction().less_equal(&query_time) {
                                println!("Query time ({:?}) no longer available in output (output from {:?}).", query_time, trace.get_logical_compaction());
                            }
                            else {
                                println!("Report at {:?}", query_time);
                                // enumerate the contents of `trace` at `query_time`.
                                let (mut cursor, storage) = trace.cursor();
                                while let Some(key) = cursor.get_key(&storage) {
                                    while let Some(_val) = cursor.get_val(&storage) {
                                        let mut sum = 0;
                                        cursor.map_times(&storage,
                                            |time, diff| if time.less_equal(&query_time) { sum += diff; }
                                        );
                                        cursor.step_val(&storage);
                                        if sum != 0 {
                                            println!("    values with occurrence count {:?}: {:?}", key, sum);
                                        }
                                    }
                                    cursor.step_key(&storage);
                                }
                                println!("Report complete");
                            }
                        },
                        _ => {
                            println!("Command not recognized: {:?} with {} arguments.", command, arguments.len());
                        }
                    }
                }
                else {
                    println!("Error parsing command arguments");
                }
            }
        }
    }).unwrap();
}

/// This module contains a definition of a new timestamp time, a "pair" or product.
///
/// This is a minimal self-contained implementation, in that it doesn't borrow anything
/// from the rest of the library other than the traits it needs to implement. With this
/// type and its implementations, you can use it as a timestamp type.
mod pair {

    /// A pair of timestamps, partially ordered by the product order.
    #[derive(Hash, Default, Clone, Eq, PartialEq, Ord, PartialOrd, Abomonation)]
    pub struct Pair<S, T> {
        pub first: S,
        pub second: T,
    }

    impl<S, T> Pair<S, T> {
        /// Create a new pair.
        pub fn new(first: S, second: T) -> Self {
            Pair { first, second }
        }
    }

    // Implement timely dataflow's `PartialOrder` trait.
    use timely::order::PartialOrder;
    impl<S: PartialOrder, T: PartialOrder> PartialOrder for Pair<S, T> {
        fn less_equal(&self, other: &Self) -> bool {
            self.first.less_equal(&other.first) && self.second.less_equal(&other.second)
        }
    }

    use timely::progress::timestamp::Refines;
    impl<S: Timestamp, T: Timestamp> Refines<()> for Pair<S, T> {
        fn to_inner(_outer: ()) -> Self { Self::minimum() }
        fn to_outer(self) -> () { () }
        fn summarize(_summary: <Self>::Summary) -> () { () }
    }

    // Implement timely dataflow's `PathSummary` trait.
    // This is preparation for the `Timestamp` implementation below.
    use timely::progress::PathSummary;

    impl<S: Timestamp, T: Timestamp> PathSummary<Pair<S,T>> for () {
        fn results_in(&self, timestamp: &Pair<S, T>) -> Option<Pair<S,T>> {
            Some(timestamp.clone())
        }
        fn followed_by(&self, other: &Self) -> Option<Self> {
            Some(other.clone())
        }
    }

    // Implement timely dataflow's `Timestamp` trait.
    use timely::progress::Timestamp;
    impl<S: Timestamp, T: Timestamp> Timestamp for Pair<S, T> {
        fn minimum() -> Self { Pair { first: S::minimum(), second: T::minimum() }}
        type Summary = ();
    }

    // Implement differential dataflow's `Lattice` trait.
    // This extends the `PartialOrder` implementation with additional structure.
    use differential_dataflow::lattice::Lattice;
    impl<S: Lattice, T: Lattice> Lattice for Pair<S, T> {
        fn join(&self, other: &Self) -> Self {
            Pair {
                first: self.first.join(&other.first),
                second: self.second.join(&other.second),
            }
        }
        fn meet(&self, other: &Self) -> Self {
            Pair {
                first: self.first.meet(&other.first),
                second: self.second.meet(&other.second),
            }
        }
    }

    use std::fmt::{Formatter, Error, Debug};

    /// Debug implementation to avoid seeing fully qualified path names.
    impl<TOuter: Debug, TInner: Debug> Debug for Pair<TOuter, TInner> {
        fn fmt(&self, f: &mut Formatter) -> Result<(), Error> {
            f.write_str(&format!("({:?}, {:?})", self.first, self.second))
        }
    }

}

/// This module contains a definition of a new timestamp time, a "pair" or product.
///
/// This is a minimal self-contained implementation, in that it doesn't borrow anything
/// from the rest of the library other than the traits it needs to implement. With this
/// type and its implementations, you can use it as a timestamp type.
mod vector {

    /// A pair of timestamps, partially ordered by the product order.
    #[derive(Hash, Default, Clone, Eq, PartialEq, Ord, PartialOrd, Abomonation, Debug)]
    pub struct Vector<T> {
        pub vector: Vec<T>,
    }

    impl<T> Vector<T> {
        /// Create a new pair.
        pub fn new(vector: Vec<T>) -> Self {
            Vector { vector }
        }
    }

    // Implement timely dataflow's `PartialOrder` trait.
    use timely::order::PartialOrder;
    impl<T: PartialOrder+Timestamp> PartialOrder for Vector<T> {
        fn less_equal(&self, other: &Self) -> bool {
            self.vector
                .iter()
                .enumerate()
                .all(|(index, time)| time.less_equal(other.vector.get(index).unwrap_or(&T::minimum())))
        }
    }

    use timely::progress::timestamp::Refines;
    impl<T: Timestamp> Refines<()> for Vector<T> {
        fn to_inner(_outer: ()) -> Self { Self { vector: Vec::new() } }
        fn to_outer(self) -> () { () }
        fn summarize(_summary: <Self>::Summary) -> () { () }
    }

    // Implement timely dataflow's `PathSummary` trait.
    // This is preparation for the `Timestamp` implementation below.
    use timely::progress::PathSummary;

    impl<T: Timestamp> PathSummary<Vector<T>> for () {
        fn results_in(&self, timestamp: &Vector<T>) -> Option<Vector<T>> {
            Some(timestamp.clone())
        }
        fn followed_by(&self, other: &Self) -> Option<Self> {
            Some(other.clone())
        }
    }

    // Implement timely dataflow's `Timestamp` trait.
    use timely::progress::Timestamp;
    impl<T: Timestamp> Timestamp for Vector<T> {
        fn minimum() -> Self { Self { vector: Vec::new() } }
        type Summary = ();
    }

    // Implement differential dataflow's `Lattice` trait.
    // This extends the `PartialOrder` implementation with additional structure.
    use differential_dataflow::lattice::Lattice;
    impl<T: Lattice+Timestamp+Clone> Lattice for Vector<T> {
        fn join(&self, other: &Self) -> Self {
            let min_len = ::std::cmp::min(self.vector.len(), other.vector.len());
            let max_len = ::std::cmp::max(self.vector.len(), other.vector.len());
            let mut vector = Vec::with_capacity(max_len);
            for index in 0 .. min_len {
                vector.push(self.vector[index].join(&other.vector[index]));
            }
            for time in &self.vector[min_len..] {
                vector.push(time.clone());
            }
            for time in &other.vector[min_len..] {
                vector.push(time.clone());
            }
            Self { vector }
        }
        fn meet(&self, other: &Self) -> Self {
            let min_len = ::std::cmp::min(self.vector.len(), other.vector.len());
            let mut vector = Vec::with_capacity(min_len);
            for index in 0 .. min_len {
                vector.push(self.vector[index].meet(&other.vector[index]));
            }
            Self { vector }
        }
    }
}

/// Read a command and its arguments.
fn read_integers<'a>(input: impl Iterator<Item=&'a str>) -> Result<Vec<isize>, std::num::ParseIntError> {
    let mut integers = Vec::new();
    for text in input {
        integers.push(text.parse()?);
    }
    Ok(integers)
}