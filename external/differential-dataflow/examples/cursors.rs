//! An example illustrating the use of the cursors API to enumerate arrangements.
//!
//! Dataflow graph construction:
//! * Create a trivial dataflow graph with a single input collection that stores graph
//!   edges of type `(u32, u32)`.
//! * Arrange this collection by the first coordinate of an edge; return the trace of
//!   this arrangement, so that it can be searched and enumerated at runtime.
//!
//! At runtime:
//! * At every round `i`, insert a pair of edges `(i,i+1)`, `(i+1, i)` to the graph, and
//!   delete one existing edge added at the previous round: `(i-1, i)`.
//! * At the end of the round, dump the entire content of the trace using the
//!   `CursorDebug::to_vec` method.
//! * Validate the entire content of the trace after the last round.
//!
//! Example invocation (3 rounds, 4 workers): `cargo run --example cursors -- 3 -w 4`.
//!
//! Expected output:
//! ```
//! round 1, w0 1:(1, 2): [(0, 1)]
//! round 1, w3 2:(2, 1): [(0, 1)]
//! round 2, w2 3:(3, 2): [(1, 1)]
//! round 2, w3 2:(2, 1): [(0, 1)]
//! round 2, w3 2:(2, 3): [(1, 1)]
//! round 2, w0 1:(1, 2): [(0, 1), (1, -1)]
//! round 3, w3 2:(2, 1): [(2, 1)]
//! round 3, w3 2:(2, 3): [(2, 1), (2, -1)]
//! round 3, w2 3:(3, 2): [(2, 1)]
//! round 3, w2 3:(3, 4): [(2, 1)]
//! round 3, w1 4:(4, 3): [(2, 1)]
//! Final graph: {(2, 1): 1, (3, 2): 1, (3, 4): 1, (4, 3): 1}
//! ```

extern crate differential_dataflow;
extern crate timely;

use std::fmt::Debug;
use std::collections::BTreeMap;

use timely::dataflow::operators::probe::Handle;
use timely::progress::frontier::AntichainRef;
use timely::dataflow::operators::Probe;

use differential_dataflow::input::Input;
use differential_dataflow::operators::arrange::ArrangeByKey;
use differential_dataflow::trace::cursor::Cursor;
use differential_dataflow::trace::TraceReader;

type Node = u32;
type Edge = (Node, Node);
type Time = u32;
type Diff = isize;

fn main() {
    let rounds: u32 = std::env::args().nth(1).unwrap().parse().unwrap();

    let mut summaries = timely::execute_from_args(std::env::args(), move |worker| {
        let mut probe = Handle::new();
        let (mut graph, mut graph_trace) = worker.dataflow(|scope| {
            let (graph_input, graph) = scope.new_collection();

            let graph_arr = graph.map(|(x, y): Edge| (x, (x, y))).arrange_by_key();
            let graph_trace = graph_arr.trace.clone();

            /* Be sure to attach probe to arrangements we want to enumerate;
             * so we know when all updates for a given epoch have been added to the arrangement. */
            graph_arr
                .stream
                //.inspect(move |x| println!("{:?}", x))
                .probe_with(&mut probe);

            /* Return `graph_trace`, so we can obtain cursor for the arrangement at runtime. */
            (graph_input, graph_trace)
        });

        if worker.index() != 0 {
            graph.close();
            for i in 1..rounds + 1 {
                /* Advance the trace frontier to enable trace compaction. */
                graph_trace.set_physical_compaction(AntichainRef::new(&[i]));
                graph_trace.set_logical_compaction(AntichainRef::new(&[i]));
                worker.step_while(|| probe.less_than(&i));
                dump_cursor(i, worker.index(), &mut graph_trace);
            }
        } else {
            /* Only worker 0 feeds inputs to the dataflow. */
            for i in 1..rounds + 1 {
                graph.insert((i, i + 1));
                graph.insert((i + 1, i));
                if i > 1 {
                    graph.remove((i - 1, i));
                }
                graph.advance_to(i);
                graph.flush();
                graph_trace.set_physical_compaction(AntichainRef::new(&[i]));
                graph_trace.set_logical_compaction(AntichainRef::new(&[i]));
                worker.step_while(|| probe.less_than(graph.time()));
                dump_cursor(i, worker.index(), &mut graph_trace);
            }
        }

        /* Return trace content after the last round. */
        let (mut cursor, storage) = graph_trace.cursor();
        cursor.to_vec(&storage)
    })
    .unwrap().join();

    /* Aggregate trace summaries from individual workers to reconstrust the content of the graph. */
    let mut graph_content: BTreeMap<Edge, Diff> = BTreeMap::new();
    for summary in summaries.drain(..) {

        let mut summary_vec: Vec<((Node, Edge), Vec<(Time, Diff)>)> = summary.unwrap();

        for ((_, edge), timestamps) in summary_vec.drain(..) {
            /* Sum up all diffs to get the number of occurrences of `edge` at the end of the
             * computation. */
            let diff: Diff = timestamps.iter().map(|(_,d)|d).sum();
            if diff != 0 {
                *graph_content.entry(edge).or_insert(0) += diff;
            }
        }
    }

    println!("Final graph: {:?}", graph_content);

    /* Make sure that final graph content is correct. */
    let mut expected_graph_content: BTreeMap<Edge, Diff> = BTreeMap::new();
    for i in 1..rounds+1 {
        expected_graph_content.insert((i+1, i), 1);
    }
    expected_graph_content.insert((rounds, rounds+1), 1);
    assert_eq!(graph_content, expected_graph_content);
}

fn dump_cursor<Tr>(round: u32, index: usize, trace: &mut Tr)
where
    Tr: TraceReader,
    Tr::Key: Debug + Clone,
    Tr::Val: Debug + Clone,
    Tr::Time: Debug + Clone,
    Tr::R: Debug + Clone,
{
    let (mut cursor, storage) = trace.cursor();
    for ((k, v), diffs) in cursor.to_vec(&storage).iter() {
        println!("round {}, w{} {:?}:{:?}: {:?}", round, index, *k, *v, diffs);
    }
}
