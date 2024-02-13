//! A demonstration of timely dataflow progress tracking, using differential dataflow operators.

extern crate timely;
extern crate differential_dataflow;

use timely::PartialOrder;
use timely::dataflow::*;
use timely::dataflow::operators::probe::Handle;

use differential_dataflow::input::Input;
use differential_dataflow::Collection;
use differential_dataflow::operators::*;

use differential_dataflow::lattice::Lattice;

use timely::progress::{Timestamp, Source, Target, Location};
use timely::progress::timestamp::PathSummary;

fn main() {

    timely::execute_from_args(std::env::args(), move |worker| {

        let timer = worker.timer();
        let mut probe = Handle::new();

        let (mut nodes, mut edges, mut times) = worker.dataflow::<usize,_,_>(|scope| {

            let (node_input, nodes) = scope.new_collection();
            let (edge_input, edges) = scope.new_collection();
            let (time_input, times) = scope.new_collection();

            // Detect cycles that do not increment timestamps.
            find_cycles::<_,usize>(nodes.clone(), edges.clone())
                .inspect(move |x| println!("{:?}\tcycles: {:?}", timer.elapsed(), x))
                .probe_with(&mut probe);

            // Summarize all paths to inputs of operator zero.
            summarize::<_,usize>(nodes.clone(), edges.clone())
                .inspect(move |x| println!("{:?}\tsummary: {:?}", timer.elapsed(), x))
                .probe_with(&mut probe);

            // Track the frontier at each dataflow location.
            frontier::<_,usize>(nodes, edges, times)
                .inspect(move |x| println!("{:?}\tfrontier: {:?}", timer.elapsed(), x))
                .probe_with(&mut probe);

            (node_input, edge_input, time_input)
        });

        // A PageRank-like graph, as represented here:
        //  https://github.com/TimelyDataflow/diagnostics/blob/master/examples/pagerank.png
        nodes.insert((Target::new(2, 0), Source::new(2, 0), 1));
        nodes.insert((Target::new(3, 0), Source::new(3, 0), 0));
        nodes.insert((Target::new(3, 1), Source::new(3, 0), 0));
        nodes.insert((Target::new(4, 0), Source::new(4, 0), 0));

        edges.insert((Source::new(1, 0), Target::new(3, 0)));
        edges.insert((Source::new(3, 0), Target::new(4, 0)));
        edges.insert((Source::new(4, 0), Target::new(2, 0)));
        edges.insert((Source::new(2, 0), Target::new(3, 1)));

        // Initially no capabilities.
        nodes.advance_to(1); nodes.flush();
        edges.advance_to(1); edges.flush();
        times.advance_to(1); times.flush();

        while probe.less_than(times.time()) {
            worker.step();
        }

        // Introduce a new input capability at time zero.
        times.insert((Location::new_source(1, 0), 0));

        nodes.advance_to(2); nodes.flush();
        edges.advance_to(2); edges.flush();
        times.advance_to(2); times.flush();

        while probe.less_than(times.time()) {
            worker.step();
        }

        // Remove input capability and produce a message.
        times.remove((Location::new_source(1, 0), 0));
        times.insert((Location::new_target(3, 0), 0));

        nodes.advance_to(3); nodes.flush();
        edges.advance_to(3); edges.flush();
        times.advance_to(3); times.flush();

        while probe.less_than(times.time()) {
            worker.step();
        }

        // Consume the message, and .. do nothing, I guess.
        times.remove((Location::new_target(3, 0), 0));

        nodes.advance_to(4); nodes.flush();
        edges.advance_to(4); edges.flush();
        times.advance_to(4); times.flush();

        while probe.less_than(times.time()) {
            worker.step();
        }

        println!("finished; elapsed: {:?}", timer.elapsed());
    }).unwrap();
}

/// Propagates times along a timely dataflow graph.
///
/// Timely dataflow graphs are described by nodes with interconnected input and output ports,
/// and edges which connect output ports to input ports of what may be other nodes.
///
/// A set of times at various locations (input or output ports) could traverse nodes and
/// edges to arrive at various other locations. Each location can then track minimal times
/// that can reach them: those times not greater than some other time that can reach it.
///
/// The computation to determine this, and to maintain it as times change, is an iterative
/// computation that propagates times and maintains the minimal elements at each location.
fn frontier<G: Scope, T: Timestamp>(
    nodes: Collection<G, (Target, Source, T::Summary)>,
    edges: Collection<G, (Source, Target)>,
    times: Collection<G, (Location, T)>,
) -> Collection<G, (Location, T)>
where
    G::Timestamp: Lattice+Ord,
    T::Summary: differential_dataflow::ExchangeData,
{
    // Translate node and edge transitions into a common Location to Location edge with an associated Summary.
    let nodes = nodes.map(|(target, source, summary)| (Location::from(target), (Location::from(source), summary)));
    let edges = edges.map(|(source, target)| (Location::from(source), (Location::from(target), Default::default())));
    let transitions: Collection<G, (Location, (Location, T::Summary))> = nodes.concat(&edges);

    times
        .iterate(|reach| {
            transitions
                .enter(&reach.scope())
                .join_map(&reach, |_from, (dest, summ), time| (dest.clone(), summ.results_in(time)))
                .flat_map(|(dest, time)| time.map(move |time| (dest, time)))
                .concat(&times.enter(&reach.scope()))
                .reduce(|_location, input, output: &mut Vec<(T, isize)>| {
                    // retain the lower envelope of times.
                    for (t1, _count1) in input.iter() {
                        if !input.iter().any(|(t2, _count2)| t2.less_than(t1)) {
                            output.push(((*t1).clone(), 1));
                        }
                    }
                })
        })
        .consolidate()
}

/// Summary paths from locations to operator zero inputs.
fn summarize<G: Scope, T: Timestamp>(
    nodes: Collection<G, (Target, Source, T::Summary)>,
    edges: Collection<G, (Source, Target)>,
) -> Collection<G, (Location, (Location, T::Summary))>
where
    G::Timestamp: Lattice+Ord,
    T::Summary: differential_dataflow::ExchangeData+std::hash::Hash,
{
    // Start from trivial reachability from each input to itself.
    let zero_inputs =
    edges
        .map(|(_source, target)| Location::from(target))
        .filter(|location| location.node == 0)
        .map(|location| (location, (location, Default::default())));

    // Retain node connections along "default" timestamp summaries.
    let nodes = nodes.map(|(target, source, summary)| (Location::from(source), (Location::from(target), summary)));
    let edges = edges.map(|(source, target)| (Location::from(target), (Location::from(source), Default::default())));
    let transitions: Collection<G, (Location, (Location, T::Summary))> = nodes.concat(&edges);

    zero_inputs
        .iterate(|summaries| {
            transitions
                .enter(&summaries.scope())
                .join_map(summaries, |_middle, (from, summ1), (to, summ2)| (from.clone(), to.clone(), summ1.followed_by(summ2)))
                .flat_map(|(from, to, summ)| summ.map(move |summ| (from, (to, summ))))
                .concat(&zero_inputs.enter(&summaries.scope()))
                .map(|(from, (to, summary))| ((from, to), summary))
                .reduce(|_from_to, input, output| {
                    for (summary, _count) in input.iter() {
                        if !input.iter().any(|(sum2, _count2)| sum2.less_than(*summary)) {
                            output.push(((*summary).clone(), 1));
                        }
                    }
                })
                .map(|((from, to), summary)| (from, (to, summary)))

        })
        .consolidate()
}


/// Identifies cycles along paths that do not increment timestamps.
fn find_cycles<G: Scope, T: Timestamp>(
    nodes: Collection<G, (Target, Source, T::Summary)>,
    edges: Collection<G, (Source, Target)>,
) -> Collection<G, (Location, Location)>
where
    G::Timestamp: Lattice+Ord,
    T::Summary: differential_dataflow::ExchangeData,
{
    // Retain node connections along "default" timestamp summaries.
    let nodes = nodes.flat_map(|(target, source, summary)| {
        if summary == Default::default() {
            Some((Location::from(target), Location::from(source)))
        }
        else {
            None
        }
    });
    let edges = edges.map(|(source, target)| (Location::from(source), Location::from(target)));
    let transitions: Collection<G, (Location, Location)> = nodes.concat(&edges);

    // Repeatedly restrict to locations with an incoming path.
    transitions
        .iterate(|locations| {
            let active =
            locations
                .map(|(_source, target)| target)
                .distinct();
            transitions
                .enter(&locations.scope())
                .semijoin(&active)
        })
        .consolidate()
}