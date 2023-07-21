extern crate rand;
extern crate timely;
extern crate differential_dataflow;

use rand::{Rng, SeedableRng, StdRng};

use std::sync::{Arc, Mutex};
use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::mem;

use timely::Config;

use timely::dataflow::*;
use timely::dataflow::operators::Capture;
use timely::dataflow::operators::capture::Extract;

use differential_dataflow::input::Input;
use differential_dataflow::Collection;

use differential_dataflow::operators::*;
use differential_dataflow::lattice::Lattice;

type Node = usize;
type Edge = (Node, Node);

#[test] fn scc_10_20_1000() { test_sizes(10, 20, 1000, Config::process(3)); }
#[test] fn scc_100_200_10() { test_sizes(100, 200, 10, Config::process(3)); }
#[test] fn scc_100_2000_1() { test_sizes(100, 2000, 1, Config::process(3)); }

fn test_sizes(nodes: usize, edges: usize, rounds: usize, config: Config) {

    let mut edge_list = Vec::new();

    let seed: &[_] = &[1, 2, 3, 4];
    let mut rng1: StdRng = SeedableRng::from_seed(seed);    // rng for edge additions
    let mut rng2: StdRng = SeedableRng::from_seed(seed);    // rng for edge deletions

    for _ in 0 .. edges {
        edge_list.push(((rng1.gen_range(0, nodes), rng1.gen_range(0, nodes)), 0, 1));
    }

    for round in 1 .. rounds {
        edge_list.push(((rng1.gen_range(0, nodes), rng1.gen_range(0, nodes)), round, 1));
        edge_list.push(((rng2.gen_range(0, nodes), rng2.gen_range(0, nodes)), round,-1));
    }

    // for thing in edge_list.iter() {
    //     println!("input: {:?}", thing);
    // }

    let mut results1 = scc_sequential(edge_list.clone());
    let mut results2 = scc_differential(edge_list.clone(), config);

    results1.sort();
    results1.sort_by(|x,y| x.1.cmp(&y.1));
    results2.sort();
    results2.sort_by(|x,y| x.1.cmp(&y.1));

    if results1 != results2 {
        println!("RESULTS INEQUAL!!!");
        for x in &results1 {
            if !results2.contains(x) {
                println!("  in seq, not diff: {:?}", x);
            }
        }
        for x in &results2 {
            if !results1.contains(x) {
                println!("  in diff, not seq: {:?}", x);
            }
        }

    }

    assert_eq!(results1, results2);
}


fn scc_sequential(
    edge_list: Vec<((usize, usize), usize, isize)>)
-> Vec<((usize, usize), usize, isize)> {

    let mut rounds = 0;
    for &(_, time, _) in &edge_list { rounds = ::std::cmp::max(rounds, time + 1); }

    let mut output = Vec::new();    // edges produced in each round.
    let mut results = Vec::new();

    for round in 0 .. rounds {

        let mut edges = ::std::collections::HashMap::new();
        for &((src, dst), time, diff) in &edge_list {
            if time <= round { *edges.entry((src, dst)).or_insert(0) += diff; }
        }
        edges.retain(|_k,v| *v > 0);

        let mut forward = ::std::collections::HashMap::new();
        let mut reverse = ::std::collections::HashMap::new();
        for &(src, dst) in edges.keys() {
            forward.entry(src).or_insert(Vec::new()).push(dst);
            reverse.entry(dst).or_insert(Vec::new()).push(src);
        }

        let mut visited = ::std::collections::HashSet::new();
        let mut list = Vec::new();

        for &node in forward.keys() {
            visit(node, &forward, &mut visited, &mut list)
        }

        let mut component = ::std::collections::HashMap::new();

        while let Some(node) = list.pop() {
            assign(node, node, &reverse, &mut component);
        }

        // `component` now contains component identifiers.

        let mut new_output = Vec::new();
        for (&(src, dst), &cnt) in edges.iter() {
            if component.get(&src) == component.get(&dst) {
                new_output.push(((src, dst), cnt));
            }
        }

        let mut changes = HashMap::new();
        for &((src, dst), cnt) in new_output.iter() {
            *changes.entry((src, dst)).or_insert(0) += cnt;
        }
        for &((src, dst), cnt) in output.iter() {
            *changes.entry((src, dst)).or_insert(0) -= cnt;
        }
        changes.retain(|_k,v| *v != 0);

        for ((src, dst), del) in changes.drain() {
            results.push(((src, dst), round, del));
        }

        output = new_output;
    }

    results
}

fn visit(node: usize, forward: &HashMap<usize, Vec<usize>>, visited: &mut HashSet<usize>, list: &mut Vec<usize>) {
    if !visited.contains(&node) {
        visited.insert(node);
        if let Some(edges) = forward.get(&node) {
            for &edge in edges.iter() {
                visit(edge, forward, visited, list)
            }
        }
        list.push(node);
    }
}

fn assign(node: usize, root: usize, reverse: &HashMap<usize, Vec<usize>>, component: &mut HashMap<usize, usize>) {
    if !component.contains_key(&node) {
        component.insert(node, root);
        if let Some(edges) = reverse.get(&node) {
            for &edge in edges.iter() {
                assign(edge, root, reverse, component);
            }
        }
    }
}

fn scc_differential(
    edges_list: Vec<((usize, usize), usize, isize)>,
    config: Config,
)
-> Vec<((usize, usize), usize, isize)>
{

    let (send, recv) = ::std::sync::mpsc::channel();
    let send = Arc::new(Mutex::new(send));

    timely::execute(config, move |worker| {

        let mut edges_list = edges_list.clone();

        // define BFS dataflow; return handles to roots and edges inputs
        let mut edges = worker.dataflow(|scope| {

            let send = send.lock().unwrap().clone();

            let (edge_input, edges) = scope.new_collection();

            _strongly_connected(&edges)
                .consolidate()
                .inner
                .capture_into(send);

            edge_input
        });

        // sort by decreasing insertion time.
        edges_list.sort_by(|x,y| y.1.cmp(&x.1));

        if worker.index() == 0 {
            let mut round = 0;
            while edges_list.len() > 0 {

                while edges_list.last().map(|x| x.1) == Some(round) {
                    let ((src, dst), _time, diff) = edges_list.pop().unwrap();
                    edges.update((src, dst), diff);
                }

                round += 1;
                edges.advance_to(round);
            }
        }

    }).unwrap();

    recv.extract()
        .into_iter()
        .flat_map(|(_, list)| list.into_iter().map(|((src,dst),time,diff)| ((src,dst), time, diff)))
        .collect()
}

fn _strongly_connected<G: Scope>(graph: &Collection<G, Edge>) -> Collection<G, Edge>
where G::Timestamp: Lattice+Ord+Hash {
    graph.iterate(|inner| {
        let edges = graph.enter(&inner.scope());
        let trans = edges.map_in_place(|x| mem::swap(&mut x.0, &mut x.1));
        _trim_edges(&_trim_edges(inner, &edges), &trans)
    })
}

fn _trim_edges<G: Scope>(cycle: &Collection<G, Edge>, edges: &Collection<G, Edge>)
    -> Collection<G, Edge> where G::Timestamp: Lattice+Ord+Hash {

    let nodes = edges.map_in_place(|x| x.0 = x.1)
                     .consolidate();

    let labels = _reachability(&cycle, &nodes);

    edges.consolidate()
         // .inspect(|x| println!("pre-join: {:?}", x))
         .join_map(&labels, |&e1,&e2,&l1| (e2,(e1,l1)))
         .join_map(&labels, |&e2,&(e1,l1),&l2| ((e1,e2),(l1,l2)))
         .filter(|&(_,(l1,l2))| l1 == l2)
         .map(|((x1,x2),_)| (x2,x1))
}

fn _reachability<G: Scope>(edges: &Collection<G, Edge>, nodes: &Collection<G, (Node, Node)>) -> Collection<G, Edge>
where G::Timestamp: Lattice+Ord+Hash {

    edges.filter(|_| false)
         .iterate(|inner| {
             let edges = edges.enter(&inner.scope());
             let nodes = nodes.enter_at(&inner.scope(), |r| 256 * (64 - (r.0 as u64).leading_zeros() as u64));

             inner.join_map(&edges, |_k,l,d| (*d,*l))
                  .concat(&nodes)
                  .reduce(|_, s, t| t.push((*s[0].0, 1)))

         })
}
