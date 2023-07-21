extern crate timely;
extern crate differential_dataflow;
extern crate graph_map;
extern crate core_affinity;

use std::rc::Rc;

use timely::dataflow::*;

use timely::order::Product;
use timely::dataflow::operators::ToStream;

use differential_dataflow::input::Input;
use differential_dataflow::Collection;
use differential_dataflow::operators::*;
use differential_dataflow::operators::arrange::ArrangeByKey;
use differential_dataflow::operators::arrange::ArrangeBySelf;
use differential_dataflow::operators::iterate::SemigroupVariable;
use differential_dataflow::trace::implementations::spine_fueled::Spine;
use differential_dataflow::AsCollection;

use graph_map::GraphMMap;

type Node = u32;
type Iter = u32;
type Diff = i32;

// use differential_dataflow::trace::implementations::graph::GraphBatch;
// type GraphTrace = Spine<Node, Node, (), isize, Rc<GraphBatch<Node>>>;

use differential_dataflow::trace::implementations::ord::OrdValBatch;
type GraphTrace = Spine<Node, Node, (), Diff, Rc<OrdValBatch<Node, Node, (), Diff>>>;

fn main() {

    let filename = std::env::args().nth(1).expect("Must supply filename");
    let rootnode = std::env::args().nth(2).expect("Must supply root node").parse().expect("Invalid root node");

    timely::execute_from_args(std::env::args(), move |worker| {

        let index = worker.index();
        let peers = worker.peers();
        let timer = ::std::time::Instant::now();

        let core_ids = core_affinity::get_core_ids().unwrap();
        core_affinity::set_for_current(core_ids[index % core_ids.len()]);

        // Phase 1: Forward index.
        let mut forward = worker.dataflow(|scope| {

            let graph = GraphMMap::new(&filename);
            let nodes = graph.nodes();
            let edges = (0..nodes).filter(move |node| node % peers == index)
                                  .flat_map(move |node| {
                                      let vec = graph.edges(node).to_vec();
                                      vec.into_iter().map(move |edge| ((node as Node, edge as Node), (), 1))
                                  })
                                  .to_stream(scope)
                                  .as_collection();

            edges.arrange_by_key().trace
        });

        while worker.step() { }
        if index == 0 { println!("{:?}\tphase 1:\tforward graph indexed", timer.elapsed()); }
        let timer = ::std::time::Instant::now();

        // Phase 2: Reachability.
        let mut roots = worker.dataflow(|scope| {
            let (roots_input, roots) = scope.new_collection();
            reach(&mut forward, roots);
            roots_input
        });

        if index == 0 { roots.update(rootnode, 1); }
        roots.close();
        while worker.step() { }
        if index == 0 { println!("{:?}\tphase 2:\treach complete", timer.elapsed()); }
        let timer = ::std::time::Instant::now();

        // Phase 3: Breadth-first distance labeling.
        let mut roots = worker.dataflow(|scope| {
            let (roots_input, roots) = scope.new_collection();
            bfs(&mut forward, roots);
            roots_input
        });

        if index == 0 { roots.update(rootnode, 1); }
        roots.close();
        while worker.step() { }
        if index == 0 { println!("{:?}\tphase 3:\tbfs complete", timer.elapsed()); }
        let timer = ::std::time::Instant::now();

        // Phase 4: Reverse index.
        let mut reverse = worker.dataflow(|scope| {
            forward
                .import(scope)
                .as_collection(|&k,&v| (v,k))
                .arrange_by_key()
                .trace
        });
        while worker.step() { }
        if index == 0 { println!("{:?}\tphase 4:\treverse graph indexed", timer.elapsed()); }
        let timer = ::std::time::Instant::now();

        // Phase 5: Undirected connectivity.
        worker.dataflow(|scope| { connected_components(scope, &mut forward, &mut reverse); });

        while worker.step() { }
        if index == 0 { println!("{:?}\tphase 5:\tcc complete", timer.elapsed()); }

    }).unwrap();
}

use differential_dataflow::operators::arrange::TraceAgent;

type TraceHandle = TraceAgent<GraphTrace>;

fn reach<G: Scope<Timestamp = ()>> (
    graph: &mut TraceHandle,
    roots: Collection<G, Node, Diff>
) -> Collection<G, Node, Diff> {

    let graph = graph.import(&roots.scope());

    roots.scope().iterative::<Iter,_,_>(|scope| {

        let graph = graph.enter(scope);
        let roots = roots.enter(scope);

        let inner = SemigroupVariable::new(scope, Product::new(Default::default(), 1));

        let result =
        graph.join_core(&inner.arrange_by_self(), |_src,&dst,&()| Some(dst))
             .concat(&roots)
             .threshold_total(|_,_| 1);

        inner.set(&result);
        result.leave()
    })
}


fn bfs<G: Scope<Timestamp = ()>> (
    graph: &mut TraceHandle,
    roots: Collection<G, Node, Diff>
) -> Collection<G, (Node, u32), Diff> {

    let graph = graph.import(&roots.scope());
    let roots = roots.map(|r| (r,0));

    roots.scope().iterative::<Iter,_,_>(|scope| {

        let graph = graph.enter(scope);
        let roots = roots.enter(scope);

        let inner = SemigroupVariable::new(scope, Product::new(Default::default(), 1));
        let result =
        graph.join_map(&inner, |_src,&dest,&dist| (dest, dist+1))
             .concat(&roots)
             .reduce(|_key, input, output| output.push((*input[0].0,1)));

        inner.set(&result);
        result.leave()
    })
}

fn connected_components<G: Scope<Timestamp = ()>>(
    scope: &mut G,
    forward: &mut TraceHandle,
    reverse: &mut TraceHandle,
) -> Collection<G, (Node, Node), Diff> {

    let forward = forward.import(scope);
    let reverse = reverse.import(scope);

    // each edge (x,y) means that we need at least a label for the min of x and y.
    let nodes_f = forward.flat_map_ref(|k,v| if k < v { Some(*k) } else { None });
    let nodes_r = reverse.flat_map_ref(|k,v| if k < v { Some(*k) } else { None });
    let nodes = nodes_f.concat(&nodes_r).consolidate().map(|x| (x,x));

    scope.iterative(|scope| {

        // import arrangements, nodes.
        let forward = forward.enter(scope);
        let reverse = reverse.enter(scope);
        let nodes = nodes.enter(scope);

        let inner = SemigroupVariable::new(scope, Product::new(Default::default(), 1));

        let labels = inner.arrange_by_key();
        let f_prop = labels.join_core(&forward, |_k,l,d| Some((*d,*l)));
        let r_prop = labels.join_core(&reverse, |_k,l,d| Some((*d,*l)));

        use timely::dataflow::operators::{Map, Concat, Delay};

        let result =
        nodes
            .inner
            .map_in_place(|dtr| (dtr.1).inner = 256 * ((((::std::mem::size_of::<Node>() * 8) as u32) - (dtr.0).1.leading_zeros())))
            .concat(&inner.filter(|_| false).inner)
            .delay(|dtr,_| dtr.1.clone())
            .as_collection()
            .concat(&f_prop)
            .concat(&r_prop)
            .reduce(|_, s, t| { t.push((*s[0].0, 1)); });

        inner.set(&result);
        result.leave()
    })
}
