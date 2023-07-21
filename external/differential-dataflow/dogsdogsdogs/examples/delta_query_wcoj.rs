extern crate timely;
extern crate graph_map;
extern crate differential_dataflow;

extern crate dogsdogsdogs;

use timely::dataflow::Scope;
use timely::dataflow::operators::probe::Handle;
use differential_dataflow::input::Input;
use graph_map::GraphMMap;

use dogsdogsdogs::{CollectionIndex, altneu::AltNeu};
use dogsdogsdogs::{ProposeExtensionMethod};

fn main() {

    // snag a filename to use for the input graph.
    let filename = std::env::args().nth(1).unwrap();
    let batching = std::env::args().nth(2).unwrap().parse::<usize>().unwrap();
    let inspect = std::env::args().any(|x| x == "inspect");

    timely::execute_from_args(std::env::args().skip(2), move |worker| {

        let timer = std::time::Instant::now();
        let graph = GraphMMap::new(&filename);

        let peers = worker.peers();
        let index = worker.index();

        let mut probe = Handle::new();

        let mut input = worker.dataflow::<usize,_,_>(|scope| {

            let (edges_input, edges) = scope.new_collection();

            let forward = edges.clone();
            let reverse = edges.map(|(x,y)| (y,x));

            // Q(a,b,c) :=  E1(a,b),  E2(b,c),  E3(a,c)
            let triangles = scope.scoped::<AltNeu<usize>,_,_>("DeltaQuery (Triangles)", |inner| {

                // Each relation we'll need.
                let forward = forward.enter(inner);
                let reverse = reverse.enter(inner);

                // Without using wrappers yet, maintain an "old" and a "new" copy of edges.
                let alt_forward = CollectionIndex::index(&forward);
                let alt_reverse = CollectionIndex::index(&reverse);
                let neu_forward = CollectionIndex::index(&forward.delay(|time| AltNeu::neu(time.time.clone())));
                let neu_reverse = CollectionIndex::index(&reverse.delay(|time| AltNeu::neu(time.time.clone())));

                // For each relation, we form a delta query driven by changes to that relation.
                //
                // The sequence of joined relations are such that we only introduce relations
                // which share some bound attributes with the current stream of deltas.
                // Each joined relation is delayed { alt -> neu } if its position in the
                // sequence is greater than the delta stream.
                // Each joined relation is directed { forward, reverse } by whether the
                // bound variable occurs in the first or second position.

                //   dQ/dE1 := dE1(a,b), E2(b,c), E3(a,c)
                let changes1 =
                forward
                    .extend(&mut [
                        &mut neu_forward.extend_using(|(_a,b)| *b),
                        &mut neu_forward.extend_using(|(a,_b)| *a),
                    ])
                    .map(|((a,b),c)| (a,b,c));

                //   dQ/dE2 := dE2(b,c), E1(a,b), E3(a,c)
                let changes2 =
                forward
                    .extend(&mut [
                        &mut alt_reverse.extend_using(|(b,_c)| *b),
                        &mut neu_reverse.extend_using(|(_b,c)| *c),
                    ])
                    .map(|((b,c),a)| (a,b,c));

                //   dQ/dE3 := dE3(a,c), E1(a,b), E2(b,c)
                let changes3 = forward
                    .extend(&mut [
                        &mut alt_forward.extend_using(|(a,_c)| *a),
                        &mut alt_reverse.extend_using(|(_a,c)| *c),
                    ])
                    .map(|((a,c),b)| (a,b,c));

                changes1.concat(&changes2).concat(&changes3).leave()
            });

            triangles
                .filter(move |_| inspect)
                .inspect(|x| println!("\tTriangle: {:?}", x))
                .probe_with(&mut probe);

            edges_input
        });

        let mut index = index;
        while index < graph.nodes() {
            input.advance_to(index);
            for &edge in graph.edges(index) {
                input.insert((index as u32, edge));
            }
            index += peers;
            input.advance_to(index);
            input.flush();
            if (index / peers) % batching == 0 {
                while probe.less_than(input.time()) {
                    worker.step();
                }
                println!("{:?}\tRound {} complete", timer.elapsed(), index);
            }
        }

    }).unwrap();
}