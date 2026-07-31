extern crate rand;
extern crate timely;

use std::cmp::Ordering;

use rand::{rngs::SmallRng, Rng, SeedableRng};

use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::generic::operator::Operator;
use timely::dataflow::operators::{Exchange, Input, Probe};
use timely::dataflow::*;

fn main() {
    // command-line args: numbers of nodes and edges in the random graph.
    let nodes: usize = std::env::args().nth(1).unwrap().parse().unwrap();
    let edges: usize = std::env::args().nth(2).unwrap().parse().unwrap();
    let batch: usize = std::env::args().nth(3).unwrap().parse().unwrap();

    timely::execute_from_args(std::env::args().skip(4), move |worker| {
        let index = worker.index();
        let peers = worker.peers();

        let mut input = InputHandle::new();
        let mut probe = ProbeHandle::new();

        worker.dataflow(|scope| {
            scope
                .input_from(&mut input)
                //  .exchange(move |x: &(usize, usize)| (x.0 % (peers - 1)) as u64 + 1)
                .union_find()
                .exchange(|_| 0)
                .union_find()
                .probe_with(&mut probe);
        });

        let mut rng: SmallRng = SeedableRng::seed_from_u64(index as u64);

        for edge in 0..(edges / peers) {
            input.send((rng.gen_range(0..nodes), rng.gen_range(0..nodes)));
            if edge % batch == (batch - 1) {
                let next = input.epoch() + 1;
                input.advance_to(next);
                while probe.less_than(input.time()) {
                    worker.step();
                }
            }
        }
    })
    .unwrap(); // asserts error-free execution;
}

trait UnionFind {
    fn union_find(&self) -> Self;
}

impl<G: Scope> UnionFind for Stream<G, (usize, usize)> {
    fn union_find(&self) -> Stream<G, (usize, usize)> {
        self.unary(Pipeline, "UnionFind", |_, _| {
            let mut roots = vec![]; // u32 works, and is smaller than uint/u64
            let mut ranks = vec![]; // u8 should be large enough (n < 2^256)

            move |input, output| {
                while let Some((time, data)) = input.next() {
                    let mut session = output.session(&time);
                    for &(mut x, mut y) in data.iter() {
                        // grow arrays if required.
                        let m = ::std::cmp::max(x, y);
                        for i in roots.len()..(m + 1) {
                            roots.push(i);
                            ranks.push(0);
                        }

                        // look up roots for `x` and `y`.
                        while x != roots[x] {
                            x = roots[x];
                        }
                        while y != roots[y] {
                            y = roots[y];
                        }

                        if x != y {
                            session.give((x, y));
                            match ranks[x].cmp(&ranks[y]) {
                                Ordering::Less => roots[x] = y,
                                Ordering::Greater => roots[y] = x,
                                Ordering::Equal => {
                                    roots[y] = x;
                                    ranks[x] += 1
                                }
                            }
                        }
                    }
                }
            }
        })
    }
}
