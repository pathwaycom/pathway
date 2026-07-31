extern crate timely;

use std::net::TcpListener;
use timely::dataflow::operators::capture::{EventReader, Replay};
use timely::dataflow::operators::Inspect;

fn main() {
    timely::execute_from_args(std::env::args(), |worker| {
        let source_peers = std::env::args().nth(1).unwrap().parse::<usize>().unwrap();

        // create replayers from disjoint partition of source worker identifiers.
        let replayers = (0..source_peers)
            .filter(|i| i % worker.peers() == worker.index())
            .map(|i| TcpListener::bind(format!("127.0.0.1:{}", 8000 + i)).unwrap())
            .collect::<Vec<_>>()
            .into_iter()
            .map(|l| l.incoming().next().unwrap().unwrap())
            .map(|r| EventReader::<_, u64, _>::new(r))
            .collect::<Vec<_>>();

        worker.dataflow::<u64, _, _>(|scope| {
            replayers
                .replay_into(scope)
                .inspect(|x| println!("replayed: {:?}", x));
        })
    })
    .unwrap(); // asserts error-free execution
}
