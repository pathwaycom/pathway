extern crate rand;
extern crate timely;
extern crate graph_map;
extern crate differential_dataflow;

use std::time::Instant;

use differential_dataflow::input::Input;
use differential_dataflow::operators::*;

fn main() {

    // snag a filename to use for the input graph.
    let filename = std::env::args().nth(1).unwrap();

    timely::execute_from_args(std::env::args().skip(1), move |worker| {

        let peers = worker.peers();
        let index = worker.index();

        let mut input = worker.dataflow::<(),_,_>(|scope| {

            let (input, graph) = scope.new_collection();

            let organizers = graph.explode(|(x,y)| Some((x, (1,0))).into_iter().chain(Some((y, (0,1))).into_iter()))
                                  .threshold_total(|_,w| if w.1 == 0 { 1 } else { 0 });

            organizers
                .iterate(|attend| {
                    graph.enter(&attend.scope())
                         .semijoin(attend)
                         .map(|(_,y)| y)
                         .threshold_total(|_,w| if w >= &3 { 1 } else { 0 })
                         .concat(&organizers.enter(&attend.scope()))
                         .consolidate()
                })
                .map(|_| ())
                .consolidate()
                .inspect(|x| println!("{:?}", x));

            input
        });


        let timer = Instant::now();

        use std::io::{BufReader, BufRead};
        use std::fs::File;

        let file = BufReader::new(File::open(filename.clone()).unwrap());
        for (count, readline) in file.lines().enumerate() {
            let line = readline.ok().expect("read error");
            if count % peers == index && !line.starts_with('#') {
                let mut elts = line[..].split_whitespace();
                let src: u32 = elts.next().unwrap().parse().ok().expect("malformed src");
                let dst: u32 = elts.next().unwrap().parse().ok().expect("malformed dst");
                input.insert((src, dst));
            }
        }

        println!("{:?}\tData ingested", timer.elapsed());

    }).unwrap();
}