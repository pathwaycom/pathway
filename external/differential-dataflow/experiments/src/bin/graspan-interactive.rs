extern crate timely;
extern crate graph_map;
extern crate differential_dataflow;

use std::io::{BufRead, BufReader};
use std::fs::File;

// use timely::progress::nested::product::Product;
// use timely::dataflow::operators::{Accumulate, Inspect};
use differential_dataflow::input::Input;
// use differential_dataflow::trace::Trace;
// use differential_dataflow::trace::implementations::ord::OrdValSpine;
use differential_dataflow::operators::*;
use differential_dataflow::operators::arrange::ArrangeByKey;

fn main() {

    timely::execute_from_args(std::env::args(), move |worker| {

        let timer = ::std::time::Instant::now();

        let peers = worker.peers();
        let index = worker.index();

        let (mut nodes, mut edges, probe) = worker.dataflow(|scope| {

            // let timer = timer.clone();

            let (n_handle, nodes) = scope.new_collection();
            let (e_handle, edges) = scope.new_collection();

            let edges = edges.arrange_by_key();

            // a N c  <-  a N b && b E c
            // N(a,c) <-  N(a,b), E(b, c)
            let probe =
            nodes
                .filter(|_| false)
                .iterate(|inner| {

                    let nodes = nodes.enter(&inner.scope());
                    let edges = edges.enter(&inner.scope());

                    inner
                        .map(|(a,b)| (b,a))
                        .join_core(&edges, |_b,&a,&c| Some((a,c)))
                        .concat(&nodes)
                        .distinct()
                })
                .probe();

            (n_handle, e_handle, probe)
        });

        if index == 0 { println!("{:?}:\tDataflow assembled", timer.elapsed()); }

        // snag a filename to use for the input graph.
        let filename = std::env::args().nth(1).unwrap();
        let file = BufReader::new(File::open(filename).unwrap());
        for readline in file.lines() {
            let line = readline.ok().expect("read error");
            if !line.starts_with('#') && line.len() > 0 {
                let mut elts = line[..].split_whitespace();
                let src: u32 = elts.next().unwrap().parse().ok().expect("malformed src");
                if (src as usize) % peers == index {
                    let dst: u32 = elts.next().unwrap().parse().ok().expect("malformed dst");
                    let typ: &str = elts.next().unwrap();
                    match typ {
                        "n" => { nodes.insert((src, dst)); },
                        "e" => { edges.insert((src, dst)); },
                        _ => { },
                    }
                }
            }
        }

        if index == 0 {

            edges.close();
            nodes.advance_to(1);
            nodes.flush();

            while probe.less_than(&nodes.time()) { worker.step(); }
            if index == 0 { println!("{:?}:\tData loaded", timer.elapsed()); }

            let mut counts = vec![[0u64; 16]; 64];

            let filename = std::env::args().nth(1).unwrap();
            let file = BufReader::new(File::open(filename).unwrap());
            for readline in file.lines() {
                let line = readline.ok().expect("read error");
                if !line.starts_with('#') && line.len() > 0 {
                    let mut elts = line[..].split_whitespace();
                    let src: u32 = elts.next().unwrap().parse().ok().expect("malformed src");
                    if (src as usize) % peers == index {
                        let dst: u32 = elts.next().unwrap().parse().ok().expect("malformed dst");
                        let typ: &str = elts.next().unwrap();
                        match typ {
                            "n" => {

                                nodes.remove((src, dst));
                                let round = nodes.time() + 0;
                                nodes.advance_to(round + 1);
                                nodes.flush();

                                let timer = std::time::Instant::now();
                                while probe.less_than(&nodes.time()) { worker.step(); }

                                let elapsed = timer.elapsed();
                                let elapsed_ns = 1_000_000_000 * (elapsed.as_secs() as usize) + (elapsed.subsec_nanos() as usize);
                                let count_index = elapsed_ns.next_power_of_two().trailing_zeros() as usize;
                                let low_bits = (elapsed_ns >> (count_index - 5)) & 0xF;
                                counts[count_index][low_bits as usize] += 1;
                                // println!("elapsed: {:?}", elapsed);

                                // println!("round: {:?}", round);
                                if round == 999 {
                                    let mut results = Vec::new();
                                    let total = counts.iter().map(|x| x.iter().sum::<u64>()).sum();
                                    let mut sum = 0;
                                    for index in (10 .. counts.len()).rev() {
                                        for sub in (0 .. 16).rev() {
                                            if sum > 0 && sum < total {
                                                let latency = (1 << (index-1)) + (sub << (index-5));
                                                let fraction = (sum as f64) / (total as f64);
                                                results.push((latency, fraction));
                                            }
                                            sum += counts[index][sub];
                                        }
                                    }
                                    for (latency, fraction) in results.drain(..).rev() {
                                        println!("{}\t{}", latency, fraction);
                                    }
                                    println!();
                                }
                            },
                            _ => { },
                        }
                    }
                }
            }

            let mut results = Vec::new();
            let total = counts.iter().map(|x| x.iter().sum::<u64>()).sum();
            let mut sum = 0;
            for index in (10 .. counts.len()).rev() {
                for sub in (0 .. 16).rev() {
                    if sum > 0 && sum < total {
                        let latency = (1 << (index-1)) + (sub << (index-5));
                        let fraction = (sum as f64) / (total as f64);
                        results.push((latency, fraction));
                    }
                    sum += counts[index][sub];
                }
            }
            for (latency, fraction) in results.drain(..).rev() {
                println!("{}\t{}", latency, fraction);
            }

            println!("{:?}:\tComputation complete", timer.elapsed());
        }

    }).unwrap();
}