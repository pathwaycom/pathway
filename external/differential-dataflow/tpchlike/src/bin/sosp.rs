extern crate rand;
extern crate timely;
extern crate differential_dataflow;
extern crate core_affinity;
extern crate tpchlike;

use std::rc::Rc;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::time::Instant;

use timely::dataflow::ProbeHandle;
use tpchlike::{Arrangements, Experiment, InputHandles, types::*, queries};

fn main() {

    timely::execute_from_args(std::env::args().skip(4), |worker| {

        let timer = worker.timer();
        let index = worker.index();
        let peers = worker.peers();

        let core_ids = core_affinity::get_core_ids().unwrap();
        core_affinity::set_for_current(core_ids[index]);

        let mut args = ::std::env::args();
        args.next();
        let prefix = args.next().unwrap();
        let logical_batch = args.next().unwrap().parse::<usize>().expect("logical batch must be an integer");
        let physical_batch = args.next().unwrap().parse::<usize>().expect("physical batch must be an integer");
        let concurrent = args.next().unwrap().parse::<usize>().expect("concurrency must be an integer");
        let arrange: bool = args.next().unwrap().parse().unwrap();
        let seal: bool = ::std::env::args().any(|x| x == "seal-inputs");

        // Build all indices, as we plan to use all of them.
        let (mut inputs, mut probe, mut traces) = worker.dataflow::<usize,_,_>(move |scope| {

            let mut inputs = InputHandles::new();
            let mut probe = ProbeHandle::new();
            let traces = Arrangements::new(&mut inputs, scope, &mut probe, arrange);

            (inputs, probe, traces)
        });

        // customer.tbl lineitem.tbl    nation.tbl  orders.tbl  part.tbl    partsupp.tbl    region.tbl  supplier.tbl
        let mut customers = load::<Customer>(prefix.as_str(), "customer.tbl", index, peers, logical_batch, physical_batch, 0);
        let mut lineitems = load::<LineItem>(prefix.as_str(), "lineitem.tbl", index, peers, logical_batch, physical_batch, 1);
        let mut nations = load::<Nation>(prefix.as_str(), "nation.tbl", index, peers, logical_batch, physical_batch, 2);
        let mut orders = load::<Order>(prefix.as_str(), "orders.tbl", index, peers, logical_batch, physical_batch, 3);
        let mut parts = load::<Part>(prefix.as_str(), "part.tbl", index, peers, logical_batch, physical_batch, 4);
        let mut partsupps = load::<PartSupp>(prefix.as_str(), "partsupp.tbl", index, peers, logical_batch, physical_batch, 5);
        let mut regions = load::<Region>(prefix.as_str(), "region.tbl", index, peers, logical_batch, physical_batch, 6);
        let mut suppliers = load::<Supplier>(prefix.as_str(), "supplier.tbl", index, peers, logical_batch, physical_batch, 7);

        println!("{:?}\tInput loaded", timer.elapsed());

        // Synchronize before starting the timer.
        let next_round = 1;
        inputs.advance_to(next_round);

        let time = next_round;
        worker.step_while(|| probe.less_than(&time));

        use rand::prelude::*;
        let mut rng = rand::thread_rng();

        let timer = Instant::now();
        let mut experiments = Vec::<Experiment>::new();
        let mut round = 0;
        while customers.len() > 0 || lineitems.len() > 0 || nations.len() > 0 || orders.len() > 0 || parts.len() > 0 || partsupps.len() > 0 || regions.len() > 0 || suppliers.len() > 0 {

            let this_round = 1 + 8 * round * physical_batch;

            let query = rng.gen_range(0, 22);

            let token = std::rc::Rc::new(());
            let mut experiment = Experiment::new(query, &token);
            // install new dataflows, retire old dataflow
            worker.dataflow_core::<usize,_,_,_>("dataflow", None, token, |_,scope| {
                match query {
                     0 => queries::query01::query_arranged(scope, &mut probe, &mut experiment, &mut traces),
                     1 => queries::query02::query_arranged(scope, &mut probe, &mut experiment, &mut traces, this_round-1),
                     2 => queries::query03::query_arranged(scope, &mut probe, &mut experiment, &mut traces),
                     3 => queries::query04::query_arranged(scope, &mut probe, &mut experiment, &mut traces),
                     4 => queries::query05::query_arranged(scope, &mut probe, &mut experiment, &mut traces),
                     5 => queries::query06::query_arranged(scope, &mut probe, &mut experiment, &mut traces),
                     6 => queries::query07::query_arranged(scope, &mut probe, &mut experiment, &mut traces),
                     7 => queries::query08::query_arranged(scope, &mut probe, &mut experiment, &mut traces),
                     8 => queries::query09::query_arranged(scope, &mut probe, &mut experiment, &mut traces),
                     9 => queries::query10::query_arranged(scope, &mut probe, &mut experiment, &mut traces),
                    10 => queries::query11::query_arranged(scope, &mut probe, &mut experiment, &mut traces, this_round-1),
                    11 => queries::query12::query_arranged(scope, &mut probe, &mut experiment, &mut traces),
                    12 => queries::query13::query_arranged(scope, &mut probe, &mut experiment, &mut traces, this_round-1),
                    13 => queries::query14::query_arranged(scope, &mut probe, &mut experiment, &mut traces),
                    14 => queries::query15::query_arranged(scope, &mut probe, &mut experiment, &mut traces),
                    15 => queries::query16::query_arranged(scope, &mut probe, &mut experiment, &mut traces, this_round-1),
                    16 => queries::query17::query_arranged(scope, &mut probe, &mut experiment, &mut traces),
                    17 => queries::query18::query_arranged(scope, &mut probe, &mut experiment, &mut traces),
                    18 => queries::query19::query_arranged(scope, &mut probe, &mut experiment, &mut traces),
                    19 => queries::query20::query_arranged(scope, &mut probe, &mut experiment, &mut traces),
                    20 => queries::query21::query_arranged(scope, &mut probe, &mut experiment, &mut traces),
                    21 => queries::query22::query_arranged(scope, &mut probe, &mut experiment, &mut traces, this_round-1),
                    _ => { },
                }
                experiment.lineitem.advance_to(this_round);
                experiments.push(experiment);
            });
            let start = timer.elapsed();
            worker.step_while(|| probe.less_than(&this_round));
            let install = timer.elapsed() - start;

            let start = timer.elapsed();
            if experiments.len() > concurrent {
                let token = experiments.remove(0).close();
                worker.step_while(|| token.upgrade().is_some());
            }
            let uninstall = timer.elapsed() - start;

            // catch all inputs up to the same (next) round.
            let next_round = 1 + 8 * (round + 1) * physical_batch;

            // introduce physical batch of data for each input with remaining data.
            if let Some(mut data) = customers.pop() { inputs.customer.send_batch(&mut data); }
            if let Some(mut data) = lineitems.pop() {
                let mut data = data.drain(..).map(|(x,y,z)| (Rc::new(x),y,z)).collect::<Vec<_>>();
                let mut temp = Vec::new();
                for experiment in experiments.iter_mut() {
                    temp.clone_from(&data);
                    experiment.lineitem.send_batch(&mut temp);
                    experiment.lineitem.advance_to(next_round);
                }
                inputs.lineitem.send_batch(&mut data);
            }
            if let Some(mut data) = nations.pop() { inputs.nation.send_batch(&mut data); }
            if let Some(mut data) = orders.pop() { inputs.order.send_batch(&mut data); }
            if let Some(mut data) = parts.pop() { inputs.part.send_batch(&mut data); }
            if let Some(mut data) = partsupps.pop() { inputs.partsupp.send_batch(&mut data); }
            if let Some(mut data) = regions.pop() { inputs.region.send_batch(&mut data); }
            if let Some(mut data) = suppliers.pop() { inputs.supplier.send_batch(&mut data); }

            inputs.advance_to(next_round);
            traces.set_logical_compaction(&[next_round]);

            let start = timer.elapsed();
            worker.step_while(|| probe.less_than(&next_round));
            let work = timer.elapsed() - start;

            println!("{:?}\t{}\t{}\t{:?}\t{:?}\t{:?}", timer.elapsed(), round, query, install.as_nanos(), uninstall.as_nanos(), work.as_nanos());

            round += 1;
        }

    }).unwrap();
}

// Returns a sequence of physical batches of ready-to-go timestamped data. Not clear that `input` can exploit the pre-arrangement yet.
fn load<T>(prefix: &str, name: &str, index: usize, peers: usize, logical_batch: usize, physical_batch: usize, off: usize)
    -> Vec<Vec<(T, usize, isize)>>
where T: for<'a> From<&'a str> {

    let mut result = Vec::new();
    let mut buffer = Vec::new();

    let path = format!("{}{}", prefix, name);

    let items_file = File::open(&path).expect("didn't find items file");
    let mut items_reader =  BufReader::new(items_file);
    let mut count = 0;

    let mut line = String::new();

    while items_reader.read_line(&mut line).unwrap() > 0 {

        if count % peers == index {

            let logical = (8 * count / logical_batch) + off;
            let physical = logical / physical_batch;
            let round = physical / 8;

            while result.len() < round {
                result.push(buffer);
                buffer = Vec::with_capacity(2 + logical_batch * physical_batch / peers);
            }

            let item = T::from(line.as_str());
            buffer.push((item, logical + 1, 1));
        }

        count += 1;

        line.clear();
    }

    if buffer.len() > 0 {
        result.push(buffer);
    }

    result.reverse();
    result
}