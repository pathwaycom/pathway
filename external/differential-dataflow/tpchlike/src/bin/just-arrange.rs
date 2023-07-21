extern crate timely;
extern crate differential_dataflow;
extern crate core_affinity;
extern crate tpchlike;

use std::rc::Rc;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::time::Instant;

use timely::dataflow::operators::*;
use differential_dataflow::AsCollection;
use tpchlike::{Context, Collections, types::*, queries};

fn main() {

    timely::execute_from_args(std::env::args().skip(4), |worker| {

        let timer = worker.timer();
        let index = worker.index();
        let peers = worker.peers();

        let core_ids = core_affinity::get_core_ids().unwrap();
        core_affinity::set_for_current(core_ids[index]);

        let prefix = ::std::env::args().nth(1).unwrap();;
        let logical_batch = ::std::env::args().nth(2).unwrap().parse::<usize>().unwrap();
        let physical_batch = ::std::env::args().nth(3).unwrap().parse::<usize>().unwrap();
        let arrange: bool = ::std::env::args().nth(4).unwrap().parse().unwrap();
        let seal: bool = ::std::env::args().any(|x| x == "seal-inputs");
        let window: bool = ::std::env::args().any(|x| x == "window");

        let (mut inputs, probe, used, mut traces) = worker.dataflow::<usize,_,_>(move |scope| {

            // create new inputs to use in workers!
            let (cust_in, cust) = scope.new_input();
            let (line_in, mut line) = scope.new_input();
            let (nats_in, nats) = scope.new_input();
            let (ords_in, ords) = scope.new_input();
            let (part_in, part) = scope.new_input();
            let (psup_in, psup) = scope.new_input();
            let (regs_in, regs) = scope.new_input();
            let (supp_in, supp) = scope.new_input();

            if window {
                line = line.map(|(x,y,z): (LineItem, usize, isize)| (x,y+1,-z)).concat(&line);
            }

            let collections = Collections::new(
                cust.as_collection(),
                line.map(|(d,t,r)| (Rc::new(d),t,r)).as_collection(),
                nats.as_collection(),
                ords.as_collection(),
                part.as_collection(),
                psup.as_collection(),
                regs.as_collection(),
                supp.as_collection(),
            );

            // use timely::dataflow::ProbeHandle;

            let mut context = Context::new(scope.clone(), collections);

            context.index = arrange;

            // return the various input handles, and the list of probes.
            let inputs = (
                Some(cust_in),
                Some(line_in),
                Some(nats_in),
                Some(ords_in),
                Some(part_in),
                Some(psup_in),
                Some(regs_in),
                Some(supp_in),
            );

            (inputs, context.probe, context.collections.used(), context.arrangements)
        });

        // customer.tbl lineitem.tbl    nation.tbl  orders.tbl  part.tbl    partsupp.tbl    region.tbl  supplier.tbl
        let mut customers = if used[0] { load::<Customer>(prefix.as_str(), "customer.tbl", index, peers, logical_batch, physical_batch, 0) } else { Vec::new() };
        let mut lineitems = if used[1] { load::<LineItem>(prefix.as_str(), "lineitem.tbl", index, peers, logical_batch, physical_batch, 1) } else { Vec::new() };
        let mut nations = if used[2] { load::<Nation>(prefix.as_str(), "nation.tbl", index, peers, logical_batch, physical_batch, 2) } else { Vec::new() };
        let mut orders = if used[3] { load::<Order>(prefix.as_str(), "orders.tbl", index, peers, logical_batch, physical_batch, 3) } else { Vec::new() };
        let mut parts = if used[4] { load::<Part>(prefix.as_str(), "part.tbl", index, peers, logical_batch, physical_batch, 4) } else { Vec::new() };
        let mut partsupps = if used[5] { load::<PartSupp>(prefix.as_str(), "partsupp.tbl", index, peers, logical_batch, physical_batch, 5) } else { Vec::new() };
        let mut regions = if used[6] { load::<Region>(prefix.as_str(), "region.tbl", index, peers, logical_batch, physical_batch, 6) } else { Vec::new() };
        let mut suppliers = if used[7] { load::<Supplier>(prefix.as_str(), "supplier.tbl", index, peers, logical_batch, physical_batch, 7) } else { Vec::new() };

        println!("{:?}\tInput loaded", timer.elapsed());

        let mut tuples = 0usize;
        tuples += customers.iter().map(|x| x.len()).sum::<usize>();
        tuples += lineitems.iter().map(|x| x.len()).sum::<usize>();
        tuples += nations.iter().map(|x| x.len()).sum::<usize>();
        tuples += orders.iter().map(|x| x.len()).sum::<usize>();
        tuples += parts.iter().map(|x| x.len()).sum::<usize>();
        tuples += partsupps.iter().map(|x| x.len()).sum::<usize>();
        tuples += regions.iter().map(|x| x.len()).sum::<usize>();
        tuples += suppliers.iter().map(|x| x.len()).sum::<usize>();

        // Synchronize before starting the timer.
        let next_round = 1;
        inputs.0.as_mut().map(|x| x.advance_to(next_round));
        inputs.1.as_mut().map(|x| x.advance_to(next_round));
        inputs.2.as_mut().map(|x| x.advance_to(next_round));
        inputs.3.as_mut().map(|x| x.advance_to(next_round));
        inputs.4.as_mut().map(|x| x.advance_to(next_round));
        inputs.5.as_mut().map(|x| x.advance_to(next_round));
        inputs.6.as_mut().map(|x| x.advance_to(next_round));
        inputs.7.as_mut().map(|x| x.advance_to(next_round));

        let time = next_round;
        worker.step_while(|| probe.less_than(&time));

        let timer = Instant::now();
        let mut round = 0;
        while customers.len() > 0 || lineitems.len() > 0 || nations.len() > 0 || orders.len() > 0 || parts.len() > 0 || partsupps.len() > 0 || regions.len() > 0 || suppliers.len() > 0 {

            println!("{:?}\tInjecting data for round {}", timer.elapsed(), round);

            // introduce physical batch of data for each input with remaining data.
            if let Some(mut data) = customers.pop() { inputs.0.as_mut().map(|x| x.send_batch(&mut data)); } else { if seal { inputs.0 = None; } }
            if let Some(mut data) = lineitems.pop() { inputs.1.as_mut().map(|x| x.send_batch(&mut data)); } else { if seal { inputs.1 = None; } }
            if let Some(mut data) = nations.pop() { inputs.2.as_mut().map(|x| x.send_batch(&mut data)); } else { if seal { inputs.2 = None; } }
            if let Some(mut data) = orders.pop() { inputs.3.as_mut().map(|x| x.send_batch(&mut data)); } else { if seal { inputs.3 = None; } }
            if let Some(mut data) = parts.pop() { inputs.4.as_mut().map(|x| x.send_batch(&mut data)); } else { if seal { inputs.4 = None; } }
            if let Some(mut data) = partsupps.pop() { inputs.5.as_mut().map(|x| x.send_batch(&mut data)); } else { if seal { inputs.5 = None; } }
            if let Some(mut data) = regions.pop() { inputs.6.as_mut().map(|x| x.send_batch(&mut data)); } else { if seal { inputs.6 = None; } }
            if let Some(mut data) = suppliers.pop() { inputs.7.as_mut().map(|x| x.send_batch(&mut data)); } else { if seal { inputs.7 = None; } }

            // catch all inputs up to the same (next) round.
            let next_round = 1 + 8 * (round + 1) * physical_batch;
            inputs.0.as_mut().map(|x| x.advance_to(next_round));
            inputs.1.as_mut().map(|x| x.advance_to(next_round));
            inputs.2.as_mut().map(|x| x.advance_to(next_round));
            inputs.3.as_mut().map(|x| x.advance_to(next_round));
            inputs.4.as_mut().map(|x| x.advance_to(next_round));
            inputs.5.as_mut().map(|x| x.advance_to(next_round));
            inputs.6.as_mut().map(|x| x.advance_to(next_round));
            inputs.7.as_mut().map(|x| x.advance_to(next_round));

            let time = next_round;

            traces.set_logical_compaction(&[next_round]);

            worker.step_while(|| probe.less_than(&time));
            round += 1;
        }

        // Finish outstanding work before stopping the timer.
        let next_round = usize::max_value();
        inputs.0.as_mut().map(|x| x.advance_to(next_round));
        inputs.1.as_mut().map(|x| x.advance_to(next_round));
        inputs.2.as_mut().map(|x| x.advance_to(next_round));
        inputs.3.as_mut().map(|x| x.advance_to(next_round));
        inputs.4.as_mut().map(|x| x.advance_to(next_round));
        inputs.5.as_mut().map(|x| x.advance_to(next_round));
        inputs.6.as_mut().map(|x| x.advance_to(next_round));
        inputs.7.as_mut().map(|x| x.advance_to(next_round));

        let time = next_round;
        worker.step_while(|| probe.less_than(&time));

        // let query_name = if query < 10 { format!("q0{}", query) } else { format!("q{}", query) };
        let elapsed = timer.elapsed();
        let nanos = elapsed.as_secs() * 1000000000 + elapsed.subsec_nanos() as u64;
        if index == 0 {
            let rate = ((peers * tuples) as f64) / (nanos as f64 / 1000000000.0);
            // Query, Logical, Physical, Workers, Rate, Time
            println!("{}\t{}\t{}\t{}\t{}", logical_batch, physical_batch, peers, rate, nanos);
            // println!("query: {}, elapsed: {:?}, tuples: {:?}, rate: {:?}", query_name, timer.elapsed(), peers * tuples, ((peers * tuples) as f64) / (nanos as f64 / 1000000000.0));
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