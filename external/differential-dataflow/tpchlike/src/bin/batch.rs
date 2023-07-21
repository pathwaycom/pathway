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

use tpchlike::{Collections, types::*, queries};

fn main() {

    timely::execute_from_args(std::env::args().skip(2), |worker| {

        let index = worker.index();
        let peers = worker.peers();

        let core_ids = core_affinity::get_core_ids().unwrap();
        core_affinity::set_for_current(core_ids[index]);

        let prefix = ::std::env::args().nth(1).unwrap();;
        let query: usize = ::std::env::args().nth(2).unwrap().parse().unwrap();

        let (mut inputs, probe, used) = worker.dataflow::<(),_,_>(move |scope| {

            // create new inputs to use in workers!
            let (cust_in, cust) = scope.new_input();
            let (line_in, line) = scope.new_input();
            let (nats_in, nats) = scope.new_input();
            let (ords_in, ords) = scope.new_input();
            let (part_in, part) = scope.new_input();
            let (psup_in, psup) = scope.new_input();
            let (regs_in, regs) = scope.new_input();
            let (supp_in, supp) = scope.new_input();

            let mut collections = Collections::new(
                cust.as_collection(),
                line.map(|(d,t,r)| (Rc::new(d),t,r)).as_collection(),
                nats.as_collection(),
                ords.as_collection(),
                part.as_collection(),
                psup.as_collection(),
                regs.as_collection(),
                supp.as_collection(),
            );

            let mut probe = timely::dataflow::ProbeHandle::new();

            match query {
                1  => queries::query01::query(&mut collections, &mut probe),
                2  => queries::query02::query(&mut collections, &mut probe),
                3  => queries::query03::query(&mut collections, &mut probe),
                4  => queries::query04::query(&mut collections, &mut probe),
                5  => queries::query05::query(&mut collections, &mut probe),
                6  => queries::query06::query(&mut collections, &mut probe),
                7  => queries::query07::query(&mut collections, &mut probe),
                8  => queries::query08::query(&mut collections, &mut probe),
                9  => queries::query09::query(&mut collections, &mut probe),
                10 => queries::query10::query(&mut collections, &mut probe),
                11 => queries::query11::query(&mut collections, &mut probe),
                12 => queries::query12::query(&mut collections, &mut probe),
                13 => queries::query13::query(&mut collections, &mut probe),
                14 => queries::query14::query(&mut collections, &mut probe),
                15 => queries::query15::query(&mut collections, &mut probe),
                16 => queries::query16::query(&mut collections, &mut probe),
                17 => queries::query17::query(&mut collections, &mut probe),
                18 => queries::query18::query(&mut collections, &mut probe),
                19 => queries::query19::query(&mut collections, &mut probe),
                20 => queries::query20::query(&mut collections, &mut probe),
                21 => queries::query21::query(&mut collections, &mut probe),
                22 => queries::query22::query(&mut collections, &mut probe),
                _ => panic!("query: {:?} unimplemented", query),
            }

            // return the various input handles, and the probe.
            ((Some(cust_in), Some(line_in), Some(nats_in), Some(ords_in), Some(part_in), Some(psup_in), Some(regs_in), Some(supp_in)), probe, collections.used())
        });

        // customer.tbl lineitem.tbl    nation.tbl  orders.tbl  part.tbl    partsupp.tbl    region.tbl  supplier.tbl
        let mut customers = if used[0] { load::<Customer>(prefix.as_str(), "customer.tbl", index, peers) } else { Vec::new() };
        let mut lineitems = if used[1] { load::<LineItem>(prefix.as_str(), "lineitem.tbl", index, peers) } else { Vec::new() };
        let mut nations = if used[2] { load::<Nation>(prefix.as_str(), "nation.tbl", index, peers) } else { Vec::new() };
        let mut orders = if used[3] { load::<Order>(prefix.as_str(), "orders.tbl", index, peers) } else { Vec::new() };
        let mut parts = if used[4] { load::<Part>(prefix.as_str(), "part.tbl", index, peers) } else { Vec::new() };
        let mut partsupps = if used[5] { load::<PartSupp>(prefix.as_str(), "partsupp.tbl", index, peers) } else { Vec::new() };
        let mut regions = if used[6] { load::<Region>(prefix.as_str(), "region.tbl", index, peers) } else { Vec::new() };
        let mut suppliers = if used[7] { load::<Supplier>(prefix.as_str(), "supplier.tbl", index, peers) } else { Vec::new() };

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
        let probe2 = worker.dataflow::<(),_,_>(|scope| {
            let (_, input) = scope.new_input::<()>();
            input.probe()
        });
        while !probe2.done() { worker.step(); }

        let timer = Instant::now();

        // introduce physical batch of data for each input with remaining data.
        for _ in 0..10 { worker.step(); } while let Some(mut data) = regions.pop() { inputs.6.as_mut().map(|x| x.send_batch(&mut data)); } inputs.6 = None;
        for _ in 0..10 { worker.step(); } while let Some(mut data) = nations.pop() { inputs.2.as_mut().map(|x| x.send_batch(&mut data)); } inputs.2 = None;
        for _ in 0..10 { worker.step(); } while let Some(mut data) = suppliers.pop() { inputs.7.as_mut().map(|x| x.send_batch(&mut data)); } inputs.7 = None;
        for _ in 0..10 { worker.step(); } while let Some(mut data) = parts.pop() { inputs.4.as_mut().map(|x| x.send_batch(&mut data)); } inputs.4 = None;
        for _ in 0..10 { worker.step(); } while let Some(mut data) = customers.pop() { inputs.0.as_mut().map(|x| x.send_batch(&mut data)); } inputs.0 = None;
        for _ in 0..10 { worker.step(); } while let Some(mut data) = lineitems.pop() { inputs.1.as_mut().map(|x| x.send_batch(&mut data)); } inputs.1 = None;
        for _ in 0..10 { worker.step(); } while let Some(mut data) = orders.pop() { inputs.3.as_mut().map(|x| x.send_batch(&mut data)); } inputs.3 = None;
        for _ in 0..10 { worker.step(); } while let Some(mut data) = partsupps.pop() { inputs.5.as_mut().map(|x| x.send_batch(&mut data)); } inputs.5 = None;

        worker.step_while(|| probe.done());

        let query_name = if query < 10 { format!("q0{}", query) } else { format!("q{}", query) };
        let elapsed = timer.elapsed();
        let nanos = elapsed.as_secs() * 1000000000 + elapsed.subsec_nanos() as u64;
        if index == 0 {
            let rate = ((peers * tuples) as f64) / (nanos as f64 / 1000000000.0);
            // Query, Logical, Physical, Workers, Rate, Time
            println!("{}\t{}\t{}\t{}", query_name, peers, rate, nanos);
        }

    }).unwrap();
}

// Returns a sequence of physical batches of ready-to-go timestamped data. Not clear that `input` can exploit the pre-arrangement yet.
fn load<T>(prefix: &str, name: &str, index: usize, peers: usize)
    -> Vec<Vec<(T, (), isize)>>
where T: for<'a> From<&'a str> {

    let mut buffer = Vec::new();

    let path = format!("{}{}", prefix, name);

    let items_file = File::open(&path).expect("didn't find items file");
    let mut items_reader =  BufReader::new(items_file);
    let mut count = 0;

    let mut line = String::new();

    while items_reader.read_line(&mut line).unwrap() > 0 {

        if count % peers == index {
            let item = T::from(line.as_str());
            buffer.push((item, (), 1));
        }

        count += 1;

        line.clear();
    }

    vec![buffer]
}
