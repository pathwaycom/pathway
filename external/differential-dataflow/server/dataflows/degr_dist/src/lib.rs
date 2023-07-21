extern crate timely;
extern crate differential_dataflow;
extern crate dd_server;

use std::rc::Rc;
use std::cell::RefCell;

// use timely::dataflow::operators::{Probe, Operator};
use differential_dataflow::operators::CountTotal;
use dd_server::{Environment, TraceHandle};

// load ./dataflows/degr_dist/target/release/libdegr_dist.dylib build <graph_name>

#[no_mangle]
pub fn build((dataflow, handles, probe, _timer, args): Environment) -> Result<(), String> {

    if args.len() != 1 { return Err(format!("expected one argument, instead: {:?}", args)); }

    handles
        .get_mut::<Rc<RefCell<Option<TraceHandle>>>>(&args[0])?
        .borrow_mut().as_mut().unwrap()
        .import(dataflow)
        .as_collection(|&src,_dst| src)
        .count_total()
        .map(|(_deg, cnt)| cnt as usize)
        .count_total()
        .probe_with(probe);

    Ok(())
}