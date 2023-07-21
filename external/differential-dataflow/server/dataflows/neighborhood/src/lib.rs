extern crate differential_dataflow;
extern crate dd_server;

use std::rc::Rc;
use std::cell::RefCell;

use differential_dataflow::input::Input;
use differential_dataflow::operators::JoinCore;
use differential_dataflow::operators::Consolidate;

use dd_server::{Environment, TraceHandle};

// load ./dataflows/neighborhood/target/release/libneighborhood.dylib build <graph_name> 0

#[no_mangle]
pub fn build((dataflow, handles, probe, _timer, args): Environment) -> Result<(), String> {

    if args.len() != 2 { return Err(format!("expected two arguments; instead: {:?}", args)); }

    let edges = handles
        .get_mut::<Rc<RefCell<Option<TraceHandle>>>>(&args[0])?
        .borrow_mut().as_mut().unwrap().import(dataflow);

    let source = args[1].parse::<usize>().map_err(|_| format!("parse error, source: {:?}", args[1]))?; 
    let (_input, query) = dataflow.new_collection_from(Some(source));

    let timer = ::std::time::Instant::now();

    query
        .map(|x| (x, x))
        .join_core(&edges, |_n, &q, &d| Some((d, q)))   // one hop
        .join_core(&edges, |_n, &q, &d| Some((d, q)))   // two hops
        .join_core(&edges, |_n, &q, &d| Some((d, q)))   // three hops
        .map(|x| x.1)
        .consolidate()
        .inspect(move |x| println!("{:?}:\t{:?}", timer.elapsed(), x))
        .probe_with(probe);

    Ok(())
}