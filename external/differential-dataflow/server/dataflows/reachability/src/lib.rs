extern crate differential_dataflow;
extern crate dd_server;

use std::rc::Rc;
use std::cell::RefCell;

use differential_dataflow::input::Input;
use differential_dataflow::operators::{Iterate, JoinCore, Threshold};
use differential_dataflow::operators::arrange::ArrangeBySelf;

use dd_server::{Environment, TraceHandle};

#[no_mangle]
pub fn build((dataflow, handles, probe, _timer, args): Environment) -> Result<(), String> {

    if args.len() != 2 { return Err(format!("expected two arguments; instead: {:?}", args)); }

    let edges = handles
        .get_mut::<Rc<RefCell<Option<TraceHandle>>>>(&args[0])?
        .borrow_mut().as_mut().unwrap().import(dataflow);

    let source = args[1].parse::<usize>().map_err(|_| format!("parse error, source: {:?}", args[1]))?; 
    let (_input, roots) = dataflow.new_collection_from(Some(source));

    // repeatedly update minimal distances each node can be reached from each root
    roots.iterate(|dists| {
        let edges = edges.enter(&dists.scope());
        let roots = roots.enter(&dists.scope());
        dists.arrange_by_self()
             .join_core(&edges, |_src, _, &dst| Some(dst))
             .concat(&roots)
             .distinct()
    })
    .probe_with(probe);

    Ok(())
}