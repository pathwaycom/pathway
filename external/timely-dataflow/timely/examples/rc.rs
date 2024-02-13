extern crate abomonation;
extern crate timely;

use std::rc::Rc;
use timely::dataflow::{InputHandle, ProbeHandle};
use timely::dataflow::operators::{Input, Inspect, Probe};
use abomonation::Abomonation;

#[derive(Debug, Clone)]
pub struct Test {
    _field: Rc<usize>,
}

impl Abomonation for Test {
    unsafe fn entomb<W: ::std::io::Write>(&self, _write: &mut W) -> ::std::io::Result<()> { panic!() }
    unsafe fn exhume<'a,'b>(&'a mut self, _bytes: &'b mut [u8]) -> Option<&'b mut [u8]> { panic!()  }
}

fn main() {
    // initializes and runs a timely dataflow.
    timely::execute_from_args(std::env::args(), |worker| {
        // create a new input, exchange data, and inspect its output
        let index = worker.index();
        let mut input = InputHandle::new();
        let mut probe = ProbeHandle::new();
        worker.dataflow(|scope| {
            scope.input_from(&mut input)
                 //.exchange(|x| *x) // <-- cannot exchange this; Rc is not Send.
                 .inspect(move |x| println!("worker {}:\thello {:?}", index, x))
                 .probe_with(&mut probe);
        });

        // introduce data and watch!
        for round in 0..10 {
            input.send(Test { _field: Rc::new(round) } );
            input.advance_to(round + 1);
            worker.step_while(|| probe.less_than(input.time()));
        }
    }).unwrap();
}
