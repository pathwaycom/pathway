extern crate timely;

use std::net::TcpStream;
use timely::dataflow::operators::capture::{Capture, EventWriter};
use timely::dataflow::operators::ToStream;

fn main() {
    timely::execute_from_args(std::env::args(), |worker| {
        let addr = format!("127.0.0.1:{}", 8000 + worker.index());
        let send = TcpStream::connect(addr).unwrap();

        worker.dataflow::<u64, _, _>(|scope| {
            (0..10u64)
                .to_stream(scope)
                .capture_into(EventWriter::new(send))
        });
    })
    .unwrap();
}
