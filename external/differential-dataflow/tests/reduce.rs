extern crate timely;
extern crate differential_dataflow;

use timely::dataflow::operators::{ToStream, Capture, Map};
use timely::dataflow::operators::capture::Extract;
use differential_dataflow::AsCollection;
use differential_dataflow::operators::{Reduce, Count};

#[test]
fn reduce() {

    let data = timely::example(|scope| {

        let col1 = vec![((0,0), Default::default(), 1),((1,2),Default::default(), 1),((1,1),Default::default(), 1)]
                        .into_iter()
                        .to_stream(scope)
                        .as_collection();

        col1.reduce(|_,s,t| t.push((*s[0].0, s.len() as isize))).inner.capture()
    });

    let extracted = data.extract();
    assert_eq!(extracted.len(), 1);
    assert_eq!(extracted[0].1, vec![((0,0),Default::default(), 1), ((1,1),Default::default(), 2)]);
}

#[test]
fn reduce_scaling() {

    let data = timely::example(|scope| {

        // let scale = 100;
        let scale = 100_000;

        (0 .. 1).to_stream(scope)
                .flat_map(move |_| (0..scale).map(|i| ((), i, 1)))
                .as_collection()
                .count()
                .inner
                .capture()
    });

    let extracted = data.extract();
    assert_eq!(extracted.len(), 1);
}