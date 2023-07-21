extern crate timely;
extern crate differential_dataflow;

use timely::dataflow::operators::{ToStream, Capture, Map};
use timely::dataflow::operators::capture::Extract;
use differential_dataflow::AsCollection;
use differential_dataflow::operators::{Consolidate, Join, Count};

#[test]
fn join() {

    let data = timely::example(|scope| {

        let col1 = vec![((0,0), Default::default(), 1),((1,2), Default::default(), 1)]
                        .into_iter()
                        .to_stream(scope)
                        .as_collection();

        let col2 = vec![((0,'a'), Default::default(), 1),((1,'B'), Default::default(), 1)]
                        .into_iter()
                        .to_stream(scope)
                        .as_collection();

        // should produce triples `(0,0,'a')` and `(1,2,'B')`.
        col1.join(&col2).inner.capture()
    });

    let extracted = data.extract();
    assert_eq!(extracted.len(), 1);
    assert_eq!(extracted[0].1, vec![((0,(0,'a')), Default::default(), 1), ((1,(2,'B')), Default::default(), 1)]);

}

#[test]
fn join_map() {

    let data = timely::example(|scope| {
        let col1 = vec![((0,0), Default::default(),1),((1,2), Default::default(),1)].into_iter().to_stream(scope).as_collection();
        let col2 = vec![((0,'a'), Default::default(),1),((1,'B'), Default::default(),1)].into_iter().to_stream(scope).as_collection();

        // should produce records `(0 + 0,'a')` and `(1 + 2,'B')`.
        col1.join_map(&col2, |k,v1,v2| (*k + *v1, *v2)).inner.capture()
    });

    let extracted = data.extract();
    assert_eq!(extracted.len(), 1);
    assert_eq!(extracted[0].1, vec![((0,'a'), Default::default(),1), ((3,'B'), Default::default(),1)]);
}

#[test]
fn semijoin() {
    let data = timely::example(|scope| {
        let col1 = vec![((0,0), Default::default(),1),((1,2), Default::default(),1)].into_iter().to_stream(scope).as_collection();
        let col2 = vec![(0, Default::default(),1)].into_iter().to_stream(scope).as_collection();

        // should retain record `(0,0)` and discard `(1,2)`.
        col1.semijoin(&col2).inner.capture()
    });

    let extracted = data.extract();
    assert_eq!(extracted.len(), 1);
    assert_eq!(extracted[0].1, vec![((0,0), Default::default(),1)]);
}

#[test]
fn antijoin() {
    let data = timely::example(|scope| {
        let col1 = vec![((0,0), Default::default(),1),((1,2), Default::default(),1)].into_iter().to_stream(scope).as_collection();
        let col2 = vec![(0, Default::default(),1)].into_iter().to_stream(scope).as_collection();

        // should retain record `(1,2)` and discard `(0,0)`.
        col1.antijoin(&col2).consolidate().inner.capture()
    });
    let extracted = data.extract();
    assert_eq!(extracted.len(), 1);
    assert_eq!(extracted[0].1, vec![((1,2), Default::default(),1)]);
}

#[test] fn join_scale_1() { join_scaling(1); }
#[test] fn join_scale_10() { join_scaling(10); }
#[test] fn join_scale_100() { join_scaling(100); }
#[test] fn join_scale_1000() { join_scaling(1000); }
#[test] fn join_scale_10000() { join_scaling(10000); }
#[test] fn join_scale_100000() { join_scaling(100000); }
#[test] fn join_scale_1000000() { join_scaling(1000000); }

fn join_scaling(scale: u64) {

    let data = timely::example(move |scope| {

        let counts = (0 .. 1).to_stream(scope)
                             .flat_map(move |_| (0..scale).map(|i| ((), i, 1)))
                             .as_collection()
                             .count();

        let odds = counts.filter(|x| x.1 % 2 == 1);
        let evens = counts.filter(|x| x.1 % 2 == 0);

        odds.join(&evens).inner.capture()
    });

    let extracted = data.extract();
    assert_eq!(extracted.len(), 0);
}