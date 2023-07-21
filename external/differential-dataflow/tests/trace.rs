extern crate timely;
extern crate differential_dataflow;

use std::rc::Rc;

use timely::dataflow::operators::generic::OperatorInfo;
use timely::progress::{Antichain, frontier::AntichainRef};

use differential_dataflow::trace::implementations::ord::OrdValBatch;
use differential_dataflow::trace::{Trace, TraceReader, Batch, Batcher};
use differential_dataflow::trace::cursor::Cursor;
use differential_dataflow::trace::implementations::spine_fueled::Spine;

pub type OrdValSpine<K, V, T, R> = Spine<Rc<OrdValBatch<K, V, T, R>>>;

type IntegerTrace = OrdValSpine<u64, u64, usize, i64>;

fn get_trace() -> Spine<Rc<OrdValBatch<u64, u64, usize, i64>>> {
    let op_info = OperatorInfo::new(0, 0, &[]);
    let mut trace = IntegerTrace::new(op_info, None, None);
    {
        let mut batcher = <<IntegerTrace as TraceReader>::Batch as Batch>::Batcher::new();

        use timely::communication::message::RefOrMut;
        batcher.push_batch(RefOrMut::Mut(&mut vec![
            ((1, 2), 0, 1),
            ((2, 3), 1, 1),
            ((2, 3), 2, -1),
        ]));

        let batch_ts = &[1, 2, 3];
        let batches = batch_ts.iter().map(move |i| batcher.seal(Antichain::from_elem(*i)));
        for b in batches {
            trace.insert(b);
        }
    }
    trace
}

#[test]
fn test_trace() {
    let mut trace = get_trace();

    let (mut cursor1, storage1) = trace.cursor_through(AntichainRef::new(&[1])).unwrap();
    let vec_1 = cursor1.to_vec(&storage1);
    assert_eq!(vec_1, vec![((1, 2), vec![(0, 1)])]);

    let (mut cursor2, storage2) = trace.cursor_through(AntichainRef::new(&[2])).unwrap();
    let vec_2 = cursor2.to_vec(&storage2);
    println!("--> {:?}", vec_2);
    assert_eq!(vec_2, vec![
               ((1, 2), vec![(0, 1)]),
               ((2, 3), vec![(1, 1)]),
    ]);

    let (mut cursor3, storage3) = trace.cursor_through(AntichainRef::new(&[3])).unwrap();
    let vec_3 = cursor3.to_vec(&storage3);
    assert_eq!(vec_3, vec![
               ((1, 2), vec![(0, 1)]),
               ((2, 3), vec![(1, 1), (2, -1)]),
    ]);

    let (mut cursor4, storage4) = trace.cursor();
    let vec_4 = cursor4.to_vec(&storage4);
    assert_eq!(vec_4, vec_3);
}
