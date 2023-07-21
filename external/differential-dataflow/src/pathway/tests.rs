use timely::dataflow::operators::generic::OperatorInfo;
use timely::progress::Antichain;
use trace::implementations::ord::OrdKeyBatch;
use trace::Batch;
use trace::Builder;

use std::rc::Rc;
use trace::implementations::ord::OrdKeySpine;
use trace::Cursor;

use trace::Trace;

use crate::pathway::cursor::BidirectionalCursor;
use crate::pathway::trace::BidirectionalTraceReader;
use crate::trace::TraceReader;

type IntegerTrace = OrdKeySpine<i32, usize, i32>;

// test basic cursor functionality:
// - first iterate over a trace containing single batch,
// - then iterate over a trace containing two batches
// the first batch contains even numbers, the second odd numbers, both from the same range
// as such, the test verifies whether CursorList can properly alternate between the cursors
#[test]
fn test_cursor() {
    let op_info = OperatorInfo::new(0, 0, &[]);
    let mut trace = IntegerTrace::new(op_info, None, None);

    let mut batch_builder: <OrdKeyBatch<i32, usize, i32> as crate::trace::Batch>::Builder =
        <OrdKeyBatch<i32, usize, i32> as Batch>::Builder::new();
    for i in 1..=10 {
        batch_builder.push((2 * i, (), i as usize, 1));
    }

    let batch_1 = batch_builder.done(
        Antichain::from_elem(0),
        Antichain::from_elem(10),
        Antichain::from_elem(0),
    );

    trace.insert(Rc::from(batch_1));

    let (mut cursor, mut storage) = trace.cursor();

    cursor.rewind_keys(&storage);
    let mut should_be = 0;
    while cursor.key_valid(&storage) {
        should_be += 2;
        assert!(cursor.key(&storage).eq(&should_be));
        cursor.step_key(&storage);
    }
    assert!(should_be.eq(&20));

    batch_builder = <OrdKeyBatch<i32, usize, i32> as Batch>::Builder::new();
    for i in 1..=10 {
        batch_builder.push((2 * i + 1, (), 10 + (i as usize), 1));
    }

    let batch_2 = batch_builder.done(
        Antichain::from_elem(10),
        Antichain::from_elem(20),
        Antichain::from_elem(0),
    );
    trace.insert(Rc::from(batch_2));

    (cursor, storage) = trace.cursor();
    cursor.rewind_keys(&storage);
    should_be = 1;
    while cursor.key_valid(&storage) {
        should_be += 1;
        assert!(cursor.key(&storage).eq(&should_be));
        cursor.step_key(&storage);
    }
    assert!(should_be.eq(&21));
}

// test basic bidirectional cursor functionality:
// - first iterate 3 times over a whole trace containing single batch, in directions: ascending, descending, ascending
// - then iterate 3 times over a whole trace containing two batches, in in directions: ascending, descending, ascending
// the first batch contains even numbers, the second odd numbers, both from the same range
// as such, the test verifies whether CursorList can properly alterante between the cursors
// also, by gluing the runs together (ascending-descending-ascending) we test whether cursor behaves as it should (it should stop)
// when it goes out of range it should invalidate itself, but is should be able to resume iteration in other direction
#[test]
fn test_bidirectional_cursor_basic() {
    let op_info = OperatorInfo::new(0, 0, &[]);
    let mut trace = IntegerTrace::new(op_info, None, None);

    let mut batch_builder: <OrdKeyBatch<i32, usize, i32> as crate::trace::Batch>::Builder =
        <OrdKeyBatch<i32, usize, i32> as Batch>::Builder::new();
    for i in 1..=10 {
        batch_builder.push((2 * i, (), i as usize, 1));
    }

    let batch_1 = batch_builder.done(
        Antichain::from_elem(0),
        Antichain::from_elem(10),
        Antichain::from_elem(0),
    );

    trace.insert(Rc::from(batch_1));

    let (mut cursor, mut storage) = trace.bidirectional_cursor();

    cursor.rewind_keys(&storage);
    let mut should_be = 0;
    while cursor.key_valid(&storage) {
        should_be += 2;
        assert!(cursor.key(&storage).eq(&should_be));
        cursor.step_key(&storage);
    }
    assert!(should_be.eq(&20));
    //ask cursor to go further, even though we are out of range
    cursor.step_key(&storage);
    cursor.step_key(&storage);
    cursor.step_back_key(&storage);
    assert!(cursor.key_valid(&storage));
    while cursor.key_valid(&storage) {
        assert!(cursor.key(&storage).eq(&should_be));
        should_be -= 2;
        cursor.step_back_key(&storage);
    }
    assert!(should_be.eq(&0));
    //ask cursor to go further back, even though we are out of range
    cursor.step_back_key(&storage);
    cursor.step_back_key(&storage);
    assert!(!cursor.key_valid(&storage));
    cursor.step_key(&storage);
    while cursor.key_valid(&storage) {
        should_be += 2;
        assert!(cursor.key(&storage).eq(&should_be));
        cursor.step_key(&storage);
    }
    assert!(should_be.eq(&20));

    batch_builder = <OrdKeyBatch<i32, usize, i32> as Batch>::Builder::new();
    for i in 1..=10 {
        batch_builder.push((2 * i + 1, (), 10 + (i as usize), 1));
    }

    let batch_2 = batch_builder.done(
        Antichain::from_elem(10),
        Antichain::from_elem(20),
        Antichain::from_elem(0),
    );
    trace.insert(Rc::from(batch_2));

    (cursor, storage) = trace.bidirectional_cursor();
    cursor.rewind_keys(&storage);
    should_be = 1;
    while cursor.key_valid(&storage) {
        should_be += 1;
        assert!(cursor.key(&storage).eq(&should_be));
        cursor.step_key(&storage);
    }
    assert!(should_be.eq(&21));

    cursor.step_back_key(&storage);
    assert!(cursor.key_valid(&storage));
    while cursor.key_valid(&storage) {
        assert!(cursor.key(&storage).eq(&should_be));
        should_be -= 1;
        cursor.step_back_key(&storage);
    }
    assert!(should_be.eq(&1));
    cursor.step_key(&storage);
    while cursor.key_valid(&storage) {
        should_be += 1;
        assert!(cursor.key(&storage).eq(&should_be));
        cursor.step_key(&storage);
    }
    assert!(should_be.eq(&21));
}

#[test]
fn test_bidirectional_cursor_seek() {
    let op_info = OperatorInfo::new(0, 0, &[]);
    let mut trace = IntegerTrace::new(op_info, None, None);

    let mut batch_builder: <OrdKeyBatch<i32, usize, i32> as crate::trace::Batch>::Builder =
        <OrdKeyBatch<i32, usize, i32> as Batch>::Builder::new();
    for i in 1..=10 {
        batch_builder.push((4 * i, (), i as usize, 1));
    }

    let batch_1 = batch_builder.done(
        Antichain::from_elem(0),
        Antichain::from_elem(10),
        Antichain::from_elem(0),
    );

    trace.insert(Rc::from(batch_1));

    let (mut cursor, mut storage) = trace.bidirectional_cursor();

    //upper bound and move down
    cursor.rewind_keys(&storage);
    cursor.seek_key(&storage, &11);
    assert!(cursor.key(&storage).eq(&12));
    cursor.step_back_key(&storage);
    assert!(cursor.key(&storage).eq(&8));
    //upper bound and move up
    cursor.rewind_keys(&storage);
    cursor.seek_key(&storage, &11);
    assert!(cursor.key(&storage).eq(&12));
    cursor.step_key(&storage);
    assert!(cursor.key(&storage).eq(&16));
    //lower bound and move down
    cursor.rewind_keys(&storage);
    cursor.seek_smaller_eq_key(&storage, &11);
    assert!(cursor.key(&storage).eq(&8));
    cursor.step_back_key(&storage);
    assert!(cursor.key(&storage).eq(&4));
    //lower bound and move up
    cursor.rewind_keys(&storage);
    cursor.seek_smaller_eq_key(&storage, &11);
    assert!(cursor.key(&storage).eq(&8));
    cursor.step_key(&storage);
    assert!(cursor.key(&storage).eq(&12));
    //find exact via upper bound, move down
    cursor.rewind_keys(&storage);
    cursor.seek_key(&storage, &12);
    assert!(cursor.key(&storage).eq(&12));
    cursor.step_back_key(&storage);
    assert!(cursor.key(&storage).eq(&8));
    //find exact via lower bounds, move up
    cursor.rewind_keys(&storage);
    cursor.seek_smaller_eq_key(&storage, &12);
    assert!(cursor.key(&storage).eq(&12));
    cursor.step_key(&storage);
    assert!(cursor.key(&storage).eq(&16));

    batch_builder = <OrdKeyBatch<i32, usize, i32> as Batch>::Builder::new();
    for i in 1..=10 {
        batch_builder.push((4 * i + 2, (), 10 + (i as usize), 1));
    }

    let batch_2 = batch_builder.done(
        Antichain::from_elem(10),
        Antichain::from_elem(20),
        Antichain::from_elem(0),
    );
    trace.insert(Rc::from(batch_2));
    (cursor, storage) = trace.bidirectional_cursor();

    //below, we are testing only some configurations

    //upper bound and move down
    cursor.rewind_keys(&storage);
    cursor.seek_key(&storage, &11);
    assert!(cursor.key(&storage).eq(&12));
    cursor.step_back_key(&storage);
    assert!(cursor.key(&storage).eq(&10));

    cursor.rewind_keys(&storage);
    cursor.seek_key(&storage, &13);
    assert!(cursor.key(&storage).eq(&14));
    cursor.step_back_key(&storage);
    assert!(cursor.key(&storage).eq(&12));

    //lower bound and move up
    cursor.rewind_keys(&storage);
    cursor.seek_smaller_eq_key(&storage, &11);
    assert!(cursor.key(&storage).eq(&10));
    cursor.step_key(&storage);
    assert!(cursor.key(&storage).eq(&12));

    cursor.rewind_keys(&storage);
    cursor.seek_smaller_eq_key(&storage, &13);
    assert!(cursor.key(&storage).eq(&12));
    cursor.step_key(&storage);
    assert!(cursor.key(&storage).eq(&14));

    //find exact via lower bounds, move up
    cursor.rewind_keys(&storage);
    cursor.seek_smaller_eq_key(&storage, &12);
    assert!(cursor.key(&storage).eq(&12));
    cursor.step_key(&storage);
    assert!(cursor.key(&storage).eq(&14));

    // seek lower bound too low
    cursor.rewind_keys(&storage);
    cursor.seek_smaller_eq_key(&storage, &3);
    assert!(!cursor.key_valid(&storage));
    cursor.step_key(&storage);
    assert!(cursor.key(&storage).eq(&4));

    //seek upper bound too low
    cursor.rewind_keys(&storage);
    cursor.seek_key(&storage, &3);
    assert!(cursor.key(&storage).eq(&4));

    // seek lower bound too high
    cursor.rewind_keys(&storage);
    cursor.seek_smaller_eq_key(&storage, &100);
    assert!(cursor.key(&storage).eq(&42));

    //seek upper bound too high
    cursor.rewind_keys(&storage);
    cursor.seek_key(&storage, &100);
    assert!(!cursor.key_valid(&storage));
    cursor.step_back_key(&storage);
    assert!(cursor.key(&storage).eq(&42));
}
