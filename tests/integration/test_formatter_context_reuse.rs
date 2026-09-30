// Copyright © 2026 Pathway

use pathway_engine::connectors::data_format::{
    DsvFormatter, DsvSettings, Formatter, FormatterContext, IdentityFormatter, JsonLinesFormatter,
    SingleColumnFormatter,
};
use pathway_engine::engine::{Key, Timestamp, Value};

use super::helpers::assert_document_raw_byte_contents;

/// Formats `rows` one after another through a single reused context and
/// returns the payload bytes of every row (the writer's view of the row,
/// taken right after `format_into`), plus the values the context carried.
fn format_rows(
    formatter: &mut dyn Formatter,
    rows: &[(Vec<Value>, isize)],
) -> Vec<(Vec<Vec<u8>>, Vec<Value>, isize)> {
    let mut context = FormatterContext::default();
    let mut seen = Vec::new();
    for (index, (values, diff)) in rows.iter().enumerate() {
        let key = Key::for_value(&Value::from(index as i64));
        context.start_row(key, values, Timestamp(index as u64), *diff);
        formatter.format_into(&mut context).unwrap();
        assert_eq!(context.key, key);
        assert_eq!(context.time, Timestamp(index as u64));
        let payloads = context
            .payloads
            .iter()
            .map(|payload| payload.as_raw_bytes().unwrap().to_vec())
            .collect();
        seen.push((payloads, context.values.clone(), context.diff));
    }
    seen
}

#[test]
fn test_dsv_rows_reuse_the_context_without_leftovers() {
    // A long row followed by a short one: the short row's payload must not
    // carry any byte of the long one, and the header line is written once.
    let mut formatter = DsvFormatter::new(DsvSettings::new(
        None,
        vec!["a".to_string(), "b".to_string()],
        ',',
    ));
    let rows = vec![
        (
            vec![
                Value::from("a rather long first value"),
                Value::from("second"),
            ],
            1,
        ),
        (vec![Value::from("x"), Value::Int(7)], -1),
        (vec![Value::from(""), Value::None], 1),
    ];
    let seen = format_rows(&mut formatter, &rows);
    assert_eq!(seen[0].0.len(), 2, "header line and the first row");
    assert_document_raw_byte_contents(
        &seen[0].0[0].clone().into(),
        b"\"a\",\"b\",\"time\",\"diff\"",
    );
    assert_document_raw_byte_contents(
        &seen[0].0[1].clone().into(),
        b"\"a rather long first value\",\"second\",\"0\",\"1\"",
    );
    assert_eq!(seen[1].0, vec![b"\"x\",\"7\",\"1\",\"-1\"".to_vec()]);
    assert_eq!(seen[2].0, vec![b"\"\",\"None\",\"2\",\"1\"".to_vec()]);
    for ((payloads, values, diff), (row_values, row_diff)) in seen.iter().zip(&rows) {
        assert_eq!(values, row_values);
        assert_eq!(diff, row_diff);
        assert!(payloads.len() <= 2);
    }
}

#[test]
fn test_json_rows_reuse_the_context_without_leftovers() {
    let mut formatter = JsonLinesFormatter::new(vec!["a".to_string()], None);
    let rows = vec![
        (vec![Value::from("a long string value here")], 1),
        (vec![Value::Int(1)], 1),
    ];
    let seen = format_rows(&mut formatter, &rows);
    assert_eq!(
        seen[0].0,
        vec![br#"{"a":"a long string value here","diff":1,"time":0}"#.to_vec()]
    );
    assert_eq!(seen[1].0, vec![br#"{"a":1,"diff":1,"time":1}"#.to_vec()]);
}

#[test]
fn test_single_column_rows_reuse_the_context_without_leftovers() {
    let mut formatter = SingleColumnFormatter::new(0);
    let rows = vec![
        (vec![Value::from("long payload of the first row")], 1),
        (vec![Value::Bytes(b"xy".as_slice().into())], 1),
    ];
    let seen = format_rows(&mut formatter, &rows);
    assert_eq!(seen[0].0, vec![b"long payload of the first row".to_vec()]);
    assert_eq!(seen[1].0, vec![b"xy".to_vec()]);
}

#[test]
fn test_identity_rows_take_the_diff_from_the_column_per_row() {
    // The diff column decides the diff of each row, and a row's diff never
    // leaks into the next row's context.
    let mut formatter = IdentityFormatter::new(Some(1));
    let rows = vec![
        (vec![Value::from("a"), Value::Int(-1)], 1),
        (vec![Value::from("b"), Value::Int(1)], 1),
    ];
    let seen = format_rows(&mut formatter, &rows);
    assert_eq!(seen[0].2, -1);
    assert_eq!(seen[1].2, 1);
    assert_eq!(seen[0].0, vec![Vec::<u8>::new()]);
    assert_eq!(seen[1].0, vec![Vec::<u8>::new()]);
}

#[test]
fn test_cloned_context_keeps_the_row_but_not_the_scratch() {
    // A writer that keeps rows for a batch clones the context: the clone is
    // the row, and formatting the next row into the original leaves it alone.
    let mut formatter = JsonLinesFormatter::new(vec!["a".to_string()], None);
    let mut context = FormatterContext::default();
    context.start_row(Key::random(), &[Value::from("first")], Timestamp(1), 1);
    formatter.format_into(&mut context).unwrap();
    let kept = context.clone();
    context.start_row(Key::random(), &[Value::from("second")], Timestamp(2), -1);
    formatter.format_into(&mut context).unwrap();
    assert_eq!(kept.values, vec![Value::from("first")]);
    assert_eq!(kept.time, Timestamp(1));
    assert_eq!(kept.diff, 1);
    assert_eq!(
        kept.payloads[0].as_raw_bytes().unwrap(),
        br#"{"a":"first","diff":1,"time":1}"#
    );
    assert_eq!(
        context.payloads[0].as_raw_bytes().unwrap(),
        br#"{"a":"second","diff":-1,"time":2}"#
    );
}
