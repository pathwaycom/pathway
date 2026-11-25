// Copyright © 2026 Pathway

use crate::helpers::ErrorPlacement;
use crate::helpers::ReplaceErrors;

use super::helpers::{
    assert_error_shown, assert_error_shown_for_reader_context, new_filesystem_reader,
};

use std::collections::HashMap;
use std::collections::HashSet;

use pathway_engine::connectors::data_format::{
    json_schema::JsonSchema, DsvParser, DsvSettings, InnerSchemaField, ParseResult, ParsedEvent,
    ParsedEventWithErrors, Parser,
};
use pathway_engine::connectors::data_storage::{
    ConnectorMode, DataEventType, ReadMethod, ReadResult, ReadResult::Data, Reader, ReaderContext,
};
use pathway_engine::engine::{Key, Type, Value};

#[test]
fn test_dsv_nested_json_validation_and_error_paths() -> eyre::Result<()> {
    let validation: JsonSchema = serde_json::from_value(serde_json::json!({
        "type": "object", "fields": [{"name": "two", "schema": {
            "type": "list", "items": {"type": "object", "fields": [
                {"name": "three", "schema": {"type": "float"}},
                {"name": "unit", "schema": {"type": "string"}, "default": {"value": "USD"}}
            ]}
        }}]
    }))?;
    for transport in 0..3 {
        let context = |token: &str| match transport {
            0 => ReaderContext::from_raw_bytes(DataEventType::Insert, token.as_bytes().to_vec()),
            1 => {
                ReaderContext::from_tokenized_entries(DataEventType::Insert, vec![token.to_owned()])
            }
            _ => ReaderContext::from_csv_record(
                DataEventType::Insert,
                csv::StringRecord::from(vec![token]),
            ),
        };
        let mut field = InnerSchemaField::new(Type::Json, None);
        field.json_schema = Some(validation.clone());
        let mut parser = DsvParser::new(
            DsvSettings::new(None, vec!["one".to_owned()], ';'),
            [("one".to_owned(), field)].into(),
        )?;
        assert!(parser.parse(&context("one")).unwrap().is_empty());
        let events = parser
            .parse(&context(r#"{"two":[{"three":1}],"extra":true}"#))
            .unwrap();
        let [ParsedEventWithErrors::Insert((_, values))] = events.as_slice() else {
            panic!("expected one insert event");
        };
        assert_eq!(
            values[0].as_ref().unwrap(),
            &Value::from(serde_json::json!({
                "two": [{"three": 1, "unit": "USD"}], "extra": true
            }))
        );

        let events = parser
            .parse(&context(r#"{"two":[{"three":"wrong"}]}"#))
            .unwrap();
        let [ParsedEventWithErrors::Insert((_, values))] = events.as_slice() else {
            panic!("expected one insert event with a field error");
        };
        let error = values[0].as_ref().unwrap_err().to_string();
        assert!(error.contains("/one/two/0/three"), "{error}");
        assert!(error.contains("expected float"), "{error}");
    }
    Ok(())
}

#[test]
fn test_dsv_nested_json_empty_defaults_and_explicit_null() -> eyre::Result<()> {
    use serde_json::json;
    use std::sync::Arc;

    let object = json!({"type": "object", "fields": [
        {"name": "value", "schema": {"type": "int"}}
    ]});
    for optional in [false, true] {
        let dtype = if optional {
            Type::Optional(Arc::new(Type::Json))
        } else {
            Type::Json
        };
        let validation = if optional {
            json!({"type": "optional", "wrapped": object})
        } else {
            object.clone()
        };
        let default = Value::from(json!({"value": 2}));
        let mut field = InnerSchemaField::new(dtype, Some(default.clone()));
        field.json_schema = Some(serde_json::from_value(validation)?);
        let mut parser = DsvParser::new(
            DsvSettings::new(None, vec!["one".to_owned()], ','),
            [("one".to_owned(), field)].into(),
        )?;
        let context = |token: &str| {
            ReaderContext::from_tokenized_entries(DataEventType::Insert, vec![token.to_owned()])
        };
        parser.parse(&context("one")).unwrap();
        for token in ["", "null", " NULL ", "None", "  "] {
            let events = parser.parse(&context(token)).unwrap();
            let [ParsedEventWithErrors::Insert((_, values))] = events.as_slice() else {
                panic!("expected one insert event");
            };
            if token.is_empty() {
                assert_eq!(values[0].as_ref().unwrap(), &default);
            } else if optional {
                assert_eq!(values[0].as_ref().unwrap(), &Value::None);
            } else {
                assert!(values[0].is_err(), "{token:?}");
            }
        }
    }
    Ok(())
}

#[test]
fn test_dsv_read_ok() -> eyre::Result<()> {
    let mut reader = new_filesystem_reader(
        "tests/data/sample.txt",
        ConnectorMode::Static,
        ReadMethod::ByLine,
        "*",
        false,
    )?;
    let mut parser = DsvParser::new(
        DsvSettings::new(Some(vec!["a".to_string()]), vec!["b".to_string()], ','),
        [
            ("a".to_string(), InnerSchemaField::new(Type::String, None)),
            ("b".to_string(), InnerSchemaField::new(Type::String, None)),
        ]
        .into(),
    )?;

    reader.read()?;
    let header_read_result = reader.read()?;
    match header_read_result {
        Data(bytes, _) => {
            let row_parse_result: ParseResult = parser.parse(&bytes);
            assert!(row_parse_result.is_ok());
        }
        _ => panic!("header_read_result is not Data"),
    }

    let row_read_result = reader.read()?;

    match row_read_result {
        Data(bytes, _) => {
            let row_parse_result: Vec<_> = parser
                .parse(&bytes)
                .expect("entries should parse correctly")
                .into_iter()
                .map(|entry| entry.replace_errors())
                .collect();
            assert_eq!(
                row_parse_result,
                vec![ParsedEvent::Insert((
                    Some(vec![Value::from("0")]),
                    vec![Value::from("0")]
                ))]
            );
        }
        _ => panic!("row_read_result is not Data"),
    }

    Ok(())
}

#[test]
fn test_dsv_column_does_not_exist() -> eyre::Result<()> {
    let reader = new_filesystem_reader(
        "tests/data/sample.txt",
        ConnectorMode::Static,
        ReadMethod::ByLine,
        "*",
        false,
    )?;
    let parser = DsvParser::new(
        DsvSettings::new(Some(vec!["a".to_string()]), vec!["c".to_string()], ','),
        [
            ("a".to_string(), InnerSchemaField::new(Type::Int, None)),
            ("c".to_string(), InnerSchemaField::new(Type::Int, None)),
        ]
        .into(),
    )?;

    assert_error_shown(
        Box::new(reader),
        Box::new(parser),
        r#"some fields weren't found in the header (fields present in table: ["a", "b"], fields specified in connector: ["c"])"#,
        ErrorPlacement::Message,
    );

    Ok(())
}

#[test]
fn test_dsv_rows_parsing_ignore_type() -> eyre::Result<()> {
    let mut reader = new_filesystem_reader(
        "tests/data/sample_str_int.txt",
        ConnectorMode::Static,
        ReadMethod::ByLine,
        "*",
        false,
    )?;
    let mut parser = DsvParser::new(
        DsvSettings::new(Some(vec!["a".to_string()]), vec!["b".to_string()], ','),
        [
            ("a".to_string(), InnerSchemaField::new(Type::String, None)),
            ("b".to_string(), InnerSchemaField::new(Type::Int, None)),
        ]
        .into(),
    )?;

    reader.read()?;
    let header_read_result = reader.read()?;
    match header_read_result {
        Data(bytes, _) => {
            let row_parse_result: ParseResult = parser.parse(&bytes);
            assert!(row_parse_result.is_ok());
        }
        _ => panic!("header_read_result is not Data"),
    }

    let row_read_result = reader.read()?;
    match row_read_result {
        Data(bytes, _) => {
            let row_parse_result: ParseResult = parser.parse(&bytes);
            assert!(row_parse_result.is_ok());
        }
        _ => panic!("row_read_result is not Data"),
    }

    Ok(())
}

#[test]
fn test_dsv_not_enough_columns() -> eyre::Result<()> {
    let mut reader = new_filesystem_reader(
        "tests/data/sample_bad_lines.txt",
        ConnectorMode::Static,
        ReadMethod::ByLine,
        "*",
        false,
    )?;
    let mut parser = DsvParser::new(
        DsvSettings::new(Some(vec!["a".to_string()]), vec!["b".to_string()], ','),
        [
            ("a".to_string(), InnerSchemaField::new(Type::Int, None)),
            ("b".to_string(), InnerSchemaField::new(Type::Int, None)),
        ]
        .into(),
    )?;

    let _ = reader
        .read()
        .expect("new data source read event should not fail");

    let row_read_result = reader
        .read()
        .expect("first line read event should not fail");
    if let Data(ctx, _) = row_read_result {
        let _ = parser
            .parse(&ctx)
            .expect("parsing of the header should work");
    } else {
        panic!("header is not Data");
    }

    let row_read_result = reader
        .read()
        .expect("second line read event should not fail");
    if let Data(ctx, _) = row_read_result {
        assert_error_shown_for_reader_context(
            &ctx,
            Box::new(parser),
            "too small number of csv tokens in the line: 1",
            ErrorPlacement::Message,
        );
    } else {
        panic!("Data enum element was expected");
    }

    Ok(())
}

#[test]
fn test_dsv_autogenerate_pkey() -> eyre::Result<()> {
    let mut reader = new_filesystem_reader(
        "tests/data/sample.txt",
        ConnectorMode::Static,
        ReadMethod::ByLine,
        "*",
        false,
    )?;
    let mut parser = DsvParser::new(
        DsvSettings::new(None, vec!["a".to_string(), "b".to_string()], ','),
        [
            ("a".to_string(), InnerSchemaField::new(Type::Int, None)),
            ("b".to_string(), InnerSchemaField::new(Type::Int, None)),
        ]
        .into(),
    )?;

    let mut keys: HashSet<Key> = HashSet::new();

    loop {
        let read_result = reader.read()?;
        match read_result {
            ReadResult::Data(bytes, _) => {
                let row_parse_result: ParseResult = parser.parse(&bytes);
                assert!(row_parse_result.is_ok());

                for event in row_parse_result.expect("entries should parse correctly") {
                    let event = event.replace_errors();
                    if let ParsedEvent::Insert((raw_key, _values)) = event {
                        let key = match raw_key {
                            None => Key::random(),
                            Some(values) => Key::for_values(&values),
                        };
                        assert!(!keys.contains(&key));
                        keys.insert(key);
                    }
                }
            }
            ReadResult::Finished => break,
            ReadResult::FinishedSource { .. } => continue,
            ReadResult::NewSource(_) => continue,
        }
    }

    Ok(())
}

#[test]
fn test_dsv_composite_pkey() -> eyre::Result<()> {
    let mut reader = new_filesystem_reader(
        "tests/data/sample_composite_pkey.txt",
        ConnectorMode::Static,
        ReadMethod::ByLine,
        "*",
        false,
    )?;
    let mut parser = DsvParser::new(
        DsvSettings::new(
            Some(vec!["a".to_string(), "b".to_string()]),
            vec!["c".to_string()],
            ',',
        ),
        [
            ("a".to_string(), InnerSchemaField::new(Type::Int, None)),
            ("b".to_string(), InnerSchemaField::new(Type::Int, None)),
            ("c".to_string(), InnerSchemaField::new(Type::Int, None)),
        ]
        .into(),
    )?;

    let mut keys = Vec::new();

    loop {
        let read_result = reader.read()?;
        match read_result {
            ReadResult::Data(bytes, _) => {
                let row_parse_result: ParseResult = parser.parse(&bytes);
                assert!(row_parse_result.is_ok());

                for event in row_parse_result.expect("entries should parse correctly") {
                    let event = event.replace_errors();
                    if let ParsedEvent::Insert((raw_key, _values)) = event {
                        let key = match raw_key {
                            None => Key::random(),
                            Some(values) => Key::for_values(&values),
                        };
                        keys.push(key);
                    }
                }
            }
            ReadResult::Finished => break,
            ReadResult::FinishedSource { .. } => continue,
            ReadResult::NewSource(_) => continue,
        }
    }

    assert_eq!(keys.len(), 3);
    assert!(keys[0] != keys[1]);
    assert!(keys[1] != keys[2]);
    assert_eq!(keys[0], keys[2]);

    Ok(())
}

#[test]
fn test_dsv_read_schema_ok() -> eyre::Result<()> {
    let mut schema = HashMap::new();
    schema.insert("key".to_string(), InnerSchemaField::new(Type::String, None));
    schema.insert("bool".to_string(), InnerSchemaField::new(Type::Bool, None));
    schema.insert("int".to_string(), InnerSchemaField::new(Type::Int, None));
    schema.insert(
        "float".to_string(),
        InnerSchemaField::new(Type::Float, None),
    );
    schema.insert(
        "string".to_string(),
        InnerSchemaField::new(Type::String, None),
    );

    let mut reader = new_filesystem_reader(
        "tests/data/schema.txt",
        ConnectorMode::Static,
        ReadMethod::ByLine,
        "*",
        false,
    )?;
    let mut parser = DsvParser::new(
        DsvSettings::new(
            Some(vec!["key".to_string()]),
            vec![
                "bool".to_string(),
                "int".to_string(),
                "float".to_string(),
                "string".to_string(),
            ],
            ',',
        ),
        schema,
    )?;

    reader.read()?;
    let header_read_result = reader.read()?;
    match header_read_result {
        Data(bytes, _) => {
            let row_parse_result: ParseResult = parser.parse(&bytes);
            assert!(row_parse_result.is_ok());
        }
        _ => panic!("header_read_result is not Data"),
    }

    let row_read_result = reader.read()?;

    match row_read_result {
        Data(bytes, _) => {
            let row_parse_result: Vec<_> = parser
                .parse(&bytes)
                .expect("entries should parse correctly")
                .into_iter()
                .map(|entry| entry.replace_errors())
                .collect();
            assert_eq!(
                row_parse_result,
                vec![ParsedEvent::Insert((
                    Some(vec![Value::from("id")]),
                    vec![
                        Value::Bool(true),
                        Value::Int(5),
                        Value::Float(6.4.into()),
                        Value::from("hkadhsfk")
                    ]
                ))]
            );
        }
        _ => panic!("row_read_result is not Data"),
    }

    Ok(())
}

#[test]
fn test_dsv_read_schema_nonparsable() -> eyre::Result<()> {
    let mut schema = HashMap::new();
    schema.insert("bool".to_string(), InnerSchemaField::new(Type::Bool, None));
    schema.insert("int".to_string(), InnerSchemaField::new(Type::Int, None));
    schema.insert(
        "float".to_string(),
        InnerSchemaField::new(Type::Float, None),
    );
    schema.insert(
        "string".to_string(),
        InnerSchemaField::new(Type::String, None),
    );

    let mut reader = new_filesystem_reader(
        "tests/data/incorrect_types.txt",
        ConnectorMode::Static,
        ReadMethod::ByLine,
        "*",
        false,
    )?;
    let mut parser = DsvParser::new(
        DsvSettings::new(
            None,
            vec![
                "bool".to_string(),
                "int".to_string(),
                "float".to_string(),
                "string".to_string(),
            ],
            ',',
        ),
        schema,
    )?;

    reader.read()?;
    let header_read_result = reader.read()?;
    match header_read_result {
        Data(bytes, _) => {
            let row_parse_result: ParseResult = parser.parse(&bytes);
            assert!(row_parse_result.is_ok());
        }
        _ => panic!("header_read_result is not Data"),
    }

    let row_read_result = reader.read()?;

    match row_read_result {
        Data(bytes, _) => {
            assert_error_shown_for_reader_context(
                &bytes,
                Box::new(parser),
                r#"failed to parse value "zzz" at field "int" according to the type int in schema: invalid digit found in string"#,
                ErrorPlacement::Value(1),
            );
        }
        _ => panic!("row_read_result is not Data"),
    }

    Ok(())
}
