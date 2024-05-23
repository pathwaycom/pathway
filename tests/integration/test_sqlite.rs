// Copyright © 2024 Pathway

use std::collections::HashMap;
use std::sync::Arc;

use pathway_engine::connectors::data_format::InnerSchemaField;
use pathway_engine::connectors::data_format::ParseError;
use pathway_engine::connectors::data_format::TransparentParser;
use pathway_engine::connectors::data_storage::DataEventType;
use pathway_engine::connectors::data_storage::ReaderContext;
use pathway_engine::connectors::SessionType;
use pathway_engine::engine::Type;
use rusqlite::Connection as SqliteConnection;
use rusqlite::OpenFlags as SqliteOpenFlags;

use pathway_engine::connectors::data_format::{ParsedEvent, Parser};
use pathway_engine::connectors::data_storage::{ReadResult, Reader, SqliteReader};
use pathway_engine::connectors::offset::EMPTY_OFFSET;
use pathway_engine::engine::Value;

use crate::helpers::assert_error_shown_for_reader_context;
use crate::helpers::ErrorPlacement;
use crate::helpers::ReplaceErrors;

#[test]
fn test_sqlite_read_table() -> eyre::Result<()> {
    let connection = SqliteConnection::open_with_flags(
        "tests/data/sqlite/goods_test.db",
        SqliteOpenFlags::SQLITE_OPEN_READ_ONLY,
    )?;
    let value_field_names = vec![
        "id".to_string(),
        "name".to_string(),
        "price".to_string(),
        "photo".to_string(),
    ];
    let mut reader = SqliteReader::new(connection, "goods".to_string(), value_field_names);
    let mut read_results = Vec::new();
    loop {
        let entry = reader.read()?;
        let is_last_entry = matches!(entry, ReadResult::FinishedSource { .. });
        read_results.push(entry);
        if is_last_entry {
            break;
        }
    }
    assert_eq!(
        read_results,
        vec![
            ReadResult::NewSource(None),
            ReadResult::Data(
                ReaderContext::Diff((
                    DataEventType::Insert,
                    Some(vec![Value::Int(1)]),
                    HashMap::from([
                        ("id".to_owned(), Value::Int(1)),
                        ("name".to_owned(), Value::String("Milk".into())),
                        ("price".to_owned(), Value::Float(1.1.into())),
                        ("photo".to_owned(), Value::None)
                    ])
                    .into(),
                )),
                EMPTY_OFFSET
            ),
            ReadResult::Data(
                ReaderContext::Diff((
                    DataEventType::Insert,
                    Some(vec![Value::Int(2)]),
                    HashMap::from([
                        ("id".to_owned(), Value::Int(2)),
                        ("name".to_owned(), Value::String("Bread".into())),
                        ("price".to_owned(), Value::Float(0.75.into())),
                        ("photo".to_owned(), Value::Bytes(Arc::new([0, 0])))
                    ])
                    .into(),
                )),
                EMPTY_OFFSET
            ),
            ReadResult::FinishedSource {
                commit_allowed: true
            }
        ]
    );
    Ok(())
}

#[test]
fn test_sqlite_read_table_with_parser() -> eyre::Result<()> {
    let connection = SqliteConnection::open_with_flags(
        "tests/data/sqlite/goods_test.db",
        SqliteOpenFlags::SQLITE_OPEN_READ_ONLY,
    )?;
    let value_field_names = vec![
        "id".to_string(),
        "name".to_string(),
        "price".to_string(),
        "photo".to_string(),
    ];
    let schema = HashMap::from([
        (
            "id".to_owned(),
            InnerSchemaField::new(Type::Int, false, None),
        ),
        (
            "name".to_owned(),
            InnerSchemaField::new(Type::String, false, None),
        ),
        (
            "price".to_owned(),
            InnerSchemaField::new(Type::Float, false, None),
        ),
        (
            "photo".to_owned(),
            InnerSchemaField::new(Type::Bytes, true, None),
        ),
    ]);
    let mut reader = SqliteReader::new(connection, "goods".to_string(), value_field_names.clone());
    let mut parser = TransparentParser::new(None, value_field_names, schema, SessionType::Native);

    let mut parsed_events: Vec<ParsedEvent> = Vec::new();
    loop {
        let entry = reader.read()?;
        let is_last_entry = matches!(entry, ReadResult::FinishedSource { .. });
        if let ReadResult::Data(entry, _) = entry {
            for event in parser.parse(&entry).map_err(ParseError::from)? {
                let event = event.replace_errors();
                parsed_events.push(event);
            }
        }
        if is_last_entry {
            break;
        }
    }
    assert_eq!(
        parsed_events,
        vec![
            ParsedEvent::Insert((
                Some(vec![Value::Int(1)]),
                vec![
                    Value::Int(1),
                    Value::String("Milk".into()),
                    Value::Float(1.1.into()),
                    Value::None
                ]
            )),
            ParsedEvent::Insert((
                Some(vec![Value::Int(2)]),
                vec![
                    Value::Int(2),
                    Value::String("Bread".into()),
                    Value::Float(0.75.into()),
                    Value::Bytes(Arc::new([0, 0]))
                ]
            )),
        ]
    );
    Ok(())
}

#[test]
fn test_sqlite_read_table_nonparsable() -> eyre::Result<()> {
    let connection = SqliteConnection::open_with_flags(
        "tests/data/sqlite/goods_test.db",
        SqliteOpenFlags::SQLITE_OPEN_READ_ONLY,
    )?;
    let value_field_names = vec![
        "id".to_string(),
        "name".to_string(),
        "price".to_string(),
        "photo".to_string(),
    ];
    let schema = HashMap::from([
        (
            "id".to_owned(),
            InnerSchemaField::new(Type::Int, false, None),
        ),
        (
            "name".to_owned(),
            InnerSchemaField::new(Type::String, false, None),
        ),
        (
            "price".to_owned(),
            InnerSchemaField::new(Type::Float, false, None),
        ),
        (
            "photo".to_owned(),
            InnerSchemaField::new(Type::Bytes, false, None),
        ),
    ]);
    let mut reader = SqliteReader::new(connection, "goods".to_string(), value_field_names.clone());
    let parser = TransparentParser::new(None, value_field_names, schema, SessionType::Native);

    reader.read()?;
    let read_result = reader.read()?;
    match read_result {
        ReadResult::Data(context, _) => assert_error_shown_for_reader_context(
            &context,
            Box::new(parser),
            r#"value None in field "photo" is inconsistent with type Bytes from schema"#,
            ErrorPlacement::Value(3),
        ),
        _ => panic!("row_read_result is not Data"),
    }
    reader.read()?;
    assert_eq!(
        reader.read()?,
        ReadResult::FinishedSource {
            commit_allowed: true,
        },
    );
    Ok(())
}
