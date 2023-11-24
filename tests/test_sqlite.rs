use std::sync::Arc;

use rusqlite::Connection as SqliteConnection;
use rusqlite::OpenFlags as SqliteOpenFlags;

use pathway_engine::connectors::data_format::{ParsedEvent, Parser, TransparentParser};
use pathway_engine::connectors::data_storage::{ReadResult, Reader, SqliteReader};
use pathway_engine::connectors::offset::EMPTY_OFFSET;
use pathway_engine::engine::Value;

#[test]
fn test_sqlite_read_table() -> eyre::Result<()> {
    let connection = SqliteConnection::open_with_flags(
        "tests/data/sqlite/goods_test.db",
        SqliteOpenFlags::SQLITE_OPEN_READ_ONLY,
    )?;
    let mut reader = SqliteReader::new(
        connection,
        "goods".to_string(),
        vec![
            "id".to_string(),
            "name".to_string(),
            "price".to_string(),
            "photo".to_string(),
        ],
    );
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
            ReadResult::from_event(
                ParsedEvent::Insert((
                    Some(vec![Value::Int(1)]),
                    vec![
                        Value::Int(1),
                        Value::String("Milk".into()),
                        Value::Float(1.1.into()),
                        Value::None
                    ]
                )),
                EMPTY_OFFSET
            ),
            ReadResult::from_event(
                ParsedEvent::Insert((
                    Some(vec![Value::Int(2)]),
                    vec![
                        Value::Int(2),
                        Value::String("Bread".into()),
                        Value::Float(0.75.into()),
                        Value::Bytes(Arc::new([0, 0]))
                    ]
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
    let mut reader = SqliteReader::new(
        connection,
        "goods".to_string(),
        vec![
            "id".to_string(),
            "name".to_string(),
            "price".to_string(),
            "photo".to_string(),
        ],
    );
    let mut parser = TransparentParser::new(4);

    let mut parsed_events: Vec<ParsedEvent> = Vec::new();
    loop {
        let entry = reader.read()?;
        let is_last_entry = matches!(entry, ReadResult::FinishedSource { .. });
        if let ReadResult::Data(entry, _) = entry {
            for event in parser.parse(&entry)? {
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
