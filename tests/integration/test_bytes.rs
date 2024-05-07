// Copyright © 2024 Pathway

use pathway_engine::connectors::data_format::{IdentityParser, ParseResult, ParsedEvent, Parser};
use pathway_engine::connectors::data_storage::{
    ConnectorMode, FilesystemReader, ReadMethod, ReadResult, Reader,
};
use pathway_engine::connectors::SessionType;
use pathway_engine::engine::Value;

use crate::helpers::ReplaceErrors;

fn read_bytes_from_path(path: &str) -> eyre::Result<Vec<ParsedEvent>> {
    let mut reader =
        FilesystemReader::new(path, ConnectorMode::Static, None, ReadMethod::Full, "*")?;
    let mut parser = IdentityParser::new(vec!["data".to_string()], false, SessionType::Native);
    let mut events = Vec::new();

    loop {
        let read_result = reader.read()?;
        match read_result {
            ReadResult::Data(bytes, _) => {
                let row_parse_result: ParseResult = parser.parse(&bytes);
                assert!(row_parse_result.is_ok());

                for event in row_parse_result.expect("entries should parse correctly") {
                    let event = event.replace_errors();
                    events.push(event);
                }
            }
            ReadResult::Finished => break,
            ReadResult::FinishedSource { .. } => continue,
            ReadResult::NewSource(_) => continue,
        }
    }

    Ok(events)
}

#[test]
fn test_bytes_read_from_file() -> eyre::Result<()> {
    let events = read_bytes_from_path("tests/data/binary")?;
    assert_eq!(events.len(), 1);
    Ok(())
}

#[test]
fn test_empty() -> eyre::Result<()> {
    let events = read_bytes_from_path("tests/data/empty")?;
    assert_eq!(
        events,
        vec![ParsedEvent::Insert((
            None,
            vec![Value::Bytes(Vec::new().into())]
        )),]
    );

    Ok(())
}

#[test]
fn test_empty_files_folder() -> eyre::Result<()> {
    let events = read_bytes_from_path("tests/data/empty_files/")?;
    assert_eq!(
        events,
        vec![
            ParsedEvent::Insert((None, vec![Value::Bytes(Vec::new().into())])),
            ParsedEvent::Insert((None, vec![Value::Bytes(Vec::new().into())])),
            ParsedEvent::Insert((None, vec![Value::Bytes(Vec::new().into())])),
        ]
    );

    Ok(())
}

#[test]
fn test_bytes_read_from_folder() -> eyre::Result<()> {
    let events = read_bytes_from_path("tests/data/csvdir")?;
    assert_eq!(
        events,
        vec![
            ParsedEvent::Insert((
                None,
                vec![Value::Bytes(b"key,foo\n1,abc\n2,def\n".to_vec().into())]
            )),
            ParsedEvent::Insert((
                None,
                vec![Value::Bytes(b"key,foo\n3,ghi\n4,jkl\n".to_vec().into())]
            )),
            ParsedEvent::Insert((
                None,
                vec![Value::Bytes(b"key,foo\n5,mno\n6,pqr\n".to_vec().into())]
            ))
        ]
    );

    Ok(())
}
