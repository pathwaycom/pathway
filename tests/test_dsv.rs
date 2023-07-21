mod helpers;
use helpers::{assert_error_shown, assert_error_shown_for_reader_context};

use std::collections::HashMap;
use std::collections::HashSet;
use std::path::PathBuf;
use std::str::FromStr;

use pathway_engine::connectors::data_format::{
    DsvParser, DsvSettings, InnerSchemaField, ParseResult, ParsedEvent, Parser,
};
use pathway_engine::connectors::data_storage::{
    FilesystemReader, ReadResult, ReadResult::Data, Reader,
};
use pathway_engine::engine::{Key, Type, Value};

#[test]
fn test_dsv_read_ok() -> eyre::Result<()> {
    let mut reader = FilesystemReader::new(PathBuf::from("tests/data/sample.txt"), false, None)?;
    let mut parser = DsvParser::new(
        DsvSettings::new(Some(vec!["a".to_string()]), vec!["b".to_string()], ','),
        HashMap::new(),
    );

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
            assert_eq!(
                row_parse_result.expect("entries should parse correctly"),
                vec![ParsedEvent::Insert((
                    Some(vec![Value::from_str("0")?]),
                    vec![Value::from_str("0")?]
                ))]
            );
        }
        _ => panic!("row_read_result is not Data"),
    }

    Ok(())
}

#[test]
fn test_dsv_column_does_not_exist() -> eyre::Result<()> {
    let reader = FilesystemReader::new(PathBuf::from("tests/data/sample.txt"), false, None)?;
    let parser = DsvParser::new(
        DsvSettings::new(Some(vec!["a".to_string()]), vec!["c".to_string()], ','),
        HashMap::new(),
    );

    assert_error_shown(
        Box::new(reader),
        Box::new(parser),
        r#"some fields weren't found in the header (fields present in table: ["a", "b"], fields specified in connector: ["c"])"#,
    );

    Ok(())
}

#[test]
fn test_dsv_rows_parsing_ignore_type() -> eyre::Result<()> {
    let mut reader =
        FilesystemReader::new(PathBuf::from("tests/data/sample_str_int.txt"), false, None)?;
    let mut parser = DsvParser::new(
        DsvSettings::new(Some(vec!["a".to_string()]), vec!["b".to_string()], ','),
        HashMap::new(),
    );

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
    let mut reader = FilesystemReader::new(
        PathBuf::from("tests/data/sample_bad_lines.txt"),
        false,
        None,
    )?;
    let mut parser = DsvParser::new(
        DsvSettings::new(Some(vec!["a".to_string()]), vec!["b".to_string()], ','),
        HashMap::new(),
    );

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
        );
    } else {
        panic!("Data enum element was expected");
    }

    Ok(())
}

#[test]
fn test_dsv_autogenerate_pkey() -> eyre::Result<()> {
    let mut reader = FilesystemReader::new(PathBuf::from("tests/data/sample.txt"), false, None)?;
    let mut parser = DsvParser::new(
        DsvSettings::new(None, vec!["a".to_string(), "b".to_string()], ','),
        HashMap::new(),
    );

    let mut keys: HashSet<Key> = HashSet::new();

    loop {
        let read_result = reader.read()?;
        match read_result {
            ReadResult::Data(bytes, _) => {
                let row_parse_result: ParseResult = parser.parse(&bytes);
                assert!(row_parse_result.is_ok());

                for event in row_parse_result.expect("entries should parse correctly") {
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
            ReadResult::NewSource => continue,
        }
    }

    Ok(())
}

#[test]
fn test_dsv_composite_pkey() -> eyre::Result<()> {
    let mut reader = FilesystemReader::new(
        PathBuf::from("tests/data/sample_composite_pkey.txt"),
        false,
        None,
    )?;
    let mut parser = DsvParser::new(
        DsvSettings::new(
            Some(vec!["a".to_string(), "b".to_string()]),
            vec!["c".to_string()],
            ',',
        ),
        HashMap::new(),
    );

    let mut keys = Vec::new();

    loop {
        let read_result = reader.read()?;
        match read_result {
            ReadResult::Data(bytes, _) => {
                let row_parse_result: ParseResult = parser.parse(&bytes);
                assert!(row_parse_result.is_ok());

                for event in row_parse_result.expect("entries should parse correctly") {
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
            ReadResult::NewSource => continue,
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

    let mut reader = FilesystemReader::new(PathBuf::from("tests/data/schema.txt"), false, None)?;
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
    );

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
            assert_eq!(
                row_parse_result.expect("entries should parse correctly"),
                vec![ParsedEvent::Insert((
                    Some(vec![Value::from_str("id")?]),
                    vec![
                        Value::Bool(true),
                        Value::Int(5),
                        Value::Float(6.4.into()),
                        Value::from_str("hkadhsfk")?
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
    schema.insert("bool".to_string(), InnerSchemaField::new(Type::Int, None));
    schema.insert("int".to_string(), InnerSchemaField::new(Type::Bool, None));
    schema.insert(
        "float".to_string(),
        InnerSchemaField::new(Type::Float, None),
    );
    schema.insert(
        "string".to_string(),
        InnerSchemaField::new(Type::String, None),
    );

    let mut reader = FilesystemReader::new(PathBuf::from("tests/data/schema.txt"), false, None)?;
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
    );

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
            if let Err(e) = row_parse_result {
                assert_eq!(
                    format!("{e}"),
                    r#"failed to parse value "true" according to the type "int" in schema: invalid digit found in string"#
                );
            } else {
                panic!("The parsing should have been finished with error");
            }
        }
        _ => panic!("row_read_result is not Data"),
    }

    Ok(())
}
