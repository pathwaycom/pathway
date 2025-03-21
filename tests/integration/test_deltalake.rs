// Copyright © 2024 Pathway

use std::collections::HashMap;
use std::path::Path;

use assert_matches::assert_matches;
use deltalake::datafusion::parquet::file::reader::SerializedFileReader;
use ndarray::ArrayD;
use serde_json::json;
use tempfile::tempdir;

use pathway_engine::connectors::data_format::{
    Formatter, IdentityFormatter, InnerSchemaField, ParsedEvent, TransparentParser,
};
use pathway_engine::connectors::data_lake::{parquet_row_into_values_map, DeltaBatchWriter};
use pathway_engine::connectors::data_storage::{
    ConnectorMode, ConversionError, DeltaTableReader, LakeWriter, ObjectDownloader, WriteError,
    Writer,
};
use pathway_engine::connectors::SessionType;
use pathway_engine::engine::{
    DateTimeNaive, DateTimeUtc, Duration, Duration as EngineDuration, Key, Result, Timestamp, Type,
    Value,
};
use pathway_engine::python_api::ValueField;

use crate::helpers::read_data_from_reader;

fn run_single_column_save(type_: Type, values: &[Value]) -> eyre::Result<()> {
    let test_storage = tempdir().expect("tempdir creation failed");
    let test_storage_path = test_storage.path();

    let value_fields = vec![ValueField {
        name: "field".to_string(),
        type_: Type::Optional(type_.clone().into()),
        default: None,
        metadata: None,
    }];

    let batch_writer = DeltaBatchWriter::new(
        test_storage_path.to_str().unwrap(),
        &value_fields,
        HashMap::new(),
        Vec::new(),
    )?;
    let mut writer = LakeWriter::new(Box::new(batch_writer), &value_fields, None)?;
    let mut formatter = IdentityFormatter::new();

    for value in values {
        let context = formatter
            .format(&Key::random(), &[value.clone()], Timestamp(0), 1)
            .expect("formatter failed");
        writer.write(context)?;
    }
    writer.flush(true)?;
    let rows_present: Vec<_> = read_from_deltalake(test_storage_path.to_str().unwrap(), &type_)
        .into_iter()
        .map(|item| item.unwrap())
        .collect();
    assert_eq!(rows_present, values);

    let rows_roundtrip = read_with_connector(test_storage_path.to_str().unwrap(), &type_)?;
    assert_eq!(rows_roundtrip, values);

    let is_optional = matches!(type_, Type::Optional(_));
    if !is_optional {
        // If the type isn't optional, we run a test for its optional version.
        // To do that, we create an optional version of the type, append a null-value
        // to the end of the tested values vector, and run test on the parameters
        // modified this way.
        let mut values_with_nulls = values.to_vec();
        values_with_nulls.push(Value::None);
        run_single_column_save(Type::Optional(type_.into()), &values_with_nulls)?;
    } else {
        // If the type is optional, we've previously added None to the end of the vector.
        // Then we need to check that the without optionality would fail.
        let mut rows_roundtrip =
            read_from_deltalake(test_storage_path.to_str().unwrap(), type_.unoptionalize());
        assert!(rows_roundtrip.pop().unwrap().is_err());
        let rows_roundtrip: Vec<_> = rows_roundtrip
            .into_iter()
            .map(|item| item.unwrap())
            .collect();
        assert_eq!(rows_roundtrip, values[..rows_roundtrip.len()]);

        let mut rows_roundtrip =
            read_with_connector(test_storage_path.to_str().unwrap(), type_.unoptionalize())?;
        assert_eq!(rows_roundtrip.pop().unwrap(), Value::Error);
        assert_eq!(rows_roundtrip, values[..rows_roundtrip.len()]);
    }

    Ok(())
}

fn read_with_connector(path: &str, type_: &Type) -> Result<Vec<Value>> {
    let mut schema = HashMap::new();
    schema.insert(
        "field".to_string(),
        InnerSchemaField::new(type_.clone(), None),
    );
    let mut type_map = HashMap::new();
    type_map.insert("field".to_string(), type_.clone());
    let reader = DeltaTableReader::new(
        path,
        ObjectDownloader::Local,
        HashMap::new(),
        type_map,
        ConnectorMode::Static,
        None,
        true,
        Vec::new(),
    )
    .unwrap();
    let parser =
        TransparentParser::new(None, vec!["field".to_string()], schema, SessionType::Native)?;
    let values_read = read_data_from_reader(Box::new(reader), Box::new(parser)).unwrap();
    let mut result = Vec::new();
    for event in values_read {
        let ParsedEvent::Insert((_key, values)) = event else {
            panic!("Unexpected event type: {event:?}")
        };
        assert_eq!(values.len(), 1);
        let value = values[0].clone();
        result.push(value);
    }
    Ok(result)
}

fn read_from_deltalake(path: &str, type_: &Type) -> Vec<Result<Value, Box<ConversionError>>> {
    let mut reread_values = Vec::new();
    let mut column_types = HashMap::new();
    column_types.insert("field".to_string(), type_.clone());
    column_types.insert("time".to_string(), Type::Int);
    column_types.insert("diff".to_string(), Type::Int);
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let table = deltalake::open_table(path).await.unwrap();
            let mut parquet_files = Vec::<String>::new();
            for file_name in table.get_file_uris().unwrap() {
                let full_path = Path::new(path).join(file_name);
                parquet_files.push(full_path.to_string_lossy().to_string());
            }
            let rows = parquet_files
                .iter()
                .map(|p| SerializedFileReader::try_from(Path::new(p)).unwrap())
                .flat_map(|r| r.into_iter());
            for row in rows {
                let row = row.expect("row reading failed");
                let values_map = parquet_row_into_values_map(&row, &column_types);
                reread_values.push(values_map.get("field").unwrap().clone());
                assert!(values_map.get("time").is_some());
                assert!(values_map.get("diff").is_some());
            }
        });

    reread_values
}

#[test]
fn test_save_bool() -> eyre::Result<()> {
    run_single_column_save(Type::Bool, &[Value::Bool(true), Value::Bool(false)])
}

#[test]
fn test_save_int() -> eyre::Result<()> {
    run_single_column_save(Type::Int, &[Value::Int(10)])
}

#[test]
fn test_save_float() -> eyre::Result<()> {
    run_single_column_save(Type::Float, &[Value::Float(0.01.into())])
}

#[test]
fn test_save_string() -> eyre::Result<()> {
    run_single_column_save(
        Type::String,
        &[Value::String("abc".into()), Value::String("".into())],
    )
}

#[test]
fn test_save_bytes() -> eyre::Result<()> {
    let test_bytes: &[u8] = &[1, 10, 5, 19, 55, 67, 9, 87, 28];
    run_single_column_save(
        Type::Bytes,
        &[Value::Bytes([].into()), Value::Bytes(test_bytes.into())],
    )
}

#[test]
fn test_save_datetimenaive() -> eyre::Result<()> {
    run_single_column_save(
        Type::DateTimeNaive,
        &[
            Value::DateTimeNaive(DateTimeNaive::from_timestamp(0, "s")?),
            Value::DateTimeNaive(DateTimeNaive::from_timestamp(10000, "s")?),
            Value::DateTimeNaive(DateTimeNaive::from_timestamp(-10000, "s")?),
        ],
    )
}

#[test]
fn test_save_datetimeutc() -> eyre::Result<()> {
    run_single_column_save(
        Type::DateTimeUtc,
        &[
            Value::DateTimeUtc(DateTimeUtc::new(0)),
            Value::DateTimeUtc(DateTimeUtc::new(10_000_000_000_000)),
            Value::DateTimeUtc(DateTimeUtc::new(-10_000_000_000_000)),
        ],
    )
}

#[test]
fn test_save_duration() -> eyre::Result<()> {
    run_single_column_save(
        Type::Duration,
        &[
            Value::Duration(Duration::new(0)),
            Value::Duration(Duration::new(10_000_000_000_000)),
            Value::Duration(Duration::new(-10_000_000_000_000)),
        ],
    )
}

#[test]
fn test_save_json() -> eyre::Result<()> {
    run_single_column_save(Type::Json, &[Value::from(json!({"A": 100}))])
}

#[test]
fn test_save_list() -> eyre::Result<()> {
    let value_list_1 = vec![
        Value::Duration(EngineDuration::new_with_unit(-1, "s")?),
        Value::Duration(EngineDuration::new_with_unit(2, "ms")?),
        Value::Duration(EngineDuration::new_with_unit(0, "ns")?),
    ];
    let value_list_2 = vec![
        Value::Duration(EngineDuration::new_with_unit(-10, "s")?),
        Value::Duration(EngineDuration::new_with_unit(20, "ms")?),
        Value::Duration(EngineDuration::new_with_unit(0, "ns")?),
    ];
    run_single_column_save(
        Type::List(Type::Duration.into()),
        &[
            Value::Tuple(value_list_1.into()),
            Value::Tuple(value_list_2.into()),
        ],
    )
}

#[test]
fn test_save_optionals_list() -> eyre::Result<()> {
    let value_list_1 = vec![
        Value::Duration(EngineDuration::new_with_unit(-1, "s")?),
        Value::Duration(EngineDuration::new_with_unit(2, "ms")?),
        Value::Duration(EngineDuration::new_with_unit(0, "ns")?),
        Value::None,
    ];
    let value_list_2 = vec![
        Value::Duration(EngineDuration::new_with_unit(-10, "s")?),
        Value::None,
        Value::Duration(EngineDuration::new_with_unit(20, "ms")?),
        Value::Duration(EngineDuration::new_with_unit(0, "ns")?),
    ];
    run_single_column_save(
        Type::List(Type::Optional(Type::Duration.into()).into()),
        &[
            Value::Tuple(value_list_1.into()),
            Value::Tuple(value_list_2.into()),
        ],
    )
}

#[test]
fn test_save_pointer() -> eyre::Result<()> {
    run_single_column_save(Type::Pointer, &[Value::Pointer(Key::random())])
}

#[test]
fn test_save_int_array() -> eyre::Result<()> {
    let array1 = ArrayD::<i64>::from_shape_vec(vec![2, 3], vec![0, 1, 2, 3, 4, 5]).unwrap();
    let array2 =
        ArrayD::<i64>::from_shape_vec(vec![2, 2, 2], vec![0, 1, 2, 3, 4, 5, 6, 7]).unwrap();
    run_single_column_save(
        Type::Array(None, Type::Int.into()),
        &[Value::from(array1), Value::from(array2)],
    )
}

#[test]
fn test_save_float_array() -> eyre::Result<()> {
    let array1 =
        ArrayD::<f64>::from_shape_vec(vec![2, 3], vec![0.0, 0.1, 0.2, 0.3, 0.4, 0.5]).unwrap();
    let array2 =
        ArrayD::<f64>::from_shape_vec(vec![2, 2, 2], vec![0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7])
            .unwrap();
    run_single_column_save(
        Type::Array(None, Type::Float.into()),
        &[Value::from(array1), Value::from(array2)],
    )
}

#[test]
fn test_save_tuple() -> eyre::Result<()> {
    let tuple_contents = vec![Type::String, Type::Int];
    let tuple_type = Type::Tuple(tuple_contents.into());

    run_single_column_save(
        tuple_type,
        &[
            Value::Tuple(vec![Value::String("hello".into()), Value::Int(10)].into()),
            Value::Tuple(vec![Value::String("world".into()), Value::Int(20)].into()),
        ],
    )
}

#[test]
fn test_save_tuple_with_optionals() -> eyre::Result<()> {
    let tuple_contents = vec![
        Type::String,
        Type::Optional(Type::Int.into()),
        Type::Optional(Type::Bool.into()),
    ];
    let tuple_type = Type::Tuple(tuple_contents.into());

    run_single_column_save(
        tuple_type,
        &[
            Value::Tuple(vec![Value::String("lorem".into()), Value::Int(10), Value::None].into()),
            Value::Tuple(
                vec![
                    Value::String("ipsum".into()),
                    Value::Int(20),
                    Value::Bool(true),
                ]
                .into(),
            ),
            Value::Tuple(
                vec![
                    Value::String("dolor".into()),
                    Value::None,
                    Value::Bool(false),
                ]
                .into(),
            ),
            Value::Tuple(vec![Value::String("sit".into()), Value::None, Value::None].into()),
        ],
    )
}

#[test]
fn test_save_tuple_nested_tuples() -> eyre::Result<()> {
    // (String, (Int, (Bool, Bytes)))
    let tuple_contents = vec![
        Type::String,
        Type::Tuple(vec![Type::Int, Type::Tuple(vec![Type::Bool, Type::Bytes].into())].into()),
    ];
    let tuple_type = Type::Tuple(tuple_contents.into());

    run_single_column_save(
        tuple_type,
        &[
            Value::Tuple(
                vec![
                    Value::String("lorem".into()),
                    Value::Tuple(
                        vec![
                            Value::Int(10),
                            Value::Tuple(
                                vec![Value::Bool(true), Value::Bytes(b"lorem".to_vec().into())]
                                    .into(),
                            ),
                        ]
                        .into(),
                    ),
                ]
                .into(),
            ),
            Value::Tuple(
                vec![
                    Value::String("ipsum".into()),
                    Value::Tuple(
                        vec![
                            Value::Int(20),
                            Value::Tuple(
                                vec![Value::Bool(false), Value::Bytes(b"ipsum".to_vec().into())]
                                    .into(),
                            ),
                        ]
                        .into(),
                    ),
                ]
                .into(),
            ),
        ],
    )
}

#[test]
fn test_save_tuple_nested_tuples_with_arrays() -> eyre::Result<()> {
    // (String, (Int, (Optional<IntArray>, FloatArray)))
    let tuple_contents = vec![
        Type::String,
        Type::Tuple(
            vec![
                Type::Int,
                Type::Tuple(
                    vec![
                        Type::Optional(Type::Array(None, Type::Int.into()).into()),
                        Type::Array(None, Type::Float.into()),
                    ]
                    .into(),
                ),
            ]
            .into(),
        ),
    ];
    let tuple_type = Type::Tuple(tuple_contents.into());

    let int_array_1 = ArrayD::<i64>::from_shape_vec(vec![2, 3], vec![0, 1, 2, 3, 4, 5]).unwrap();
    let int_array_2 =
        ArrayD::<i64>::from_shape_vec(vec![2, 2, 2], vec![0, 1, 2, 3, 4, 5, 6, 7]).unwrap();
    let float_array_1 =
        ArrayD::<f64>::from_shape_vec(vec![2, 3], vec![0.0, 0.1, 0.2, 0.3, 0.4, 0.5]).unwrap();
    let float_array_2 =
        ArrayD::<f64>::from_shape_vec(vec![2, 2, 2], vec![0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7])
            .unwrap();
    let float_array_3 = ArrayD::<f64>::from_shape_vec(vec![2, 1], vec![-1.1, 1.2]).unwrap();

    run_single_column_save(
        tuple_type,
        &[
            Value::Tuple(
                vec![
                    Value::String("lorem".into()),
                    Value::Tuple(
                        vec![
                            Value::Int(10),
                            Value::Tuple(
                                vec![Value::from(int_array_1), Value::from(float_array_1)].into(),
                            ),
                        ]
                        .into(),
                    ),
                ]
                .into(),
            ),
            Value::Tuple(
                vec![
                    Value::String("ipsum".into()),
                    Value::Tuple(
                        vec![
                            Value::Int(20),
                            Value::Tuple(
                                vec![Value::from(int_array_2), Value::from(float_array_2)].into(),
                            ),
                        ]
                        .into(),
                    ),
                ]
                .into(),
            ),
            Value::Tuple(
                vec![
                    Value::String("dolor".into()),
                    Value::Tuple(
                        vec![
                            Value::Int(30),
                            Value::Tuple(vec![Value::None, Value::from(float_array_3)].into()),
                        ]
                        .into(),
                    ),
                ]
                .into(),
            ),
        ],
    )
}

#[test]
fn test_py_object_wrapper_makes_no_error() -> eyre::Result<()> {
    run_single_column_save(Type::PyObjectWrapper, &[])
}

#[test]
fn test_save_any_is_unsupported() -> eyre::Result<()> {
    let save_result = run_single_column_save(Type::Any, &[Value::from(json!({"A": 100}))]);
    assert_matches!(
        save_result.err().unwrap().downcast::<WriteError>(),
        Ok(WriteError::UnsupportedType(_))
    );
    Ok(())
}
