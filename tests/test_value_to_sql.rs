// Copyright © 2024 Pathway

use bytes::BytesMut;
use postgres::types::{IsNull, ToSql, Type};

use pathway_engine::engine::Value;

fn assert_success<T: ToSql>(value: Value, postgres_type: &Type, expected: T) {
    let mut value_bytes = BytesMut::new();
    let value_is_null = value
        .to_sql_checked(postgres_type, &mut value_bytes)
        .unwrap_or_else(|e| panic!("converting {value:?} to {postgres_type} failed: {e}"));

    let mut expected_bytes = BytesMut::new();
    let expected_is_null = expected
        .to_sql_checked(postgres_type, &mut expected_bytes)
        .unwrap_or_else(|e| panic!("converting {expected:?} to {postgres_type} failed: {e}"));

    match (value_is_null, expected_is_null) {
        (IsNull::Yes, IsNull::No) => panic!("expected {value:?} not to be null"),
        (IsNull::No, IsNull::Yes) => panic!("expected {value:?} to be null"),
        (IsNull::Yes, IsNull::Yes) => {}
        (IsNull::No, IsNull::No) => {
            assert_eq!(
                value_bytes, expected_bytes,
                "expected {value:?} to convert to {postgres_type} as {expected_bytes:?} but got {value_bytes:?}"
            );
        }
    }
}

fn assert_failure(value: Value, postgres_type: &Type) {
    let res = value.to_sql_checked(postgres_type, &mut BytesMut::new());
    if res.is_ok() {
        panic!("expected {value:?} not to convert to {postgres_type}");
    }
}

#[test]
fn test_none() {
    assert_success(Value::None, &Type::BOOL, Option::<bool>::None);
    assert_success(Value::None, &Type::INT8, Option::<i64>::None);
    assert_success(Value::None, &Type::TEXT, Option::<String>::None);
}

#[test]
fn test_bool() {
    assert_success(Value::Bool(true), &Type::BOOL, true);
    assert_failure(Value::Bool(true), &Type::TEXT);
}

#[test]
fn test_int() {
    assert_success(Value::Int(-42), &Type::CHAR, -42i8);
    assert_success(Value::Int(42), &Type::CHAR, 42i8);
    assert_failure(Value::Int(42 << 8), &Type::CHAR);
    assert_failure(Value::Int(42 << 16), &Type::CHAR);
    assert_failure(Value::Int(42 << 32), &Type::CHAR);

    assert_success(Value::Int(-42), &Type::INT2, -42i16);
    assert_success(Value::Int(42), &Type::INT2, 42i16);
    assert_success(Value::Int(42 << 8), &Type::INT2, 42i16 << 8);
    assert_failure(Value::Int(42 << 16), &Type::INT2);
    assert_failure(Value::Int(42 << 32), &Type::INT2);

    assert_success(Value::Int(-42), &Type::INT4, -42i32);
    assert_success(Value::Int(42), &Type::INT4, 42i32);
    assert_success(Value::Int(42 << 8), &Type::INT4, 42i32 << 8);
    assert_success(Value::Int(42 << 16), &Type::INT4, 42i32 << 16);
    assert_failure(Value::Int(42 << 32), &Type::INT4);

    assert_success(Value::Int(-42), &Type::INT8, -42i64);
    assert_success(Value::Int(42), &Type::INT8, 42i64);
    assert_success(Value::Int(42 << 8), &Type::INT8, 42i64 << 8);
    assert_success(Value::Int(42 << 16), &Type::INT8, 42i64 << 16);
    assert_success(Value::Int(42 << 32), &Type::INT8, 42i64 << 32);

    assert_success(Value::Int(-42), &Type::INT8, -42i64);
    assert_success(Value::Int(42), &Type::INT8, 42i64);
    assert_success(Value::Int(42 << 8), &Type::INT8, 42i64 << 8);
    assert_success(Value::Int(42 << 16), &Type::INT8, 42i64 << 16);
    assert_success(Value::Int(42 << 32), &Type::INT8, 42i64 << 32);

    assert_success(Value::Int(-42), &Type::FLOAT8, -42.0f64);
    assert_success(Value::Int(42), &Type::FLOAT8, 42.0f64);

    assert_success(Value::Int(-42), &Type::FLOAT4, -42.0f32);
    assert_success(Value::Int(42), &Type::FLOAT4, 42.0f32);

    assert_failure(Value::Int(42), &Type::TEXT);
}

#[test]
fn test_float() {
    assert_failure(Value::Float(42.0.into()), &Type::CHAR);
    assert_failure(Value::Float(42.0.into()), &Type::INT2);
    assert_failure(Value::Float(42.0.into()), &Type::INT4);
    assert_failure(Value::Float(42.0.into()), &Type::INT8);

    assert_success(Value::Float(42.5.into()), &Type::FLOAT8, 42.5f64);
    assert_success(
        Value::Float(f64::INFINITY.into()),
        &Type::FLOAT8,
        f64::INFINITY,
    );
    assert_success(
        Value::Float(f64::NEG_INFINITY.into()),
        &Type::FLOAT8,
        f64::NEG_INFINITY,
    );
    assert_success(Value::Float(f64::MAX.into()), &Type::FLOAT8, f64::MAX);
    assert_success(Value::Float(f64::MIN.into()), &Type::FLOAT8, f64::MIN);

    assert_success(Value::Float(42.5.into()), &Type::FLOAT4, 42.5f32);
    assert_success(
        Value::Float(f64::INFINITY.into()),
        &Type::FLOAT4,
        f32::INFINITY,
    );
    assert_success(
        Value::Float(f64::NEG_INFINITY.into()),
        &Type::FLOAT4,
        f32::NEG_INFINITY,
    );
    assert_success(Value::Float(f64::MAX.into()), &Type::FLOAT4, f32::INFINITY);
    assert_success(
        Value::Float(f64::MIN.into()),
        &Type::FLOAT4,
        f32::NEG_INFINITY,
    );

    assert_failure(Value::Float(42.5.into()), &Type::TEXT);
}
