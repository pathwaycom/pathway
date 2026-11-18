// Copyright © 2026 Pathway

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

    assert_success(Value::Int(-42), &Type::JSONB, serde_json::json!(-42));
    assert_success(Value::Int(42), &Type::JSONB, serde_json::json!(42));
    assert_success(
        Value::Int(i64::MAX),
        &Type::JSONB,
        serde_json::json!(i64::MAX),
    );
    assert_success(
        Value::Int(i64::MIN),
        &Type::JSONB,
        serde_json::json!(i64::MIN),
    );
    assert_success(Value::Int(-42), &Type::JSON, serde_json::json!(-42));
    assert_success(Value::Int(42), &Type::JSON, serde_json::json!(42));

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

fn postgis_type(name: &str) -> Type {
    Type::new(
        name.to_string(),
        0,
        postgres::types::Kind::Simple,
        "public".to_string(),
    )
}

// POINT(30.5 59.9) as little-endian WKB, optionally wrapped into EWKB with an SRID
fn point_ewkb(srid: Option<u32>) -> Vec<u8> {
    let mut ewkb = vec![0x01];
    match srid {
        Some(srid) => {
            ewkb.extend_from_slice(&0x2000_0001u32.to_le_bytes()); // point + SRID flag
            ewkb.extend_from_slice(&srid.to_le_bytes());
        }
        None => ewkb.extend_from_slice(&1u32.to_le_bytes()),
    }
    ewkb.extend_from_slice(&30.5f64.to_le_bytes());
    ewkb.extend_from_slice(&59.9f64.to_le_bytes());
    ewkb
}

fn convert(value: Value, postgres_type: &Type) -> BytesMut {
    let mut out = BytesMut::new();
    value
        .to_sql_checked(postgres_type, &mut out)
        .unwrap_or_else(|e| panic!("converting {value:?} to {postgres_type} failed: {e}"));
    out
}

#[test]
fn test_string_wkt_to_postgis_geometry() {
    let expected = point_ewkb(None);
    for type_name in ["geometry", "geography"] {
        let out = convert(
            Value::String("POINT(30.5 59.9)".into()),
            &postgis_type(type_name),
        );
        assert_eq!(&out[..], &expected[..]);
    }
}

// POINT ZM (1 2 3 4) as little-endian EWKB, optionally with an SRID
fn point_zm_ewkb(srid: Option<u32>) -> Vec<u8> {
    let mut ewkb = vec![0x01];
    let type_with_flags = 1u32 | 0x8000_0000 | 0x4000_0000 | srid.map_or(0, |_| 0x2000_0000);
    ewkb.extend_from_slice(&type_with_flags.to_le_bytes());
    if let Some(srid) = srid {
        ewkb.extend_from_slice(&srid.to_le_bytes());
    }
    for coord in [1.0f64, 2.0, 3.0, 4.0] {
        ewkb.extend_from_slice(&coord.to_le_bytes());
    }
    ewkb
}

#[test]
fn test_string_wkt_dimension_suffix_spellings_keep_all_coordinates() {
    // The dimension tokens can be separated from the type keyword or glued
    // to it; both spellings must keep the Z / M coordinates.
    let expected = point_zm_ewkb(None);
    for wkt in [
        "POINT ZM (1 2 3 4)",
        "POINTZM(1 2 3 4)",
        "POINT ZM(1 2 3 4)",
        "point zm (1 2 3 4)",
    ] {
        let out = convert(Value::String(wkt.into()), &postgis_type("geometry"));
        assert_eq!(&out[..], &expected[..], "spelling {wkt:?}");
    }
    let out = convert(
        Value::String("SRID=4326;POINTZM(1 2 3 4)".into()),
        &postgis_type("geometry"),
    );
    assert_eq!(&out[..], &point_zm_ewkb(Some(4326))[..]);
    // Z-only and M-only glued spellings keep their third coordinate too.
    for (wkt, flag) in [
        ("POINTZ(1 2 3)", 0x8000_0000u32),
        ("POINTM(1 2 3)", 0x4000_0000u32),
    ] {
        let out = convert(Value::String(wkt.into()), &postgis_type("geometry"));
        let mut expected = vec![0x01];
        expected.extend_from_slice(&(1u32 | flag).to_le_bytes());
        for coord in [1.0f64, 2.0, 3.0] {
            expected.extend_from_slice(&coord.to_le_bytes());
        }
        assert_eq!(&out[..], &expected[..], "spelling {wkt:?}");
    }
}

#[test]
fn test_string_ewkt_with_srid_to_postgis_geometry() {
    let out = convert(
        Value::String("SRID=4326;POINT(30.5 59.9)".into()),
        &postgis_type("geometry"),
    );
    assert_eq!(&out[..], &point_ewkb(Some(4326))[..]);
}

#[test]
fn test_string_hex_ewkb_to_postgis_geometry() {
    let expected = point_ewkb(Some(4326));
    let hex_upper: String = expected.iter().map(|b| format!("{b:02X}")).collect();
    let hex_lower = hex_upper.to_lowercase();
    for hex in [hex_upper, hex_lower] {
        let out = convert(Value::String(hex.into()), &postgis_type("geometry"));
        assert_eq!(&out[..], &expected[..]);
    }
}

#[test]
fn test_postgis_geometry_read_write_roundtrip() {
    use postgres::types::FromSql;
    let geometry = postgis_type("geometry");
    let ewkb = point_ewkb(Some(4326));
    // reading a geometry column produces hex-encoded EWKB
    let value = Value::from_sql(&geometry, &ewkb).unwrap();
    let Value::String(ref hex) = value else {
        panic!("expected a string value, got {value:?}");
    };
    assert!(hex.chars().all(|c| c.is_ascii_hexdigit()));
    // writing that string back produces the original bytes
    let out = convert(value.clone(), &geometry);
    assert_eq!(&out[..], &ewkb[..]);
}

fn nan_coords(n: usize) -> Vec<u8> {
    (0..n).flat_map(|_| f64::NAN.to_le_bytes()).collect()
}

fn conversion_error(value: Value, postgres_type: &Type) -> String {
    match value.to_sql_checked(postgres_type, &mut BytesMut::new()) {
        Ok(_) => panic!("expected {value:?} not to convert to {postgres_type}"),
        Err(error) => error.to_string(),
    }
}

#[test]
fn test_string_empty_point_to_postgis_geometry() {
    // PostGIS encodes POINT EMPTY as a point with NaN coordinates.
    let mut expected = vec![0x01];
    expected.extend_from_slice(&1u32.to_le_bytes());
    expected.extend(nan_coords(2));
    let out = convert(
        Value::String("POINT EMPTY".into()),
        &postgis_type("geometry"),
    );
    assert_eq!(&out[..], &expected[..]);

    let mut expected = vec![0x01];
    expected.extend_from_slice(&0xA000_0001u32.to_le_bytes()); // point + Z + SRID
    expected.extend_from_slice(&4326u32.to_le_bytes());
    expected.extend(nan_coords(3));
    let out = convert(
        Value::String("SRID=4326;POINT Z EMPTY".into()),
        &postgis_type("geometry"),
    );
    assert_eq!(&out[..], &expected[..]);

    // Other empty geometries keep their declared dimensions.
    let mut expected = vec![0x01];
    expected.extend_from_slice(&0x8000_0002u32.to_le_bytes()); // linestring + Z
    expected.extend_from_slice(&0u32.to_le_bytes());
    let out = convert(
        Value::String("LINESTRING Z EMPTY".into()),
        &postgis_type("geometry"),
    );
    assert_eq!(&out[..], &expected[..]);
}

#[test]
fn test_string_ewkt_srid_prefix_is_case_insensitive() {
    for wkt in ["Srid=4326;POINT(30.5 59.9)", "sRiD=4326; POINT(30.5 59.9)"] {
        let out = convert(Value::String(wkt.into()), &postgis_type("geometry"));
        assert_eq!(&out[..], &point_ewkb(Some(4326))[..], "spelling {wkt:?}");
    }
}

#[test]
fn test_string_damaged_hex_ewkb_reports_hex_error() {
    let message = conversion_error(Value::String("010100000".into()), &postgis_type("geometry"));
    assert!(message.contains("malformed hex EWKB"), "{message}");
    let message = conversion_error(
        Value::String("0102ZZ0000000000".into()),
        &postgis_type("geometry"),
    );
    assert!(message.contains("malformed hex EWKB"), "{message}");
}

#[test]
fn test_string_inconsistent_wkt_dimensions_are_rejected() {
    // PostGIS refuses to mix dimensionalities; the coordinate must not be
    // zero-filled silently.
    let message = conversion_error(
        Value::String("GEOMETRYCOLLECTION Z (POINT(1 2))".into()),
        &postgis_type("geometry"),
    );
    assert!(message.contains("mixed coordinate dimensions"), "{message}");
    // An empty point inside a collection has no WKB representation here.
    assert_failure(
        Value::String("MULTIPOINT(EMPTY,(1 2))".into()),
        &postgis_type("geometry"),
    );
    // The declared dimensions may be carried by the members alone.
    let out = convert(
        Value::String("GEOMETRYCOLLECTION(POINT Z (1 2 3))".into()),
        &postgis_type("geometry"),
    );
    assert_eq!(out[1..5], 0x8000_0007u32.to_le_bytes()); // collection + Z
}

#[test]
fn test_string_invalid_geometry() {
    assert_failure(
        Value::String("not a geometry".into()),
        &postgis_type("geometry"),
    );
    assert_failure(
        Value::String("SRID=oops;POINT(1 2)".into()),
        &postgis_type("geometry"),
    );
    assert_failure(Value::String("0102ZZ".into()), &postgis_type("geometry"));
}

#[test]
fn test_scalars_to_jsonb() {
    for json_type in [Type::JSONB, Type::JSON] {
        assert_success(
            Value::Float(42.5.into()),
            &json_type,
            serde_json::json!(42.5),
        );
        assert_success(Value::Bool(true), &json_type, serde_json::json!(true));
        assert_success(Value::Bool(false), &json_type, serde_json::json!(false));
        assert_success(
            Value::String("foo".into()),
            &json_type,
            serde_json::json!("foo"),
        );
        // a string holding a JSON document stays a JSON string, not a document
        assert_success(
            Value::String("{\"a\": 1}".into()),
            &json_type,
            serde_json::json!("{\"a\": 1}"),
        );
        assert_success(
            Value::String("123".into()),
            &json_type,
            serde_json::json!("123"),
        );
    }

    // non-finite floats are not representable in JSON
    assert_failure(Value::Float(f64::NAN.into()), &Type::JSONB);
    assert_failure(Value::Float(f64::INFINITY.into()), &Type::JSONB);
    assert_failure(Value::Float(f64::NEG_INFINITY.into()), &Type::JSONB);
}
