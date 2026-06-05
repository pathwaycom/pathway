// Copyright © 2026 Pathway

use pathway_engine::connectors::data_storage::clickhouse::clickhouse_type_compatible;

#[test]
fn type_compatibility_matches_clickhouse_insert_cast_rules() {
    // Exact matches are always compatible.
    assert!(clickhouse_type_compatible("Int64", "Int64"));
    assert!(clickhouse_type_compatible("Bool", "Bool"));
    assert!(clickhouse_type_compatible(
        "Nullable(Int64)",
        "Nullable(Int64)"
    ));
    assert!(clickhouse_type_compatible(
        "DateTime64(9, 'UTC')",
        "DateTime64(9, 'UTC')"
    ));

    // ClickHouse inserts do not widen or convert numbers, so different
    // numeric types are incompatible.
    assert!(!clickhouse_type_compatible("Int64", "Int32"));
    assert!(!clickhouse_type_compatible("Int64", "UInt64"));
    assert!(!clickhouse_type_compatible("Float64", "Float32"));
    assert!(!clickhouse_type_compatible("Int8", "Int16"));

    // Nullability must match exactly.
    assert!(!clickhouse_type_compatible("Int64", "Nullable(Int64)"));
    assert!(!clickhouse_type_compatible("String", "Nullable(String)"));
    assert!(!clickhouse_type_compatible("Nullable(Int64)", "Int64"));
    // ...and a nullable inner mismatch is still caught.
    assert!(!clickhouse_type_compatible(
        "Nullable(Int64)",
        "Nullable(Int32)"
    ));

    // A String value may be inserted into a FixedString column.
    assert!(clickhouse_type_compatible("String", "FixedString(10)"));
    assert!(clickhouse_type_compatible(
        "Nullable(String)",
        "Nullable(FixedString(8))"
    ));
    // But a FixedString value would not necessarily fit a String here; the
    // connector only ever produces `String`, so the reverse never arises and
    // a genuinely different type is rejected.
    assert!(!clickhouse_type_compatible("String", "Int64"));

    // A datetime value casts to any DateTime/DateTime64 destination.
    assert!(clickhouse_type_compatible(
        "DateTime64(9, 'UTC')",
        "DateTime"
    ));
    assert!(clickhouse_type_compatible(
        "DateTime64(9, 'UTC')",
        "DateTime64(3)"
    ));
    assert!(clickhouse_type_compatible(
        "Nullable(DateTime64(9, 'UTC'))",
        "Nullable(DateTime64(3, 'UTC'))"
    ));
    // A datetime value cannot go into a non-datetime column.
    assert!(!clickhouse_type_compatible("DateTime64(9, 'UTC')", "Int64"));

    // Arrays are compatible iff their element types are; the relaxations
    // recurse through the element type.
    assert!(clickhouse_type_compatible("Array(Int64)", "Array(Int64)"));
    assert!(clickhouse_type_compatible(
        "Array(String)",
        "Array(FixedString(4))"
    ));
    assert!(!clickhouse_type_compatible("Array(Int64)", "Array(Int32)"));
    // An array and a scalar of the same element name are not interchangeable.
    assert!(!clickhouse_type_compatible("Array(Int64)", "Int64"));
    assert!(!clickhouse_type_compatible("Int64", "Array(Int64)"));
}
