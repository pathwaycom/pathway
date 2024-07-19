// Copyright © 2024 Pathway

use pathway_engine::engine::Type;

#[test]
fn test_type_display() {
    assert_eq!(Type::Any.to_string(), "Any");
    assert_eq!(Type::Bool.to_string(), "bool");
    assert_eq!(Type::Int.to_string(), "int");
    assert_eq!(Type::Float.to_string(), "float");
    assert_eq!(Type::Pointer.to_string(), "Pointer");
    assert_eq!(Type::String.to_string(), "str");
    assert_eq!(Type::Bytes.to_string(), "bytes");
    assert_eq!(Type::DateTimeNaive.to_string(), "DateTimeNaive");
    assert_eq!(Type::DateTimeUtc.to_string(), "DateTimeUtc");
    assert_eq!(Type::Duration.to_string(), "Duration");
    assert_eq!(
        Type::Array(Some(2), Type::Int.into()).to_string(),
        "Array(2, int)"
    );
    assert_eq!(
        Type::Array(None, Type::Float.into()).to_string(),
        "Array(float)"
    );
    assert_eq!(Type::Json.to_string(), "Json");
    assert_eq!(Type::Tuple([].into()).to_string(), "tuple[]");
    assert_eq!(
        Type::Tuple([Type::String, Type::Bytes].into()).to_string(),
        "tuple[str, bytes]"
    );
    assert_eq!(Type::List(Type::Bool.into()).to_string(), "list[bool]");
    assert_eq!(Type::List(Type::Int.into()).to_string(), "list[int]");
    assert_eq!(Type::PyObjectWrapper.to_string(), "PyObjectWrapper");
    assert_eq!(
        Type::Optional(Type::Pointer.into()).to_string(),
        "Pointer | None"
    );
    assert_eq!(Type::Optional(Type::Int.into()).to_string(), "int | None");
}
