// Copyright © 2026 Pathway

//! The storage-agnostic side of schema exploration: what a connector-specific
//! `explore_schema` routine returns, and how the deduced types travel to the
//! Python schema constructor.
//!
//! Every storage that supports the deduction maps its own type system to the
//! engine [`Type`] inside its own module; nothing here (or on the Python
//! side) knows any storage-specific type names. The types cross the FFI
//! boundary in the dtype-dict grammar of `pathway.internals.dtype`
//! (`parse_dtype_from_dict`) — the same one the `DeltaLake` column metadata
//! uses — so the Python side rebuilds them with the existing parser instead
//! of a parallel one.

use serde_json::{json, Value as JsonValue};

use crate::engine::{Type, Value};

/// One column of a deduced schema.
#[derive(Debug, Clone)]
pub struct ExploredField {
    pub name: String,
    pub type_: Type,
    /// The default value of the column, when the source schema defines one
    /// representable by the engine.
    pub default: Option<Value>,
    /// The documentation string of the field, when the source schema carries
    /// one.
    pub doc: Option<String>,
}

/// The `parse_dtype_from_dict` form of an engine type, consumed by the
/// Python schema constructor.
pub fn type_descriptor(type_: &Type) -> JsonValue {
    match type_ {
        Type::Bool => json!({"type": "BOOL"}),
        Type::Int => json!({"type": "INT"}),
        Type::Float => json!({"type": "FLOAT"}),
        Type::String => json!({"type": "STR"}),
        Type::Bytes => json!({"type": "BYTES"}),
        Type::Json => json!({"type": "Json"}),
        Type::DateTimeUtc => json!({"type": "DATE_TIME_UTC"}),
        Type::DateTimeNaive => json!({"type": "DATE_TIME_NAIVE"}),
        Type::Duration => json!({"type": "DURATION"}),
        Type::Optional(inner) => {
            json!({"type": "OPTIONAL", "wrapped": type_descriptor(inner)})
        }
        Type::List(element) => json!({"type": "LIST", "wrapped": type_descriptor(element)}),
        Type::Tuple(elements) => json!({
            "type": "TUPLE",
            "wrapped": elements.iter().map(type_descriptor).collect::<Vec<_>>(),
        }),
        // The remaining engine types never come out of a deduction; falling
        // back to "ANY" keeps the descriptor total.
        _ => json!({"type": "ANY"}),
    }
}
