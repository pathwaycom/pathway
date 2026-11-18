// Copyright © 2026 Pathway

//! Static classification of a table column as a vector of some kind, shared by
//! the vector-store output connectors (Qdrant, Pinecone, ...). The decision is
//! made once, at startup, from the column's engine [`Type`], so every connector
//! branches on the same rules and reports the same expected spellings.

use crate::engine::Type;

/// The vector-capable kinds a column's static dtype can map to. Columns mapping
/// to `None` are not vectors at all (they go to the payload / metadata, or are
/// rejected, depending on the connector).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VectorKind {
    /// `list[float]` or a 1-D numeric `numpy.ndarray`.
    Dense,
    /// `list[tuple[int, float]]` — pairs of (index, weight).
    Sparse,
    /// `list[list[float]]` — multivectors, not supported by any connector yet.
    Multi,
}

impl VectorKind {
    /// Human-readable name used in connector error messages.
    pub fn name(self) -> &'static str {
        match self {
            Self::Dense => "dense",
            Self::Sparse => "sparse",
            Self::Multi => "multivector",
        }
    }

    /// The column dtype this kind is spelled with, as shown to the user when a
    /// connector reports a kind mismatch.
    pub fn expected_dtype(self) -> &'static str {
        match self {
            Self::Dense => "list[float] (or a 1-D numeric numpy.ndarray)",
            Self::Sparse => "list[tuple[int, float]]",
            Self::Multi => "list[list[float]]",
        }
    }
}

pub fn vector_kind_of(dtype: &Type) -> Option<VectorKind> {
    match dtype {
        Type::List(element) => match element.as_ref() {
            Type::Float => Some(VectorKind::Dense),
            Type::Tuple(fields)
                if fields.len() == 2 && fields[0] == Type::Int && fields[1] == Type::Float =>
            {
                Some(VectorKind::Sparse)
            }
            Type::List(inner) if **inner == Type::Float => Some(VectorKind::Multi),
            _ => None,
        },
        // Any ndarray column is a dense candidate: the value-level extraction
        // still rejects non-numeric or multi-dimensional arrays. A bare
        // `np.ndarray` annotation arrives as `Array(Any)`, so requiring a
        // numeric element type here would reject the common spelling.
        Type::Array(_, _) => Some(VectorKind::Dense),
        _ => None,
    }
}
