// Copyright © 2026 Pathway

use crate::engine::{Key, Result, Timestamp, Value};

use super::{FormattedDocument, Formatter, FormatterContext, FormatterError};

pub struct NullFormatter {}

impl NullFormatter {
    pub fn new() -> NullFormatter {
        NullFormatter {}
    }
}

impl Default for NullFormatter {
    fn default() -> Self {
        Self::new()
    }
}

impl Formatter for NullFormatter {
    fn format(
        &mut self,
        key: &Key,
        _values: &[Value],
        time: Timestamp,
        diff: isize,
    ) -> Result<FormatterContext, FormatterError> {
        Ok(FormatterContext::new(
            Vec::<FormattedDocument>::new(),
            *key,
            Vec::new(),
            time,
            diff,
        ))
    }
}
