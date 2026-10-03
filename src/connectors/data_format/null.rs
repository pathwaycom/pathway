// Copyright © 2026 Pathway

use crate::engine::Result;

use super::{Formatter, FormatterContext, FormatterError};

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
    fn format_into(&mut self, context: &mut FormatterContext) -> Result<(), FormatterError> {
        // Nothing is written anywhere; the row's values are not even kept.
        context.values.clear();
        Ok(())
    }
}
