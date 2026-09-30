// Copyright © 2026 Pathway

use crate::engine::{Result, Value};

use super::{Formatter, FormatterContext, FormatterError};

pub struct SingleColumnFormatter {
    value_field_index: usize,
}

impl SingleColumnFormatter {
    pub fn new(value_field_index: usize) -> SingleColumnFormatter {
        SingleColumnFormatter { value_field_index }
    }
}

impl Formatter for SingleColumnFormatter {
    fn format_into(&mut self, context: &mut FormatterContext) -> Result<(), FormatterError> {
        let mut buffer = context.take_payload_buffer();
        match context
            .values
            .get(self.value_field_index)
            .ok_or(FormatterError::IncorrectColumnIndex)?
        {
            Value::Bytes(bytes) => buffer.extend_from_slice(bytes),
            Value::String(string) => buffer.extend_from_slice(string.as_bytes()),
            _ => return Err(FormatterError::UnsupportedValueType),
        }
        context.push_payload(buffer);
        Ok(())
    }
}
