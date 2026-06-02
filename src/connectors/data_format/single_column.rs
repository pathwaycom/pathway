// Copyright © 2026 Pathway

use crate::engine::{Key, Result, Timestamp, Value};

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
    fn format(
        &mut self,
        key: &Key,
        values: &[Value],
        time: Timestamp,
        diff: isize,
    ) -> Result<FormatterContext, FormatterError> {
        let payload = match &values
            .get(self.value_field_index)
            .ok_or(FormatterError::IncorrectColumnIndex)?
        {
            Value::Bytes(bytes) => bytes.to_vec(),
            Value::String(string) => string.as_bytes().to_vec(),
            _ => return Err(FormatterError::UnsupportedValueType),
        };
        Ok(FormatterContext::new_single_payload(
            payload,
            *key,
            values.to_vec(),
            time,
            diff,
        ))
    }
}
