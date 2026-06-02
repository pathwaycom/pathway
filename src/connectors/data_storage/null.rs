// Copyright © 2026 Pathway

use std::fmt::Debug;

use crate::connectors::data_format::FormatterContext;

use super::{WriteError, Writer};

#[derive(Default, Debug)]
pub struct NullWriter;

impl NullWriter {
    pub fn new() -> Self {
        Self
    }
}

impl Writer for NullWriter {
    fn write(&mut self, _data: FormatterContext) -> Result<(), WriteError> {
        Ok(())
    }

    fn single_threaded(&self) -> bool {
        false
    }
}
