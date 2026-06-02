// Copyright © 2026 Pathway

use std::io::BufWriter;
use std::io::Write;

use crate::connectors::data_format::FormatterContext;

use super::{WriteError, Writer};

pub struct FileWriter {
    writer: BufWriter<std::fs::File>,
    output_path: String,
}

impl FileWriter {
    pub fn new(writer: BufWriter<std::fs::File>, output_path: String) -> FileWriter {
        FileWriter {
            writer,
            output_path,
        }
    }
}

impl Writer for FileWriter {
    fn write(&mut self, data: FormatterContext) -> Result<(), WriteError> {
        for payload in data.payloads {
            self.writer.write_all(&payload.into_raw_bytes()?)?;
            self.writer.write_all(b"\n")?;
        }
        Ok(())
    }

    fn flush(&mut self, _forced: bool) -> Result<(), WriteError> {
        self.writer.flush()?;
        Ok(())
    }

    fn name(&self) -> String {
        format!("FileSystem({})", self.output_path)
    }
}
