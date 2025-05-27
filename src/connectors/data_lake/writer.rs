use std::time::{Duration, Instant};

use crate::connectors::data_format::FormatterContext;
use crate::connectors::data_lake::buffering::ColumnBuffer;
use crate::connectors::data_lake::LakeBatchWriter;
use crate::connectors::{WriteError, Writer};

#[allow(clippy::module_name_repetitions)]
pub struct LakeWriter {
    batch_writer: Box<dyn LakeBatchWriter>,
    buffer: Box<dyn ColumnBuffer>,
    min_commit_frequency: Option<Duration>,
    last_commit_at: Instant,
}

impl LakeWriter {
    pub fn new(
        batch_writer: Box<dyn LakeBatchWriter>,
        buffer: Box<dyn ColumnBuffer>,
        min_commit_frequency: Option<Duration>,
    ) -> Result<Self, WriteError> {
        Ok(Self {
            batch_writer,
            buffer,
            min_commit_frequency,

            // before the first commit, the time should be
            // measured from the moment of the start
            last_commit_at: Instant::now(),
        })
    }
}

impl Writer for LakeWriter {
    fn write(&mut self, data: FormatterContext) -> Result<(), WriteError> {
        self.buffer.add_event(data)
    }

    fn flush(&mut self, forced: bool) -> Result<(), WriteError> {
        let commit_needed = self.buffer.has_updates()
            && (self
                .min_commit_frequency
                .map_or(true, |f| self.last_commit_at.elapsed() >= f)
                || forced);
        if commit_needed {
            let (batch, payload_type) = self.buffer.build_update_record_batch()?;
            self.batch_writer.write_batch(batch, payload_type)?;
            self.buffer.on_changes_written();
        }
        Ok(())
    }

    fn name(&self) -> String {
        self.batch_writer.name()
    }
}
