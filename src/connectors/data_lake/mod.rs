use deltalake::arrow::array::RecordBatch as ArrowRecordBatch;

use crate::connectors::WriteError;
use crate::engine::Type;

pub mod delta;
pub mod iceberg;
pub mod writer;

pub use delta::DeltaBatchWriter;
pub use iceberg::IcebergBatchWriter;
pub use writer::LakeWriter;

const SPECIAL_OUTPUT_FIELDS: [(&str, Type); 2] = [("time", Type::Int), ("diff", Type::Int)];

pub trait LakeBatchWriter: Send {
    fn write_batch(&mut self, batch: ArrowRecordBatch) -> Result<(), WriteError>;
}
