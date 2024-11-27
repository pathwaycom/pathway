// Copyright © 2024 Pathway

use log::error;
use std::io::BufReader;
use std::io::Read;
use std::mem::take;

use csv::Reader as CsvReader;
use csv::ReaderBuilder as CsvReaderBuilder;

use crate::connectors::data_storage::ReadMethod;
use crate::connectors::{DataEventType, ReadError, ReaderContext};

type TokenizedEntry = (ReaderContext, u64); // The second value is a position of the record within the object read

pub trait Tokenize: Send + 'static {
    fn set_new_reader(
        &mut self,
        source: Box<dyn Read + Send + 'static>,
        data_event_type: DataEventType,
    ) -> Result<(), ReadError>;
    fn next_entry(&mut self) -> Result<Option<TokenizedEntry>, ReadError>;
    fn seek(&mut self, bytes_offset: u64) -> Result<(), ReadError>;
}

pub struct CsvTokenizer {
    parser_builder: CsvReaderBuilder,
    current_event_type: DataEventType,
    csv_reader: Option<CsvReader<Box<dyn Read + Send + 'static>>>,
    deferred_next_entry: Option<TokenizedEntry>,
}

impl CsvTokenizer {
    pub fn new(parser_builder: CsvReaderBuilder) -> Self {
        Self {
            parser_builder,
            current_event_type: DataEventType::Insert,
            csv_reader: None,
            deferred_next_entry: None,
        }
    }
}

impl Tokenize for CsvTokenizer {
    fn set_new_reader(
        &mut self,
        source: Box<dyn Read + Send + 'static>,
        data_event_type: DataEventType,
    ) -> Result<(), ReadError> {
        self.csv_reader = Some(self.parser_builder.flexible(true).from_reader(source));
        self.current_event_type = data_event_type;
        Ok(())
    }

    fn next_entry(&mut self) -> Result<Option<(ReaderContext, u64)>, ReadError> {
        if let Some(deferred_next_entry) = take(&mut self.deferred_next_entry) {
            return Ok(Some(deferred_next_entry));
        }

        if let Some(ref mut csv_reader) = self.csv_reader {
            let mut current_record = csv::StringRecord::new();
            if csv_reader.read_record(&mut current_record)? {
                Ok(Some((
                    ReaderContext::from_tokenized_entries(
                        self.current_event_type,
                        current_record
                            .iter()
                            .map(std::string::ToString::to_string)
                            .collect(),
                    ),
                    csv_reader.position().byte(),
                )))
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }

    fn seek(&mut self, bytes_offset: u64) -> Result<(), ReadError> {
        if bytes_offset == 0 {
            return Ok(());
        }
        let csv_reader = self
            .csv_reader
            .as_mut()
            .expect("seek for an uninitialized tokenizer");

        let mut header_record = csv::StringRecord::new();
        if csv_reader.read_record(&mut header_record)? {
            let header_reader_context = ReaderContext::from_tokenized_entries(
                self.current_event_type,
                header_record
                    .iter()
                    .map(std::string::ToString::to_string)
                    .collect(),
            );
            self.deferred_next_entry = Some((header_reader_context, bytes_offset));
        }

        let mut current_offset = csv_reader.position().byte();
        let mut current_record = csv::StringRecord::new();
        while current_offset < bytes_offset && csv_reader.read_record(&mut current_record)? {
            current_offset = csv_reader.position().byte();
        }
        if current_offset != bytes_offset {
            error!("Inconsistent bytes position in rewinded CSV object: expected {bytes_offset}, got {current_offset}");
        }
        Ok(())
    }
}

pub struct BufReaderTokenizer {
    current_event_type: DataEventType,
    reader: Option<BufReader<Box<dyn Read + Send + 'static>>>,
    read_method: ReadMethod,
    current_bytes_read: u64,
}

impl BufReaderTokenizer {
    pub fn new(read_method: ReadMethod) -> Self {
        Self {
            current_event_type: DataEventType::Insert,
            reader: None,
            read_method,
            current_bytes_read: 0,
        }
    }
}

impl Tokenize for BufReaderTokenizer {
    fn set_new_reader(
        &mut self,
        source: Box<dyn Read + Send + 'static>,
        data_event_type: DataEventType,
    ) -> Result<(), ReadError> {
        self.reader = Some(BufReader::new(source));
        self.current_event_type = data_event_type;
        self.current_bytes_read = 0;
        Ok(())
    }

    fn next_entry(&mut self) -> Result<Option<(ReaderContext, u64)>, ReadError> {
        if let Some(ref mut reader) = self.reader {
            let mut line = Vec::new();
            let len = self.read_method.read_next_bytes(reader, &mut line)?;
            if len > 0 || self.read_method == ReadMethod::Full {
                self.current_bytes_read += len as u64;
                if self.read_method == ReadMethod::Full {
                    self.reader = None;
                }
                Ok(Some((
                    ReaderContext::from_raw_bytes(self.current_event_type, line),
                    self.current_bytes_read,
                )))
            } else {
                self.reader = None;
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }

    fn seek(&mut self, bytes_offset: u64) -> Result<(), ReadError> {
        if bytes_offset == 0 {
            return Ok(());
        }

        let reader = self
            .reader
            .as_mut()
            .expect("seek for an uninitialized tokenizer");

        let mut bytes_read = 0;
        while bytes_read < bytes_offset {
            let mut current_line = Vec::new();
            let len = self
                .read_method
                .read_next_bytes(reader, &mut current_line)?;
            if len == 0 {
                break;
            }
            bytes_read += len as u64;
        }

        if bytes_read != bytes_offset {
            if bytes_read == bytes_offset + 1 || bytes_read == bytes_offset + 2 {
                error!("Read {} bytes instead of expected {bytes_read}. If the file did not have newline at the end, you can ignore this message", bytes_offset);
            } else {
                error!("Inconsistent bytes position in rewinded plaintext object: expected {bytes_read}, got {bytes_offset}");
            }
        }

        self.current_bytes_read = bytes_read;
        Ok(())
    }
}
