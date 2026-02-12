// Copyright © 2026 Pathway

use std::mem::take;

use crate::async_runtime::create_async_tokio_runtime;
use crate::connectors::data_format::FormatterContext;
use crate::connectors::{WriteError, Writer};

use elasticsearch::{BulkParts, Elasticsearch};
use tokio::runtime::Runtime as TokioRuntime;

pub struct ElasticSearchWriter {
    runtime: TokioRuntime,
    client: Elasticsearch,
    index_name: String,
    max_batch_size: Option<usize>,

    docs_buffer: Vec<Vec<u8>>,
}

impl ElasticSearchWriter {
    pub fn new(
        client: Elasticsearch,
        index_name: String,
        max_batch_size: Option<usize>,
    ) -> Result<Self, WriteError> {
        Ok(ElasticSearchWriter {
            runtime: create_async_tokio_runtime()?,
            client,
            index_name,
            max_batch_size,
            docs_buffer: Vec::new(),
        })
    }
}

impl Writer for ElasticSearchWriter {
    fn write(&mut self, data: FormatterContext) -> Result<(), WriteError> {
        for payload in data.payloads {
            self.docs_buffer.push(b"{\"index\": {}}".to_vec());
            self.docs_buffer.push(payload.into_raw_bytes()?);
        }

        if let Some(max_batch_size) = self.max_batch_size {
            if self.docs_buffer.len() / 2 >= max_batch_size {
                self.flush(true)?;
            }
        }

        Ok(())
    }

    fn flush(&mut self, _forced: bool) -> Result<(), WriteError> {
        if self.docs_buffer.is_empty() {
            return Ok(());
        }
        self.runtime.block_on(async {
            self.client
                .bulk(BulkParts::Index(&self.index_name))
                .body(take(&mut self.docs_buffer))
                .send()
                .await
                .map_err(WriteError::Elasticsearch)?
                .error_for_status_code()
                .map_err(WriteError::Elasticsearch)?;

            Ok(())
        })
    }

    fn name(&self) -> String {
        format!("ElasticSearch({})", self.index_name)
    }

    fn single_threaded(&self) -> bool {
        false
    }
}
