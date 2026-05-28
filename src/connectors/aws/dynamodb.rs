use log::error;
use std::collections::HashMap;
use std::mem::take;

use aws_sdk_dynamodb::error::SdkError;
use aws_sdk_dynamodb::operation::batch_write_item::BatchWriteItemError;
use aws_sdk_dynamodb::operation::create_table::builders::CreateTableFluentBuilder;
use aws_sdk_dynamodb::operation::create_table::CreateTableError;
use aws_sdk_dynamodb::operation::delete_table::DeleteTableError;
use aws_sdk_dynamodb::operation::describe_table::DescribeTableError;
use aws_sdk_dynamodb::types::{
    AttributeDefinition, AttributeValue, BillingMode, DeleteRequest, KeySchemaElement, KeyType,
    PutRequest, ScalarAttributeType, WriteRequest,
};
use aws_sdk_dynamodb::Client;
use aws_smithy_runtime_api::http::Response as AwsHttpResponse;
use ndarray::ArrayD;
use tokio::runtime::Runtime as TokioRuntime;

use crate::connectors::data_format::{
    FormatterContext, FormatterError, NDARRAY_ELEMENTS_FIELD_NAME, NDARRAY_SHAPE_FIELD_NAME,
};
use crate::connectors::data_storage::TableWriterInitMode;
use crate::connectors::{WriteError, Writer};
use crate::engine::{Type, Value};
use crate::python_api::ValueField;
use crate::retry::RetryConfig;

// No more than 25 items can be sent in a single batch
// There is no public constant for that, so we create our own
// https://docs.rs/aws-sdk-dynamodb/latest/aws_sdk_dynamodb/operation/batch_write_item/builders/struct.BatchWriteItemFluentBuilder.html
pub const MAX_BATCH_WRITE_SIZE: usize = 25;
pub const N_SEND_ATTEMPTS: usize = 5;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Create table error, error details: {0:?}")]
    CreateTable(#[from] SdkError<CreateTableError, AwsHttpResponse>),

    #[error("Delete table error, error details: {0:?}")]
    DeleteTable(#[from] SdkError<DeleteTableError, AwsHttpResponse>),

    #[error("Describe table error, error details: {0:?}")]
    DescribeTable(#[from] SdkError<DescribeTableError, AwsHttpResponse>),

    #[error("Batch write error, error details: {0:?}")]
    BatchWrite(#[from] SdkError<BatchWriteItemError, AwsHttpResponse>),
}

// The primary key of a pending row: the partition key value plus the optional
// sort key value. Used to deduplicate operations within a single batch, since
// DynamoDB rejects a `BatchWriteItem` that targets the same key more than once.
type PrimaryKey = (Value, Option<Value>);

pub struct DynamoDBWriter {
    runtime: TokioRuntime,
    client: Client,
    table_name: String,
    value_fields: Vec<ValueField>,
    write_requests: HashMap<PrimaryKey, WriteRequest>,
    partition_key_index: usize,
    sort_key_index: Option<usize>,
}

impl DynamoDBWriter {
    pub fn new(
        runtime: TokioRuntime,
        client: Client,
        table_name: String,
        value_fields: Vec<ValueField>,
        partition_key_index: usize,
        sort_key_index: Option<usize>,
        init_mode: TableWriterInitMode,
    ) -> Result<Self, WriteError> {
        let writer = Self {
            runtime,
            client,
            table_name,
            value_fields,
            write_requests: HashMap::new(),
            partition_key_index,
            sort_key_index,
        };

        match init_mode {
            TableWriterInitMode::Default => {}
            TableWriterInitMode::Replace => {
                if writer.table_exists()? {
                    writer.delete_table()?;
                }
                writer.create_table_from_schema()?;
            }
            TableWriterInitMode::CreateIfNotExists => {
                if !writer.table_exists()? {
                    writer.create_table_from_schema()?;
                }
            }
        }

        if !writer.table_exists()? {
            return Err(WriteError::TableDoesNotExist(writer.table_name));
        }

        Ok(writer)
    }

    fn rust_type_to_dynamodb_index_type(ty: &Type) -> Result<ScalarAttributeType, WriteError> {
        // A DynamoDB key attribute can only be one of three scalar types: String
        // (`S`), Number (`N`) or Binary (`B`). There is no Boolean key type, so a
        // `bool` column (which serializes to `AttributeValue::Bool`) cannot be a
        // key and must be rejected here rather than declared as some other scalar
        // type — otherwise the table would be created with a key type that every
        // write then mismatches. The arms below must stay in sync with the scalar
        // outputs of `value_to_attribute`.
        match ty {
            Type::Int | Type::Float | Type::Duration => Ok(ScalarAttributeType::N),
            Type::String | Type::Pointer | Type::DateTimeNaive | Type::DateTimeUtc | Type::Json => {
                Ok(ScalarAttributeType::S)
            }
            Type::Bytes | Type::PyObjectWrapper => Ok(ScalarAttributeType::B),
            _ => Err(WriteError::NotIndexType(ty.clone())),
        }
    }

    fn add_attribute_definition(
        mut builder: CreateTableFluentBuilder,
        field: &ValueField,
        key_type: KeyType,
    ) -> Result<CreateTableFluentBuilder, WriteError> {
        let scalar_type = Self::rust_type_to_dynamodb_index_type(&field.type_)?;
        builder = builder.attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name(field.name.clone())
                .attribute_type(scalar_type)
                .build()?,
        );
        builder = builder.key_schema(
            KeySchemaElement::builder()
                .attribute_name(field.name.clone())
                .key_type(key_type)
                .build()?,
        );
        Ok(builder)
    }

    fn create_table_from_schema(&self) -> Result<(), WriteError> {
        self.runtime.block_on(async {
            let mut builder = self
                .client
                .create_table()
                .table_name(self.table_name.clone())
                .billing_mode(BillingMode::PayPerRequest);
            builder = Self::add_attribute_definition(
                builder,
                &self.value_fields[self.partition_key_index],
                KeyType::Hash,
            )?;
            if let Some(sort_key_index) = self.sort_key_index {
                builder = Self::add_attribute_definition(
                    builder,
                    &self.value_fields[sort_key_index],
                    KeyType::Range,
                )?;
            }

            // Convert the possible error first into Error,
            // and then to WriteError, if needed.
            builder.send().await.map_err(Error::from)?;
            Ok(())
        })
    }

    fn delete_table(&self) -> Result<(), WriteError> {
        self.runtime.block_on(async {
            self.client
                .delete_table()
                .table_name(self.table_name.clone())
                .send()
                .await?;
            Ok::<(), Error>(())
        })?;
        Ok(())
    }

    fn table_exists(&self) -> Result<bool, WriteError> {
        self.runtime.block_on(async {
            let table_description = self
                .client
                .describe_table()
                .table_name(self.table_name.clone())
                .send()
                .await;

            match table_description {
                Ok(_table_info) => Ok(true),
                Err(err) => {
                    if matches!(
                        err.as_service_error(),
                        Some(DescribeTableError::ResourceNotFoundException(_))
                    ) {
                        Ok(false)
                    } else {
                        Err(Error::from(err).into())
                    }
                }
            }
        })
    }

    fn array_to_attribute_value<T>(arr: &ArrayD<T>) -> AttributeValue
    where
        T: ToString,
    {
        let mut value = HashMap::with_capacity(2);

        let list = arr
            .iter()
            .map(|i| AttributeValue::N(i.to_string()))
            .collect::<Vec<_>>();
        value.insert(
            NDARRAY_ELEMENTS_FIELD_NAME.to_string(),
            AttributeValue::L(list),
        );

        let shape = arr
            .shape()
            .iter()
            .map(|i| AttributeValue::N(i.to_string()))
            .collect::<Vec<_>>();
        value.insert(
            NDARRAY_SHAPE_FIELD_NAME.to_string(),
            AttributeValue::L(shape),
        );

        AttributeValue::M(value)
    }

    fn value_to_attribute(value: &Value) -> Result<AttributeValue, WriteError> {
        match value {
            Value::None => Ok(AttributeValue::Null(true)),
            Value::Bool(b) => Ok(AttributeValue::Bool(*b)),
            Value::Int(i) => Ok(AttributeValue::N(i.to_string())),
            Value::Float(f) => Ok(AttributeValue::N(f.to_string())),
            Value::String(s) => Ok(AttributeValue::S(s.to_string())),
            Value::Bytes(b) => Ok(AttributeValue::B(b.to_vec().into())),
            Value::Pointer(p) => Ok(AttributeValue::S(p.to_string())),
            Value::Tuple(t) => {
                let list = t
                    .iter()
                    .map(Self::value_to_attribute)
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(AttributeValue::L(list))
            }
            Value::IntArray(arr) => Ok(Self::array_to_attribute_value(arr)),
            Value::FloatArray(arr) => Ok(Self::array_to_attribute_value(arr)),
            Value::DateTimeNaive(dt) => Ok(AttributeValue::S(dt.to_string())),
            Value::DateTimeUtc(dt) => Ok(AttributeValue::S(dt.to_string())),
            Value::Duration(d) => Ok(AttributeValue::N(d.nanoseconds().to_string())),
            Value::Json(j) => Ok(AttributeValue::S(j.to_string())),
            Value::PyObjectWrapper(v) => Ok(AttributeValue::B(
                bincode::serialize(v).map_err(|e| *e)?.into(),
            )),
            Value::Error | Value::Pending => {
                Err(FormatterError::ValueNonSerializable(value.kind(), "DynamoDB").into())
            }
        }
    }

    fn create_upsert_request(&self, data: &FormatterContext) -> Result<WriteRequest, WriteError> {
        let mut values_prepared_as_map = HashMap::with_capacity(self.value_fields.len());
        for (value_field, entry) in self.value_fields.iter().zip(data.values.iter()) {
            values_prepared_as_map
                .insert(value_field.name.clone(), Self::value_to_attribute(entry)?);
        }

        Ok(WriteRequest::builder()
            .put_request(
                PutRequest::builder()
                    .set_item(Some(values_prepared_as_map))
                    .build()?,
            )
            .build())
    }

    fn create_delete_request(&self, data: &FormatterContext) -> Result<WriteRequest, WriteError> {
        let mut values_prepared_as_map = HashMap::with_capacity(2);
        values_prepared_as_map.insert(
            self.value_fields[self.partition_key_index].name.clone(),
            Self::value_to_attribute(&data.values[self.partition_key_index])?,
        );
        if let Some(sort_key_index) = self.sort_key_index {
            values_prepared_as_map.insert(
                self.value_fields[sort_key_index].name.clone(),
                Self::value_to_attribute(&data.values[sort_key_index])?,
            );
        }

        Ok(WriteRequest::builder()
            .delete_request(
                DeleteRequest::builder()
                    .set_key(Some(values_prepared_as_map))
                    .build()?,
            )
            .build())
    }

    fn primary_key(&self, data: &FormatterContext) -> PrimaryKey {
        let partition_key = data.values[self.partition_key_index].clone();
        let sort_key = self.sort_key_index.map(|index| data.values[index].clone());
        (partition_key, sort_key)
    }
}

impl Writer for DynamoDBWriter {
    fn write(&mut self, data: FormatterContext) -> Result<(), WriteError> {
        let primary_key = self.primary_key(&data);
        match data.diff {
            1 => {
                // An insertion always wins over a deletion of the same key within
                // a batch. DynamoDB rejects a `BatchWriteItem` that targets the
                // same key with both a put and a delete, and snapshot semantics
                // keep the freshly-inserted row, so we overwrite any pending
                // deletion for this key.
                let request = self.create_upsert_request(&data)?;
                self.write_requests.insert(primary_key, request);
            }
            -1 => {
                // Record the deletion only if no insertion for this key is already
                // pending in the current batch (the insertion takes precedence,
                // regardless of the order the two events arrive in).
                if !self.write_requests.contains_key(&primary_key) {
                    let request = self.create_delete_request(&data)?;
                    self.write_requests.insert(primary_key, request);
                }
            }
            _ => unreachable!("diff can only be 1 or -1"),
        }
        if self.write_requests.len() >= MAX_BATCH_WRITE_SIZE {
            self.flush(false)?;
        }
        Ok(())
    }

    fn flush(&mut self, _forced: bool) -> Result<(), WriteError> {
        if self.write_requests.is_empty() {
            return Ok(());
        }
        let requests: Vec<WriteRequest> = take(&mut self.write_requests).into_values().collect();
        let mut request_items = HashMap::with_capacity(1);
        request_items.insert(self.table_name.clone(), requests);

        self.runtime.block_on(async {
            let mut retry = RetryConfig::default();

            for _ in 0..N_SEND_ATTEMPTS {
                let response = self
                    .client
                    .batch_write_item()
                    .set_request_items(Some(request_items.clone()))
                    .send()
                    .await;

                match response {
                    Ok(response) => {
                        // If there are unprocessed items in the response, save them for the next request.
                        // Otherwise the request has succeeded, and has no items to process further.
                        if let Some(unprocessed_items) = response.unprocessed_items {
                            request_items = unprocessed_items;
                        } else {
                            request_items.clear();
                        }

                        if let Some(unprocessed_requests) = request_items.get(&self.table_name) {
                            // If there's a non-empty array with unprocessed items, it must be retried.
                            // Otherwise, the method may terminate.
                            if unprocessed_requests.is_empty() {
                                return Ok(());
                            }
                        } else {
                            // If there's no vector with the items waiting for submission, it means that
                            // everything has been sent
                            return Ok(());
                        }
                    }
                    Err(e) => {
                        error!(
                            "An attempt to save item batch has failed: {}",
                            Error::from(e)
                        );
                    }
                }

                retry.sleep_after_error();
            }
            let unprocessed_items = request_items.remove(&self.table_name);
            if let Some(unprocessed_items) = unprocessed_items {
                Err(WriteError::SomeItemsNotDelivered(unprocessed_items.len()))
            } else {
                Ok(())
            }
        })
    }

    fn name(&self) -> String {
        format!("DynamoDB({})", self.table_name)
    }

    fn single_threaded(&self) -> bool {
        false
    }
}
