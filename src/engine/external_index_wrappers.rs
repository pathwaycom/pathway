// Copyright © 2024 Pathway

use crate::engine::ColumnPath;
use crate::engine::TableHandle;

pub struct ExternalIndexData {
    pub table: TableHandle,
    pub data_column: ColumnPath,
    pub filter_data_column: Option<ColumnPath>,
}

pub struct ExternalIndexQuery {
    pub table: TableHandle,
    pub query_column: ColumnPath,
    pub limit_column: Option<ColumnPath>,
    pub filter_column: Option<ColumnPath>,
}
