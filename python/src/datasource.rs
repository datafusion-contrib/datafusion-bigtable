use pyo3::prelude::*;
use std::sync::Arc;

use datafusion::datasource::TableProvider;
use datafusion_bigtable::datasource::BigtableDataSource;

use crate::errors::DataFusionError;
use crate::utils::wait_for_future;
use arrow::datatypes::{DataType, Field};

#[pyclass(name = "BigtableTable", module = "datafusion_bigtable", subclass)]
pub(crate) struct PyBigtableTable {
    table: Arc<dyn TableProvider>,
}

impl PyBigtableTable {
    pub fn table(&self) -> Arc<dyn TableProvider> {
        self.table.clone()
    }
}

#[pymethods]
impl PyBigtableTable {
    #[new]
    fn new(
        project: String,
        instance: String,
        table: String,
        column_family: String,
        table_partition_cols: Vec<String>,
        table_partition_separator: String,
        qualifiers: Vec<String>,
        only_read_latest: bool,
        py: Python,
    ) -> PyResult<Self> {
        let mut columns: Vec<Field> = vec![];
        for qualifier in &qualifiers {
            columns.push(Field::new(qualifier, DataType::Utf8, false))
        }
        let result = BigtableDataSource::new(
            project,
            instance,
            table,
            column_family,
            table_partition_cols,
            table_partition_separator,
            columns,
            only_read_latest,
        );
        let source = wait_for_future(py, result).map_err(DataFusionError::from)?;

        Ok(Self {
            table: Arc::new(source),
        })
    }
}
