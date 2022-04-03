use pyo3::prelude::*;
use std::sync::Arc;

use datafusion_bigtable::datasource::BigtableDataSource;

use arrow::datatypes::{DataType, Field};
use datafusion_python::catalog::PyTable;
use datafusion_python::errors::DataFusionError;
use datafusion_python::utils::wait_for_future;

#[pyclass(name = "BigtableTable", module = "datafusion_bigtable", extends=PyTable, subclass)]
pub(crate) struct PyBigtableTable {}

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
        int_qualifiers: Vec<String>,
        str_qualifiers: Vec<String>,
        only_read_latest: bool,
        py: Python,
    ) -> PyResult<(Self, PyTable)> {
        let mut columns: Vec<Field> = vec![];
        for qualifier in &int_qualifiers {
            columns.push(Field::new(qualifier, DataType::Int64, false))
        }
        for qualifier in &str_qualifiers {
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

        Ok((Self {}, PyTable::new(Arc::new(source))))
    }
}
