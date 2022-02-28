use std::any::Any;
use std::sync::Arc;
use std::collections::HashMap;

use async_trait::async_trait;

use arrow::datatypes::{SchemaRef, Schema, Field};
use datafusion::datasource::TableProvider;
use datafusion::datasource::datasource::TableProviderFilterPushDown;
use datafusion::error::Result;
use datafusion::logical_plan::Expr;
use datafusion::physical_plan::ExecutionPlan;

use datafusion::physical_plan::project_schema;
use datafusion::physical_plan::empty::EmptyExec;

pub struct BigtableTable {
    schema: SchemaRef,
}

impl BigtableTable {
    pub fn new(instance: &str, table: &str, column_family: &str, columns: Vec<Field>, only_read_latest: bool) -> Self {
        let metadata = HashMap::from([
            ("instance".to_string(), instance.to_string()),
            ("table".to_string(), table.to_string()),
            ("column_family".to_string(), column_family.to_string()),
            ("only_read_latest".to_string(), only_read_latest.to_string()),
        ]);
        let schema = Arc::new(Schema::new_with_metadata(columns, metadata));
        Self { schema }
    }
}

#[async_trait]
impl TableProvider for BigtableTable {
    /// Returns the table provider as [`Any`](std::any::Any) so that it can be
    /// downcast to a specific implementation.
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Get a reference to the schema for this table
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// Create an ExecutionPlan that will scan the table.
    /// The table provider will be usually responsible of grouping
    /// the source data into partitions that can be efficiently
    /// parallelized or distributed.
    async fn scan(
        &self,
        projection: &Option<Vec<usize>>,
        filters: &[Expr],
        // limit can be used to reduce the amount scanned
        // from the datasource as a performance optimization.
        // If set, it contains the amount of rows needed by the `LogicalPlan`,
        // The datasource should return *at least* this number of rows if available.
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let projected_schema = project_schema(&self.schema, projection.as_ref())?;
        Ok(Arc::new(EmptyExec::new(false, projected_schema)))
    }

    /// Tests whether the table provider can make use of a filter expression
    /// to optimise data retrieval.
    fn supports_filter_pushdown(
        &self,
        _filter: &Expr,
    ) -> Result<TableProviderFilterPushDown> {
        Ok(TableProviderFilterPushDown::Inexact)
    }
}
