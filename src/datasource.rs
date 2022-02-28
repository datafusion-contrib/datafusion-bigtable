use std::any::Any;
use std::sync::Arc;
use std::collections::HashMap;
use std::time::Duration;

use async_trait::async_trait;

use arrow::datatypes::{SchemaRef, Schema, Field};
use datafusion::datasource::TableProvider;
use datafusion::datasource::datasource::TableProviderFilterPushDown;
use datafusion::error::Result;
use datafusion::logical_plan::Expr;
use datafusion::physical_plan::ExecutionPlan;

use datafusion::physical_plan::project_schema;
use datafusion::physical_plan::empty::EmptyExec;

use bigtable_rs::bigtable::{BigTableConnection, BigTable};

const RESERVED_ROWKEY: &str = "_row_key";
const RESERVED_TIMESTAMP: &str = "_timestamp";
const READ_ONLY:bool = true;
const CHANNEL_SIZE:usize = 1;
const TIMEOUT_IN_SECONDS:u64 = 600;

pub struct BigtableTable {
    schema: SchemaRef,
    client: Option<BigTable>,
}

impl BigtableTable {
    pub fn new(project: &str, instance: &str, table: &str, column_family: &str, columns: Vec<Field>, only_read_latest: bool) -> Self {
        let metadata = HashMap::from([
            ("project".to_string(), project.to_string()),
            ("instance".to_string(), instance.to_string()),
            ("table".to_string(), table.to_string()),
            ("column_family".to_string(), column_family.to_string()),
            ("only_read_latest".to_string(), only_read_latest.to_string()),
        ]);
        let schema = Arc::new(Schema::new_with_metadata(columns, metadata));
        let client = None;
        Self { schema, client }
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
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        match self.client {
            Some(x) => x,
            None => {
                let connection = BigTableConnection::new(
                    self.schema.metadata().get(&"project".to_string()).unwrap(),
                    self.schema.metadata().get(&"instance".to_string()).unwrap(),
                    READ_ONLY,
                    CHANNEL_SIZE,
                    Some(Duration::from_secs(TIMEOUT_IN_SECONDS)),
                ).await?;
                self.client = Some(connection.client())
            }
        }

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
