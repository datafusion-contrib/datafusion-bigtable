use std::any::Any;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::datasource::datasource::TableProviderFilterPushDown;
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::logical_plan::Expr;
use datafusion::physical_plan::expressions::PhysicalSortExpr;
use datafusion::physical_plan::file_format::DEFAULT_PARTITION_COLUMN_DATATYPE;
use datafusion::physical_plan::memory::MemoryStream;
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::DisplayFormatType;
use datafusion::physical_plan::Distribution;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::Partitioning;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::physical_plan::Statistics;

use arrow::array::Int64Array;
use arrow::array::StringArray;
use arrow::array::TimestampMicrosecondArray;
use arrow::record_batch::RecordBatch;

use bigtable_rs::bigtable::BigTableConnection;
use bigtable_rs::google::bigtable::v2::row_filter::Chain;
use bigtable_rs::google::bigtable::v2::row_filter::Filter;
use bigtable_rs::google::bigtable::v2::ReadRowsRequest;
use bigtable_rs::google::bigtable::v2::RowFilter;
use bigtable_rs::google::bigtable::v2::RowRange;
use bigtable_rs::google::bigtable::v2::RowSet;

use byteorder::{BigEndian, ByteOrder};

mod composer;

const DEFAULT_SEPARATOR: &str = "#";
const RESERVED_ROWKEY: &str = "_row_key";
const RESERVED_TIMESTAMP: &str = "_timestamp";
const READ_ONLY: bool = true;
const CHANNEL_SIZE: usize = 1;
const TIMEOUT_IN_SECONDS: u64 = 600;

#[derive(Clone)]
pub struct BigtableDataSource {
    project: String,
    instance: String,
    table: String,
    column_family: String,
    table_partition_cols: Vec<String>,
    table_partition_separator: String,
    only_read_latest: bool,
    schema: SchemaRef,
    connection: BigTableConnection,
}

impl fmt::Debug for BigtableDataSource {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "BigtableDataSource {}, {}, {}, {}, {:?}, {}, {}, {}",
            self.project,
            self.instance,
            self.table,
            self.column_family,
            self.table_partition_cols,
            self.table_partition_separator,
            self.only_read_latest,
            self.schema
        )
    }
}

impl BigtableDataSource {
    pub async fn new(
        project: String,
        instance: String,
        table: String,
        column_family: String,
        table_partition_cols: Vec<String>,
        columns: Vec<Field>,
        only_read_latest: bool,
    ) -> Result<Self> {
        let mut table_fields = columns.clone();
        for part in &table_partition_cols {
            table_fields.push(Field::new(
                part,
                DEFAULT_PARTITION_COLUMN_DATATYPE.clone(),
                false,
            ));
        }
        table_fields.push(Field::new(
            RESERVED_TIMESTAMP,
            DataType::Timestamp(TimeUnit::Second, None),
            false,
        ));

        match BigTableConnection::new(
            &project,
            &instance,
            READ_ONLY,
            CHANNEL_SIZE,
            Some(Duration::from_secs(TIMEOUT_IN_SECONDS)),
        )
        .await
        {
            Ok(connection) => Ok(Self {
                project: project,
                instance: instance,
                table: table,
                column_family: column_family,
                table_partition_cols: table_partition_cols,
                table_partition_separator: DEFAULT_SEPARATOR.to_string(),
                only_read_latest: only_read_latest,
                schema: Arc::new(Schema::new(table_fields)),
                connection: connection,
            }),
            Err(err) => Err(DataFusionError::External(Box::new(err))),
        }
    }
}

#[async_trait]
impl TableProvider for BigtableDataSource {
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
        let (row_ranges, row_filters) = composer::compose(self.clone(), projection, filters);
        Ok(Arc::new(BigtableExec::new(
            self.clone(),
            row_ranges,
            row_filters,
        )))
    }

    /// Tests whether the table provider can make use of a filter expression
    /// to optimise data retrieval.
    fn supports_filter_pushdown(&self, _filter: &Expr) -> Result<TableProviderFilterPushDown> {
        Ok(TableProviderFilterPushDown::Inexact)
    }
}

#[derive(Debug, Clone)]
struct BigtableExec {
    datasource: BigtableDataSource,
    row_ranges: Vec<RowRange>,
    row_filters: Vec<RowFilter>,
}

impl BigtableExec {
    fn new(
        datasource: BigtableDataSource,
        row_ranges: Vec<RowRange>,
        row_filters: Vec<RowFilter>,
    ) -> Self {
        Self {
            datasource,
            row_ranges,
            row_filters,
        }
    }
}

#[async_trait]
impl ExecutionPlan for BigtableExec {
    /// Returns the execution plan as [`Any`](std::any::Any) so that it can be
    /// downcast to a specific implementation.
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Get the schema for this execution plan
    fn schema(&self) -> SchemaRef {
        self.datasource.schema.clone()
    }

    /// Specifies the output partitioning scheme of this plan
    fn output_partitioning(&self) -> Partitioning {
        datafusion::physical_plan::Partitioning::UnknownPartitioning(1)
    }
    /// Specifies the data distribution requirements of all the children for this operator
    fn required_child_distribution(&self) -> Distribution {
        Distribution::UnspecifiedDistribution
    }
    /// Get a list of child execution plans that provide the input for this plan. The returned list
    /// will be empty for leaf nodes, will contain a single value for unary nodes, or two
    /// values for binary nodes (such as joins).
    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![]
    }
    /// Returns a new plan where all children were replaced by new plans.
    /// The size of `children` must be equal to the size of `ExecutionPlan::children()`.
    fn with_new_children(
        &self,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.is_empty() {
            Ok(Arc::new(self.clone()))
        } else {
            Err(DataFusionError::Internal(format!(
                "Children cannot be replaced in {:?}",
                self
            )))
        }
    }

    /// creates an iterator
    async fn execute(
        &self,
        _partition: usize,
        _runtime: Arc<RuntimeEnv>,
    ) -> Result<SendableRecordBatchStream> {
        let mut bigtable = self.datasource.connection.client();
        let request = ReadRowsRequest {
            app_profile_id: "default".to_owned(),
            table_name: self.datasource.table.to_string(),
            rows: Some(RowSet {
                row_keys: vec![],
                row_ranges: self.row_ranges.clone(),
            }),
            filter: Some(RowFilter {
                filter: Some(Filter::Chain(Chain {
                    filters: self.row_filters.clone(),
                })),
            }),
            ..ReadRowsRequest::default()
        };

        match bigtable.read_rows(request).await {
            Err(err) => Err(DataFusionError::External(Box::new(err))),
            Ok(resp) => {
                let mut rowkey_timestamp_qualifier_value: HashMap<
                    String,
                    HashMap<i64, HashMap<String, i64>>,
                > = HashMap::new();
                resp.into_iter().for_each(|(key, data)| {
                    let row_key = String::from_utf8(key.clone()).unwrap();
                    rowkey_timestamp_qualifier_value
                        .entry(row_key.to_owned())
                        .or_insert(HashMap::new());
                    data.into_iter().for_each(|row_cell| {
                        let timestamp = row_cell.timestamp_micros;
                        rowkey_timestamp_qualifier_value
                            .get_mut(&row_key)
                            .unwrap()
                            .entry(timestamp)
                            .or_insert(HashMap::new());

                        let qualifier = String::from_utf8(row_cell.qualifier).unwrap();
                        let cell_value = BigEndian::read_i64(&row_cell.value);
                        rowkey_timestamp_qualifier_value
                            .get_mut(&row_key)
                            .unwrap()
                            .get_mut(&timestamp)
                            .unwrap()
                            .entry(qualifier)
                            .or_insert(cell_value);
                    })
                });

                let mut field_array = vec![
                    Field::new(RESERVED_ROWKEY, DataType::Utf8, false),
                    Field::new(RESERVED_TIMESTAMP, DataType::Utf8, false),
                ];
                field_array.push(Field::new("pressure", DataType::Int64, false));

                let mut row_key_array = vec![];
                let mut timestamp_array = vec![];
                let mut pressure_array = vec![];

                for (row_key, timestamp_qualifier_value) in rowkey_timestamp_qualifier_value.iter()
                {
                    for (timestamp, qualifier_value) in timestamp_qualifier_value.iter() {
                        row_key_array.push(row_key.to_owned());
                        timestamp_array.push(timestamp.to_owned());
                        pressure_array.push(
                            qualifier_value
                                .get(&"pressure".to_owned())
                                .unwrap()
                                .to_owned(),
                        );
                    }
                }

                Ok(Box::pin(MemoryStream::try_new(
                    vec![RecordBatch::try_new(
                        Arc::new(Schema::new(field_array)),
                        vec![
                            Arc::new(StringArray::from(row_key_array)),
                            Arc::new(TimestampMicrosecondArray::from(timestamp_array)),
                            Arc::new(Int64Array::from(pressure_array)),
                        ],
                    )?],
                    self.datasource.schema(),
                    None,
                )?))
            }
        }
    }

    /// Return a snapshot of the set of [`Metric`]s for this
    /// [`ExecutionPlan`].
    ///
    /// While the values of the metrics in the returned
    /// [`MetricsSet`]s may change as execution progresses, the
    /// specific metrics will not.
    ///
    /// Once `self.execute()` has returned (technically the future is
    /// resolved) for all available partitions, the set of metrics
    /// should be complete. If this function is called prior to
    /// `execute()` new metrics may appear in subsequent calls.
    fn metrics(&self) -> Option<MetricsSet> {
        None
    }

    /// Format this `ExecutionPlan` to `f` in the specified type.
    ///
    /// Should not include a newline
    ///
    /// Note this function prints a placeholder by default to preserve
    /// backwards compatibility.
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ExecutionPlan(BigtableExec)")
    }

    /// Returns the global output statistics for this `ExecutionPlan` node.
    fn statistics(&self) -> Statistics {
        todo!()
    }
    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use crate::datasource::{BigtableDataSource, RESERVED_ROWKEY};
    use arrow::datatypes::{DataType, Field};
    use datafusion::assert_batches_eq;
    use datafusion::error::Result;
    use datafusion::prelude::ExecutionContext;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_sql_query() -> Result<()> {
        let bigtable_datasource = BigtableDataSource::new(
            "emulator".to_string(),
            "dev".to_string(),
            "weather_balloons".to_string(),
            "measurements".to_string(),
            vec![RESERVED_ROWKEY.to_string()],
            vec![Field::new("pressure", DataType::UInt64, false)],
            true,
        )
        .await
        .unwrap();
        let mut ctx = ExecutionContext::new();
        ctx.register_table("weather_balloons", Arc::new(bigtable_datasource))
            .unwrap();
        let batches = ctx.sql("SELECT \"_row_key\", pressure, \"_timestamp\" FROM weather_balloons where \"_row_key\" = 'us-west2#3698#2021-03-05-1200'").await?.collect().await?;
        let expected = vec![
            "+----------+----------+------------+",
            "| _row_key | pressure | _timestamp |",
            "+----------+----------+------------+",
        ];
        assert_batches_eq!(expected, &batches);
        Ok(())
    }
}
