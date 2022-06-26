use std::any::Any;
use std::fmt;
use std::sync::Arc;


use async_trait::async_trait;

use arrow::datatypes::{DataType, SchemaRef};
use std::collections::HashMap;
use arrow::array::ArrayRef;
use arrow::array::Int64Array;
use arrow::array::StringArray;
use arrow::array::TimestampMicrosecondArray;
use arrow::record_batch::RecordBatch;

use bigtable_rs::google::bigtable::v2::row_filter::Chain;
use bigtable_rs::google::bigtable::v2::row_filter::Filter;
use bigtable_rs::google::bigtable::v2::ReadRowsRequest;
use bigtable_rs::google::bigtable::v2::RowFilter;
use bigtable_rs::google::bigtable::v2::RowRange;
use bigtable_rs::google::bigtable::v2::RowSet;

use byteorder::{BigEndian, ByteOrder};

use datafusion::error::{DataFusionError, Result};
use datafusion::execution::runtime_env::RuntimeEnv;

use datafusion::physical_plan::expressions::PhysicalSortExpr;
use datafusion::physical_plan::memory::MemoryStream;
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::project_schema;
use datafusion::physical_plan::DisplayFormatType;
use datafusion::physical_plan::Distribution;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::Partitioning;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::physical_plan::Statistics;

use crate::datasource::BigtableDataSource;

#[derive(Debug, Clone)]
pub struct BigtableExec {
    datasource: BigtableDataSource,
    projected_schema: SchemaRef,
    row_ranges: Vec<RowRange>,
    row_filters: Vec<RowFilter>,
}

impl BigtableExec {
    pub fn new(
        datasource: BigtableDataSource,
        projection: &Option<Vec<usize>>,
        row_ranges: Vec<RowRange>,
        row_filters: Vec<RowFilter>,
    ) -> Self {
        let projected_schema = project_schema(&datasource.schema, projection.as_ref()).unwrap();
        Self {
            datasource,
            projected_schema,
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
        self.projected_schema.clone()
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
            table_name: bigtable.get_full_table_name(&self.datasource.table),
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
                    HashMap<i64, HashMap<String, Vec<u8>>>,
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
                        rowkey_timestamp_qualifier_value
                            .get_mut(&row_key)
                            .unwrap()
                            .get_mut(&timestamp)
                            .unwrap()
                            .entry(qualifier)
                            .or_insert(row_cell.value);
                    })
                });

                let mut table_partition_col_values: HashMap<String, Vec<String>> = HashMap::new();
                let mut timestamps = vec![];
                let mut qualifier_values: HashMap<String, Vec<Vec<u8>>> = HashMap::new();
                for field in self.projected_schema.fields() {
                    if self.datasource.is_qualifier(field.name()) {
                        qualifier_values.insert(field.name().clone(), vec![]);
                    }
                }

                for (row_key, timestamp_qualifier_value) in rowkey_timestamp_qualifier_value.iter()
                {
                    for (timestamp, qualifier_value) in timestamp_qualifier_value.iter() {
                        if self.datasource.table_partition_cols.len() == 1 {
                            // no need to split
                            let table_partition_col = &self.datasource.table_partition_cols[0];
                            table_partition_col_values
                                .entry(table_partition_col.to_owned())
                                .or_insert(vec![]);
                            table_partition_col_values
                                .get_mut(table_partition_col)
                                .unwrap()
                                .push(row_key.to_owned())
                        } else {
                            let parts: Vec<&str> = row_key
                                .split(&self.datasource.table_partition_separator)
                                .collect();
                            for (i, table_partition_col) in
                                self.datasource.table_partition_cols.iter().enumerate()
                            {
                                table_partition_col_values
                                    .entry(table_partition_col.to_owned())
                                    .or_insert(vec![]);
                                table_partition_col_values
                                    .get_mut(table_partition_col)
                                    .unwrap()
                                    .push(parts[i].to_owned())
                            }
                        }
                        timestamps.push(timestamp.to_owned());

                        for field in self.projected_schema.fields() {
                            if self.datasource.is_qualifier(field.name()) {
                                let qualifier = field.name();
                                match qualifier_value.get(qualifier) {
                                    Some(cell_value) => {
                                        qualifier_values
                                            .get_mut(qualifier)
                                            .unwrap()
                                            .push(cell_value.to_owned());
                                    }
                                    _ => {
                                        qualifier_values.get_mut(qualifier).unwrap().push(vec![]);
                                    }
                                }
                            }
                        }
                    }
                }

                let mut data: Vec<ArrayRef> = vec![];
                for table_partition_col in self.datasource.table_partition_cols.clone() {
                    let values: &Vec<String> = table_partition_col_values
                        .get(&table_partition_col)
                        .unwrap();
                    data.push(Arc::new(StringArray::from(values.clone())));
                }

                data.push(Arc::new(TimestampMicrosecondArray::from(timestamps)));

                for field in self.projected_schema.fields() {
                    if self.datasource.is_qualifier(field.name()) {
                        let qualifier = field.name();
                        match field.data_type() {
                            DataType::Int64 => {
                                let mut decoded_values: Vec<i64> = vec![];
                                for encoded_value in qualifier_values.get(qualifier).unwrap() {
                                    decoded_values.push(BigEndian::read_i64(encoded_value));
                                }
                                data.push(Arc::new(Int64Array::from(decoded_values)));
                            }
                            _ => {
                                let mut decoded_values: Vec<String> = vec![];
                                for encoded_value in qualifier_values.get(qualifier).unwrap() {
                                    decoded_values
                                        .push(String::from_utf8(encoded_value.to_vec()).unwrap());
                                }
                                data.push(Arc::new(StringArray::from(decoded_values)));
                            }
                        }
                    }
                }

                Ok(Box::pin(MemoryStream::try_new(
                    vec![RecordBatch::try_new(self.schema(), data)?],
                    self.schema(),
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

