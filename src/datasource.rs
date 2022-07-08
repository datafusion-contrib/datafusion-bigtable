use std::any::Any;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::datasource::datasource::TableProviderFilterPushDown;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_plan::Expr;
use datafusion::physical_plan::ExecutionPlan;

use bigtable_rs::bigtable::BigTableConnection;

mod composer;
use crate::execute_plan::BigtableExec;

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
    pub table: String,
    column_family: String,
    pub table_partition_cols: Vec<String>,
    pub table_partition_separator: String,
    only_read_latest: bool,
    pub schema: SchemaRef,
    pub connection: BigTableConnection,
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
        table_partition_separator: String,
        columns: Vec<Field>,
        only_read_latest: bool,
    ) -> Result<Self> {
        let mut table_fields: Vec<Field> = vec![];
        for part in &table_partition_cols {
            table_fields.push(Field::new(part, DataType::Utf8, false));
        }
        table_fields.push(Field::new(
            RESERVED_TIMESTAMP,
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ));
        for column in &columns {
            table_fields.push(column.clone());
        }

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
                table_partition_separator: table_partition_separator,
                only_read_latest: only_read_latest,
                schema: Arc::new(Schema::new(table_fields)),
                connection: connection,
            }),
            Err(err) => Err(DataFusionError::External(Box::new(err))),
        }
    }

    pub fn is_qualifier(&self, field_name: &String) -> bool {
        if field_name == RESERVED_ROWKEY {
            return false;
        }
        if field_name == RESERVED_TIMESTAMP {
            return false;
        }
        if self.table_partition_cols.contains(field_name) {
            return false;
        }
        return true;
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

    fn table_type(&self) -> TableType {
        TableType::Base
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
        match composer::compose(self.clone(), projection, filters) {
            Ok((row_ranges, row_filters)) => Ok(Arc::new(BigtableExec::new(
                self.clone(),
                projection,
                row_ranges,
                row_filters,
            ))),
            Err(err) => Err(err),
        }
    }

    /// Tests whether the table provider can make use of a filter expression
    /// to optimise data retrieval.
    fn supports_filter_pushdown(&self, _filter: &Expr) -> Result<TableProviderFilterPushDown> {
        Ok(TableProviderFilterPushDown::Inexact)
    }
}

#[cfg(test)]
mod tests {
    use crate::datasource::{BigtableDataSource, DEFAULT_SEPARATOR, RESERVED_ROWKEY};
    use arrow::datatypes::{DataType, Field};
    use datafusion::assert_batches_eq;
    use datafusion::error::Result;
    use datafusion::prelude::SessionContext;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_simple_row_key() -> Result<()> {
        let bigtable_datasource = BigtableDataSource::new(
            "emulator".to_owned(),
            "dev".to_owned(),
            "weather_balloons".to_owned(),
            "measurements".to_owned(),
            vec![RESERVED_ROWKEY.to_owned()],
            DEFAULT_SEPARATOR.to_owned(),
            vec![
                Field::new("pressure", DataType::Int64, false),
                // Bigtable does not support float number, so store as string
                Field::new("temperature", DataType::Utf8, false),
            ],
            true,
        )
        .await
        .unwrap();
        let ctx = SessionContext::new();
        ctx.register_table("weather_balloons", Arc::new(bigtable_datasource))
            .unwrap();
        let mut batches = ctx.sql("SELECT \"_row_key\", pressure, \"_timestamp\" FROM weather_balloons where \"_row_key\" = 'us-west2#3698#2021-03-05-1200'").await?.collect().await?;
        let mut expected = vec![
            "+-------------------------------+----------+-------------------------+",
            "| _row_key                      | pressure | _timestamp              |",
            "+-------------------------------+----------+-------------------------+",
            "| us-west2#3698#2021-03-05-1200 | 94558    | 2021-03-05 12:00:05.100 |",
            "+-------------------------------+----------+-------------------------+",
        ];
        assert_batches_eq!(expected, &batches);

        batches = ctx.sql("SELECT * FROM weather_balloons where \"_row_key\" = 'us-west2#3698#2021-03-05-1200'").await?.collect().await?;
        expected = vec![
            "+-------------------------------+-------------------------+----------+-------------+",
            "| _row_key                      | _timestamp              | pressure | temperature |",
            "+-------------------------------+-------------------------+----------+-------------+",
            "| us-west2#3698#2021-03-05-1200 | 2021-03-05 12:00:05.100 | 94558    | 9.6         |",
            "+-------------------------------+-------------------------+----------+-------------+",
        ];
        assert_batches_eq!(expected, &batches);

        batches = ctx.sql("SELECT \"_row_key\", pressure, \"_timestamp\" FROM weather_balloons where \"_row_key\" IN ('us-west2#3698#2021-03-05-1200', 'us-west2#3698#2021-03-05-1201') ORDER BY \"_timestamp\"").await?.collect().await?;
        expected = vec![
            "+-------------------------------+----------+-------------------------+",
            "| _row_key                      | pressure | _timestamp              |",
            "+-------------------------------+----------+-------------------------+",
            "| us-west2#3698#2021-03-05-1200 | 94558    | 2021-03-05 12:00:05.100 |",
            "| us-west2#3698#2021-03-05-1201 | 94122    | 2021-03-05 12:01:05.200 |",
            "+-------------------------------+----------+-------------------------+",
        ];
        assert_batches_eq!(expected, &batches);

        batches = ctx.sql("SELECT \"_row_key\", pressure, \"_timestamp\" FROM weather_balloons where \"_row_key\" BETWEEN 'us-west2#3698#2021-03-05-1200' AND 'us-west2#3698#2021-03-05-1202' ORDER BY \"_timestamp\"").await?.collect().await?;
        expected = vec![
            "+-------------------------------+----------+-------------------------+",
            "| _row_key                      | pressure | _timestamp              |",
            "+-------------------------------+----------+-------------------------+",
            "| us-west2#3698#2021-03-05-1200 | 94558    | 2021-03-05 12:00:05.100 |",
            "| us-west2#3698#2021-03-05-1201 | 94122    | 2021-03-05 12:01:05.200 |",
            "| us-west2#3698#2021-03-05-1202 | 95992    | 2021-03-05 12:02:05.300 |",
            "+-------------------------------+----------+-------------------------+",
        ];
        assert_batches_eq!(expected, &batches);
        Ok(())
    }

    #[tokio::test]
    async fn test_composite_row_key() -> Result<()> {
        let bigtable_datasource = BigtableDataSource::new(
            "emulator".to_owned(),
            "dev".to_owned(),
            "weather_balloons".to_owned(),
            "measurements".to_owned(),
            vec![
                "region".to_owned(),
                "balloon_id".to_owned(),
                "event_minute".to_owned(),
            ],
            DEFAULT_SEPARATOR.to_owned(),
            vec![
                Field::new("pressure", DataType::Int64, false),
                // Bigtable does not support float number, so store as string
                Field::new("temperature", DataType::Utf8, false),
            ],
            true,
        )
        .await
        .unwrap();
        let ctx = SessionContext::new();
        ctx.register_table("weather_balloons", Arc::new(bigtable_datasource))
            .unwrap();
        let mut batches = ctx.sql("SELECT region, balloon_id, event_minute, pressure, \"_timestamp\" FROM weather_balloons where region = 'us-west2' and balloon_id='3698' and event_minute = '2021-03-05-1200'").await?.collect().await?;
        let mut expected = vec![
            "+----------+------------+-----------------+----------+-------------------------+",
            "| region   | balloon_id | event_minute    | pressure | _timestamp              |",
            "+----------+------------+-----------------+----------+-------------------------+",
            "| us-west2 | 3698       | 2021-03-05-1200 | 94558    | 2021-03-05 12:00:05.100 |",
            "+----------+------------+-----------------+----------+-------------------------+",
        ];
        assert_batches_eq!(expected, &batches);

        batches = ctx.sql("SELECT region, balloon_id, event_minute, pressure, \"_timestamp\" FROM weather_balloons where region = 'us-west2' and balloon_id IN ('3698') and event_minute IN ('2021-03-05-1200', '2021-03-05-1201') ORDER BY \"_timestamp\"").await?.collect().await?;
        expected = vec![
            "+----------+------------+-----------------+----------+-------------------------+",
            "| region   | balloon_id | event_minute    | pressure | _timestamp              |",
            "+----------+------------+-----------------+----------+-------------------------+",
            "| us-west2 | 3698       | 2021-03-05-1200 | 94558    | 2021-03-05 12:00:05.100 |",
            "| us-west2 | 3698       | 2021-03-05-1201 | 94122    | 2021-03-05 12:01:05.200 |",
            "+----------+------------+-----------------+----------+-------------------------+",
        ];
        assert_batches_eq!(expected, &batches);

        batches = ctx.sql("SELECT region, balloon_id, event_minute, pressure, \"_timestamp\" FROM weather_balloons where region = 'us-west2' and balloon_id IN ('3698') and event_minute BETWEEN '2021-03-05-1200' AND '2021-03-05-1201' ORDER BY \"_timestamp\"").await?.collect().await?;
        expected = vec![
            "+----------+------------+-----------------+----------+-------------------------+",
            "| region   | balloon_id | event_minute    | pressure | _timestamp              |",
            "+----------+------------+-----------------+----------+-------------------------+",
            "| us-west2 | 3698       | 2021-03-05-1200 | 94558    | 2021-03-05 12:00:05.100 |",
            "| us-west2 | 3698       | 2021-03-05-1201 | 94122    | 2021-03-05 12:01:05.200 |",
            "+----------+------------+-----------------+----------+-------------------------+",
        ];
        assert_batches_eq!(expected, &batches);
        Ok(())
    }
}
