# Datafusion-Bigtable
Bigtable data source for [Apache Arrow Datafusion](https://github.com/apache/arrow-datafusion)

## Run SQL on Bigtable

This crate implements Bigtable data source and Executor for Datafusion. It is built on top of gRPC client [tonic](https://github.com/hyperium/tonic).

## Quick Start

```
let bigtable_datasource = BigtableDataSource::new(
    "emulator".to_owned(),                               // project
    "dev".to_owned(),                                    // instance
    "weather_balloons".to_owned(),                       // table
    "measurements".to_owned(),                           // column family
    vec!["_row_key".to_owned()],                         // table_partition_cols
    "#".to_owned(),                                      // table_partition_separator
    vec![Field::new("pressure", DataType::Utf8, false)], // qualifiers
    true,                                                // only_read_latest
).await.unwrap();

let mut ctx = ExecutionContext::new();
ctx.register_table("weather_balloons", Arc::new(bigtable_datasource)).unwrap();

ctx.sql("SELECT \"_row_key\", pressure, \"_timestamp\" FROM weather_balloons where \"_row_key\" = 'us-west2#3698#2021-03-05-1200'").await?.collect().await?;
```

## Roadmap

### Bigtable

- ✅ UTF8 string
- ✅ 64-bit big-endian signed integer

### SQL
- ✅ select by `"_row_key" =`
- ✅ select by `"_row_key" IN`
- ✅ select by `"_row_key" BETWEEN`
- ✅ select by composite row keys `=`
- ✅ select by composite row keys `IN`
- [ ] select by composite row keys `BETWEEN`

### General

- ✅ Projection pushdown
- [ ] Predicate push down
    + [Value range](https://cloud.google.com/bigtable/docs/using-filters#value-range)
    + [Value Regex](https://cloud.google.com/bigtable/docs/using-filters#value-regex)
    + [Timestamp range](https://cloud.google.com/bigtable/docs/using-filters#timestamp-range))
- [ ] Multi Thread or Partition aware execution
- [ ] Production ready Bigtable SDK in Rust

Note: datafusion-bigtable provides the physical Executor for Datafusion. Any aggregation, group by, join are implemented and handled by Datafusion.
