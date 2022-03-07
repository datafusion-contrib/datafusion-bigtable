use crate::datasource::BigtableDataSource;
use bigtable_rs::google::bigtable::v2::row_filter::Filter;
use bigtable_rs::google::bigtable::v2::row_range::EndKey;
use bigtable_rs::google::bigtable::v2::row_range::StartKey;
use bigtable_rs::google::bigtable::v2::{RowFilter, RowRange};
use datafusion::logical_plan::Expr;
use datafusion::scalar::ScalarValue;

pub fn compose(
    datasource: BigtableDataSource,
    _projection: &Option<Vec<usize>>,
    filters: &[Expr],
) -> (Vec<RowRange>, Vec<RowFilter>) {
    let mut row_ranges = vec![];
    let mut row_filters = if datasource.only_read_latest {
        vec![RowFilter {
            filter: Some(Filter::CellsPerColumnLimitFilter(1)),
        }]
    } else {
        vec![]
    };
    row_filters.push(RowFilter {
        filter: Some(Filter::FamilyNameRegexFilter(datasource.column_family)),
    });

    for filter in filters {
        match filter {
            Expr::BinaryExpr { left, op: _, right } => match left.as_ref() {
                Expr::Column(col) => {
                    if datasource.table_partition_cols.contains(&col.name) {
                        match right.as_ref() {
                            Expr::Literal(ScalarValue::Utf8(Some(key))) => {
                                row_ranges.push(RowRange {
                                    start_key: Some(StartKey::StartKeyClosed(
                                        key.clone().into_bytes(),
                                    )),
                                    end_key: Some(EndKey::EndKeyClosed(key.clone().into_bytes())),
                                })
                            }
                            _ => (),
                        }
                    }
                }
                _ => (),
            },
            _ => (),
        }
    }

    (row_ranges, row_filters)
}
