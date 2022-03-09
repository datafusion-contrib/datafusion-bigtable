use crate::datasource::BigtableDataSource;
use bigtable_rs::google::bigtable::v2::row_filter::Filter;
use bigtable_rs::google::bigtable::v2::row_range::EndKey;
use bigtable_rs::google::bigtable::v2::row_range::StartKey;
use bigtable_rs::google::bigtable::v2::{RowFilter, RowRange};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_plan::Expr;
use datafusion::logical_plan::Operator;
use datafusion::scalar::ScalarValue;

pub fn compose(
    datasource: BigtableDataSource,
    _projection: &Option<Vec<usize>>,
    filters: &[Expr],
) -> Result<(Vec<RowRange>, Vec<RowFilter>)> {
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
            Expr::BinaryExpr { left, op, right } => match left.as_ref() {
                Expr::Column(col) => {
                    if datasource.table_partition_cols.contains(&col.name) {
                        match op {
                            Operator::Eq => match right.as_ref() {
                                Expr::Literal(ScalarValue::Utf8(Some(key))) => {
                                    row_ranges.push(RowRange {
                                        start_key: Some(StartKey::StartKeyClosed(
                                            key.clone().into_bytes(),
                                        )),
                                        end_key: Some(EndKey::EndKeyClosed(
                                            key.clone().into_bytes(),
                                        )),
                                    })
                                }
                                _ => (),
                            },
                            _ => (),
                        }
                    }
                }
                _ => (),
            },
            Expr::InList {
                expr,
                list,
                negated,
            } => match expr.as_ref() {
                Expr::Column(col) => {
                    if datasource.table_partition_cols.contains(&col.name) {
                        if negated.to_owned() {
                            return Err(DataFusionError::Execution(
                                "_row_key filter: NOT IN is not supported".to_owned(),
                            ));
                        }
                        for right in list {
                            match right {
                                Expr::Literal(ScalarValue::Utf8(Some(key))) => {
                                    row_ranges.push(RowRange {
                                        start_key: Some(StartKey::StartKeyClosed(
                                            key.clone().into_bytes(),
                                        )),
                                        end_key: Some(EndKey::EndKeyClosed(
                                            key.clone().into_bytes(),
                                        )),
                                    })
                                }
                                _ => (),
                            }
                        }
                    }
                }
                _ => (),
            },
            _ => (),
        }
    }

    if row_ranges.is_empty() {
        return Err(DataFusionError::Execution(
            "_row_key filter is not provided or not supported".to_owned(),
        ));
    }

    Ok((row_ranges, row_filters))
}
