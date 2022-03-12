use std::collections::HashMap;

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
    projection: &Option<Vec<usize>>,
    filters: &[Expr],
) -> Result<(Vec<RowRange>, Vec<RowFilter>)> {
    let mut row_filters = vec![RowFilter {
        filter: Some(Filter::FamilyNameRegexFilter(datasource.column_family)),
    }];
    if datasource.only_read_latest {
        row_filters.push(RowFilter {
            filter: Some(Filter::CellsPerColumnLimitFilter(1)),
        });
    }

    let mut qualifiers: Vec<String> = vec![];
    let fields = datasource.schema.fields();
    match projection {
        Some(positions) => {
            for &position in positions {
                let field = fields[position].clone();
                if !datasource.table_partition_cols.contains(&field.name()) {
                    qualifiers.push(field.name().clone())
                }
            }
            row_filters.push(RowFilter {
                filter: Some(Filter::ColumnQualifierRegexFilter(
                    qualifiers.join("|").into_bytes(),
                )),
            });
        }
        _ => (),
    }

    let mut row_ranges = vec![];
    let mut table_partition_col_mapping: HashMap<String, Vec<String>> = HashMap::new();

    for filter in filters {
        match filter {
            Expr::BinaryExpr { left, op, right } => match left.as_ref() {
                Expr::Column(col) => {
                    if datasource.table_partition_cols.contains(&col.name) {
                        match op {
                            Operator::Eq => match right.as_ref() {
                                Expr::Literal(ScalarValue::Utf8(Some(key))) => {
                                    table_partition_col_mapping
                                        .entry(col.name.to_owned())
                                        .or_insert(vec![]);
                                    table_partition_col_mapping
                                        .get_mut(&col.name)
                                        .unwrap()
                                        .push(key.clone())
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
                                "_row_key: filter NOT IN is not supported".to_owned(),
                            ));
                        }
                        for right in list {
                            match right {
                                Expr::Literal(ScalarValue::Utf8(Some(key))) => {
                                    table_partition_col_mapping
                                        .entry(col.name.to_owned())
                                        .or_insert(vec![]);
                                    table_partition_col_mapping
                                        .get_mut(&col.name)
                                        .unwrap()
                                        .push(key.clone())
                                }
                                _ => (),
                            }
                        }
                    }
                }
                _ => (),
            },
            Expr::Between {
                expr,
                negated,
                low,
                high,
            } => match expr.as_ref() {
                Expr::Column(col) => {
                    if datasource.table_partition_cols.contains(&col.name) {
                        if negated.to_owned() {
                            return Err(DataFusionError::Execution(
                                "_row_key: filter NOT IN is not supported".to_owned(),
                            ));
                        }
                        match low.as_ref() {
                            Expr::Literal(ScalarValue::Utf8(Some(low_key))) => {
                                match high.as_ref() {
                                    Expr::Literal(ScalarValue::Utf8(Some(high_key))) => row_ranges
                                        .push(RowRange {
                                            start_key: Some(StartKey::StartKeyClosed(
                                                low_key.clone().into_bytes(),
                                            )),
                                            end_key: Some(EndKey::EndKeyClosed(
                                                high_key.clone().into_bytes(),
                                            )),
                                        }),
                                    _ => (),
                                }
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

    if !table_partition_col_mapping.is_empty() {
        let mut batch_parts: Vec<Vec<String>> = vec![];
        for table_partition_col in datasource.table_partition_cols {
            match table_partition_col_mapping.get(&table_partition_col) {
                Some(list) => {
                    if batch_parts.is_empty() {
                        // initialize
                        // batch_parts = [], list = ["us-east1", "us-west2"]
                        // => batch_parts = [ ["us-east1"], ["us-west2"] ]
                        for value in list {
                            batch_parts.push(vec![value.to_owned()]);
                        }
                    } else {
                        // cross product
                        // batch_parts = [ ["us-east1"], ["us-west2"] ], list = ["3698", "3700"]
                        // => batch_parts = [ ["us-east1", "3698"], ["us-west2", "3698"], ["us-east1", "3700"], ["us-west2", "3700"] ]
                        batch_parts = partial_cartesian(batch_parts, list);
                    }
                }
                _ => {
                    return Err(DataFusionError::Execution(format!(
                        "{}: filter is required",
                        table_partition_col
                    )));
                }
            }
        }

        for parts in batch_parts {
            let key = parts.join(&datasource.table_partition_separator);
            row_ranges.push(RowRange {
                start_key: Some(StartKey::StartKeyClosed(key.clone().into_bytes())),
                end_key: Some(EndKey::EndKeyClosed(key.clone().into_bytes())),
            });
        }
    }

    if row_ranges.is_empty() {
        return Err(DataFusionError::Execution(
            "_row_key: filter is not provided or not supported".to_owned(),
        ));
    }

    Ok((row_ranges, row_filters))
}

pub fn partial_cartesian<T: Clone>(a: Vec<Vec<T>>, b: &[T]) -> Vec<Vec<T>> {
    a.into_iter()
        .flat_map(|xs| {
            b.iter()
                .cloned()
                .map(|y| {
                    let mut vec = xs.clone();
                    vec.push(y);
                    vec
                })
                .collect::<Vec<_>>()
        })
        .collect()
}
