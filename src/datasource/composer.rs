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
    let row_ranges = prepare_partition_pruning(datasource.clone(), filters)?;
    let row_filters = prepare_pushdown(datasource.clone(), projection)?;
    Ok((row_ranges, row_filters))
}

fn prepare_partition_pruning(
    datasource: BigtableDataSource,
    filters: &[Expr],
) -> Result<Vec<RowRange>> {
    let mut partition_col_values: HashMap<String, Vec<String>> = HashMap::new();
    let mut tail_partition_col_range: Option<(String, String)> = None;

    for filter in filters {
        match filter {
            Expr::BinaryExpr { left, op, right } => process_binary_expr(
                datasource.clone(),
                &mut partition_col_values,
                left,
                op,
                right,
            ),
            Expr::InList {
                expr,
                list,
                negated,
            } => process_in_list(
                datasource.clone(),
                &mut partition_col_values,
                expr,
                list,
                negated,
            ),
            Expr::Between {
                expr,
                negated,
                low,
                high,
            } => process_between(
                datasource.clone(),
                &mut tail_partition_col_range,
                expr,
                negated,
                low,
                high,
            ),
            _ => (),
        }
    }

    let row_ranges = try_merge(
        datasource.clone(),
        partition_col_values,
        tail_partition_col_range,
    )?;
    if row_ranges.is_empty() {
        return Err(DataFusionError::Execution(
            "table_partition_cols: filter is not provided or not supported".to_owned(),
        ));
    }
    Ok(row_ranges)
}

fn try_merge(
    datasource: BigtableDataSource,
    partition_col_values: HashMap<String, Vec<String>>,
    tail_partition_col_range: Option<(String, String)>,
) -> Result<Vec<RowRange>> {
    if partition_col_values.is_empty() {
        match tail_partition_col_range {
            Some((low, high)) => {
                return Ok(vec![RowRange {
                    start_key: Some(StartKey::StartKeyClosed(low.into_bytes())),
                    end_key: Some(EndKey::EndKeyClosed(high.into_bytes())),
                }]);
            }
            _ => (),
        }
        return Ok(vec![]);
    }

    merge(datasource, partition_col_values, tail_partition_col_range)
}

fn merge(
    datasource: BigtableDataSource,
    partition_col_values: HashMap<String, Vec<String>>,
    tail_partition_col_range: Option<(String, String)>,
) -> Result<Vec<RowRange>> {
    let mut batch_parts: Vec<Vec<String>> = vec![];
    let tail_partition_col = datasource.table_partition_cols.last().unwrap();

    for table_partition_col in datasource.table_partition_cols.clone() {
        match partition_col_values.get(&table_partition_col) {
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
                if &table_partition_col == tail_partition_col && tail_partition_col_range.is_none()
                {
                    return Err(DataFusionError::Execution(format!(
                        "{}: filter is required",
                        table_partition_col
                    )));
                }
                // tail_partition_col_range will be used when join batch_parts later
            }
        }
    }

    let mut row_ranges = vec![];
    for parts in batch_parts {
        let key = parts.join(&datasource.table_partition_separator);
        match tail_partition_col_range {
            Some((ref low, ref high)) => {
                row_ranges.push(RowRange {
                    start_key: Some(StartKey::StartKeyClosed(
                        (key.clone() + &datasource.table_partition_separator + &low).into_bytes(),
                    )),
                    end_key: Some(EndKey::EndKeyClosed(
                        (key.clone() + &datasource.table_partition_separator + &high).into_bytes(),
                    )),
                });
            }
            _ => {
                row_ranges.push(RowRange {
                    start_key: Some(StartKey::StartKeyClosed(key.clone().into_bytes())),
                    end_key: Some(EndKey::EndKeyClosed(key.clone().into_bytes())),
                });
            }
        }
    }
    Ok(row_ranges)
}
fn process_binary_expr(
    datasource: BigtableDataSource,
    partition_col_values: &mut HashMap<String, Vec<String>>,
    left: &Box<Expr>,
    op: &Operator,
    right: &Box<Expr>,
) {
    match left.as_ref() {
        Expr::Column(col) => {
            if datasource.table_partition_cols.contains(&col.name) {
                match op {
                    Operator::Eq => match right.as_ref() {
                        Expr::Literal(ScalarValue::Utf8(Some(key))) => {
                            partition_col_values
                                .entry(col.name.to_owned())
                                .or_insert(vec![]);
                            partition_col_values
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
    }
}

fn process_in_list(
    datasource: BigtableDataSource,
    partition_col_values: &mut HashMap<String, Vec<String>>,
    expr: &Box<Expr>,
    list: &Vec<Expr>,
    negated: &bool,
) {
    match expr.as_ref() {
        Expr::Column(col) => {
            if datasource.table_partition_cols.contains(&col.name) && !negated.to_owned() {
                for right in list {
                    match right {
                        Expr::Literal(ScalarValue::Utf8(Some(key))) => {
                            partition_col_values
                                .entry(col.name.to_owned())
                                .or_insert(vec![]);
                            partition_col_values
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
    }
}

fn process_between(
    datasource: BigtableDataSource,
    tail_partition_col_range: &mut Option<(String, String)>,
    expr: &Box<Expr>,
    negated: &bool,
    low: &Box<Expr>,
    high: &Box<Expr>,
) {
    match expr.as_ref() {
        Expr::Column(col) => {
            if datasource.table_partition_cols.last().unwrap() == &col.name && !negated.to_owned() {
                match low.as_ref() {
                    Expr::Literal(ScalarValue::Utf8(Some(low_key))) => match high.as_ref() {
                        Expr::Literal(ScalarValue::Utf8(Some(high_key))) => {
                            *tail_partition_col_range = Some((low_key.clone(), high_key.clone()));
                        }
                        _ => (),
                    },
                    _ => (),
                }
            }
        }
        _ => (),
    }
}
fn prepare_pushdown(
    datasource: BigtableDataSource,
    projection: &Option<Vec<usize>>,
) -> Result<Vec<RowFilter>> {
    // basic filter for column family
    let mut row_filters = vec![RowFilter {
        filter: Some(Filter::FamilyNameRegexFilter(datasource.column_family)),
    }];
    if datasource.only_read_latest {
        row_filters.push(RowFilter {
            filter: Some(Filter::CellsPerColumnLimitFilter(1)),
        });
    }

    // projection pushdown
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

    Ok(row_filters)
}

// https://gist.github.com/kylewlacy/115965b40e02a3325558
fn partial_cartesian<T: Clone>(a: Vec<Vec<T>>, b: &[T]) -> Vec<Vec<T>> {
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
