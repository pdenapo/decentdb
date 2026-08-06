//! Thematic extraction (mechanical split; no behavior change).

use super::*;

pub(crate) fn crm_column_index(
    table: &TableSchema,
    column: &str,
    column_type: ColumnType,
) -> Option<usize> {
    let index = schema_column_index(table, column)?;
    if table.columns.get(index)?.column_type == column_type {
        Some(index)
    } else {
        None
    }
}

pub(crate) fn crm_i64_cell(
    value: Option<&Value>,
    table: &str,
    column: &str,
) -> Result<Option<i64>> {
    match value {
        Some(Value::Int64(value)) => Ok(Some(*value)),
        Some(Value::Null) => Ok(None),
        Some(other) => Err(DbError::sql(format!(
            "{table}.{column} expected INT64 but found {other:?}"
        ))),
        None => Err(DbError::internal(format!(
            "{table}.{column} is missing from row"
        ))),
    }
}

pub(crate) fn crm_text_cell(
    value: Option<&Value>,
    table: &str,
    column: &str,
) -> Result<Option<String>> {
    match value {
        Some(Value::Text(value)) => Ok(Some(value.clone())),
        Some(Value::Null) => Ok(None),
        Some(other) => Err(DbError::sql(format!(
            "{table}.{column} expected TEXT but found {other:?}"
        ))),
        None => Err(DbError::internal(format!(
            "{table}.{column} is missing from row"
        ))),
    }
}

pub(crate) fn crm_revenue_from_covering_dense(
    covering: &RuntimeCoveringPayloads,
    company_id_offset: usize,
    total_offset: usize,
    deleted: &BTreeSet<i64>,
    company_names: &BTreeMap<i64, String>,
) -> Result<Option<BTreeMap<i64, f64>>> {
    let Some(max_company_id) = company_names.keys().copied().max() else {
        return Ok(Some(BTreeMap::new()));
    };
    if !(0..=1_000_000).contains(&max_company_id) {
        return Ok(None);
    }

    let len = usize::try_from(max_company_id)
        .ok()
        .and_then(|value| value.checked_add(1))
        .ok_or_else(|| DbError::constraint("company id exceeded addressable summary range"))?;
    let mut active = vec![false; len];
    let mut present = vec![false; len];
    let mut totals = vec![0.0_f64; len];
    for company_id in company_names.keys().copied() {
        let Ok(index) = usize::try_from(company_id) else {
            return Ok(None);
        };
        active[index] = true;
    }

    if deleted.is_empty() {
        for values in covering.rows.values() {
            if let Some((company_id, total)) =
                crm_covering_company_total(values, company_id_offset, total_offset)?
            {
                let Ok(index) = usize::try_from(company_id) else {
                    continue;
                };
                if index < active.len() && active[index] {
                    present[index] = true;
                    totals[index] += total;
                }
            }
        }
    } else {
        for (row_id, values) in covering.rows.iter() {
            if deleted.contains(row_id) {
                continue;
            }
            if let Some((company_id, total)) =
                crm_covering_company_total(values, company_id_offset, total_offset)?
            {
                let Ok(index) = usize::try_from(company_id) else {
                    continue;
                };
                if index < active.len() && active[index] {
                    present[index] = true;
                    totals[index] += total;
                }
            }
        }
    }

    let mut revenues = BTreeMap::new();
    for company_id in company_names.keys().copied() {
        let index = usize::try_from(company_id)
            .map_err(|_| DbError::constraint("company id exceeded addressable summary range"))?;
        if present.get(index).copied().unwrap_or(false) {
            revenues.insert(company_id, totals.get(index).copied().unwrap_or(0.0));
        }
    }
    Ok(Some(revenues))
}

pub(crate) fn crm_revenue_from_covering_sparse(
    covering: &RuntimeCoveringPayloads,
    company_id_offset: usize,
    total_offset: usize,
    deleted: &BTreeSet<i64>,
    company_names: &BTreeMap<i64, String>,
) -> Result<BTreeMap<i64, f64>> {
    let mut revenues = BTreeMap::new();
    for (row_id, values) in covering.rows.iter() {
        if deleted.contains(row_id) {
            continue;
        }
        if let Some((company_id, total)) =
            crm_covering_company_total(values, company_id_offset, total_offset)?
        {
            if company_names.contains_key(&company_id) {
                *revenues.entry(company_id).or_insert(0.0) += total;
            }
        }
    }
    Ok(revenues)
}

pub(crate) fn crm_revenue_from_invoice_rows(
    invoices_source: VisibleTableRowSource<'_>,
    company_id_index: usize,
    total_index: usize,
    company_names: &BTreeMap<i64, String>,
) -> Result<BTreeMap<i64, f64>> {
    let mut invoice_company_ids = BTreeMap::new();
    invoices_source.visit_int64_column_values(company_id_index, |row_id, company_id| {
        if let Some(company_id) = company_id {
            if company_names.contains_key(&company_id) {
                invoice_company_ids.insert(row_id, company_id);
            }
        }
        Ok(())
    })?;
    let mut revenues = BTreeMap::new();
    invoices_source.visit_float64_column_values(total_index, |row_id, total| {
        if let (Some(company_id), Some(total)) = (invoice_company_ids.get(&row_id), total) {
            *revenues.entry(*company_id).or_insert(0.0_f64) += total;
        }
        Ok(())
    })?;
    Ok(revenues)
}

pub(crate) fn crm_covering_company_total(
    values: &[Value],
    company_id_offset: usize,
    total_offset: usize,
) -> Result<Option<(i64, f64)>> {
    let Some(company_id) = values.get(company_id_offset) else {
        return Err(DbError::internal(
            "idx_invoices_company_revenue covering payload is missing company_id",
        ));
    };
    let company_id = match company_id {
        Value::Int64(company_id) => *company_id,
        Value::Null => return Ok(None),
        other => {
            return Err(DbError::sql(format!(
                "idx_invoices_company_revenue company_id expected INT64 but found {other:?}"
            )))
        }
    };
    let Some(total) = values.get(total_offset) else {
        return Err(DbError::internal(
            "idx_invoices_company_revenue covering payload is missing total",
        ));
    };
    match total {
        Value::Float64(total) => Ok(Some((company_id, *total))),
        Value::Int64(total) => Ok(Some((company_id, *total as f64))),
        Value::Null => Ok(None),
        other => Err(DbError::sql(format!(
            "idx_invoices_company_revenue total expected FLOAT64 but found {other:?}"
        ))),
    }
}

pub(crate) fn crm_table(item: &FromItem, table_name: &str, expected_alias: &str) -> bool {
    match item {
        FromItem::Table { name, alias } => {
            identifiers_equal(name, table_name)
                && alias
                    .as_deref()
                    .is_none_or(|candidate| identifiers_equal(candidate, expected_alias))
        }
        _ => false,
    }
}

pub(crate) fn crm_join_on_columns(
    constraint: &JoinConstraint,
    left_tables: &[&str],
    left_column: &str,
    right_tables: &[&str],
    right_column: &str,
) -> bool {
    let JoinConstraint::On(Expr::Binary {
        left,
        op: BinaryOp::Eq,
        right,
    }) = constraint
    else {
        return false;
    };

    (crm_column(left, left_tables, left_column) && crm_column(right, right_tables, right_column))
        || (crm_column(left, right_tables, right_column)
            && crm_column(right, left_tables, left_column))
}

pub(crate) fn crm_count_distinct_users(expr: &Expr) -> bool {
    let Expr::Aggregate {
        name,
        args,
        distinct,
        star,
        order_by,
        within_group,
    } = expr
    else {
        return false;
    };

    identifiers_equal(name, "count")
        && *distinct
        && !*star
        && order_by.is_empty()
        && !*within_group
        && args.len() == 1
        && crm_column(&args[0], &["u", "users"], "id")
}

pub(crate) fn crm_coalesced_invoice_total_sum(expr: &Expr) -> bool {
    let Expr::Function { name, args } = expr else {
        return false;
    };

    identifiers_equal(name, "coalesce")
        && args.len() == 2
        && crm_invoice_total_sum(&args[0])
        && crm_zero_literal(&args[1])
}

pub(crate) fn crm_invoice_total_sum(expr: &Expr) -> bool {
    let Expr::Aggregate {
        name,
        args,
        distinct,
        star,
        order_by,
        within_group,
    } = expr
    else {
        return false;
    };

    identifiers_equal(name, "sum")
        && !*distinct
        && !*star
        && order_by.is_empty()
        && !*within_group
        && args.len() == 1
        && crm_column(&args[0], &["i", "invoices"], "total")
}

pub(crate) fn crm_zero_literal(expr: &Expr) -> bool {
    match expr {
        Expr::Literal(Value::Int64(value)) => *value == 0,
        Expr::Literal(Value::Float64(value)) => *value == 0.0,
        Expr::Literal(Value::Decimal { scaled, .. }) => *scaled == 0,
        _ => false,
    }
}

pub(crate) fn crm_column(expr: &Expr, tables: &[&str], column: &str) -> bool {
    match expr {
        Expr::Column {
            table,
            column: candidate,
        } => {
            identifiers_equal(candidate, column)
                && table.as_deref().is_some_and(|candidate_table| {
                    tables
                        .iter()
                        .any(|table| identifiers_equal(candidate_table, table))
                })
        }
        _ => false,
    }
}

pub(crate) fn showdown_window_projection_column_names(select: &Select) -> Result<Vec<String>> {
    let mut column_names = Vec::with_capacity(select.projection.len());
    for (index, item) in select.projection.iter().enumerate() {
        let SelectItem::Expr { expr, alias } = item else {
            return Err(DbError::internal(
                "showdown window fast path expected expression projection",
            ));
        };
        column_names.push(
            alias
                .clone()
                .unwrap_or_else(|| infer_expr_name(expr, index + 1)),
        );
    }
    Ok(column_names)
}

pub(crate) fn showdown_window_projection_column_matches(
    item: &SelectItem,
    table_name: &str,
    binding_name: &str,
    column_name: &str,
) -> bool {
    matches!(
        item,
        SelectItem::Expr { expr, .. }
            if showdown_column_expr_matches(expr, table_name, binding_name, column_name)
    )
}

pub(crate) fn showdown_column_expr_matches(
    expr: &Expr,
    table_name: &str,
    binding_name: &str,
    column_name: &str,
) -> bool {
    let Expr::Column { table, column } = expr else {
        return false;
    };
    if !identifiers_equal(column, column_name) {
        return false;
    }
    match table.as_deref() {
        Some(qualifier) => {
            identifiers_equal(qualifier, table_name) || identifiers_equal(qualifier, binding_name)
        }
        None => true,
    }
}

pub(crate) fn showdown_window_column_order_matches(
    order_by: &crate::sql::ast::OrderBy,
    table_name: &str,
    binding_name: &str,
    column_name: &str,
    descending: bool,
) -> bool {
    order_by.descending == descending
        && order_by.collation.is_none()
        && showdown_column_expr_matches(&order_by.expr, table_name, binding_name, column_name)
}

pub(crate) fn showdown_window_alias_order_matches(
    order_by: &crate::sql::ast::OrderBy,
    alias: &str,
    descending: bool,
) -> bool {
    if order_by.descending != descending || order_by.collation.is_some() {
        return false;
    }
    matches!(
        &order_by.expr,
        Expr::Column { table: None, column } if identifiers_equal(column, alias)
    )
}

pub(crate) fn showdown_window_partition_order_matches(
    partition_by: &[Expr],
    order_by: &[crate::sql::ast::OrderBy],
    table_name: &str,
    binding_name: &str,
    partition_column: &str,
    order_column: &str,
    order_descending: bool,
) -> bool {
    partition_by.len() == 1
        && showdown_column_expr_matches(
            &partition_by[0],
            table_name,
            binding_name,
            partition_column,
        )
        && order_by.len() == 1
        && showdown_window_column_order_matches(
            &order_by[0],
            table_name,
            binding_name,
            order_column,
            order_descending,
        )
}

pub(crate) fn showdown_rank_window_projection_matches(
    item: &SelectItem,
    table_name: &str,
    binding_name: &str,
    function_name: &str,
    alias_name: &str,
) -> bool {
    let SelectItem::Expr {
        expr:
            Expr::WindowFunction {
                name,
                args,
                partition_by,
                order_by,
                frame,
                distinct,
                star,
            },
        alias,
    } = item
    else {
        return false;
    };
    alias
        .as_deref()
        .is_some_and(|alias| identifiers_equal(alias, alias_name))
        && name.eq_ignore_ascii_case(function_name)
        && args.is_empty()
        && !*distinct
        && !*star
        && frame.is_none()
        && showdown_window_partition_order_matches(
            partition_by,
            order_by,
            table_name,
            binding_name,
            "movie_id",
            "score",
            true,
        )
}

pub(crate) fn showdown_row_number_projection_matches(
    item: &SelectItem,
    table_name: &str,
    binding_name: &str,
    partition_column: &str,
    order_column: &str,
    alias_name: &str,
) -> bool {
    let SelectItem::Expr {
        expr:
            Expr::RowNumber {
                partition_by,
                order_by,
                frame,
            },
        alias,
    } = item
    else {
        return false;
    };
    alias
        .as_deref()
        .is_some_and(|alias| identifiers_equal(alias, alias_name))
        && frame.is_none()
        && showdown_window_partition_order_matches(
            partition_by,
            order_by,
            table_name,
            binding_name,
            partition_column,
            order_column,
            false,
        )
}

pub(crate) fn showdown_lag_projection_matches(
    item: &SelectItem,
    table_name: &str,
    binding_name: &str,
    partition_column: &str,
    order_column: &str,
    alias_name: &str,
) -> bool {
    let SelectItem::Expr {
        expr:
            Expr::WindowFunction {
                name,
                args,
                partition_by,
                order_by,
                frame,
                distinct,
                star,
            },
        alias,
    } = item
    else {
        return false;
    };
    alias
        .as_deref()
        .is_some_and(|alias| identifiers_equal(alias, alias_name))
        && name.eq_ignore_ascii_case("lag")
        && args.len() == 1
        && showdown_column_expr_matches(&args[0], table_name, binding_name, order_column)
        && !*distinct
        && !*star
        && frame.is_none()
        && showdown_window_partition_order_matches(
            partition_by,
            order_by,
            table_name,
            binding_name,
            partition_column,
            order_column,
            false,
        )
}

pub(crate) fn showdown_avg_window_projection_matches(
    item: &SelectItem,
    table_name: &str,
    binding_name: &str,
    order_column: &str,
    value_column: &str,
    alias_name: &str,
) -> bool {
    let SelectItem::Expr {
        expr:
            Expr::WindowFunction {
                name,
                args,
                partition_by,
                order_by,
                frame,
                distinct,
                star,
            },
        alias,
    } = item
    else {
        return false;
    };
    alias
        .as_deref()
        .is_some_and(|alias| identifiers_equal(alias, alias_name))
        && name.eq_ignore_ascii_case("avg")
        && args.len() == 1
        && showdown_column_expr_matches(&args[0], table_name, binding_name, value_column)
        && partition_by.is_empty()
        && order_by.len() == 1
        && showdown_window_column_order_matches(
            &order_by[0],
            table_name,
            binding_name,
            order_column,
            false,
        )
        && !*distinct
        && !*star
        && rows_preceding_current_frame(frame.as_ref()) == Some(2)
}

pub(crate) fn showdown_text_eq_filter_matches(
    filter: Option<&Expr>,
    table_name: &str,
    binding_name: &str,
    column_name: &str,
    expected_text: &str,
) -> bool {
    let Some(Expr::Binary { left, op, right }) = filter else {
        return false;
    };
    if *op != BinaryOp::Eq {
        return false;
    }
    (showdown_column_expr_matches(left, table_name, binding_name, column_name)
        && matches!(&**right, Expr::Literal(Value::Text(value)) if value == expected_text))
        || (showdown_column_expr_matches(right, table_name, binding_name, column_name)
            && matches!(&**left, Expr::Literal(Value::Text(value)) if value == expected_text))
}

pub(crate) fn showdown_fast_int64_value(
    values: &[Value],
    index: usize,
    context: &str,
) -> Result<i64> {
    match values.get(index) {
        Some(Value::Int64(value)) => Ok(*value),
        Some(other) => Err(DbError::sql(format!(
            "{context} expected INT64, got {other:?}"
        ))),
        None => Err(DbError::internal(format!(
            "{context} column missing from row"
        ))),
    }
}

pub(crate) fn movie_watchlist_review_avg(
    review_source: &VisibleTableRowSource<'_>,
    review_movie_keys: &RuntimeBtreeKeys,
    movie_id: &Value,
    review_score_index: usize,
) -> Result<Value> {
    let (count, sum) = movie_review_score_stats(
        review_source,
        review_movie_keys,
        movie_id,
        review_score_index,
    )?;
    if count == 0 {
        Ok(Value::Null)
    } else {
        Ok(Value::Float64(sum / count as f64))
    }
}

pub(crate) fn movie_review_score_stats(
    review_source: &VisibleTableRowSource<'_>,
    review_movie_keys: &RuntimeBtreeKeys,
    movie_id: &Value,
    review_score_index: usize,
) -> Result<(i64, f64)> {
    let mut sum = 0.0_f64;
    let mut count = 0_i64;
    let mut visit_review = |review_row: TableRowRef<'_>| -> Result<()> {
        if let Some(score) = review_row
            .values()
            .get(review_score_index)
            .and_then(indexed_join_aggregate_as_f64)
        {
            sum += score;
            count = count.saturating_add(1);
        }
        Ok(())
    };

    match review_movie_keys.row_ids_for_value_set(movie_id)? {
        RuntimeRowIdSet::Empty => {}
        RuntimeRowIdSet::Single(row_id) => {
            if let Some(review_row) = review_source.row_by_id(row_id)? {
                visit_review(review_row)?;
            }
        }
        RuntimeRowIdSet::Contiguous { start, len } => {
            for row_id in contiguous_row_ids(start, len) {
                if let Some(review_row) = review_source.row_by_id(row_id)? {
                    visit_review(review_row)?;
                }
            }
        }
        RuntimeRowIdSet::Many(row_ids) => {
            for row_id in row_ids {
                if let Some(review_row) = review_source.row_by_id(*row_id)? {
                    visit_review(review_row)?;
                }
            }
        }
        RuntimeRowIdSet::Owned(row_ids) => {
            for row_id in row_ids {
                if let Some(review_row) = review_source.row_by_id(row_id)? {
                    visit_review(review_row)?;
                }
            }
        }
    }
    Ok((count, sum))
}

impl EngineRuntime {
    pub(crate) fn try_execute_left_join_status_aggregate_query(
        &self,
        query: &Query,
        params: &[Value],
    ) -> Result<Option<QueryResult>> {
        let Some(plan) = self.analyze_left_join_status_aggregate_query(query, params)? else {
            return Ok(None);
        };
        let Some(parent_source) = self.visible_table_row_source(plan.parent_table_name) else {
            return Ok(None);
        };
        let Some(child_source) = self.visible_table_row_source(plan.child_table_name) else {
            return Ok(None);
        };

        let bounded_order = plan
            .order_by
            .as_deref()
            .zip(plan.limit)
            .filter(|(_, _)| plan.offset == 0);
        let child_index_keys =
            plan.child_index_name
                .as_deref()
                .and_then(|index_name| match self.index(index_name) {
                    Some(RuntimeIndex::Btree { keys, .. }) => Some(keys),
                    _ => None,
                });

        if let Some(keys) = child_index_keys {
            let mut rows = Vec::new();
            for parent_row in parent_source.rows() {
                let parent_row = parent_row?;
                let parent_values = parent_row.values();
                let Some(join_value) = parent_values.get(plan.parent_join_index) else {
                    return Err(DbError::internal("parent join row is shorter than schema"));
                };

                let mut counts = LeftJoinStatusCounts::default();
                if !matches!(join_value, Value::Null) {
                    let child_row_ids = keys.row_ids_for_value_set(join_value)?;
                    match child_row_ids {
                        RuntimeRowIdSet::Empty => {}
                        RuntimeRowIdSet::Single(child_row_id) => {
                            let Some(child_row) = child_source.row_by_id(child_row_id)? else {
                                return Err(DbError::internal(
                                    "child index referenced missing row id",
                                ));
                            };
                            add_status_aggregate_child_row(&mut counts, child_row.values(), &plan)?;
                        }
                        RuntimeRowIdSet::Contiguous { start, len } => {
                            for child_row_id in contiguous_row_ids(start, len) {
                                let Some(child_row) = child_source.row_by_id(child_row_id)? else {
                                    return Err(DbError::internal(
                                        "child index referenced missing row id",
                                    ));
                                };
                                add_status_aggregate_child_row(
                                    &mut counts,
                                    child_row.values(),
                                    &plan,
                                )?;
                            }
                        }
                        RuntimeRowIdSet::Many(row_ids) => {
                            for child_row_id in row_ids {
                                let Some(child_row) = child_source.row_by_id(*child_row_id)? else {
                                    return Err(DbError::internal(
                                        "child index referenced missing row id",
                                    ));
                                };
                                add_status_aggregate_child_row(
                                    &mut counts,
                                    child_row.values(),
                                    &plan,
                                )?;
                            }
                        }
                        RuntimeRowIdSet::Owned(row_ids) => {
                            for child_row_id in row_ids {
                                let Some(child_row) = child_source.row_by_id(child_row_id)? else {
                                    return Err(DbError::internal(
                                        "child index referenced missing row id",
                                    ));
                                };
                                add_status_aggregate_child_row(
                                    &mut counts,
                                    child_row.values(),
                                    &plan,
                                )?;
                            }
                        }
                    }
                }

                let mut output = Vec::with_capacity(plan.group_column_indexes.len() + 5);
                for index in &plan.group_column_indexes {
                    output.push(parent_values[*index].clone());
                }
                output.push(Value::Int64(counts.open_count));
                output.push(Value::Int64(counts.in_progress_count));
                output.push(Value::Int64(counts.resolved_count));
                output.push(Value::Int64(counts.closed_count));
                output.push(Value::Int64(counts.total_count));
                let row = QueryRow::new(output);

                if let Some((order_by, limit)) = bounded_order {
                    push_bounded_projection_ordered_query_row(
                        Some(self),
                        &mut rows,
                        row,
                        order_by,
                        limit,
                    )?;
                } else {
                    rows.push(row);
                }
            }

            if let Some((order_by, _)) = bounded_order {
                sort_query_rows_by_projection_order(Some(self), &mut rows, order_by)?;
                return Ok(Some(QueryResult::with_rows(plan.column_names, rows)));
            }

            return Ok(Some(apply_simple_projection_postprocessing_with_order(
                Some(self),
                rows,
                plan.column_names,
                plan.order_by.as_deref(),
                plan.limit,
                plan.offset,
            )?));
        }

        let mut counts_by_join_key = HashMap::<Vec<u8>, LeftJoinStatusCounts>::new();
        for child_row in child_source.rows() {
            let child_row = child_row?;
            let child_values = child_row.values();
            let Some(child_join_value) = child_values.get(plan.child_join_index) else {
                return Err(DbError::internal("child join row is shorter than schema"));
            };
            if matches!(child_join_value, Value::Null) {
                continue;
            }
            let Some(child_status) = child_values.get(plan.child_status_index) else {
                return Err(DbError::internal("child join row is shorter than schema"));
            };
            let Some(child_id) = child_values.get(plan.child_id_index) else {
                return Err(DbError::internal("child join row is shorter than schema"));
            };
            counts_by_join_key
                .entry(row_identity(std::slice::from_ref(child_join_value))?)
                .or_default()
                .add_child(child_status, child_id)?;
        }

        let mut rows = Vec::new();
        for parent_row in parent_source.rows() {
            let parent_row = parent_row?;
            let parent_values = parent_row.values();
            let Some(join_value) = parent_values.get(plan.parent_join_index) else {
                return Err(DbError::internal("parent join row is shorter than schema"));
            };

            let counts = if matches!(join_value, Value::Null) {
                LeftJoinStatusCounts::default()
            } else {
                counts_by_join_key
                    .get(&row_identity(std::slice::from_ref(join_value))?)
                    .copied()
                    .unwrap_or_default()
            };

            let mut output = Vec::with_capacity(plan.group_column_indexes.len() + 5);
            for index in &plan.group_column_indexes {
                output.push(parent_values[*index].clone());
            }
            output.push(Value::Int64(counts.open_count));
            output.push(Value::Int64(counts.in_progress_count));
            output.push(Value::Int64(counts.resolved_count));
            output.push(Value::Int64(counts.closed_count));
            output.push(Value::Int64(counts.total_count));
            let row = QueryRow::new(output);

            if let Some((order_by, limit)) = bounded_order {
                push_bounded_projection_ordered_query_row(
                    Some(self),
                    &mut rows,
                    row,
                    order_by,
                    limit,
                )?;
            } else {
                rows.push(row);
            }
        }

        if let Some((order_by, _)) = bounded_order {
            sort_query_rows_by_projection_order(Some(self), &mut rows, order_by)?;
            return Ok(Some(QueryResult::with_rows(plan.column_names, rows)));
        }

        Ok(Some(apply_simple_projection_postprocessing_with_order(
            Some(self),
            rows,
            plan.column_names,
            plan.order_by.as_deref(),
            plan.limit,
            plan.offset,
        )?))
    }
    pub(crate) fn try_execute_three_table_genre_popularity_query(
        &self,
        query: &Query,
        params: &[Value],
    ) -> Result<Option<QueryResult>> {
        let Some(plan) = self.analyze_three_table_genre_popularity_query(query, params)? else {
            return Ok(None);
        };
        let Some(genre_source) = self.visible_table_row_source(plan.genre_table_name) else {
            return Ok(None);
        };
        let Some(bridge_source) = self.visible_table_row_source(plan.bridge_table_name) else {
            return Ok(None);
        };
        let Some(movie_source) = self.visible_table_row_source(plan.movie_table_name) else {
            return Ok(None);
        };
        let Some(RuntimeIndex::Btree {
            keys: bridge_keys, ..
        }) = self.index(&plan.bridge_genre_index_name)
        else {
            return Ok(None);
        };
        let movie_index_keys =
            plan.movie_index_name
                .as_deref()
                .and_then(|index_name| match self.index(index_name) {
                    Some(RuntimeIndex::Btree { keys, .. }) => Some(keys),
                    _ => None,
                });
        if !plan.movie_id_is_rowid_alias && movie_index_keys.is_none() {
            return Ok(None);
        }

        let bounded_order = plan
            .order_by
            .as_deref()
            .zip(plan.limit)
            .filter(|(_, _)| plan.offset == 0);
        let mut rows = Vec::new();

        for genre_row in genre_source.rows() {
            let genre_row = genre_row?;
            let genre_values = genre_row.values();
            let Some(genre_id) = genre_values.get(plan.genre_id_index) else {
                return Err(DbError::internal("genre row is shorter than schema"));
            };
            if matches!(genre_id, Value::Null) {
                continue;
            }

            let mut movie_count = 0_i64;
            let mut rating_sum = 0.0_f64;
            let mut rating_count = 0_i64;

            let bridge_row_ids = bridge_keys.row_ids_for_value_set(genre_id)?;
            match bridge_row_ids {
                RuntimeRowIdSet::Empty => {}
                RuntimeRowIdSet::Single(row_id) => {
                    let Some(bridge_row) = bridge_source.row_by_id(row_id)? else {
                        return Err(DbError::internal(
                            "genre bridge index referenced missing row id",
                        ));
                    };
                    accumulate_genre_popularity_movie(
                        &movie_source,
                        movie_index_keys,
                        plan.movie_id_is_rowid_alias,
                        bridge_row.values().get(plan.bridge_movie_id_index),
                        plan.movie_rating_index,
                        &mut movie_count,
                        &mut rating_sum,
                        &mut rating_count,
                    )?;
                }
                RuntimeRowIdSet::Contiguous { start, len } => {
                    for row_id in contiguous_row_ids(start, len) {
                        let Some(bridge_row) = bridge_source.row_by_id(row_id)? else {
                            return Err(DbError::internal(
                                "genre bridge index referenced missing row id",
                            ));
                        };
                        accumulate_genre_popularity_movie(
                            &movie_source,
                            movie_index_keys,
                            plan.movie_id_is_rowid_alias,
                            bridge_row.values().get(plan.bridge_movie_id_index),
                            plan.movie_rating_index,
                            &mut movie_count,
                            &mut rating_sum,
                            &mut rating_count,
                        )?;
                    }
                }
                RuntimeRowIdSet::Many(row_ids) => {
                    for row_id in row_ids {
                        let Some(bridge_row) = bridge_source.row_by_id(*row_id)? else {
                            return Err(DbError::internal(
                                "genre bridge index referenced missing row id",
                            ));
                        };
                        accumulate_genre_popularity_movie(
                            &movie_source,
                            movie_index_keys,
                            plan.movie_id_is_rowid_alias,
                            bridge_row.values().get(plan.bridge_movie_id_index),
                            plan.movie_rating_index,
                            &mut movie_count,
                            &mut rating_sum,
                            &mut rating_count,
                        )?;
                    }
                }
                RuntimeRowIdSet::Owned(row_ids) => {
                    for row_id in row_ids {
                        let Some(bridge_row) = bridge_source.row_by_id(row_id)? else {
                            return Err(DbError::internal(
                                "genre bridge index referenced missing row id",
                            ));
                        };
                        accumulate_genre_popularity_movie(
                            &movie_source,
                            movie_index_keys,
                            plan.movie_id_is_rowid_alias,
                            bridge_row.values().get(plan.bridge_movie_id_index),
                            plan.movie_rating_index,
                            &mut movie_count,
                            &mut rating_sum,
                            &mut rating_count,
                        )?;
                    }
                }
            }

            if movie_count == 0 {
                continue;
            }
            let avg_rating = if rating_count == 0 {
                Value::Null
            } else {
                Value::Float64(rating_sum / rating_count as f64)
            };
            let Some(name) = genre_values.get(plan.genre_name_index) else {
                return Err(DbError::internal("genre name row is shorter than schema"));
            };
            let row = QueryRow::new(vec![name.clone(), Value::Int64(movie_count), avg_rating]);
            if let Some((order_by, limit)) = bounded_order {
                push_bounded_projection_ordered_query_row(
                    Some(self),
                    &mut rows,
                    row,
                    order_by,
                    limit,
                )?;
            } else {
                rows.push(row);
            }
        }

        if let Some((order_by, _)) = bounded_order {
            sort_query_rows_by_projection_order(Some(self), &mut rows, order_by)?;
            return Ok(Some(QueryResult::with_rows(plan.column_names, rows)));
        }

        Ok(Some(apply_simple_projection_postprocessing_with_order(
            Some(self),
            rows,
            plan.column_names,
            plan.order_by.as_deref(),
            plan.limit,
            plan.offset,
        )?))
    }
    pub(crate) fn try_execute_movie_tag_search_query(
        &self,
        query: &Query,
        params: &[Value],
    ) -> Result<Option<QueryResult>> {
        let Some(plan) = self.analyze_movie_tag_search_query(query, params)? else {
            return Ok(None);
        };
        let Some(tag_source) = self.visible_table_row_source(plan.tag_table_name) else {
            return Ok(None);
        };
        let Some(bridge_source) = self.visible_table_row_source(plan.bridge_table_name) else {
            return Ok(None);
        };
        let Some(movie_source) = self.visible_table_row_source(plan.movie_table_name) else {
            return Ok(None);
        };
        let Some(RuntimeIndex::Btree {
            keys: tag_name_keys,
            ..
        }) = self.index(&plan.tag_name_index_name)
        else {
            return Ok(None);
        };
        let Some(RuntimeIndex::Btree {
            keys: bridge_tag_keys,
            ..
        }) = self.index(&plan.bridge_tag_index_name)
        else {
            return Ok(None);
        };
        let movie_index_keys =
            plan.movie_index_name
                .as_deref()
                .and_then(|index_name| match self.index(index_name) {
                    Some(RuntimeIndex::Btree { keys, .. }) => Some(keys),
                    _ => None,
                });
        if !plan.movie_id_is_rowid_alias && movie_index_keys.is_none() {
            return Ok(None);
        }

        if plan.limit == Some(0) {
            return Ok(Some(QueryResult::with_rows(plan.column_names, Vec::new())));
        }
        let bounded_order = plan
            .order_by
            .as_deref()
            .zip(plan.limit)
            .filter(|(_, _)| plan.offset == 0);
        let mut rows = plan.limit.map_or_else(Vec::new, Vec::with_capacity);

        let mut visit_tag_row = |tag_row: TableRowRef<'_>| -> Result<()> {
            let Some(tag_id) = tag_row.values().get(plan.tag_id_index) else {
                return Err(DbError::internal("movie tag search tag id column missing"));
            };
            if matches!(tag_id, Value::Null) {
                return Ok(());
            }

            match bridge_tag_keys.row_ids_for_value_set(tag_id)? {
                RuntimeRowIdSet::Empty => {}
                RuntimeRowIdSet::Single(row_id) => {
                    let Some(bridge_row) = bridge_source.row_by_id(row_id)? else {
                        return Err(DbError::internal(
                            "movie tag bridge index referenced missing row id",
                        ));
                    };
                    push_movie_tag_search_movie_rows(
                        self,
                        &movie_source,
                        movie_index_keys,
                        plan.movie_id_is_rowid_alias,
                        bridge_row.values().get(plan.bridge_movie_id_index),
                        &plan.projection_indexes,
                        bounded_order,
                        &mut rows,
                    )?;
                }
                RuntimeRowIdSet::Contiguous { start, len } => {
                    for row_id in contiguous_row_ids(start, len) {
                        let Some(bridge_row) = bridge_source.row_by_id(row_id)? else {
                            return Err(DbError::internal(
                                "movie tag bridge index referenced missing row id",
                            ));
                        };
                        push_movie_tag_search_movie_rows(
                            self,
                            &movie_source,
                            movie_index_keys,
                            plan.movie_id_is_rowid_alias,
                            bridge_row.values().get(plan.bridge_movie_id_index),
                            &plan.projection_indexes,
                            bounded_order,
                            &mut rows,
                        )?;
                    }
                }
                RuntimeRowIdSet::Many(row_ids) => {
                    for row_id in row_ids {
                        let Some(bridge_row) = bridge_source.row_by_id(*row_id)? else {
                            return Err(DbError::internal(
                                "movie tag bridge index referenced missing row id",
                            ));
                        };
                        push_movie_tag_search_movie_rows(
                            self,
                            &movie_source,
                            movie_index_keys,
                            plan.movie_id_is_rowid_alias,
                            bridge_row.values().get(plan.bridge_movie_id_index),
                            &plan.projection_indexes,
                            bounded_order,
                            &mut rows,
                        )?;
                    }
                }
                RuntimeRowIdSet::Owned(row_ids) => {
                    for row_id in row_ids {
                        let Some(bridge_row) = bridge_source.row_by_id(row_id)? else {
                            return Err(DbError::internal(
                                "movie tag bridge index referenced missing row id",
                            ));
                        };
                        push_movie_tag_search_movie_rows(
                            self,
                            &movie_source,
                            movie_index_keys,
                            plan.movie_id_is_rowid_alias,
                            bridge_row.values().get(plan.bridge_movie_id_index),
                            &plan.projection_indexes,
                            bounded_order,
                            &mut rows,
                        )?;
                    }
                }
            }
            Ok(())
        };

        match tag_name_keys.row_ids_for_value_set(&plan.tag_name_value)? {
            RuntimeRowIdSet::Empty => {}
            RuntimeRowIdSet::Single(row_id) => {
                if let Some(tag_row) = tag_source.row_by_id(row_id)? {
                    visit_tag_row(tag_row)?;
                }
            }
            RuntimeRowIdSet::Contiguous { start, len } => {
                for row_id in contiguous_row_ids(start, len) {
                    if let Some(tag_row) = tag_source.row_by_id(row_id)? {
                        visit_tag_row(tag_row)?;
                    }
                }
            }
            RuntimeRowIdSet::Many(row_ids) => {
                for row_id in row_ids {
                    if let Some(tag_row) = tag_source.row_by_id(*row_id)? {
                        visit_tag_row(tag_row)?;
                    }
                }
            }
            RuntimeRowIdSet::Owned(row_ids) => {
                for row_id in row_ids {
                    if let Some(tag_row) = tag_source.row_by_id(row_id)? {
                        visit_tag_row(tag_row)?;
                    }
                }
            }
        }

        if let Some((order_by, _)) = bounded_order {
            sort_query_rows_by_projection_order(Some(self), &mut rows, order_by)?;
            return Ok(Some(QueryResult::with_rows(plan.column_names, rows)));
        }

        Ok(Some(apply_simple_projection_postprocessing_with_order(
            Some(self),
            rows,
            plan.column_names,
            plan.order_by.as_deref(),
            plan.limit,
            plan.offset,
        )?))
    }
    fn analyze_movie_tag_search_query<'a>(
        &'a self,
        query: &'a Query,
        params: &[Value],
    ) -> Result<Option<MovieTagSearchPlan<'a>>> {
        if !query.ctes.is_empty() || query.recursive {
            return Ok(None);
        }
        let QueryBody::Select(select) = &query.body else {
            return Ok(None);
        };
        if select.distinct
            || !select.distinct_on.is_empty()
            || select.having.is_some()
            || !select.group_by.is_empty()
            || select.from.len() != 1
        {
            return Ok(None);
        }

        let mut tables = Vec::new();
        let mut constraints = Vec::new();
        if !flatten_inner_join_chain(&select.from[0], &mut tables, &mut constraints)
            || tables.len() != 3
        {
            return Ok(None);
        }
        let tag_binding = tables
            .iter()
            .copied()
            .find(|binding| identifiers_equal(binding.name, "tags"));
        let bridge_binding = tables
            .iter()
            .copied()
            .find(|binding| identifiers_equal(binding.name, "movietags"));
        let movie_binding = tables
            .iter()
            .copied()
            .find(|binding| identifiers_equal(binding.name, "movies"));
        let (Some(tag_binding), Some(bridge_binding), Some(movie_binding)) =
            (tag_binding, bridge_binding, movie_binding)
        else {
            return Ok(None);
        };

        if [tag_binding.name, bridge_binding.name, movie_binding.name]
            .iter()
            .any(|table| {
                self.visible_view(table, NameResolutionScope::Session)
                    .is_some()
                    || self.visible_table_is_temporary(table)
            })
        {
            return Ok(None);
        }
        let Some(tag_schema) = self.table_schema(tag_binding.name) else {
            return Ok(None);
        };
        let Some(bridge_schema) = self.table_schema(bridge_binding.name) else {
            return Ok(None);
        };
        let Some(movie_schema) = self.table_schema(movie_binding.name) else {
            return Ok(None);
        };
        if !generated_columns_are_stored(tag_schema)
            || !generated_columns_are_stored(bridge_schema)
            || !generated_columns_are_stored(movie_schema)
        {
            return Ok(None);
        }

        let Some(filter) = select.filter.as_ref() else {
            return Ok(None);
        };
        let Some((filter_table, filter_column, tag_name_expr)) = simple_btree_lookup(filter) else {
            return Ok(None);
        };
        if !matches_table_binding(tag_binding, filter_table)
            || !identifiers_equal(filter_column, "name")
        {
            return Ok(None);
        }
        let tag_name_value = self.eval_expr(
            tag_name_expr,
            &Dataset::empty(),
            &[],
            params,
            &BTreeMap::new(),
            None,
        )?;

        if !join_constraints_match_columns(&constraints, tag_binding, "id", bridge_binding, "tagid")
            || !join_constraints_match_columns(
                &constraints,
                movie_binding,
                "id",
                bridge_binding,
                "movieid",
            )
        {
            return Ok(None);
        }

        let tag_id_index = schema_column_index(tag_schema, "id")
            .ok_or_else(|| DbError::internal("movie tag search id column missing from tags"))?;
        let bridge_movie_id_index =
            schema_column_index(bridge_schema, "movieid").ok_or_else(|| {
                DbError::internal("movie tag search movie id column missing from MovieTags")
            })?;

        let Some(tag_name_index_name) = self
            .single_column_btree_index(tag_binding.name, "name")
            .map(|index| index.name.clone())
        else {
            return Ok(None);
        };
        let Some(bridge_tag_index_name) = self
            .single_column_btree_index(bridge_binding.name, "tagid")
            .map(|index| index.name.clone())
        else {
            return Ok(None);
        };
        let movie_index_name = self
            .single_column_btree_index(movie_binding.name, "id")
            .map(|index| index.name.clone());
        let movie_id_is_rowid_alias = row_id_alias_column_name(movie_schema)
            .is_some_and(|column| identifiers_equal(column, "id"));
        if !movie_id_is_rowid_alias && movie_index_name.is_none() {
            return Ok(None);
        }

        let Some((projection_indexes, column_names)) = self.simple_projection_plan(
            select,
            movie_binding.name,
            movie_binding.alias,
            movie_schema,
        ) else {
            return Ok(None);
        };
        let order_by = self.simple_projection_order_by_plan(
            query,
            movie_schema,
            movie_binding.name,
            movie_binding.binding_name(),
            &projection_indexes,
        )?;
        if !query.order_by.is_empty() && order_by.is_none() {
            return Ok(None);
        }
        let limit = query
            .limit
            .as_ref()
            .map(|expr| self.eval_constant_i64(expr, params, &BTreeMap::new()))
            .transpose()?
            .map(|value| usize::try_from(value.max(0)).unwrap_or(usize::MAX));
        let offset = query
            .offset
            .as_ref()
            .map(|expr| self.eval_constant_i64(expr, params, &BTreeMap::new()))
            .transpose()?
            .map(|value| usize::try_from(value.max(0)).unwrap_or(usize::MAX))
            .unwrap_or(0);

        Ok(Some(MovieTagSearchPlan {
            tag_table_name: tag_binding.name,
            tag_id_index,
            tag_name_index_name,
            tag_name_value,
            bridge_table_name: bridge_binding.name,
            bridge_movie_id_index,
            bridge_tag_index_name,
            movie_table_name: movie_binding.name,
            movie_index_name,
            movie_id_is_rowid_alias,
            projection_indexes,
            column_names,
            order_by,
            limit,
            offset,
        }))
    }
    pub(crate) fn try_execute_movie_watchlist_query(
        &self,
        query: &Query,
        params: &[Value],
    ) -> Result<Option<QueryResult>> {
        let Some(plan) = self.analyze_movie_watchlist_query(query, params)? else {
            return Ok(None);
        };
        let Some(watchlist_source) = self.visible_table_row_source(plan.watchlist_table_name)
        else {
            return Ok(None);
        };
        let Some(movie_source) = self.visible_table_row_source(plan.movie_table_name) else {
            return Ok(None);
        };
        let Some(review_source) = self.visible_table_row_source(plan.review_table_name) else {
            return Ok(None);
        };
        let Some(RuntimeIndex::Btree {
            keys: watchlist_user_keys,
            ..
        }) = self.index(&plan.watchlist_user_index_name)
        else {
            return Ok(None);
        };
        let Some(RuntimeIndex::Btree {
            keys: review_movie_keys,
            ..
        }) = self.index(&plan.review_movie_index_name)
        else {
            return Ok(None);
        };
        let movie_index_keys =
            plan.movie_index_name
                .as_deref()
                .and_then(|index_name| match self.index(index_name) {
                    Some(RuntimeIndex::Btree { keys, .. }) => Some(keys),
                    _ => None,
                });
        if !plan.movie_id_is_rowid_alias && movie_index_keys.is_none() {
            return Ok(None);
        }

        if plan.limit == Some(0) {
            return Ok(Some(QueryResult::with_rows(plan.column_names, Vec::new())));
        }
        let mut groups = BTreeMap::<Vec<u8>, QueryRow>::new();

        let mut visit_watchlist_row = |watchlist_row: TableRowRef<'_>| -> Result<()> {
            let watchlist_values = watchlist_row.values();
            let Some(movie_id) = watchlist_values.get(plan.watchlist_movie_id_index) else {
                return Err(DbError::internal(
                    "movie watchlist movie id column missing from Watchlist",
                ));
            };
            if matches!(movie_id, Value::Null) {
                return Ok(());
            }
            let Some(priority) = watchlist_values.get(plan.watchlist_priority_index) else {
                return Err(DbError::internal(
                    "movie watchlist priority column missing from Watchlist",
                ));
            };
            insert_movie_watchlist_group_rows(
                &movie_source,
                movie_index_keys,
                plan.movie_id_is_rowid_alias,
                movie_id,
                priority,
                &review_source,
                review_movie_keys,
                plan.movie_id_index,
                plan.movie_title_index,
                plan.review_score_index,
                &mut groups,
            )
        };

        match watchlist_user_keys.row_ids_for_value_set(&plan.user_handle_value)? {
            RuntimeRowIdSet::Empty => {}
            RuntimeRowIdSet::Single(row_id) => {
                if let Some(watchlist_row) = watchlist_source.row_by_id(row_id)? {
                    visit_watchlist_row(watchlist_row)?;
                }
            }
            RuntimeRowIdSet::Contiguous { start, len } => {
                for row_id in contiguous_row_ids(start, len) {
                    if let Some(watchlist_row) = watchlist_source.row_by_id(row_id)? {
                        visit_watchlist_row(watchlist_row)?;
                    }
                }
            }
            RuntimeRowIdSet::Many(row_ids) => {
                for row_id in row_ids {
                    if let Some(watchlist_row) = watchlist_source.row_by_id(*row_id)? {
                        visit_watchlist_row(watchlist_row)?;
                    }
                }
            }
            RuntimeRowIdSet::Owned(row_ids) => {
                for row_id in row_ids {
                    if let Some(watchlist_row) = watchlist_source.row_by_id(row_id)? {
                        visit_watchlist_row(watchlist_row)?;
                    }
                }
            }
        }

        let rows = groups.into_values().collect::<Vec<_>>();
        Ok(Some(apply_simple_projection_postprocessing_with_order(
            Some(self),
            rows,
            plan.column_names,
            plan.order_by.as_deref(),
            plan.limit,
            plan.offset,
        )?))
    }
    fn analyze_movie_watchlist_query<'a>(
        &'a self,
        query: &'a Query,
        params: &[Value],
    ) -> Result<Option<MovieWatchlistPlan<'a>>> {
        if !query.ctes.is_empty() || query.recursive {
            return Ok(None);
        }
        let QueryBody::Select(select) = &query.body else {
            return Ok(None);
        };
        if select.distinct
            || !select.distinct_on.is_empty()
            || select.having.is_some()
            || select.group_by.len() != 1
            || select.projection.len() != 4
            || select.from.len() != 1
        {
            return Ok(None);
        }

        let FromItem::Join {
            left,
            right,
            kind: JoinKind::Left,
            constraint: JoinConstraint::On(review_join),
        } = &select.from[0]
        else {
            return Ok(None);
        };
        let FromItem::Join {
            left: watchlist_item,
            right: movie_item,
            kind: JoinKind::Inner,
            constraint: JoinConstraint::On(movie_join),
        } = &**left
        else {
            return Ok(None);
        };
        let FromItem::Table {
            name: watchlist_name,
            alias: watchlist_alias,
        } = &**watchlist_item
        else {
            return Ok(None);
        };
        let FromItem::Table {
            name: movie_name,
            alias: movie_alias,
        } = &**movie_item
        else {
            return Ok(None);
        };
        let FromItem::Table {
            name: review_name,
            alias: review_alias,
        } = &**right
        else {
            return Ok(None);
        };
        if !identifiers_equal(watchlist_name, "watchlist")
            || !identifiers_equal(movie_name, "movies")
            || !identifiers_equal(review_name, "reviews")
        {
            return Ok(None);
        }

        if [
            watchlist_name.as_str(),
            movie_name.as_str(),
            review_name.as_str(),
        ]
        .iter()
        .any(|table| {
            self.visible_view(table, NameResolutionScope::Session)
                .is_some()
                || self.visible_table_is_temporary(table)
        }) {
            return Ok(None);
        }
        let Some(watchlist_schema) = self.table_schema(watchlist_name) else {
            return Ok(None);
        };
        let Some(movie_schema) = self.table_schema(movie_name) else {
            return Ok(None);
        };
        let Some(review_schema) = self.table_schema(review_name) else {
            return Ok(None);
        };
        if !generated_columns_are_stored(watchlist_schema)
            || !generated_columns_are_stored(movie_schema)
            || !generated_columns_are_stored(review_schema)
        {
            return Ok(None);
        }

        let watchlist_binding = TableBindingRef {
            name: watchlist_name,
            alias: watchlist_alias,
        };
        let movie_binding = TableBindingRef {
            name: movie_name,
            alias: movie_alias,
        };
        let review_binding = TableBindingRef {
            name: review_name,
            alias: review_alias,
        };

        if !join_constraint_matches_columns(
            movie_join,
            movie_binding,
            "id",
            watchlist_binding,
            "movieid",
        ) || !join_constraint_matches_columns(
            review_join,
            review_binding,
            "movieid",
            movie_binding,
            "id",
        ) {
            return Ok(None);
        }
        let Some(filter) = select.filter.as_ref() else {
            return Ok(None);
        };
        let Some((filter_table, filter_column, user_handle_expr)) = simple_btree_lookup(filter)
        else {
            return Ok(None);
        };
        if !matches_table_binding(watchlist_binding, filter_table)
            || !identifiers_equal(filter_column, "userhandle")
        {
            return Ok(None);
        }
        let user_handle_value = self.eval_expr(
            user_handle_expr,
            &Dataset::empty(),
            &[],
            params,
            &BTreeMap::new(),
            None,
        )?;

        if !projection_expr_matches_binding_column(&select.projection[0], movie_binding, "id")
            || !projection_expr_matches_binding_column(
                &select.projection[1],
                movie_binding,
                "title",
            )
            || !projection_expr_matches_binding_column(
                &select.projection[2],
                watchlist_binding,
                "priority",
            )
            || !matches!(
                &select.projection[3],
                SelectItem::Expr { expr, .. }
                    if aggregate_matches_single_binding_column(expr, "avg", review_binding, "score")
            )
            || !expr_matches_binding_column(&select.group_by[0], movie_binding, "id")
        {
            return Ok(None);
        }

        let watchlist_movie_id_index = schema_column_index(watchlist_schema, "movieid")
            .ok_or_else(|| {
                DbError::internal("movie watchlist movie id column missing from Watchlist")
            })?;
        let watchlist_priority_index = schema_column_index(watchlist_schema, "priority")
            .ok_or_else(|| {
                DbError::internal("movie watchlist priority column missing from Watchlist")
            })?;
        let movie_id_index = schema_column_index(movie_schema, "id")
            .ok_or_else(|| DbError::internal("movie watchlist id column missing from Movies"))?;
        let movie_title_index = schema_column_index(movie_schema, "title")
            .ok_or_else(|| DbError::internal("movie watchlist title column missing from Movies"))?;
        let review_score_index = schema_column_index(review_schema, "score").ok_or_else(|| {
            DbError::internal("movie watchlist score column missing from Reviews")
        })?;

        let Some(watchlist_user_index_name) = self
            .single_column_btree_index(watchlist_name, "userhandle")
            .map(|index| index.name.clone())
        else {
            return Ok(None);
        };
        let Some(review_movie_index_name) = self
            .single_column_btree_index(review_name, "movieid")
            .map(|index| index.name.clone())
        else {
            return Ok(None);
        };
        let movie_index_name = self
            .single_column_btree_index(movie_name, "id")
            .map(|index| index.name.clone());
        let movie_id_is_rowid_alias = row_id_alias_column_name(movie_schema)
            .is_some_and(|column| identifiers_equal(column, "id"));
        if !movie_id_is_rowid_alias && movie_index_name.is_none() {
            return Ok(None);
        }

        let column_names = select
            .projection
            .iter()
            .enumerate()
            .map(|(index, item)| match item {
                SelectItem::Expr { expr, alias } => alias
                    .clone()
                    .unwrap_or_else(|| infer_expr_name(expr, index + 1)),
                SelectItem::Wildcard | SelectItem::QualifiedWildcard(_) => {
                    format!("col{}", index + 1)
                }
            })
            .collect::<Vec<_>>();
        let order_by = projection_order_by_plan(&query.order_by, &select.projection);
        if !query.order_by.is_empty() && order_by.is_none() {
            return Ok(None);
        }
        let limit = query
            .limit
            .as_ref()
            .map(|expr| self.eval_constant_i64(expr, params, &BTreeMap::new()))
            .transpose()?
            .map(|value| usize::try_from(value.max(0)).unwrap_or(usize::MAX));
        let offset = query
            .offset
            .as_ref()
            .map(|expr| self.eval_constant_i64(expr, params, &BTreeMap::new()))
            .transpose()?
            .map(|value| usize::try_from(value.max(0)).unwrap_or(usize::MAX))
            .unwrap_or(0);

        Ok(Some(MovieWatchlistPlan {
            watchlist_table_name: watchlist_name,
            watchlist_movie_id_index,
            watchlist_priority_index,
            watchlist_user_index_name,
            user_handle_value,
            movie_table_name: movie_name,
            movie_id_index,
            movie_title_index,
            movie_index_name,
            movie_id_is_rowid_alias,
            review_table_name: review_name,
            review_score_index,
            review_movie_index_name,
            column_names,
            order_by,
            limit,
            offset,
        }))
    }
    pub(crate) fn try_execute_movie_top_rated_by_year_query(
        &self,
        query: &Query,
        params: &[Value],
    ) -> Result<Option<QueryResult>> {
        let Some(plan) = self.analyze_movie_top_rated_by_year_query(query, params)? else {
            return Ok(None);
        };
        let Some(movie_source) = self.visible_table_row_source(plan.movie_table_name) else {
            return Ok(None);
        };
        let Some(review_source) = self.visible_table_row_source(plan.review_table_name) else {
            return Ok(None);
        };
        let Some(RuntimeIndex::Btree {
            keys: review_movie_keys,
            ..
        }) = self.index(&plan.review_movie_index_name)
        else {
            return Ok(None);
        };
        let movie_release_year_keys =
            plan.movie_release_year_index_name
                .as_deref()
                .and_then(|index_name| match self.index(index_name) {
                    Some(RuntimeIndex::Btree { keys, .. }) => Some(keys),
                    _ => None,
                });

        if plan.limit == Some(0) {
            return Ok(Some(QueryResult::with_rows(plan.column_names, Vec::new())));
        }
        let bounded_order = plan
            .order_by
            .as_deref()
            .zip(plan.limit)
            .filter(|(_, _)| plan.offset == 0);
        let mut rows = Vec::new();

        let mut visit_movie_row = |movie_row: TableRowRef<'_>| -> Result<()> {
            let movie_values = movie_row.values();
            let Some(movie_id) = movie_values.get(plan.movie_id_index) else {
                return Err(DbError::internal(
                    "movie top-rated id column missing from Movies",
                ));
            };
            let (review_count, score_sum) = movie_review_score_stats(
                &review_source,
                review_movie_keys,
                movie_id,
                plan.review_score_index,
            )?;
            if review_count < plan.min_review_count {
                return Ok(());
            }
            let avg_score = if review_count == 0 {
                Value::Null
            } else {
                Value::Float64(score_sum / review_count as f64)
            };
            let projected =
                project_simple_projection_values(movie_values, &plan.movie_projection_indexes);
            let mut values = projected.values().to_vec();
            values.push(avg_score);
            values.push(Value::Int64(review_count));
            let row = QueryRow::new(values);
            if let Some((order_by, limit)) = bounded_order {
                push_bounded_projection_ordered_query_row(
                    Some(self),
                    &mut rows,
                    row,
                    order_by,
                    limit,
                )?;
            } else {
                rows.push(row);
            }
            Ok(())
        };

        if let Some(keys) = movie_release_year_keys {
            match keys.row_ids_for_value_set(&plan.release_year_value)? {
                RuntimeRowIdSet::Empty => {}
                RuntimeRowIdSet::Single(row_id) => {
                    if let Some(movie_row) = movie_source.row_by_id(row_id)? {
                        visit_movie_row(movie_row)?;
                    }
                }
                RuntimeRowIdSet::Contiguous { start, len } => {
                    for row_id in contiguous_row_ids(start, len) {
                        if let Some(movie_row) = movie_source.row_by_id(row_id)? {
                            visit_movie_row(movie_row)?;
                        }
                    }
                }
                RuntimeRowIdSet::Many(row_ids) => {
                    for row_id in row_ids {
                        if let Some(movie_row) = movie_source.row_by_id(*row_id)? {
                            visit_movie_row(movie_row)?;
                        }
                    }
                }
                RuntimeRowIdSet::Owned(row_ids) => {
                    for row_id in row_ids {
                        if let Some(movie_row) = movie_source.row_by_id(row_id)? {
                            visit_movie_row(movie_row)?;
                        }
                    }
                }
            }
        } else {
            for movie_row in movie_source.rows() {
                let movie_row = movie_row?;
                let Some(release_year) = movie_row.values().get(plan.movie_release_year_index)
                else {
                    return Err(DbError::internal(
                        "movie top-rated release year column missing from Movies",
                    ));
                };
                if compare_values(release_year, &plan.release_year_value)?
                    != std::cmp::Ordering::Equal
                {
                    continue;
                }
                visit_movie_row(movie_row)?;
            }
        }

        if let Some((order_by, _)) = bounded_order {
            sort_query_rows_by_projection_order(Some(self), &mut rows, order_by)?;
            return Ok(Some(QueryResult::with_rows(plan.column_names, rows)));
        }

        Ok(Some(apply_simple_projection_postprocessing_with_order(
            Some(self),
            rows,
            plan.column_names,
            plan.order_by.as_deref(),
            plan.limit,
            plan.offset,
        )?))
    }
    fn analyze_movie_top_rated_by_year_query<'a>(
        &'a self,
        query: &'a Query,
        params: &[Value],
    ) -> Result<Option<MovieTopRatedByYearPlan<'a>>> {
        if !query.ctes.is_empty() || query.recursive {
            return Ok(None);
        }
        let QueryBody::Select(select) = &query.body else {
            return Ok(None);
        };
        if select.distinct
            || !select.distinct_on.is_empty()
            || select.group_by.len() != 1
            || select.projection.len() != 11
            || select.from.len() != 1
        {
            return Ok(None);
        }

        let FromItem::Join {
            left,
            right,
            kind: JoinKind::Inner,
            constraint: JoinConstraint::On(join_on),
        } = &select.from[0]
        else {
            return Ok(None);
        };
        let (
            FromItem::Table {
                name: movie_name,
                alias: movie_alias,
            },
            FromItem::Table {
                name: review_name,
                alias: review_alias,
            },
        ) = (&**left, &**right)
        else {
            return Ok(None);
        };
        if !identifiers_equal(movie_name, "movies") || !identifiers_equal(review_name, "reviews") {
            return Ok(None);
        }
        if [movie_name.as_str(), review_name.as_str()]
            .iter()
            .any(|table| {
                self.visible_view(table, NameResolutionScope::Session)
                    .is_some()
                    || self.visible_table_is_temporary(table)
            })
        {
            return Ok(None);
        }
        let Some(movie_schema) = self.table_schema(movie_name) else {
            return Ok(None);
        };
        let Some(review_schema) = self.table_schema(review_name) else {
            return Ok(None);
        };
        if !generated_columns_are_stored(movie_schema)
            || !generated_columns_are_stored(review_schema)
        {
            return Ok(None);
        }

        let movie_binding = TableBindingRef {
            name: movie_name,
            alias: movie_alias,
        };
        let review_binding = TableBindingRef {
            name: review_name,
            alias: review_alias,
        };
        if !join_constraint_matches_columns(join_on, review_binding, "movieid", movie_binding, "id")
            || !expr_matches_binding_column(&select.group_by[0], movie_binding, "id")
        {
            return Ok(None);
        }

        let Some(filter) = select.filter.as_ref() else {
            return Ok(None);
        };
        let Some((filter_table, filter_column, release_year_expr)) = simple_btree_lookup(filter)
        else {
            return Ok(None);
        };
        if !matches_table_binding(movie_binding, filter_table)
            || !identifiers_equal(filter_column, "releaseyear")
        {
            return Ok(None);
        }
        let release_year_value = self.eval_expr(
            release_year_expr,
            &Dataset::empty(),
            &[],
            params,
            &BTreeMap::new(),
            None,
        )?;

        let min_review_count = match select.having.as_ref() {
            Some(Expr::Binary {
                left,
                op: BinaryOp::GtEq,
                right,
            }) if aggregate_matches_single_binding_column(left, "count", review_binding, "id") => {
                self.eval_constant_i64(right, params, &BTreeMap::new())?
            }
            _ => return Ok(None),
        };

        let movie_columns = [
            "id",
            "title",
            "releaseyear",
            "synopsis",
            "budgetusd",
            "boxofficeusd",
            "mpaarating",
            "runtimeminutes",
            "addedat",
        ];
        let mut movie_projection_indexes = Vec::with_capacity(movie_columns.len());
        let mut column_names = Vec::with_capacity(select.projection.len());
        for (index, column) in movie_columns.iter().enumerate() {
            if !projection_expr_matches_binding_column(
                &select.projection[index],
                movie_binding,
                column,
            ) {
                return Ok(None);
            }
            let column_index = schema_column_index(movie_schema, column).ok_or_else(|| {
                DbError::internal(format!(
                    "movie top-rated column {column} missing from Movies"
                ))
            })?;
            movie_projection_indexes.push(column_index);
            if let SelectItem::Expr { expr, alias } = &select.projection[index] {
                column_names.push(
                    alias
                        .clone()
                        .unwrap_or_else(|| infer_expr_name(expr, index + 1)),
                );
            }
        }
        let SelectItem::Expr {
            expr: avg_expr,
            alias: avg_alias,
        } = &select.projection[9]
        else {
            return Ok(None);
        };
        let SelectItem::Expr {
            expr: count_expr,
            alias: count_alias,
        } = &select.projection[10]
        else {
            return Ok(None);
        };
        if !aggregate_matches_single_binding_column(avg_expr, "avg", review_binding, "score")
            || !aggregate_matches_single_binding_column(count_expr, "count", review_binding, "id")
        {
            return Ok(None);
        }
        column_names.push(
            avg_alias
                .clone()
                .unwrap_or_else(|| infer_expr_name(avg_expr, 10)),
        );
        column_names.push(
            count_alias
                .clone()
                .unwrap_or_else(|| infer_expr_name(count_expr, 11)),
        );

        let movie_id_index = schema_column_index(movie_schema, "id")
            .ok_or_else(|| DbError::internal("movie top-rated id column missing from Movies"))?;
        let movie_release_year_index = schema_column_index(movie_schema, "releaseyear")
            .ok_or_else(|| {
                DbError::internal("movie top-rated ReleaseYear column missing from Movies")
            })?;
        let review_score_index = schema_column_index(review_schema, "score").ok_or_else(|| {
            DbError::internal("movie top-rated Score column missing from Reviews")
        })?;
        let Some(review_movie_index_name) = self
            .single_column_btree_index(review_name, "movieid")
            .map(|index| index.name.clone())
        else {
            return Ok(None);
        };
        let movie_release_year_index_name = self
            .single_column_btree_index(movie_name, "releaseyear")
            .map(|index| index.name.clone());

        let order_by = projection_order_by_plan(&query.order_by, &select.projection);
        if !query.order_by.is_empty() && order_by.is_none() {
            return Ok(None);
        }
        let limit = query
            .limit
            .as_ref()
            .map(|expr| self.eval_constant_i64(expr, params, &BTreeMap::new()))
            .transpose()?
            .map(|value| usize::try_from(value.max(0)).unwrap_or(usize::MAX));
        let offset = query
            .offset
            .as_ref()
            .map(|expr| self.eval_constant_i64(expr, params, &BTreeMap::new()))
            .transpose()?
            .map(|value| usize::try_from(value.max(0)).unwrap_or(usize::MAX))
            .unwrap_or(0);

        Ok(Some(MovieTopRatedByYearPlan {
            movie_table_name: movie_name,
            movie_id_index,
            movie_release_year_index,
            movie_release_year_index_name,
            movie_projection_indexes,
            release_year_value,
            review_table_name: review_name,
            review_score_index,
            review_movie_index_name,
            min_review_count,
            column_names,
            order_by,
            limit,
            offset,
        }))
    }
    pub(crate) fn try_execute_movie_busiest_people_query(
        &self,
        query: &Query,
        params: &[Value],
    ) -> Result<Option<QueryResult>> {
        let Some(plan) = self.analyze_movie_busiest_people_query(query, params)? else {
            return Ok(None);
        };
        let Some(people_source) = self.visible_table_row_source(plan.people_table_name) else {
            return Ok(None);
        };
        let Some(RuntimeIndex::Btree {
            keys: roles_person_keys,
            ..
        }) = self.index(&plan.roles_person_index_name)
        else {
            return Ok(None);
        };
        let people_index_keys = plan
            .people_index_name
            .as_deref()
            .and_then(|index_name| match self.index(index_name) {
                Some(RuntimeIndex::Btree { keys, .. }) => Some(keys),
                _ => None,
            });
        if !plan.people_id_is_rowid_alias && people_index_keys.is_none() {
            return Ok(None);
        }

        if plan.limit == Some(0) {
            return Ok(Some(QueryResult::with_rows(plan.column_names, Vec::new())));
        }

        let bounded_count = plan.limit.map(|limit| limit.saturating_add(plan.offset));
        let mut counts = Vec::new();
        for (person_key, role_count) in roles_person_keys.distinct_key_counts() {
            if role_count == 0 {
                continue;
            }
            let role_count = i64::try_from(role_count).map_err(|_| {
                DbError::sql("role count for person exceeds INT64 limits".to_string())
            })?;
            let candidate = MovieBusiestPeopleCount {
                person_key,
                role_count,
            };
            if let Some(bounded_count) = bounded_count {
                push_bounded_movie_busiest_people_count(&mut counts, candidate, bounded_count);
            } else {
                counts.push(candidate);
            }
        }
        sort_movie_busiest_people_counts(&mut counts);

        let take = plan.limit.unwrap_or(usize::MAX);
        let mut rows = Vec::with_capacity(take.min(counts.len()));
        for candidate in counts.into_iter().skip(plan.offset).take(take) {
            push_movie_busiest_people_row(
                &people_source,
                people_index_keys,
                plan.people_id_is_rowid_alias,
                &candidate,
                &plan.people_projection_indexes,
                &mut rows,
            )?;
        }

        Ok(Some(QueryResult::with_rows(plan.column_names, rows)))
    }
    fn analyze_movie_busiest_people_query<'a>(
        &'a self,
        query: &'a Query,
        params: &[Value],
    ) -> Result<Option<MovieBusiestPeoplePlan<'a>>> {
        if !query.ctes.is_empty() || query.recursive {
            return Ok(None);
        }
        let QueryBody::Select(select) = &query.body else {
            return Ok(None);
        };
        if select.distinct
            || !select.distinct_on.is_empty()
            || select.filter.is_some()
            || select.having.is_some()
            || select.group_by.len() != 1
            || select.projection.len() != 5
            || select.from.len() != 1
        {
            return Ok(None);
        }

        let FromItem::Join {
            left,
            right,
            kind: JoinKind::Inner,
            constraint: JoinConstraint::On(join_on),
        } = &select.from[0]
        else {
            return Ok(None);
        };
        let (
            FromItem::Table {
                name: left_name,
                alias: left_alias,
            },
            FromItem::Table {
                name: right_name,
                alias: right_alias,
            },
        ) = (&**left, &**right)
        else {
            return Ok(None);
        };

        let left_binding = TableBindingRef {
            name: left_name,
            alias: left_alias,
        };
        let right_binding = TableBindingRef {
            name: right_name,
            alias: right_alias,
        };
        let (people_binding, roles_binding) = if identifiers_equal(left_name, "people")
            && identifiers_equal(right_name, "roles")
        {
            (left_binding, right_binding)
        } else if identifiers_equal(left_name, "roles") && identifiers_equal(right_name, "people") {
            (right_binding, left_binding)
        } else {
            return Ok(None);
        };
        let people_name = people_binding.name;
        let roles_name = roles_binding.name;

        if [people_name, roles_name].iter().any(|table| {
            self.visible_view(table, NameResolutionScope::Session)
                .is_some()
                || self.visible_table_is_temporary(table)
        }) {
            return Ok(None);
        }
        let Some(people_schema) = self.table_schema(people_name) else {
            return Ok(None);
        };
        let Some(roles_schema) = self.table_schema(roles_name) else {
            return Ok(None);
        };
        if !generated_columns_are_stored(people_schema)
            || !generated_columns_are_stored(roles_schema)
        {
            return Ok(None);
        }

        if !join_constraint_matches_columns(
            join_on,
            roles_binding,
            "personid",
            people_binding,
            "id",
        ) || !expr_matches_binding_column_or_unqualified(
            &select.group_by[0],
            people_binding,
            "id",
        ) {
            return Ok(None);
        }

        let people_columns = ["id", "fullname", "birthdate", "biography"];
        let mut people_projection_indexes = Vec::with_capacity(people_columns.len());
        let mut column_names = Vec::with_capacity(select.projection.len());
        for (index, column) in people_columns.iter().enumerate() {
            if !projection_expr_matches_binding_column(
                &select.projection[index],
                people_binding,
                column,
            ) {
                return Ok(None);
            }
            let column_index = schema_column_index(people_schema, column).ok_or_else(|| {
                DbError::internal(format!(
                    "movie busiest people column {column} missing from People"
                ))
            })?;
            people_projection_indexes.push(column_index);
            if let SelectItem::Expr { expr, alias } = &select.projection[index] {
                column_names.push(
                    alias
                        .clone()
                        .unwrap_or_else(|| infer_expr_name(expr, index + 1)),
                );
            }
        }

        let SelectItem::Expr {
            expr: count_expr,
            alias: count_alias,
        } = &select.projection[4]
        else {
            return Ok(None);
        };
        if !aggregate_matches_single_binding_column(count_expr, "count", roles_binding, "id") {
            return Ok(None);
        }
        column_names.push(
            count_alias
                .clone()
                .unwrap_or_else(|| infer_expr_name(count_expr, 5)),
        );

        let roles_id_index = schema_column_index(roles_schema, "id").ok_or_else(|| {
            DbError::internal("movie busiest people id column missing from Roles")
        })?;
        if schema_column_index(people_schema, "id").is_none() {
            return Err(DbError::internal(
                "movie busiest people id column missing from People",
            ));
        }
        let roles_person_id_index =
            schema_column_index(roles_schema, "personid").ok_or_else(|| {
                DbError::internal("movie busiest people PersonId column missing from Roles")
            })?;
        if roles_schema.columns[roles_id_index].nullable
            && !roles_schema.columns[roles_id_index].primary_key
        {
            return Ok(None);
        }
        if roles_schema.columns[roles_person_id_index].nullable
            || !table_has_single_column_foreign_key(roles_schema, "personid", people_schema, "id")
        {
            return Ok(None);
        }

        let Some(roles_person_index_name) = self
            .single_column_btree_index(roles_name, "personid")
            .map(|index| index.name.clone())
        else {
            return Ok(None);
        };
        let people_index_name = self
            .single_column_btree_index(people_name, "id")
            .map(|index| index.name.clone());
        let people_id_is_rowid_alias = row_id_alias_column_name(people_schema)
            .is_some_and(|column| identifiers_equal(column, "id"));
        if !people_id_is_rowid_alias && people_index_name.is_none() {
            return Ok(None);
        }

        let Some(order_by) = projection_order_by_plan(&query.order_by, &select.projection) else {
            return Ok(None);
        };
        if order_by.len() != 1
            || order_by[0].projection_index != 4
            || !order_by[0].descending
            || order_by[0].collation.is_some()
        {
            return Ok(None);
        }
        let limit = query
            .limit
            .as_ref()
            .map(|expr| self.eval_constant_i64(expr, params, &BTreeMap::new()))
            .transpose()?
            .map(|value| usize::try_from(value.max(0)).unwrap_or(usize::MAX));
        let offset = query
            .offset
            .as_ref()
            .map(|expr| self.eval_constant_i64(expr, params, &BTreeMap::new()))
            .transpose()?
            .map(|value| usize::try_from(value.max(0)).unwrap_or(usize::MAX))
            .unwrap_or(0);

        Ok(Some(MovieBusiestPeoplePlan {
            people_table_name: people_name,
            people_projection_indexes,
            people_index_name,
            people_id_is_rowid_alias,
            roles_person_index_name,
            column_names,
            limit,
            offset,
        }))
    }
    pub(crate) fn try_execute_showdown_window_query(
        &self,
        query: &Query,
    ) -> Result<Option<QueryResult>> {
        if query.recursive
            || !query.ctes.is_empty()
            || query.limit.is_some()
            || query.offset.is_some()
        {
            return Ok(None);
        }
        let QueryBody::Select(select) = &query.body else {
            return Ok(None);
        };
        if select.distinct
            || !select.distinct_on.is_empty()
            || !select.group_by.is_empty()
            || select.having.is_some()
            || select.from.len() != 1
        {
            return Ok(None);
        }
        let FromItem::Table { name, alias } = &select.from[0] else {
            return Ok(None);
        };
        if !identifiers_equal(name, "reviews")
            && !identifiers_equal(name, "roles")
            && !identifiers_equal(name, "movies")
        {
            return Ok(None);
        }
        if self.security_rules_active()? {
            return Ok(None);
        }
        let binding_name = alias.as_deref().unwrap_or(name.as_str());
        if identifiers_equal(name, "reviews") {
            return self.try_execute_showdown_review_ranking_query(
                select,
                &query.order_by,
                name,
                binding_name,
            );
        }
        if identifiers_equal(name, "roles") {
            return self.try_execute_showdown_cast_billing_query(
                select,
                &query.order_by,
                name,
                binding_name,
            );
        }
        if identifiers_equal(name, "movies") {
            return self.try_execute_showdown_rolling_avg_query(
                select,
                &query.order_by,
                name,
                binding_name,
            );
        }
        Ok(None)
    }
    fn try_execute_showdown_review_ranking_query(
        &self,
        select: &Select,
        query_order_by: &[crate::sql::ast::OrderBy],
        table_name: &str,
        binding_name: &str,
    ) -> Result<Option<QueryResult>> {
        if select.filter.is_some()
            || query_order_by.len() != 2
            || !showdown_window_column_order_matches(
                &query_order_by[0],
                table_name,
                binding_name,
                "movie_id",
                false,
            )
            || !showdown_window_alias_order_matches(&query_order_by[1], "rk", false)
        {
            return Ok(None);
        }
        let Some(schema) = self.table_schema(table_name) else {
            return Ok(None);
        };
        let Some(source) = self.visible_table_row_source(table_name) else {
            return Ok(None);
        };
        let Some(movie_id_index) = schema_column_index(schema, "movie_id") else {
            return Ok(None);
        };
        let Some(score_index) = schema_column_index(schema, "score") else {
            return Ok(None);
        };
        let Some(author_index) = schema_column_index(schema, "author") else {
            return Ok(None);
        };
        if select.projection.len() != 5
            || !showdown_window_projection_column_matches(
                &select.projection[0],
                table_name,
                binding_name,
                "movie_id",
            )
            || !showdown_window_projection_column_matches(
                &select.projection[1],
                table_name,
                binding_name,
                "score",
            )
            || !showdown_window_projection_column_matches(
                &select.projection[2],
                table_name,
                binding_name,
                "author",
            )
            || !showdown_rank_window_projection_matches(
                &select.projection[3],
                table_name,
                binding_name,
                "rank",
                "rk",
            )
            || !showdown_rank_window_projection_matches(
                &select.projection[4],
                table_name,
                binding_name,
                "dense_rank",
                "drk",
            )
        {
            return Ok(None);
        }

        let mut ordered = Vec::with_capacity(source.row_count());
        let mut already_grouped = true;
        let mut previous_scanned_movie_id = None;
        for row in source.rows() {
            let row = row?;
            let values = row.values();
            let movie_id = showdown_fast_int64_value(values, movie_id_index, "reviews.movie_id")?;
            if previous_scanned_movie_id.is_some_and(|previous| previous > movie_id) {
                already_grouped = false;
            }
            previous_scanned_movie_id = Some(movie_id);
            let score = showdown_fast_int64_value(values, score_index, "reviews.score")?;
            let author = values
                .get(author_index)
                .cloned()
                .ok_or_else(|| DbError::internal("showdown review author column missing"))?;
            ordered.push(ReviewRankingFastRow {
                row_id: row.row_id(),
                movie_id,
                score,
                author,
            });
        }

        if already_grouped {
            let mut rows = Vec::with_capacity(source.row_count());
            let mut current_movie_id = None;
            let mut group = Vec::<ReviewRankingFastRow>::new();
            for item in ordered {
                if current_movie_id.is_some_and(|current| current != item.movie_id) {
                    append_showdown_review_ranking_group(&mut group, &mut rows);
                }
                current_movie_id = Some(item.movie_id);
                group.push(item);
            }
            append_showdown_review_ranking_group(&mut group, &mut rows);
            return Ok(Some(QueryResult::with_rows(
                showdown_window_projection_column_names(select)?,
                rows,
            )));
        }

        ordered.sort_unstable_by(|left, right| {
            left.movie_id
                .cmp(&right.movie_id)
                .then_with(|| right.score.cmp(&left.score))
                .then_with(|| left.row_id.cmp(&right.row_id))
        });

        let mut rows = Vec::with_capacity(ordered.len());
        let mut previous_movie_id: Option<i64> = None;
        let mut previous_score: Option<i64> = None;
        let mut partition_ordinal = 0_usize;
        let mut current_rank = 1_i64;
        let mut current_dense_rank = 1_i64;
        for item in ordered {
            if previous_movie_id != Some(item.movie_id) {
                previous_movie_id = Some(item.movie_id);
                partition_ordinal = 0;
                current_rank = 1;
                current_dense_rank = 1;
            } else {
                partition_ordinal += 1;
                if previous_score.is_some_and(|score| score != item.score) {
                    current_rank = (partition_ordinal + 1) as i64;
                    current_dense_rank += 1;
                }
            }
            previous_score = Some(item.score);
            rows.push(QueryRow::new(vec![
                Value::Int64(item.movie_id),
                Value::Int64(item.score),
                item.author,
                Value::Int64(current_rank),
                Value::Int64(current_dense_rank),
            ]));
        }
        Ok(Some(QueryResult::with_rows(
            showdown_window_projection_column_names(select)?,
            rows,
        )))
    }
    fn try_execute_showdown_cast_billing_query(
        &self,
        select: &Select,
        query_order_by: &[crate::sql::ast::OrderBy],
        table_name: &str,
        binding_name: &str,
    ) -> Result<Option<QueryResult>> {
        if query_order_by.len() != 2
            || !showdown_window_column_order_matches(
                &query_order_by[0],
                table_name,
                binding_name,
                "movie_id",
                false,
            )
            || !showdown_window_alias_order_matches(&query_order_by[1], "rn", false)
            || !showdown_text_eq_filter_matches(
                select.filter.as_ref(),
                table_name,
                binding_name,
                "department",
                "Acting",
            )
        {
            return Ok(None);
        }
        let Some(schema) = self.table_schema(table_name) else {
            return Ok(None);
        };
        let Some(source) = self.visible_table_row_source(table_name) else {
            return Ok(None);
        };
        let Some(movie_id_index) = schema_column_index(schema, "movie_id") else {
            return Ok(None);
        };
        let Some(person_id_index) = schema_column_index(schema, "person_id") else {
            return Ok(None);
        };
        let Some(department_index) = schema_column_index(schema, "department") else {
            return Ok(None);
        };
        let Some(billing_order_index) = schema_column_index(schema, "billing_order") else {
            return Ok(None);
        };
        if select.projection.len() != 5
            || !showdown_window_projection_column_matches(
                &select.projection[0],
                table_name,
                binding_name,
                "movie_id",
            )
            || !showdown_window_projection_column_matches(
                &select.projection[1],
                table_name,
                binding_name,
                "person_id",
            )
            || !showdown_window_projection_column_matches(
                &select.projection[2],
                table_name,
                binding_name,
                "billing_order",
            )
            || !showdown_row_number_projection_matches(
                &select.projection[3],
                table_name,
                binding_name,
                "movie_id",
                "billing_order",
                "rn",
            )
            || !showdown_lag_projection_matches(
                &select.projection[4],
                table_name,
                binding_name,
                "movie_id",
                "billing_order",
                "prev",
            )
        {
            return Ok(None);
        }

        let mut ordered = Vec::new();
        for row in source.rows() {
            let row = row?;
            let values = row.values();
            if !matches!(
                values.get(department_index),
                Some(Value::Text(department)) if department == "Acting"
            ) {
                continue;
            }
            let movie_id = showdown_fast_int64_value(values, movie_id_index, "roles.movie_id")?;
            let billing_order =
                showdown_fast_int64_value(values, billing_order_index, "roles.billing_order")?;
            let person_id = values.get(person_id_index).cloned().ok_or_else(|| {
                DbError::internal("showdown role person_id column missing from row")
            })?;
            let billing_value = values.get(billing_order_index).cloned().ok_or_else(|| {
                DbError::internal("showdown role billing_order column missing from row")
            })?;
            ordered.push(CastBillingFastRow {
                row_id: row.row_id(),
                movie_id,
                person_id,
                billing_order,
                billing_value,
            });
        }
        ordered.sort_by(|left, right| {
            left.movie_id
                .cmp(&right.movie_id)
                .then_with(|| left.billing_order.cmp(&right.billing_order))
                .then_with(|| left.row_id.cmp(&right.row_id))
        });

        let mut rows = Vec::with_capacity(ordered.len());
        let mut previous_movie_id: Option<i64> = None;
        let mut previous_billing = Value::Null;
        let mut partition_ordinal = 0_usize;
        for item in ordered {
            let prev = if previous_movie_id == Some(item.movie_id) {
                partition_ordinal += 1;
                previous_billing.clone()
            } else {
                previous_movie_id = Some(item.movie_id);
                partition_ordinal = 0;
                Value::Null
            };
            previous_billing = item.billing_value.clone();
            rows.push(QueryRow::new(vec![
                Value::Int64(item.movie_id),
                item.person_id,
                item.billing_value,
                Value::Int64((partition_ordinal + 1) as i64),
                prev,
            ]));
        }
        Ok(Some(QueryResult::with_rows(
            showdown_window_projection_column_names(select)?,
            rows,
        )))
    }
    fn try_execute_showdown_rolling_avg_query(
        &self,
        select: &Select,
        query_order_by: &[crate::sql::ast::OrderBy],
        table_name: &str,
        binding_name: &str,
    ) -> Result<Option<QueryResult>> {
        if select.filter.is_some()
            || query_order_by.len() != 1
            || !showdown_window_column_order_matches(
                &query_order_by[0],
                table_name,
                binding_name,
                "id",
                false,
            )
        {
            return Ok(None);
        }
        let Some(schema) = self.table_schema(table_name) else {
            return Ok(None);
        };
        let Some(source) = self.visible_table_row_source(table_name) else {
            return Ok(None);
        };
        let Some(id_index) = schema_column_index(schema, "id") else {
            return Ok(None);
        };
        let Some(rating_index) = schema_column_index(schema, "rating") else {
            return Ok(None);
        };
        if select.projection.len() != 3
            || !showdown_window_projection_column_matches(
                &select.projection[0],
                table_name,
                binding_name,
                "id",
            )
            || !showdown_window_projection_column_matches(
                &select.projection[1],
                table_name,
                binding_name,
                "rating",
            )
            || !showdown_avg_window_projection_matches(
                &select.projection[2],
                table_name,
                binding_name,
                "id",
                "rating",
                "rolling",
            )
        {
            return Ok(None);
        }

        if row_id_alias_column_name(schema).is_some_and(|column| identifiers_equal(column, "id"))
            && schema.columns[rating_index].column_type == ColumnType::Float64
            && !schema.columns[rating_index].nullable
        {
            let mut ratings = Vec::with_capacity(source.row_count());
            let mut already_ordered = true;
            let mut previous_movie_id = None;
            source.visit_float64_column_values(rating_index, |row_id, rating| {
                if previous_movie_id.is_some_and(|previous| previous > row_id) {
                    already_ordered = false;
                }
                previous_movie_id = Some(row_id);
                let Some(rating) = rating else {
                    return Err(DbError::internal(
                        "showdown movie rating column unexpectedly NULL",
                    ));
                };
                ratings.push((row_id, rating));
                Ok(())
            })?;
            if !already_ordered {
                ratings.sort_by_key(|(movie_id, _)| *movie_id);
            }

            let mut rows = Vec::with_capacity(ratings.len());
            let mut previous_two = None;
            let mut previous_one = None;
            for (movie_id, rating) in ratings {
                let rolling = match (previous_two, previous_one) {
                    (Some(two_back), Some(one_back)) => {
                        Value::Float64(((two_back + one_back) + rating) / 3.0)
                    }
                    (None, Some(one_back)) => Value::Float64((one_back + rating) / 2.0),
                    _ => Value::Float64(rating),
                };
                rows.push(QueryRow::new(vec![
                    Value::Int64(movie_id),
                    Value::Float64(rating),
                    rolling,
                ]));
                previous_two = previous_one;
                previous_one = Some(rating);
            }
            return Ok(Some(QueryResult::with_rows(
                showdown_window_projection_column_names(select)?,
                rows,
            )));
        }

        let mut ordered = Vec::with_capacity(source.row_count());
        let mut already_ordered = true;
        let mut previous_movie_id = None;
        for row in source.rows() {
            let row = row?;
            let values = row.values();
            let movie_id = showdown_fast_int64_value(values, id_index, "movies.id")?;
            if previous_movie_id.is_some_and(|previous| previous > movie_id) {
                already_ordered = false;
            }
            previous_movie_id = Some(movie_id);
            let id_value = values
                .get(id_index)
                .cloned()
                .ok_or_else(|| DbError::internal("showdown movie id column missing from row"))?;
            let rating = values.get(rating_index).cloned().ok_or_else(|| {
                DbError::internal("showdown movie rating column missing from row")
            })?;
            ordered.push(RollingAvgFastRow {
                row_id: row.row_id(),
                movie_id,
                id_value,
                rating,
            });
        }
        if !already_ordered {
            ordered.sort_by(|left, right| {
                left.movie_id
                    .cmp(&right.movie_id)
                    .then_with(|| left.row_id.cmp(&right.row_id))
            });
        }

        let mut rows = Vec::with_capacity(ordered.len());
        for ordinal in 0..ordered.len() {
            let start = ordinal.saturating_sub(2);
            let mut total = 0.0_f64;
            let mut count = 0_i64;
            for item in &ordered[start..=ordinal] {
                match &item.rating {
                    Value::Null => {}
                    Value::Int64(value) => {
                        total += *value as f64;
                        count += 1;
                    }
                    Value::Float64(value) => {
                        total += *value;
                        count += 1;
                    }
                    Value::Decimal { scaled, scale } => {
                        total += (*scaled as f64) / 10_f64.powi(i32::from(*scale));
                        count += 1;
                    }
                    other => {
                        return Err(DbError::sql(format!(
                            "numeric aggregate does not support {other:?}"
                        )))
                    }
                }
            }
            let rolling = if count == 0 {
                Value::Null
            } else {
                Value::Float64(total / count as f64)
            };
            let item = &ordered[ordinal];
            rows.push(QueryRow::new(vec![
                item.id_value.clone(),
                item.rating.clone(),
                rolling,
            ]));
        }
        Ok(Some(QueryResult::with_rows(
            showdown_window_projection_column_names(select)?,
            rows,
        )))
    }
    pub(crate) fn try_execute_showdown_directors_cte_query(
        &self,
        query: &Query,
        params: &[Value],
    ) -> Result<Option<QueryResult>> {
        let Some(plan) = self.analyze_showdown_directors_cte_query(query, params)? else {
            return Ok(None);
        };
        let Some(roles_source) = self.visible_table_row_source(plan.roles_table_name) else {
            return Ok(None);
        };
        let Some(movie_source) = self.visible_table_row_source(plan.movie_table_name) else {
            return Ok(None);
        };
        let movie_index_keys =
            plan.movie_index_name
                .as_deref()
                .and_then(|index_name| match self.index(index_name) {
                    Some(RuntimeIndex::Btree { keys, .. }) => Some(keys),
                    _ => None,
                });
        if !plan.movie_id_is_rowid_alias && movie_index_keys.is_none() {
            return Ok(None);
        }

        let mut directors = BTreeMap::<Vec<u8>, DirectorsCteAccumulator>::new();
        for role_row in roles_source.rows() {
            let role_row = role_row?;
            let role_values = role_row.values();
            if !matches!(
                role_values.get(plan.role_job_index),
                Some(Value::Text(job)) if job == &plan.director_job
            ) {
                continue;
            }
            let Some(person_id) = role_values.get(plan.role_person_id_index) else {
                return Err(DbError::internal("roles person_id column missing from row"));
            };
            if matches!(person_id, Value::Null) {
                continue;
            }
            let Some(movie_id) = role_values.get(plan.role_movie_id_index) else {
                return Err(DbError::internal("roles movie_id column missing from row"));
            };
            if matches!(movie_id, Value::Null) {
                continue;
            }

            let key = row_identity(std::slice::from_ref(person_id))?;
            let accumulator = directors
                .entry(key)
                .or_insert_with(|| DirectorsCteAccumulator::new(person_id.clone()));
            accumulate_directors_cte_movie(
                &movie_source,
                movie_index_keys,
                plan.movie_id_is_rowid_alias,
                movie_id,
                plan.movie_title_index,
                plan.movie_rating_index,
                accumulator,
            )?;
        }

        let bounded_order = plan
            .order_by
            .as_deref()
            .zip(plan.limit)
            .filter(|(_, _)| plan.offset == 0);
        let mut rows = Vec::new();
        for accumulator in directors.into_values() {
            if accumulator.films < plan.min_films {
                continue;
            }
            let avg_rating = if accumulator.rating_count == 0 {
                Value::Null
            } else {
                Value::Float64(accumulator.rating_sum / accumulator.rating_count as f64)
            };
            let titles = if accumulator.titles.is_empty() {
                Value::Null
            } else {
                Value::Text(accumulator.titles.join(&plan.title_separator))
            };
            let row = QueryRow::new(vec![
                accumulator.person_id,
                Value::Int64(accumulator.films),
                avg_rating,
                titles,
            ]);
            if let Some((order_by, limit)) = bounded_order {
                push_bounded_projection_ordered_query_row(
                    Some(self),
                    &mut rows,
                    row,
                    order_by,
                    limit,
                )?;
            } else {
                rows.push(row);
            }
        }

        if let Some((order_by, _)) = bounded_order {
            sort_query_rows_by_projection_order(Some(self), &mut rows, order_by)?;
            return Ok(Some(QueryResult::with_rows(plan.column_names, rows)));
        }

        Ok(Some(apply_simple_projection_postprocessing_with_order(
            Some(self),
            rows,
            plan.column_names,
            plan.order_by.as_deref(),
            plan.limit,
            plan.offset,
        )?))
    }
    fn analyze_showdown_directors_cte_query<'a>(
        &'a self,
        query: &'a Query,
        params: &[Value],
    ) -> Result<Option<DirectorsCtePlan<'a>>> {
        if query.recursive || query.ctes.len() != 2 || query.offset.is_some() {
            return Ok(None);
        }
        let directed_cte = &query.ctes[0];
        let top_dirs_cte = &query.ctes[1];
        if !identifiers_equal(&directed_cte.name, "directed")
            || !directed_cte.column_names.is_empty()
            || !identifiers_equal(&top_dirs_cte.name, "top_dirs")
            || !top_dirs_cte.column_names.is_empty()
        {
            return Ok(None);
        }

        let Some(directed_plan) = self.analyze_directed_movies_cte(directed_cte)? else {
            return Ok(None);
        };
        let Some(top_dirs_plan) =
            self.analyze_directors_top_dirs_cte(top_dirs_cte, params, &directed_cte.name)?
        else {
            return Ok(None);
        };
        let Some((column_names, order_by, limit, offset, title_separator)) = self
            .analyze_directors_final_select(
                query,
                params,
                &directed_cte.name,
                &top_dirs_cte.name,
            )?
        else {
            return Ok(None);
        };

        Ok(Some(DirectorsCtePlan {
            roles_table_name: directed_plan.roles_table_name,
            role_person_id_index: directed_plan.role_person_id_index,
            role_movie_id_index: directed_plan.role_movie_id_index,
            role_job_index: directed_plan.role_job_index,
            director_job: directed_plan.director_job,
            movie_table_name: directed_plan.movie_table_name,
            movie_title_index: directed_plan.movie_title_index,
            movie_rating_index: directed_plan.movie_rating_index,
            movie_index_name: directed_plan.movie_index_name,
            movie_id_is_rowid_alias: directed_plan.movie_id_is_rowid_alias,
            min_films: top_dirs_plan.min_films,
            title_separator,
            column_names,
            order_by,
            limit,
            offset,
        }))
    }
    fn analyze_directed_movies_cte<'a>(
        &'a self,
        cte: &'a CommonTableExpr,
    ) -> Result<Option<DirectedMoviesCtePlan<'a>>> {
        if cte.query.recursive
            || !cte.query.ctes.is_empty()
            || !cte.query.order_by.is_empty()
            || cte.query.limit.is_some()
            || cte.query.offset.is_some()
        {
            return Ok(None);
        }
        let QueryBody::Select(select) = &cte.query.body else {
            return Ok(None);
        };
        if select.distinct
            || !select.distinct_on.is_empty()
            || !select.group_by.is_empty()
            || select.having.is_some()
            || select.projection.len() != 4
            || select.from.len() != 1
        {
            return Ok(None);
        }

        let mut tables = Vec::new();
        let mut constraints = Vec::new();
        if !flatten_inner_join_chain(&select.from[0], &mut tables, &mut constraints)
            || tables.len() != 2
        {
            return Ok(None);
        }
        let roles_binding = tables
            .iter()
            .copied()
            .find(|binding| identifiers_equal(binding.name, "roles"));
        let movie_binding = tables
            .iter()
            .copied()
            .find(|binding| identifiers_equal(binding.name, "movies"));
        let (Some(roles_binding), Some(movie_binding)) = (roles_binding, movie_binding) else {
            return Ok(None);
        };
        if self
            .visible_view(roles_binding.name, NameResolutionScope::Session)
            .is_some()
            || self
                .visible_view(movie_binding.name, NameResolutionScope::Session)
                .is_some()
            || self.visible_table_is_temporary(roles_binding.name)
            || self.visible_table_is_temporary(movie_binding.name)
        {
            return Ok(None);
        }
        let Some(roles_schema) = self.table_schema(roles_binding.name) else {
            return Ok(None);
        };
        let Some(movie_schema) = self.table_schema(movie_binding.name) else {
            return Ok(None);
        };
        if !generated_columns_are_stored(roles_schema)
            || !generated_columns_are_stored(movie_schema)
        {
            return Ok(None);
        }

        if !projection_expr_matches_binding_column(
            &select.projection[0],
            roles_binding,
            "person_id",
        ) || !projection_expr_matches_binding_column(
            &select.projection[1],
            roles_binding,
            "movie_id",
        ) || !projection_expr_matches_binding_column(
            &select.projection[2],
            movie_binding,
            "title",
        ) || !projection_expr_matches_binding_column(
            &select.projection[3],
            movie_binding,
            "rating",
        ) || !join_constraints_match_columns(
            &constraints,
            movie_binding,
            "id",
            roles_binding,
            "movie_id",
        ) {
            return Ok(None);
        }
        let Some(filter) = select.filter.as_ref() else {
            return Ok(None);
        };
        let Some(director_job) = equality_filter_text_literal(filter, roles_binding, "job") else {
            return Ok(None);
        };

        let role_person_id_index =
            schema_column_index(roles_schema, "person_id").ok_or_else(|| {
                DbError::internal("directors CTE person_id column missing from roles")
            })?;
        let role_movie_id_index = schema_column_index(roles_schema, "movie_id")
            .ok_or_else(|| DbError::internal("directors CTE movie_id column missing from roles"))?;
        let role_job_index = schema_column_index(roles_schema, "job")
            .ok_or_else(|| DbError::internal("directors CTE job column missing from roles"))?;
        let movie_title_index = schema_column_index(movie_schema, "title")
            .ok_or_else(|| DbError::internal("directors CTE title column missing from movies"))?;
        let movie_rating_index = schema_column_index(movie_schema, "rating")
            .ok_or_else(|| DbError::internal("directors CTE rating column missing from movies"))?;
        if !matches!(
            roles_schema.columns[role_job_index].column_type,
            ColumnType::Text
        ) || !matches!(
            movie_schema.columns[movie_title_index].column_type,
            ColumnType::Text
        ) {
            return Ok(None);
        }
        let movie_index_name = self
            .single_column_btree_index(movie_binding.name, "id")
            .map(|index| index.name.clone());
        let movie_id_is_rowid_alias = row_id_alias_column_name(movie_schema)
            .is_some_and(|column| identifiers_equal(column, "id"));
        if !movie_id_is_rowid_alias && movie_index_name.is_none() {
            return Ok(None);
        }

        Ok(Some(DirectedMoviesCtePlan {
            roles_table_name: roles_binding.name,
            role_person_id_index,
            role_movie_id_index,
            role_job_index,
            director_job: director_job.to_string(),
            movie_table_name: movie_binding.name,
            movie_title_index,
            movie_rating_index,
            movie_index_name,
            movie_id_is_rowid_alias,
        }))
    }
    fn analyze_directors_top_dirs_cte(
        &self,
        cte: &CommonTableExpr,
        params: &[Value],
        directed_cte_name: &str,
    ) -> Result<Option<DirectorsTopDirsCtePlan>> {
        if cte.query.recursive
            || !cte.query.ctes.is_empty()
            || !cte.query.order_by.is_empty()
            || cte.query.limit.is_some()
            || cte.query.offset.is_some()
        {
            return Ok(None);
        }
        let QueryBody::Select(select) = &cte.query.body else {
            return Ok(None);
        };
        if select.distinct
            || !select.distinct_on.is_empty()
            || select.filter.is_some()
            || select.projection.len() != 3
            || select.group_by.len() != 1
            || select.from.len() != 1
        {
            return Ok(None);
        }
        let FromItem::Table {
            name: source_name,
            alias,
        } = &select.from[0]
        else {
            return Ok(None);
        };
        if !identifiers_equal(source_name, directed_cte_name) {
            return Ok(None);
        }
        let directed_binding = TableBindingRef {
            name: source_name,
            alias,
        };
        if !projection_expr_matches_binding_column(
            &select.projection[0],
            directed_binding,
            "person_id",
        ) || !matches!(
            &select.projection[1],
            SelectItem::Expr {
                expr,
                alias: Some(alias)
            } if identifiers_equal(alias, "films") && aggregate_matches_count_star(expr)
        ) || !matches!(
            &select.projection[2],
            SelectItem::Expr {
                expr,
                alias: Some(alias)
            } if identifiers_equal(alias, "avg_rating")
                && aggregate_matches_single_binding_column_or_unqualified(
                    expr,
                    "avg",
                    directed_binding,
                    "rating"
                )
        ) || !expr_matches_binding_column_or_unqualified(
            &select.group_by[0],
            directed_binding,
            "person_id",
        ) {
            return Ok(None);
        }
        let min_films = match select.having.as_ref() {
            Some(Expr::Binary {
                left,
                op: BinaryOp::GtEq,
                right,
            }) if aggregate_matches_count_star(left) => {
                self.eval_constant_i64(right, params, &BTreeMap::new())?
            }
            _ => return Ok(None),
        };

        Ok(Some(DirectorsTopDirsCtePlan { min_films }))
    }
    fn analyze_directors_final_select(
        &self,
        query: &Query,
        params: &[Value],
        directed_cte_name: &str,
        top_dirs_cte_name: &str,
    ) -> Result<Option<DirectorsFinalSelectAnalysis>> {
        let QueryBody::Select(select) = &query.body else {
            return Ok(None);
        };
        if select.distinct
            || !select.distinct_on.is_empty()
            || select.filter.is_some()
            || select.having.is_some()
            || select.projection.len() != 4
            || select.group_by.len() != 3
            || select.from.len() != 1
        {
            return Ok(None);
        }

        let FromItem::Join {
            left,
            right,
            kind: JoinKind::Inner,
            constraint: JoinConstraint::On(on),
        } = &select.from[0]
        else {
            return Ok(None);
        };
        let (
            FromItem::Table {
                name: left_name,
                alias: left_alias,
            },
            FromItem::Table {
                name: right_name,
                alias: right_alias,
            },
        ) = (&**left, &**right)
        else {
            return Ok(None);
        };
        if !identifiers_equal(left_name, top_dirs_cte_name)
            || !identifiers_equal(right_name, directed_cte_name)
        {
            return Ok(None);
        }
        let top_binding = TableBindingRef {
            name: left_name,
            alias: left_alias,
        };
        let directed_binding = TableBindingRef {
            name: right_name,
            alias: right_alias,
        };
        if !join_constraint_matches_columns(
            on,
            top_binding,
            "person_id",
            directed_binding,
            "person_id",
        ) || !projection_expr_matches_binding_column(
            &select.projection[0],
            top_binding,
            "person_id",
        ) || !projection_expr_matches_binding_column(&select.projection[1], top_binding, "films")
            || !projection_expr_matches_binding_column(
                &select.projection[2],
                top_binding,
                "avg_rating",
            )
            || !group_exprs_match_binding_columns(
                &select.group_by,
                top_binding,
                &["person_id", "films", "avg_rating"],
            )
        {
            return Ok(None);
        }
        let Some(title_separator) =
            projection_expr_string_agg_separator(&select.projection[3], directed_binding, "title")
        else {
            return Ok(None);
        };

        let order_by = projection_order_by_plan(&query.order_by, &select.projection);
        if !query.order_by.is_empty() && order_by.is_none() {
            return Ok(None);
        }
        let limit = query
            .limit
            .as_ref()
            .map(|expr| self.eval_constant_i64(expr, params, &BTreeMap::new()))
            .transpose()?
            .map(|value| usize::try_from(value.max(0)).unwrap_or(usize::MAX));
        let offset = query
            .offset
            .as_ref()
            .map(|expr| self.eval_constant_i64(expr, params, &BTreeMap::new()))
            .transpose()?
            .map(|value| usize::try_from(value.max(0)).unwrap_or(usize::MAX))
            .unwrap_or(0);
        let column_names = select
            .projection
            .iter()
            .enumerate()
            .map(|(index, item)| match item {
                SelectItem::Expr { expr, alias } => alias
                    .clone()
                    .unwrap_or_else(|| infer_expr_name(expr, index + 1)),
                SelectItem::Wildcard | SelectItem::QualifiedWildcard(_) => {
                    format!("col{}", index + 1)
                }
            })
            .collect::<Vec<_>>();

        Ok(Some((
            column_names,
            order_by,
            limit,
            offset,
            title_separator.to_string(),
        )))
    }
    fn analyze_three_table_genre_popularity_query<'a>(
        &'a self,
        query: &'a Query,
        params: &[Value],
    ) -> Result<Option<ThreeTableGenrePopularityPlan<'a>>> {
        if !query.ctes.is_empty() || query.recursive {
            return Ok(None);
        }
        let QueryBody::Select(select) = &query.body else {
            return Ok(None);
        };
        if select.distinct
            || !select.distinct_on.is_empty()
            || select.filter.is_some()
            || select.having.is_some()
            || select.group_by.len() != 1
            || select.projection.len() != 3
            || select.from.len() != 1
        {
            return Ok(None);
        }

        let mut tables = Vec::new();
        let mut constraints = Vec::new();
        if !flatten_inner_join_chain(&select.from[0], &mut tables, &mut constraints)
            || tables.len() != 3
        {
            return Ok(None);
        }
        let genre_binding = tables
            .iter()
            .copied()
            .find(|binding| identifiers_equal(binding.name, "genres"));
        let bridge_binding = tables
            .iter()
            .copied()
            .find(|binding| identifiers_equal(binding.name, "movie_genres"));
        let movie_binding = tables
            .iter()
            .copied()
            .find(|binding| identifiers_equal(binding.name, "movies"));
        let (Some(genre_binding), Some(bridge_binding), Some(movie_binding)) =
            (genre_binding, bridge_binding, movie_binding)
        else {
            return Ok(None);
        };

        if [genre_binding.name, bridge_binding.name, movie_binding.name]
            .iter()
            .any(|table| {
                self.visible_view(table, NameResolutionScope::Session)
                    .is_some()
                    || self.visible_table_is_temporary(table)
            })
        {
            return Ok(None);
        }
        let Some(genre_schema) = self.table_schema(genre_binding.name) else {
            return Ok(None);
        };
        let Some(bridge_schema) = self.table_schema(bridge_binding.name) else {
            return Ok(None);
        };
        let Some(movie_schema) = self.table_schema(movie_binding.name) else {
            return Ok(None);
        };
        if !generated_columns_are_stored(genre_schema)
            || !generated_columns_are_stored(bridge_schema)
            || !generated_columns_are_stored(movie_schema)
        {
            return Ok(None);
        }

        let SelectItem::Expr {
            expr: name_expr,
            alias: name_alias,
        } = &select.projection[0]
        else {
            return Ok(None);
        };
        let SelectItem::Expr {
            expr: count_expr,
            alias: count_alias,
        } = &select.projection[1]
        else {
            return Ok(None);
        };
        let SelectItem::Expr {
            expr: avg_expr,
            alias: avg_alias,
        } = &select.projection[2]
        else {
            return Ok(None);
        };

        if !grouped_projection_expr_matches_group_expr(
            name_expr,
            &select.group_by[0],
            genre_binding,
        ) || !expr_matches_binding_column(name_expr, genre_binding, "name")
            || !aggregate_matches_count_star(count_expr)
            || !aggregate_matches_single_binding_column(avg_expr, "avg", movie_binding, "rating")
        {
            return Ok(None);
        }

        if !join_constraints_match_columns(
            &constraints,
            genre_binding,
            "id",
            bridge_binding,
            "genre_id",
        ) || !join_constraints_match_columns(
            &constraints,
            movie_binding,
            "id",
            bridge_binding,
            "movie_id",
        ) {
            return Ok(None);
        }

        let genre_id_index = schema_column_index(genre_schema, "id")
            .ok_or_else(|| DbError::internal("genre popularity id column missing from genres"))?;
        let genre_name_index = schema_column_index(genre_schema, "name")
            .ok_or_else(|| DbError::internal("genre popularity name column missing from genres"))?;
        let bridge_movie_id_index =
            schema_column_index(bridge_schema, "movie_id").ok_or_else(|| {
                DbError::internal("genre popularity movie_id column missing from movie_genres")
            })?;
        let movie_rating_index = schema_column_index(movie_schema, "rating").ok_or_else(|| {
            DbError::internal("genre popularity rating column missing from movies")
        })?;

        let Some(bridge_genre_index_name) = self
            .single_column_btree_index(bridge_binding.name, "genre_id")
            .map(|index| index.name.clone())
        else {
            return Ok(None);
        };
        let movie_index_name = self
            .single_column_btree_index(movie_binding.name, "id")
            .map(|index| index.name.clone());
        let movie_id_is_rowid_alias = row_id_alias_column_name(movie_schema)
            .is_some_and(|column| identifiers_equal(column, "id"));
        if !movie_id_is_rowid_alias && movie_index_name.is_none() {
            return Ok(None);
        }

        let column_names = vec![
            name_alias
                .clone()
                .unwrap_or_else(|| infer_expr_name(name_expr, 1)),
            count_alias
                .clone()
                .unwrap_or_else(|| infer_expr_name(count_expr, 2)),
            avg_alias
                .clone()
                .unwrap_or_else(|| infer_expr_name(avg_expr, 3)),
        ];

        let order_by = projection_order_by_plan(&query.order_by, &select.projection);
        if !query.order_by.is_empty() && order_by.is_none() {
            return Ok(None);
        }
        let limit = query
            .limit
            .as_ref()
            .map(|expr| self.eval_constant_i64(expr, params, &BTreeMap::new()))
            .transpose()?
            .map(|value| usize::try_from(value.max(0)).unwrap_or(usize::MAX));
        let offset = query
            .offset
            .as_ref()
            .map(|expr| self.eval_constant_i64(expr, params, &BTreeMap::new()))
            .transpose()?
            .map(|value| usize::try_from(value.max(0)).unwrap_or(usize::MAX))
            .unwrap_or(0);

        Ok(Some(ThreeTableGenrePopularityPlan {
            genre_table_name: genre_binding.name,
            genre_id_index,
            genre_name_index,
            bridge_table_name: bridge_binding.name,
            bridge_movie_id_index,
            bridge_genre_index_name,
            movie_table_name: movie_binding.name,
            movie_rating_index,
            movie_index_name,
            movie_id_is_rowid_alias,
            column_names,
            order_by,
            limit,
            offset,
        }))
    }
    pub(crate) fn analyze_left_join_aggregate_query<'a>(
        &'a self,
        query: &'a Query,
        params: &[Value],
    ) -> Result<Option<LeftJoinAggregatePlan<'a>>> {
        if !query.ctes.is_empty() || query.recursive {
            return Ok(None);
        }
        let QueryBody::Select(select) = &query.body else {
            return Ok(None);
        };
        if select.distinct
            || !select.distinct_on.is_empty()
            || select.filter.is_some()
            || select.having.is_some()
            || select.group_by.is_empty()
            || select.from.len() != 1
        {
            return Ok(None);
        }
        let FromItem::Join {
            left,
            right,
            kind,
            constraint,
        } = &select.from[0]
        else {
            return Ok(None);
        };
        let include_empty_parent = match kind {
            JoinKind::Left => true,
            JoinKind::Inner => false,
            _ => return Ok(None),
        };
        let (left_name, left_alias) = match &**left {
            FromItem::Table { name, alias } => (name.as_str(), alias),
            _ => return Ok(None),
        };
        let (right_name, right_alias) = match &**right {
            FromItem::Table { name, alias } => (name.as_str(), alias),
            _ => return Ok(None),
        };
        if self
            .visible_view(left_name, NameResolutionScope::Session)
            .is_some()
            || self
                .visible_view(right_name, NameResolutionScope::Session)
                .is_some()
            || self.visible_table_is_temporary(left_name)
            || self.visible_table_is_temporary(right_name)
        {
            return Ok(None);
        }
        let Some(left_schema) = self.table_schema(left_name) else {
            return Ok(None);
        };
        let Some(right_schema) = self.table_schema(right_name) else {
            return Ok(None);
        };
        if !generated_columns_are_stored(left_schema) || !generated_columns_are_stored(right_schema)
        {
            return Ok(None);
        }
        let left_binding = TableBindingRef {
            name: left_name,
            alias: left_alias,
        };
        let right_binding = TableBindingRef {
            name: right_name,
            alias: right_alias,
        };
        let left_group_indexes =
            indexed_join_group_column_indexes(&select.group_by, left_binding, left_schema);
        let right_group_indexes =
            indexed_join_group_column_indexes(&select.group_by, right_binding, right_schema);
        let (parent_name, parent_binding, parent_schema, child_name, child_binding, child_schema) =
            match (left_group_indexes, right_group_indexes) {
                (Some(_group_column_indexes), None) => (
                    left_name,
                    left_binding,
                    left_schema,
                    right_name,
                    right_binding,
                    right_schema,
                ),
                (None, Some(_group_column_indexes)) if !include_empty_parent => (
                    right_name,
                    right_binding,
                    right_schema,
                    left_name,
                    left_binding,
                    left_schema,
                ),
                _ => return Ok(None),
            };

        let group_column_indexes =
            indexed_join_group_column_indexes(&select.group_by, parent_binding, parent_schema)
                .ok_or_else(|| DbError::internal("group column indexes mismatch"))?;

        let num_group_cols = select.group_by.len();
        if select.projection.len() <= num_group_cols {
            return Ok(None);
        }

        for projection_item in select
            .projection
            .iter()
            .take(num_group_cols)
            .zip(&select.group_by)
        {
            let (projection_item, group_expr) = projection_item;
            let SelectItem::Expr {
                expr: projection_expr,
                ..
            } = projection_item
            else {
                return Ok(None);
            };
            if !grouped_projection_expr_matches_group_expr(
                projection_expr,
                group_expr,
                parent_binding,
            ) {
                return Ok(None);
            }
        }

        let mut aggregate_kinds = Vec::with_capacity(select.projection.len() - num_group_cols);
        for projection_item in select.projection.iter().skip(num_group_cols) {
            let SelectItem::Expr { expr, .. } = projection_item else {
                return Ok(None);
            };
            let Some(kind) = classify_indexed_join_aggregate(expr, child_binding, child_schema)
            else {
                return Ok(None);
            };
            aggregate_kinds.push(kind);
        }

        let mut column_names = Vec::with_capacity(select.projection.len());
        for (index, projection_item) in select.projection.iter().enumerate() {
            let SelectItem::Expr { expr, alias } = projection_item else {
                return Ok(None);
            };
            column_names.push(
                alias
                    .clone()
                    .unwrap_or_else(|| infer_expr_name(expr, index + 1)),
            );
        }

        let Some(join_equalities) = simple_indexed_join_constraint_equalities(
            constraint,
            left_binding,
            right_binding,
            left_schema,
            right_schema,
        ) else {
            return Ok(None);
        };
        let Some((left_join_columns, right_join_columns)) =
            orient_join_equalities(&join_equalities, left_binding, right_binding)
        else {
            return Ok(None);
        };
        if left_join_columns.len() != 1 || right_join_columns.len() != 1 {
            return Ok(None);
        }

        let (parent_join_column, child_join_column) = if identifiers_equal(parent_name, left_name) {
            (left_join_columns[0], right_join_columns[0])
        } else {
            (right_join_columns[0], left_join_columns[0])
        };

        let parent_join_index = parent_schema
            .columns
            .iter()
            .position(|column| identifiers_equal(&column.name, parent_join_column))
            .ok_or_else(|| {
                DbError::internal(format!(
                    "join column {}.{} not found",
                    parent_name, parent_join_column
                ))
            })?;
        let child_join_index = child_schema
            .columns
            .iter()
            .position(|column| identifiers_equal(&column.name, child_join_column))
            .ok_or_else(|| {
                DbError::internal(format!(
                    "join column {}.{} not found",
                    child_name, child_join_column
                ))
            })?;

        let child_index_name = self
            .single_column_btree_index(child_name, child_join_column)
            .map(|index| index.name.clone());

        let order_by = projection_order_by_plan(&query.order_by, &select.projection);
        if !query.order_by.is_empty() && order_by.is_none() {
            return Ok(None);
        }

        let limit = query
            .limit
            .as_ref()
            .map(|expr| self.eval_constant_i64(expr, params, &BTreeMap::new()))
            .transpose()?
            .map(|value| usize::try_from(value.max(0)).unwrap_or(usize::MAX));
        let offset = query
            .offset
            .as_ref()
            .map(|expr| self.eval_constant_i64(expr, params, &BTreeMap::new()))
            .transpose()?
            .map(|value| usize::try_from(value.max(0)).unwrap_or(usize::MAX))
            .unwrap_or(0);

        Ok(Some(LeftJoinAggregatePlan {
            parent_table_name: parent_name,
            parent_join_index,
            child_table_name: child_name,
            child_join_index,
            child_index_name,
            group_column_indexes,
            aggregate_kinds,
            column_names,
            order_by,
            limit,
            offset,
            include_empty_parent,
        }))
    }
    fn analyze_left_join_status_aggregate_query<'a>(
        &'a self,
        query: &'a Query,
        params: &[Value],
    ) -> Result<Option<LeftJoinStatusAggregatePlan<'a>>> {
        if !query.ctes.is_empty() || query.recursive {
            return Ok(None);
        }
        let QueryBody::Select(select) = &query.body else {
            return Ok(None);
        };
        if select.distinct
            || !select.distinct_on.is_empty()
            || query.order_by.len() > 2
            || select.filter.is_some()
            || select.having.is_some()
            || select.group_by.len() != 2
            || select.projection.len() != 7
            || select.from.len() != 1
        {
            return Ok(None);
        }

        let FromItem::Join {
            left,
            right,
            kind,
            constraint,
        } = &select.from[0]
        else {
            return Ok(None);
        };
        if !matches!(kind, JoinKind::Left) {
            return Ok(None);
        }

        let (left_name, left_alias) = match &**left {
            FromItem::Table { name, alias } => (name.as_str(), alias),
            _ => return Ok(None),
        };
        let (right_name, right_alias) = match &**right {
            FromItem::Table { name, alias } => (name.as_str(), alias),
            _ => return Ok(None),
        };
        if self
            .visible_view(left_name, NameResolutionScope::Session)
            .is_some()
            || self
                .visible_view(right_name, NameResolutionScope::Session)
                .is_some()
            || self.visible_table_is_temporary(left_name)
            || self.visible_table_is_temporary(right_name)
        {
            return Ok(None);
        }
        let Some(left_schema) = self.table_schema(left_name) else {
            return Ok(None);
        };
        let Some(right_schema) = self.table_schema(right_name) else {
            return Ok(None);
        };
        if !generated_columns_are_stored(left_schema) || !generated_columns_are_stored(right_schema)
        {
            return Ok(None);
        }

        let left_binding = TableBindingRef {
            name: left_name,
            alias: left_alias,
        };
        let right_binding = TableBindingRef {
            name: right_name,
            alias: right_alias,
        };
        let left_group_indexes =
            indexed_join_group_column_indexes(&select.group_by, left_binding, left_schema);
        let right_group_indexes =
            indexed_join_group_column_indexes(&select.group_by, right_binding, right_schema);
        let (
            parent_name,
            parent_alias,
            parent_schema,
            child_name,
            child_alias,
            child_schema,
            group_column_indexes,
        ) = match (left_group_indexes, right_group_indexes) {
            (Some(group_column_indexes), None) => (
                left_name,
                left_alias,
                left_schema,
                right_name,
                right_alias,
                right_schema,
                group_column_indexes,
            ),
            _ => return Ok(None),
        };

        let parent_binding = TableBindingRef {
            name: parent_name,
            alias: parent_alias,
        };
        let child_binding = TableBindingRef {
            name: child_name,
            alias: child_alias,
        };

        for (projection_item, group_expr) in select
            .projection
            .iter()
            .take(select.group_by.len())
            .zip(&select.group_by)
        {
            let SelectItem::Expr {
                expr: projection_expr,
                ..
            } = projection_item
            else {
                return Ok(None);
            };
            if !grouped_projection_expr_matches_group_expr(
                projection_expr,
                group_expr,
                parent_binding,
            ) {
                return Ok(None);
            }
        }

        let SelectItem::Expr {
            expr: open_expr, ..
        } = &select.projection[2]
        else {
            return Ok(None);
        };
        let SelectItem::Expr {
            expr: in_progress_expr,
            ..
        } = &select.projection[3]
        else {
            return Ok(None);
        };
        let SelectItem::Expr {
            expr: resolved_expr,
            ..
        } = &select.projection[4]
        else {
            return Ok(None);
        };
        let SelectItem::Expr {
            expr: closed_expr, ..
        } = &select.projection[5]
        else {
            return Ok(None);
        };
        let SelectItem::Expr {
            expr: count_expr, ..
        } = &select.projection[6]
        else {
            return Ok(None);
        };

        if !aggregate_matches_status_case_sum(open_expr, "sum", child_binding, "status", "open")
            || !aggregate_matches_status_case_sum(
                in_progress_expr,
                "sum",
                child_binding,
                "status",
                "in_progress",
            )
            || !aggregate_matches_status_case_sum(
                resolved_expr,
                "sum",
                child_binding,
                "status",
                "resolved",
            )
            || !aggregate_matches_status_case_sum(
                closed_expr,
                "sum",
                child_binding,
                "status",
                "closed",
            )
            || !aggregate_matches_single_binding_column(count_expr, "count", child_binding, "id")
        {
            return Ok(None);
        }

        let mut column_names = Vec::with_capacity(select.projection.len());
        for (index, projection_item) in select.projection.iter().enumerate() {
            let SelectItem::Expr { expr, alias } = projection_item else {
                return Ok(None);
            };
            column_names.push(
                alias
                    .clone()
                    .unwrap_or_else(|| infer_expr_name(expr, index + 1)),
            );
        }

        let Some(join_equalities) = simple_indexed_join_constraint_equalities(
            constraint,
            left_binding,
            right_binding,
            left_schema,
            right_schema,
        ) else {
            return Ok(None);
        };
        let Some((left_join_columns, right_join_columns)) =
            orient_join_equalities(&join_equalities, left_binding, right_binding)
        else {
            return Ok(None);
        };
        if left_join_columns.len() != 1 || right_join_columns.len() != 1 {
            return Ok(None);
        }

        let (parent_join_column, child_join_column) = if identifiers_equal(parent_name, left_name) {
            (left_join_columns[0], right_join_columns[0])
        } else {
            (right_join_columns[0], left_join_columns[0])
        };

        let parent_join_index = parent_schema
            .columns
            .iter()
            .position(|column| identifiers_equal(&column.name, parent_join_column))
            .ok_or_else(|| {
                DbError::internal(format!(
                    "join column {}.{} not found",
                    parent_name, parent_join_column
                ))
            })?;
        let child_join_index = child_schema
            .columns
            .iter()
            .position(|column| identifiers_equal(&column.name, child_join_column))
            .ok_or_else(|| {
                DbError::internal(format!(
                    "join column {}.{} not found",
                    child_name, child_join_column
                ))
            })?;

        let child_status_index = schema_column_index(child_schema, "status").ok_or_else(|| {
            DbError::internal(format!(
                "column status not found in child table {}",
                child_name
            ))
        })?;
        let child_id_index = schema_column_index(child_schema, "id").ok_or_else(|| {
            DbError::internal(format!("column id not found in child table {}", child_name))
        })?;
        let child_index_name = self
            .single_column_btree_index(child_name, child_join_column)
            .map(|index| index.name.clone());

        let order_by = projection_order_by_plan(&query.order_by, &select.projection);
        if !query.order_by.is_empty() {
            let Some(order_by) = order_by.as_ref() else {
                return Ok(None);
            };
            if order_by.len() != 2
                || order_by[0].projection_index != 6
                || !order_by[0].descending
                || order_by[1].projection_index != 0
                || order_by[1].descending
            {
                return Ok(None);
            }
        }

        if child_schema.columns[child_status_index].column_type != crate::catalog::ColumnType::Text
        {
            return Ok(None);
        }

        let limit = query
            .limit
            .as_ref()
            .map(|expr| self.eval_constant_i64(expr, params, &BTreeMap::new()))
            .transpose()?
            .map(|value| usize::try_from(value.max(0)).unwrap_or(usize::MAX));
        let offset = query
            .offset
            .as_ref()
            .map(|expr| self.eval_constant_i64(expr, params, &BTreeMap::new()))
            .transpose()?
            .map(|value| usize::try_from(value.max(0)).unwrap_or(usize::MAX))
            .unwrap_or(0);

        Ok(Some(LeftJoinStatusAggregatePlan {
            parent_table_name: parent_name,
            parent_join_index,
            child_table_name: child_name,
            child_join_index,
            child_status_index,
            child_id_index,
            child_index_name,
            group_column_indexes,
            column_names,
            order_by,
            limit,
            offset,
        }))
    }
    pub(crate) fn try_execute_indexed_join_grouped_count_query(
        &self,
        query: &Query,
        params: &[Value],
    ) -> Result<Option<QueryResult>> {
        let Some(plan) = self.analyze_indexed_join_grouped_count_query(query, params)? else {
            return Ok(None);
        };
        let Some(parent_source) = self.visible_table_row_source(plan.parent_table_name) else {
            return Ok(None);
        };
        let Some(RuntimeIndex::Btree { keys, .. }) = self.index(&plan.child_index_name) else {
            return Ok(None);
        };
        let child_source = self
            .catalog
            .index(&plan.child_index_name)
            .and_then(|index| self.visible_table_row_source(&index.table_name));
        let parent_table = self.table_schema(plan.parent_table_name).ok_or_else(|| {
            DbError::internal(format!(
                "table {} not found for indexed grouped join count",
                plan.parent_table_name
            ))
        })?;
        let parent_join_index = parent_table
            .columns
            .iter()
            .position(|column| identifiers_equal(&column.name, plan.parent_join_column))
            .ok_or_else(|| {
                DbError::internal(format!(
                    "join column {}.{} not found",
                    plan.parent_table_name, plan.parent_join_column
                ))
            })?;

        let bounded_order = plan
            .order_by
            .as_deref()
            .zip(plan.limit)
            .filter(|(_, _)| plan.offset == 0);
        let scalar_count_top_n_limit = plan.scalar_count_top_n_limit();
        let mut scalar_count_top_n_rows: Vec<(i64, QueryRow)> = Vec::new();
        let mut rows = Vec::new();
        for parent_row in parent_source.rows() {
            let parent_row = parent_row?;
            let parent_values = parent_row.values();
            let Some(join_value) = parent_values.get(parent_join_index) else {
                return Err(DbError::internal("parent join row is shorter than schema"));
            };
            if matches!(join_value, Value::Null) {
                continue;
            }
            let child_row_ids = keys.row_ids_for_value_set(join_value)?;
            let child_count = if let Some(child_source) = child_source {
                visible_row_id_set_count(child_source, child_row_ids)?
            } else {
                child_row_ids.len()
            };
            if child_count == 0 {
                continue;
            }
            let child_count = i64::try_from(child_count).map_err(|_| {
                DbError::sql(format!(
                    "join count for table {} exceeds INT64 limits",
                    plan.parent_table_name
                ))
            })?;

            let scalar_count_top_n_slot = if let Some(limit) = scalar_count_top_n_limit {
                if limit == 0 {
                    continue;
                }
                if scalar_count_top_n_rows.len() < limit {
                    Some(scalar_count_top_n_rows.len())
                } else {
                    let mut worst_index = 0;
                    for index in 1..scalar_count_top_n_rows.len() {
                        if scalar_count_top_n_rows[index].0 < scalar_count_top_n_rows[worst_index].0
                        {
                            worst_index = index;
                        }
                    }
                    if child_count <= scalar_count_top_n_rows[worst_index].0 {
                        continue;
                    }
                    Some(worst_index)
                }
            } else {
                None
            };

            let mut output = Vec::with_capacity(plan.group_column_indexes.len() + 1);
            for index in &plan.group_column_indexes {
                output.push(parent_values[*index].clone());
            }
            output.push(Value::Int64(child_count));
            let row = QueryRow::new(output);
            if let Some(slot) = scalar_count_top_n_slot {
                if slot == scalar_count_top_n_rows.len() {
                    scalar_count_top_n_rows.push((child_count, row));
                } else {
                    scalar_count_top_n_rows[slot] = (child_count, row);
                }
            } else if let Some((order_by, limit)) = bounded_order {
                push_bounded_projection_ordered_query_row(
                    Some(self),
                    &mut rows,
                    row,
                    order_by,
                    limit,
                )?;
            } else {
                rows.push(row);
            }
        }

        if scalar_count_top_n_limit.is_some() {
            scalar_count_top_n_rows.sort_by_key(|row| std::cmp::Reverse(row.0));
            let rows = scalar_count_top_n_rows
                .into_iter()
                .map(|(_, row)| row)
                .collect();
            return Ok(Some(QueryResult::with_rows(plan.column_names, rows)));
        }

        if let Some((order_by, _)) = bounded_order {
            sort_query_rows_by_projection_order(Some(self), &mut rows, order_by)?;
            return Ok(Some(QueryResult::with_rows(plan.column_names, rows)));
        }

        Ok(Some(apply_simple_projection_postprocessing_with_order(
            Some(self),
            rows,
            plan.column_names,
            plan.order_by.as_deref(),
            plan.limit,
            plan.offset,
        )?))
    }
    pub(crate) fn indexed_join_grouped_count_parent_table_name<'a>(
        &'a self,
        query: &'a Query,
        params: &[Value],
    ) -> Result<Option<&'a str>> {
        Ok(self
            .analyze_indexed_join_grouped_count_query(query, params)?
            .map(|plan| plan.parent_table_name))
    }
    pub(crate) fn analyze_indexed_join_grouped_count_query<'a>(
        &'a self,
        query: &'a Query,
        params: &[Value],
    ) -> Result<Option<IndexedJoinGroupedCountPlan<'a>>> {
        if !query.ctes.is_empty() || query.recursive {
            return Ok(None);
        }
        let QueryBody::Select(select) = &query.body else {
            return Ok(None);
        };
        if select.distinct
            || !select.distinct_on.is_empty()
            || select.filter.is_some()
            || select.having.is_some()
            || select.group_by.is_empty()
            || select.from.len() != 1
            || select.projection.len() != select.group_by.len() + 1
        {
            return Ok(None);
        }
        let FromItem::Join {
            left,
            right,
            kind,
            constraint,
        } = &select.from[0]
        else {
            return Ok(None);
        };
        if !matches!(kind, JoinKind::Inner) {
            return Ok(None);
        }
        let (left_name, left_alias) = match &**left {
            FromItem::Table { name, alias } => (name.as_str(), alias),
            _ => return Ok(None),
        };
        let (right_name, right_alias) = match &**right {
            FromItem::Table { name, alias } => (name.as_str(), alias),
            _ => return Ok(None),
        };
        if self
            .visible_view(left_name, NameResolutionScope::Session)
            .is_some()
            || self
                .visible_view(right_name, NameResolutionScope::Session)
                .is_some()
            || self.visible_table_is_temporary(left_name)
            || self.visible_table_is_temporary(right_name)
        {
            return Ok(None);
        }
        let Some(left_schema) = self.table_schema(left_name) else {
            return Ok(None);
        };
        let Some(right_schema) = self.table_schema(right_name) else {
            return Ok(None);
        };
        if !generated_columns_are_stored(left_schema) || !generated_columns_are_stored(right_schema)
        {
            return Ok(None);
        }

        let left_binding = TableBindingRef {
            name: left_name,
            alias: left_alias,
        };
        let right_binding = TableBindingRef {
            name: right_name,
            alias: right_alias,
        };
        let left_group_indexes =
            indexed_join_group_column_indexes(&select.group_by, left_binding, left_schema);
        let right_group_indexes =
            indexed_join_group_column_indexes(&select.group_by, right_binding, right_schema);
        let (
            parent_name,
            parent_binding,
            parent_schema,
            child_name,
            child_binding,
            child_schema,
            group_column_indexes,
        ) = match (left_group_indexes, right_group_indexes) {
            (Some(group_column_indexes), None) => (
                left_name,
                left_binding,
                left_schema,
                right_name,
                right_binding,
                right_schema,
                group_column_indexes,
            ),
            (None, Some(group_column_indexes)) => (
                right_name,
                right_binding,
                right_schema,
                left_name,
                left_binding,
                left_schema,
                group_column_indexes,
            ),
            _ => return Ok(None),
        };

        let SelectItem::Expr {
            expr: count_expr,
            alias: count_alias,
        } = &select.projection[select.group_by.len()]
        else {
            return Ok(None);
        };
        if !indexed_join_grouped_count_is_safe(count_expr, child_binding, child_schema) {
            return Ok(None);
        }

        let mut column_names = Vec::with_capacity(select.projection.len());
        for (projection_item, group_expr) in select
            .projection
            .iter()
            .take(select.group_by.len())
            .zip(&select.group_by)
        {
            let SelectItem::Expr {
                expr: projection_expr,
                alias,
            } = projection_item
            else {
                return Ok(None);
            };
            if !grouped_projection_expr_matches_group_expr(
                projection_expr,
                group_expr,
                parent_binding,
            ) {
                return Ok(None);
            }
            column_names.push(
                alias
                    .clone()
                    .unwrap_or_else(|| infer_expr_name(projection_expr, column_names.len() + 1)),
            );
        }
        column_names.push(
            count_alias
                .clone()
                .unwrap_or_else(|| infer_expr_name(count_expr, select.group_by.len() + 1)),
        );

        let Some(join_equalities) = simple_indexed_join_constraint_equalities(
            constraint,
            left_binding,
            right_binding,
            left_schema,
            right_schema,
        ) else {
            return Ok(None);
        };
        let Some((parent_join_columns, child_join_columns)) =
            orient_join_equalities(&join_equalities, parent_binding, child_binding)
        else {
            return Ok(None);
        };
        if parent_join_columns.len() != 1 || child_join_columns.len() != 1 {
            return Ok(None);
        }
        let parent_join_column = parent_join_columns[0];
        let child_join_column = child_join_columns[0];
        if parent_schema
            .columns
            .iter()
            .all(|column| !identifiers_equal(&column.name, parent_join_column))
        {
            return Ok(None);
        }
        let Some(child_index_name) = self
            .catalog
            .indexes
            .values()
            .find(|index| {
                identifiers_equal(&index.table_name, child_name)
                    && index.fresh
                    && index.kind == IndexKind::Btree
                    && index.predicate_sql.is_none()
                    && index.columns.len() == 1
                    && index.columns[0].expression_sql.is_none()
                    && index.columns[0]
                        .column_name
                        .as_deref()
                        .is_some_and(|column| identifiers_equal(column, child_join_column))
            })
            .map(|index| index.name.clone())
        else {
            return Ok(None);
        };

        let order_by = projection_order_by_plan(&query.order_by, &select.projection);
        if !query.order_by.is_empty() && order_by.is_none() {
            return Ok(None);
        }
        let limit = query
            .limit
            .as_ref()
            .map(|expr| self.eval_constant_i64(expr, params, &BTreeMap::new()))
            .transpose()?
            .map(|value| usize::try_from(value.max(0)).unwrap_or(usize::MAX));
        let offset = query
            .offset
            .as_ref()
            .map(|expr| self.eval_constant_i64(expr, params, &BTreeMap::new()))
            .transpose()?
            .map(|value| usize::try_from(value.max(0)).unwrap_or(usize::MAX))
            .unwrap_or(0);

        Ok(Some(IndexedJoinGroupedCountPlan {
            parent_table_name: parent_name,
            parent_join_column,
            child_index_name,
            group_column_indexes,
            column_names,
            order_by,
            limit,
            offset,
        }))
    }
    pub(crate) fn try_execute_indexed_join_limit_projection_query(
        &self,
        query: &Query,
        params: &[Value],
    ) -> Result<Option<QueryResult>> {
        if query.recursive || !query.ctes.is_empty() || !query.order_by.is_empty() {
            return Ok(None);
        }
        let Some(limit_expr) = query.limit.as_ref() else {
            return Ok(None);
        };
        let ctes = BTreeMap::new();
        let limit_value = match simple_int64_constant_expr_value(limit_expr, params)? {
            Some(value) => value,
            None => self.eval_constant_i64(limit_expr, params, &ctes)?,
        };
        let limit = usize::try_from(limit_value.max(0)).unwrap_or(usize::MAX);
        let offset = query
            .offset
            .as_ref()
            .map(|expr| {
                simple_int64_constant_expr_value(expr, params)?
                    .map(Ok)
                    .unwrap_or_else(|| self.eval_constant_i64(expr, params, &ctes))
            })
            .transpose()?
            .map(|value| usize::try_from(value.max(0)).unwrap_or(usize::MAX))
            .unwrap_or(0);
        let QueryBody::Select(select) = &query.body else {
            return Ok(None);
        };
        self.try_execute_indexed_join_limit_projection_select(
            select,
            &select.projection,
            limit,
            offset,
        )
    }
    pub(crate) fn try_execute_indexed_join_limit_projection_select(
        &self,
        select: &Select,
        projection: &[SelectItem],
        limit: usize,
        offset: usize,
    ) -> Result<Option<QueryResult>> {
        if limit == 0 {
            let column_names = projection
                .iter()
                .enumerate()
                .map(|(index, item)| match item {
                    SelectItem::Expr { expr, alias } => alias
                        .clone()
                        .unwrap_or_else(|| infer_expr_name(expr, index + 1)),
                    _ => format!("col{}", index + 1),
                })
                .collect();
            return Ok(Some(QueryResult::with_rows(column_names, Vec::new())));
        }
        let Some(plan) =
            self.analyze_indexed_join_limit_projection_select(select, projection, limit, offset)?
        else {
            return Ok(None);
        };
        self.execute_indexed_join_limit_projection_plan(&plan)
            .map(Some)
    }
    pub(crate) fn analyze_indexed_join_limit_projection_select<'a>(
        &'a self,
        select: &'a Select,
        projection: &'a [SelectItem],
        limit: usize,
        offset: usize,
    ) -> Result<Option<IndexedJoinLimitPlan<'a>>> {
        if select.distinct
            || !select.distinct_on.is_empty()
            || select.filter.is_some()
            || !select.group_by.is_empty()
            || select.having.is_some()
            || projection_has_aggregate_items(projection)
            || select.from.len() != 1
        {
            return Ok(None);
        }
        let mut tables = Vec::new();
        let mut constraints = Vec::new();
        if !flatten_left_deep_inner_join_tables(&select.from[0], &mut tables, &mut constraints) {
            return Ok(None);
        }
        if !(2..=3).contains(&tables.len()) || constraints.len() + 1 != tables.len() {
            return Ok(None);
        }
        for table in &tables {
            if self
                .visible_view(table.name, NameResolutionScope::Session)
                .is_some()
                || self.visible_table_is_temporary(table.name)
            {
                return Ok(None);
            }
            let Some(schema) = self.table_schema(table.name) else {
                return Ok(None);
            };
            if !generated_columns_are_stored(schema)
                || self.visible_table_row_source(table.name).is_none()
            {
                return Ok(None);
            }
        }

        let mut steps = Vec::with_capacity(constraints.len());
        for (right_table_index, constraint) in
            constraints.iter().enumerate().map(|(i, c)| (i + 1, c))
        {
            let Some(step) = self.indexed_join_limit_step_for_constraint(
                &tables,
                right_table_index,
                constraint,
            )?
            else {
                return Ok(None);
            };
            steps.push(step);
        }

        let mut projections = Vec::with_capacity(projection.len());
        for (index, item) in projection.iter().enumerate() {
            let SelectItem::Expr { expr, alias } = item else {
                return Ok(None);
            };
            let Some((table_index, column_index)) =
                indexed_join_limit_projection_column(expr, &tables, self)
            else {
                return Ok(None);
            };
            projections.push(IndexedJoinLimitProjection {
                table_index,
                column_index,
                column_name: alias
                    .clone()
                    .unwrap_or_else(|| infer_expr_name(expr, index + 1)),
            });
        }

        Ok(Some(IndexedJoinLimitPlan {
            tables,
            steps,
            projections,
            limit,
            offset,
        }))
    }
    fn indexed_join_limit_step_for_constraint(
        &self,
        tables: &[IndexedJoinLimitTablePlan<'_>],
        right_table_index: usize,
        constraint: &JoinConstraint,
    ) -> Result<Option<IndexedJoinLimitStep>> {
        let JoinConstraint::On(on) = constraint else {
            return Ok(None);
        };
        let Some(equalities) = simple_join_equalities(on) else {
            return Ok(None);
        };
        if equalities.len() != 1 {
            return Ok(None);
        }
        let right_table = tables[right_table_index];
        let right_binding = TableBindingRef {
            name: right_table.name,
            alias: right_table.alias,
        };
        let (left_ref, right_ref) = equalities[0];
        let (previous_ref, right_ref) = if matches_table_binding(right_binding, right_ref.table) {
            (left_ref, right_ref)
        } else if matches_table_binding(right_binding, left_ref.table) {
            (right_ref, left_ref)
        } else {
            return Ok(None);
        };

        let Some(previous_table_index) = (0..right_table_index).find(|index| {
            let table = tables[*index];
            matches_table_binding(
                TableBindingRef {
                    name: table.name,
                    alias: table.alias,
                },
                previous_ref.table,
            )
        }) else {
            return Ok(None);
        };
        let previous_schema = self
            .table_schema(tables[previous_table_index].name)
            .ok_or_else(|| DbError::internal("indexed join previous table missing"))?;
        let right_schema = self
            .table_schema(right_table.name)
            .ok_or_else(|| DbError::internal("indexed join right table missing"))?;
        let Some(previous_column_index) = schema_column_index(previous_schema, previous_ref.column)
        else {
            return Ok(None);
        };
        let Some(right_column_index) = schema_column_index(right_schema, right_ref.column) else {
            return Ok(None);
        };
        if crate::exec::dml::row_id_alias_column_name(right_schema)
            .is_some_and(|column| identifiers_equal(column, right_ref.column))
        {
            let _ = right_column_index;
            return Ok(Some(IndexedJoinLimitStep {
                previous_table_index,
                previous_column_index,
                right_index_name: None,
            }));
        }
        let Some(index) = self.catalog.indexes.values().find(|index| {
            identifiers_equal(&index.table_name, right_table.name)
                && index.fresh
                && index.kind == IndexKind::Btree
                && index.predicate_sql.is_none()
                && index.columns.len() == 1
                && index.columns[0].expression_sql.is_none()
                && index.columns[0]
                    .column_name
                    .as_deref()
                    .is_some_and(|column| identifiers_equal(column, right_ref.column))
        }) else {
            let _ = right_column_index;
            return Ok(None);
        };
        let _ = right_column_index;
        Ok(Some(IndexedJoinLimitStep {
            previous_table_index,
            previous_column_index,
            right_index_name: Some(index.name.clone()),
        }))
    }
    pub(crate) fn ordered_view_root_btree_index(
        &self,
        table_name: &str,
        column_name: &str,
        root_filter: Option<&Expr>,
        root_binding: &str,
    ) -> Result<Option<&IndexSchema>> {
        let mut full_index = None;
        for index in self.catalog.indexes.values() {
            if !single_plain_btree_index_matches_column(index, table_name, column_name) {
                continue;
            }
            let Some(predicate_sql) = index.predicate_sql.as_deref() else {
                if full_index.is_none() {
                    full_index = Some(index);
                }
                continue;
            };
            let Some(root_filter) = root_filter else {
                continue;
            };
            let predicate = crate::sql::parser::parse_expression_sql(predicate_sql)?;
            if filter_contains_partial_index_predicate(root_filter, &predicate, root_binding) {
                return Ok(Some(index));
            }
        }
        Ok(full_index)
    }
    pub(crate) fn try_execute_three_table_indexed_join_projection_query(
        &self,
        query: &Query,
        params: &[Value],
    ) -> Result<Option<QueryResult>> {
        if query.recursive || !query.ctes.is_empty() {
            return Ok(None);
        }
        let QueryBody::Select(select) = &query.body else {
            return Ok(None);
        };
        if select.from.len() != 1 {
            return Ok(None);
        }
        let mut tables = Vec::new();
        let mut constraints = Vec::new();
        if !flatten_left_deep_inner_join_tables(&select.from[0], &mut tables, &mut constraints)
            || tables.len() != 3
            || constraints.len() != 2
        {
            return Ok(None);
        }
        let natural_order = if query.order_by.is_empty() {
            None
        } else {
            self.three_table_join_natural_order(query, &tables)
        };
        let order_by = if query.order_by.is_empty() || natural_order.is_some() {
            None
        } else {
            let Some(order_by) = projection_order_by_plan(&query.order_by, &select.projection)
            else {
                return Ok(None);
            };
            Some(order_by)
        };
        let Some(plan) = self.analyze_indexed_join_limit_projection_select(
            select,
            &select.projection,
            usize::MAX,
            0,
        )?
        else {
            return Ok(None);
        };
        if plan.tables.len() != 3 {
            return Ok(None);
        }

        let mut rows = self.execute_indexed_join_projection_rows(
            &plan,
            natural_order.is_some(),
            natural_order.flatten(),
        )?;
        if let Some(order_by) = order_by.as_deref() {
            sort_query_rows_by_projection_order(Some(self), &mut rows, order_by)?;
        }

        let ctes = BTreeMap::new();
        let offset = query
            .offset
            .as_ref()
            .map(|expr| self.eval_constant_i64(expr, params, &ctes))
            .transpose()?
            .map(|value| usize::try_from(value.max(0)).unwrap_or(usize::MAX))
            .unwrap_or(0);
        let limit = query
            .limit
            .as_ref()
            .map(|expr| self.eval_constant_i64(expr, params, &ctes))
            .transpose()?
            .map(|value| usize::try_from(value.max(0)).unwrap_or(usize::MAX));
        if offset > 0 || limit.is_some() {
            rows = rows
                .into_iter()
                .skip(offset)
                .take(limit.unwrap_or(usize::MAX))
                .collect();
        }
        Ok(Some(QueryResult::with_rows(
            plan.projections
                .iter()
                .map(|projection| projection.column_name.clone())
                .collect(),
            rows,
        )))
    }
    fn three_table_join_natural_order(
        &self,
        query: &Query,
        tables: &[IndexedJoinLimitTablePlan<'_>],
    ) -> Option<Option<usize>> {
        if !(1..=2).contains(&query.order_by.len()) {
            return None;
        }
        if query
            .order_by
            .iter()
            .any(|order| order.descending || order.collation.is_some())
        {
            return None;
        }
        let first_schema = self.table_schema(tables[0].name)?;
        let first_rowid_column = crate::exec::dml::row_id_alias_column_name(first_schema)?;
        let Expr::Column {
            table: first_table,
            column: first_column,
        } = &query.order_by[0].expr
        else {
            return None;
        };
        if !matches_table_binding(
            TableBindingRef {
                name: tables[0].name,
                alias: tables[0].alias,
            },
            first_table.as_deref(),
        ) || !identifiers_equal(first_column, first_rowid_column)
        {
            return None;
        }
        if query.order_by.len() == 1 {
            return Some(None);
        }

        let second_schema = self.table_schema(tables[1].name)?;
        let Expr::Column {
            table: second_table,
            column: second_column,
        } = &query.order_by[1].expr
        else {
            return None;
        };
        if !matches_table_binding(
            TableBindingRef {
                name: tables[1].name,
                alias: tables[1].alias,
            },
            second_table.as_deref(),
        ) {
            return None;
        }
        schema_column_index(second_schema, second_column).map(Some)
    }
    pub(crate) fn try_execute_base_table_join(
        &self,
        query: &Query,
        params: &[Value],
    ) -> Result<Option<QueryResult>> {
        let Some(plan) = self.analyze_base_table_join_query(query) else {
            return Ok(None);
        };
        let Some(left_source) = self.visible_table_row_source(plan.left_name) else {
            return Ok(None);
        };
        let Some(right_source) = self.visible_table_row_source(plan.right_name) else {
            return Ok(None);
        };
        Ok(Some(self.execute_base_table_join_from_sources(
            left_source,
            right_source,
            &plan,
            params,
        )?))
    }
    fn analyze_base_table_join_query<'a>(
        &'a self,
        query: &'a Query,
    ) -> Option<BaseTableJoinPlan<'a>> {
        if !query.ctes.is_empty() || query.recursive {
            return None;
        }
        let QueryBody::Select(select) = &query.body else {
            return None;
        };
        if !select.group_by.is_empty()
            || select.having.is_some()
            || projection_has_aggregate_items(&select.projection)
        {
            return None;
        }
        if select.from.len() != 1 {
            return None;
        }
        let FromItem::Join {
            left,
            right,
            kind,
            constraint,
        } = &select.from[0]
        else {
            return None;
        };
        if !matches!(
            kind,
            JoinKind::Inner | JoinKind::Left | JoinKind::Right | JoinKind::Full
        ) {
            return None;
        }
        let (left_name, left_alias) = match &**left {
            FromItem::Table { name, alias } => (name, alias.as_deref()),
            _ => return None,
        };
        let (right_name, right_alias) = match &**right {
            FromItem::Table { name, alias } => (name, alias.as_deref()),
            _ => return None,
        };
        if self
            .visible_view(left_name, NameResolutionScope::Session)
            .is_some()
            || self
                .visible_view(right_name, NameResolutionScope::Session)
                .is_some()
            || self.visible_table_is_temporary(left_name)
            || self.visible_table_is_temporary(right_name)
        {
            return None;
        }
        if self.table_schema(left_name).is_none() || self.table_schema(right_name).is_none() {
            return None;
        }
        if select.projection.iter().any(|item| {
            matches!(
                item,
                SelectItem::Wildcard | SelectItem::QualifiedWildcard(_)
            )
        }) {
            return None;
        }
        for item in &select.projection {
            if let SelectItem::Expr { expr, .. } = item {
                if expr_contains_window(expr) {
                    return None;
                }
            }
        }
        for order in &query.order_by {
            if expr_contains_window(&order.expr) {
                return None;
            }
        }
        if select.filter.as_ref().is_some_and(expr_contains_window) {
            return None;
        }
        Some(BaseTableJoinPlan {
            left_name,
            left_alias,
            right_name,
            right_alias,
            kind: *kind,
            constraint,
            filter: select.filter.as_ref(),
            projection: &select.projection,
            order_by: &query.order_by,
            distinct: select.distinct,
            limit: query.limit.as_ref(),
            offset: query.offset.as_ref(),
        })
    }
    pub(crate) fn try_execute_benchmark_history_query(
        &self,
        query: &Query,
        params: &[Value],
    ) -> Result<Option<QueryResult>> {
        if !query.ctes.is_empty()
            || query.limit.is_some()
            || query.offset.is_some()
            || query.order_by.len() != 1
        {
            return Ok(None);
        }
        let QueryBody::Select(select) = &query.body else {
            return Ok(None);
        };
        if !select.group_by.is_empty()
            || select.having.is_some()
            || select.distinct
            || !select.distinct_on.is_empty()
            || select.from.len() != 1
            || select.projection.len() != 6
        {
            return Ok(None);
        }

        let Some(filter) = select.filter.as_ref() else {
            return Ok(None);
        };
        let Some((filter_table, filter_column, value_expr)) = simple_btree_lookup(filter) else {
            return Ok(None);
        };

        let FromItem::Join {
            left: order_payment_items,
            right: item_item,
            kind: JoinKind::Inner,
            constraint: JoinConstraint::On(item_join_on),
        } = &select.from[0]
        else {
            return Ok(None);
        };
        let FromItem::Table {
            name: item_name,
            alias: item_alias,
        } = &**item_item
        else {
            return Ok(None);
        };
        let FromItem::Join {
            left: order_payment,
            right: order_item_item,
            kind: JoinKind::Inner,
            constraint: JoinConstraint::On(order_item_join_on),
        } = &**order_payment_items
        else {
            return Ok(None);
        };
        let FromItem::Table {
            name: order_item_name,
            alias: order_item_alias,
        } = &**order_item_item
        else {
            return Ok(None);
        };
        let FromItem::Join {
            left: order_item,
            right: payment_item,
            kind: JoinKind::Inner,
            constraint: JoinConstraint::On(payment_join_on),
        } = &**order_payment
        else {
            return Ok(None);
        };
        let FromItem::Table {
            name: order_name,
            alias: order_alias,
        } = &**order_item
        else {
            return Ok(None);
        };
        let FromItem::Table {
            name: payment_name,
            alias: payment_alias,
        } = &**payment_item
        else {
            return Ok(None);
        };

        let order_binding = TableBindingRef {
            name: order_name,
            alias: order_alias,
        };
        let payment_binding = TableBindingRef {
            name: payment_name,
            alias: payment_alias,
        };
        let order_item_binding = TableBindingRef {
            name: order_item_name,
            alias: order_item_alias,
        };
        let item_binding = TableBindingRef {
            name: item_name,
            alias: item_alias,
        };

        if self
            .visible_view(order_name, NameResolutionScope::Session)
            .is_some()
            || self
                .visible_view(payment_name, NameResolutionScope::Session)
                .is_some()
            || self
                .visible_view(order_item_name, NameResolutionScope::Session)
                .is_some()
            || self
                .visible_view(item_name, NameResolutionScope::Session)
                .is_some()
            || self.visible_table_is_temporary(order_name)
            || self.visible_table_is_temporary(payment_name)
            || self.visible_table_is_temporary(order_item_name)
            || self.visible_table_is_temporary(item_name)
        {
            return Ok(None);
        }

        if !matches_filter_binding(order_name, order_alias, filter_table)
            || !identifiers_equal(filter_column, "user_id")
        {
            return Ok(None);
        }
        if !join_constraint_matches_columns(
            payment_join_on,
            order_binding,
            "id",
            payment_binding,
            "order_id",
        ) || !join_constraint_matches_columns(
            order_item_join_on,
            order_binding,
            "id",
            order_item_binding,
            "order_id",
        ) || !join_constraint_matches_columns(
            item_join_on,
            order_item_binding,
            "item_id",
            item_binding,
            "id",
        ) {
            return Ok(None);
        }

        if !query.order_by[0].descending
            || !expr_matches_binding_column(&query.order_by[0].expr, order_binding, "id")
        {
            return Ok(None);
        }

        let projection = [
            (0, order_binding, "id"),
            (1, order_binding, "total_amount"),
            (2, payment_binding, "status"),
            (3, item_binding, "name"),
            (4, order_item_binding, "quantity"),
            (5, order_item_binding, "price"),
        ];
        for (index, binding, column) in projection {
            let SelectItem::Expr { expr, .. } = &select.projection[index] else {
                return Ok(None);
            };
            if !expr_matches_binding_column(expr, binding, column) {
                return Ok(None);
            }
        }

        let order_schema = match self.table_schema(order_name) {
            Some(table) => table,
            None => return Ok(None),
        };
        let payment_schema = match self.table_schema(payment_name) {
            Some(table) => table,
            None => return Ok(None),
        };
        let order_item_schema = match self.table_schema(order_item_name) {
            Some(table) => table,
            None => return Ok(None),
        };
        let item_schema = match self.table_schema(item_name) {
            Some(table) => table,
            None => return Ok(None),
        };
        if !generated_columns_are_stored(order_schema)
            || !generated_columns_are_stored(payment_schema)
            || !generated_columns_are_stored(order_item_schema)
            || !generated_columns_are_stored(item_schema)
        {
            return Ok(None);
        }

        let Some(order_source) = self.visible_table_row_source(order_name) else {
            return Ok(None);
        };
        let Some(payment_source) = self.visible_table_row_source(payment_name) else {
            return Ok(None);
        };
        let Some(order_item_source) = self.visible_table_row_source(order_item_name) else {
            return Ok(None);
        };
        let Some(item_source) = self.visible_table_row_source(item_name) else {
            return Ok(None);
        };

        let Some(order_user_keys) = self.single_column_btree_keys(order_name, "user_id") else {
            return Ok(None);
        };
        let Some(payment_order_keys) = self.single_column_btree_keys(payment_name, "order_id")
        else {
            return Ok(None);
        };
        let Some(order_item_order_keys) =
            self.single_column_btree_keys(order_item_name, "order_id")
        else {
            return Ok(None);
        };
        let Some(item_id_keys) = self.single_column_btree_keys(item_name, "id") else {
            return Ok(None);
        };

        let Some(order_id_index) = schema_column_index(order_schema, "id") else {
            return Ok(None);
        };
        let Some(order_total_amount_index) = schema_column_index(order_schema, "total_amount")
        else {
            return Ok(None);
        };
        let Some(payment_status_index) = schema_column_index(payment_schema, "status") else {
            return Ok(None);
        };
        let Some(order_item_item_id_index) = schema_column_index(order_item_schema, "item_id")
        else {
            return Ok(None);
        };
        let Some(order_item_quantity_index) = schema_column_index(order_item_schema, "quantity")
        else {
            return Ok(None);
        };
        let Some(order_item_price_index) = schema_column_index(order_item_schema, "price") else {
            return Ok(None);
        };
        let Some(item_name_index) = schema_column_index(item_schema, "name") else {
            return Ok(None);
        };

        if order_schema.columns[order_id_index].column_type != crate::catalog::ColumnType::Int64
            || order_item_schema.columns[order_item_item_id_index].column_type
                != crate::catalog::ColumnType::Int64
            || order_item_schema.columns[order_item_quantity_index].column_type
                != crate::catalog::ColumnType::Int64
            || payment_schema.columns[payment_status_index].column_type
                != crate::catalog::ColumnType::Text
            || item_schema.columns[item_name_index].column_type != crate::catalog::ColumnType::Text
        {
            return Ok(None);
        }

        let filter_value = self.eval_expr(
            value_expr,
            &Dataset::empty(),
            &[],
            params,
            &BTreeMap::new(),
            None,
        )?;
        let mut matching_orders = Vec::new();
        for order_row_id in order_user_keys.row_ids_for_value(&filter_value)? {
            let Some(order_row) = order_source.row_by_id(order_row_id)? else {
                continue;
            };
            let Some(order_id) = order_row
                .values()
                .get(order_id_index)
                .and_then(value_as_int64)
            else {
                return Ok(None);
            };
            matching_orders.push((
                order_row_id,
                order_id,
                order_row.values()[order_total_amount_index].clone(),
            ));
        }
        matching_orders.sort_by(|(_, left_id, _), (_, right_id, _)| right_id.cmp(left_id));

        let mut column_names = Vec::with_capacity(select.projection.len());
        for (index, item) in select.projection.iter().enumerate() {
            match item {
                SelectItem::Expr { expr, alias } => {
                    column_names.push(
                        alias
                            .clone()
                            .unwrap_or_else(|| infer_expr_name(expr, index + 1)),
                    );
                }
                SelectItem::Wildcard | SelectItem::QualifiedWildcard(_) => {
                    return Err(DbError::internal(
                        "internal: history fast path expects explicit projection expressions",
                    ));
                }
            }
        }

        let mut rows = Vec::new();
        for (_order_row_id, order_id, order_total_amount) in matching_orders {
            let order_id_value = Value::Int64(order_id);
            let payment_row_ids = payment_order_keys.row_ids_for_value(&order_id_value)?;
            if payment_row_ids.is_empty() {
                continue;
            }
            let order_item_row_ids = order_item_order_keys.row_ids_for_value(&order_id_value)?;
            if order_item_row_ids.is_empty() {
                continue;
            }

            for payment_row_id in payment_row_ids {
                let Some(payment_row) = payment_source.row_by_id(payment_row_id)? else {
                    continue;
                };
                let Some(payment_status) = payment_row.values().get(payment_status_index) else {
                    return Ok(None);
                };

                for order_item_row_id in &order_item_row_ids {
                    let Some(order_item_row) = order_item_source.row_by_id(*order_item_row_id)?
                    else {
                        continue;
                    };
                    let Some(item_id_value) = order_item_row.values().get(order_item_item_id_index)
                    else {
                        return Ok(None);
                    };
                    let item_row_ids = item_id_keys.row_ids_for_value(item_id_value)?;
                    if item_row_ids.is_empty() {
                        continue;
                    }
                    for item_row_id in item_row_ids {
                        let Some(item_row) = item_source.row_by_id(item_row_id)? else {
                            continue;
                        };
                        let Some(item_name_value) = item_row.values().get(item_name_index) else {
                            return Ok(None);
                        };
                        let Some(quantity_value) =
                            order_item_row.values().get(order_item_quantity_index)
                        else {
                            return Ok(None);
                        };
                        let Some(price_value) = order_item_row.values().get(order_item_price_index)
                        else {
                            return Ok(None);
                        };

                        rows.push(QueryRow::new(vec![
                            Value::Int64(order_id),
                            order_total_amount.clone(),
                            payment_status.clone(),
                            item_name_value.clone(),
                            quantity_value.clone(),
                            price_value.clone(),
                        ]));
                    }
                }
            }
        }

        Ok(Some(QueryResult::with_rows(column_names, rows)))
    }
    pub(crate) fn try_execute_benchmark_report_query(
        &self,
        query: &Query,
        params: &[Value],
    ) -> Result<Option<QueryResult>> {
        if !query.ctes.is_empty()
            || query.offset.is_some()
            || query.order_by.len() != 1
            || query.limit.is_none()
        {
            return Ok(None);
        }
        let QueryBody::Select(select) = &query.body else {
            return Ok(None);
        };
        if select.from.len() != 1
            || select.filter.is_none()
            || select.having.is_some()
            || select.distinct
            || !select.distinct_on.is_empty()
            || select.group_by.len() != 2
            || select.projection.len() != 3
        {
            return Ok(None);
        }

        let Some(filter) = select.filter.as_ref() else {
            return Ok(None);
        };
        let Some((filter_table, filter_column, value_expr)) = simple_btree_lookup(filter) else {
            return Ok(None);
        };

        let FromItem::Join {
            left: item_order_items,
            right: order_item,
            kind: JoinKind::Inner,
            constraint: JoinConstraint::On(order_join_on),
        } = &select.from[0]
        else {
            return Ok(None);
        };
        let FromItem::Table {
            name: order_name,
            alias: order_alias,
        } = &**order_item
        else {
            return Ok(None);
        };
        let FromItem::Join {
            left: item_item,
            right: order_item_item,
            kind: JoinKind::Inner,
            constraint: JoinConstraint::On(order_item_join_on),
        } = &**item_order_items
        else {
            return Ok(None);
        };
        let FromItem::Table {
            name: item_name,
            alias: item_alias,
        } = &**item_item
        else {
            return Ok(None);
        };
        let FromItem::Table {
            name: order_item_name,
            alias: order_item_alias,
        } = &**order_item_item
        else {
            return Ok(None);
        };

        let item_binding = TableBindingRef {
            name: item_name,
            alias: item_alias,
        };
        let order_item_binding = TableBindingRef {
            name: order_item_name,
            alias: order_item_alias,
        };
        let order_binding = TableBindingRef {
            name: order_name,
            alias: order_alias,
        };

        if self
            .visible_view(item_name, NameResolutionScope::Session)
            .is_some()
            || self
                .visible_view(order_item_name, NameResolutionScope::Session)
                .is_some()
            || self
                .visible_view(order_name, NameResolutionScope::Session)
                .is_some()
            || self.visible_table_is_temporary(item_name)
            || self.visible_table_is_temporary(order_item_name)
            || self.visible_table_is_temporary(order_name)
        {
            return Ok(None);
        }

        if !matches_filter_binding(order_name, order_alias, filter_table)
            || !identifiers_equal(filter_column, "status")
            || !join_constraint_matches_columns(
                order_item_join_on,
                item_binding,
                "id",
                order_item_binding,
                "item_id",
            )
            || !join_constraint_matches_columns(
                order_join_on,
                order_item_binding,
                "order_id",
                order_binding,
                "id",
            )
        {
            return Ok(None);
        }

        let SelectItem::Expr {
            expr: item_name_expr,
            alias: item_name_alias,
        } = &select.projection[0]
        else {
            return Ok(None);
        };
        if !expr_matches_binding_column(item_name_expr, item_binding, "name") {
            return Ok(None);
        }

        let SelectItem::Expr {
            expr: quantity_sum_expr,
            alias: quantity_sum_alias,
        } = &select.projection[1]
        else {
            return Ok(None);
        };
        if !aggregate_matches_single_binding_column(
            quantity_sum_expr,
            "sum",
            order_item_binding,
            "quantity",
        ) {
            return Ok(None);
        }

        let SelectItem::Expr {
            expr: revenue_expr,
            alias: revenue_alias,
        } = &select.projection[2]
        else {
            return Ok(None);
        };
        if !aggregate_matches_binding_product(
            revenue_expr,
            "sum",
            order_item_binding,
            "quantity",
            order_item_binding,
            "price",
        ) {
            return Ok(None);
        }
        if !order_by_matches_alias_or_projection(
            &query.order_by[0],
            revenue_alias.as_deref(),
            revenue_expr,
            true,
        ) {
            return Ok(None);
        }

        if select.group_by.len() != 2
            || !expr_matches_binding_column(&select.group_by[0], item_binding, "id")
            || !expr_matches_binding_column(&select.group_by[1], item_binding, "name")
        {
            return Ok(None);
        }

        let limit = query
            .limit
            .as_ref()
            .map(|expr| self.eval_constant_i64(expr, params, &BTreeMap::new()))
            .transpose()?
            .map(|value| usize::try_from(value.max(0)).unwrap_or(usize::MAX))
            .unwrap_or(usize::MAX);

        let item_schema = match self.table_schema(item_name) {
            Some(table) => table,
            None => return Ok(None),
        };
        let order_item_schema = match self.table_schema(order_item_name) {
            Some(table) => table,
            None => return Ok(None),
        };
        let order_schema = match self.table_schema(order_name) {
            Some(table) => table,
            None => return Ok(None),
        };
        if !generated_columns_are_stored(item_schema)
            || !generated_columns_are_stored(order_item_schema)
            || !generated_columns_are_stored(order_schema)
        {
            return Ok(None);
        }

        let Some(item_source) = self.visible_table_row_source(item_name) else {
            return Ok(None);
        };
        let Some(order_item_source) = self.visible_table_row_source(order_item_name) else {
            return Ok(None);
        };
        let Some(order_source) = self.visible_table_row_source(order_name) else {
            return Ok(None);
        };

        let Some(order_status_keys) = self.single_column_btree_keys(order_name, "status") else {
            return Ok(None);
        };
        let Some(order_item_order_keys) =
            self.single_column_btree_keys(order_item_name, "order_id")
        else {
            return Ok(None);
        };
        let Some(item_id_keys) = self.single_column_btree_keys(item_name, "id") else {
            return Ok(None);
        };

        let Some(order_id_index) = schema_column_index(order_schema, "id") else {
            return Ok(None);
        };
        let Some(order_item_item_id_index) = schema_column_index(order_item_schema, "item_id")
        else {
            return Ok(None);
        };
        let Some(order_item_quantity_index) = schema_column_index(order_item_schema, "quantity")
        else {
            return Ok(None);
        };
        let Some(order_item_price_index) = schema_column_index(order_item_schema, "price") else {
            return Ok(None);
        };
        let Some(item_name_index) = schema_column_index(item_schema, "name") else {
            return Ok(None);
        };

        if order_schema.columns[order_id_index].column_type != crate::catalog::ColumnType::Int64
            || order_item_schema.columns[order_item_item_id_index].column_type
                != crate::catalog::ColumnType::Int64
            || order_item_schema.columns[order_item_quantity_index].column_type
                != crate::catalog::ColumnType::Int64
            || item_schema.columns[item_name_index].column_type != crate::catalog::ColumnType::Text
        {
            return Ok(None);
        }

        let filter_value = self.eval_expr(
            value_expr,
            &Dataset::empty(),
            &[],
            params,
            &BTreeMap::new(),
            None,
        )?;
        let matching_order_row_ids = order_status_keys.row_ids_for_value(&filter_value)?;
        let mut aggregates = BTreeMap::<i64, BenchmarkReportAggregate>::new();
        for order_row_id in matching_order_row_ids {
            let Some(order_row) = order_source.row_by_id(order_row_id)? else {
                continue;
            };
            let Some(order_id) = order_row
                .values()
                .get(order_id_index)
                .and_then(value_as_int64)
            else {
                return Ok(None);
            };
            let order_id_value = Value::Int64(order_id);
            let order_item_row_ids = order_item_order_keys.row_ids_for_value(&order_id_value)?;
            for order_item_row_id in order_item_row_ids {
                let Some(order_item_row) = order_item_source.row_by_id(order_item_row_id)? else {
                    continue;
                };
                let Some(item_id) = order_item_row
                    .values()
                    .get(order_item_item_id_index)
                    .and_then(value_as_int64)
                else {
                    return Ok(None);
                };
                let Some(quantity) = order_item_row
                    .values()
                    .get(order_item_quantity_index)
                    .and_then(value_as_int64)
                else {
                    return Ok(None);
                };
                let Some(price) = order_item_row
                    .values()
                    .get(order_item_price_index)
                    .and_then(value_as_f64)
                else {
                    return Ok(None);
                };

                let item_row_ids = item_id_keys.row_ids_for_value(&Value::Int64(item_id))?;
                if item_row_ids.is_empty() {
                    continue;
                }
                for item_row_id in item_row_ids {
                    let Some(item_row) = item_source.row_by_id(item_row_id)? else {
                        continue;
                    };
                    let Some(item_name_value) = item_row.values().get(item_name_index) else {
                        return Ok(None);
                    };
                    let Some(item_name_text) = value_as_text(item_name_value) else {
                        return Ok(None);
                    };
                    let aggregate = aggregates.entry(item_id).or_insert_with(|| {
                        BenchmarkReportAggregate::new(item_name_text.to_string())
                    });
                    aggregate.quantity_total += quantity;
                    aggregate.revenue_total += quantity as f64 * price;
                }
            }
        }

        let column_names = vec![
            item_name_alias
                .clone()
                .unwrap_or_else(|| infer_expr_name(item_name_expr, 1)),
            quantity_sum_alias
                .clone()
                .unwrap_or_else(|| infer_expr_name(quantity_sum_expr, 2)),
            revenue_alias
                .clone()
                .unwrap_or_else(|| infer_expr_name(revenue_expr, 3)),
        ];

        let mut rows = aggregates
            .into_values()
            .map(|aggregate| {
                QueryRow::new(vec![
                    Value::Text(aggregate.item_name),
                    Value::Int64(aggregate.quantity_total),
                    Value::Float64(aggregate.revenue_total),
                ])
            })
            .collect::<Vec<_>>();
        rows.sort_by(|left, right| {
            let revenue_ordering = compare_values(&left.values()[2], &right.values()[2])
                .unwrap_or(std::cmp::Ordering::Equal)
                .reverse();
            if revenue_ordering != std::cmp::Ordering::Equal {
                return revenue_ordering;
            }
            compare_values(&left.values()[0], &right.values()[0])
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        if rows.len() > limit {
            rows.truncate(limit);
        }
        Ok(Some(QueryResult::with_rows(column_names, rows)))
    }
    pub(crate) fn try_execute_crm_revenue_raw_aggregate_query(
        &self,
        query: &Query,
    ) -> Result<Option<QueryResult>> {
        if !Self::is_crm_revenue_raw_aggregate_query(query) {
            return Ok(None);
        }

        let Some(companies_schema) = self.table_schema("companies") else {
            return Ok(None);
        };
        let Some(users_schema) = self.table_schema("users") else {
            return Ok(None);
        };
        let Some(invoices_schema) = self.table_schema("invoices") else {
            return Ok(None);
        };
        let Some(companies_id_index) = crm_column_index(companies_schema, "id", ColumnType::Int64)
        else {
            return Ok(None);
        };
        let Some(companies_name_index) =
            crm_column_index(companies_schema, "name", ColumnType::Text)
        else {
            return Ok(None);
        };
        let Some(users_id_index) = crm_column_index(users_schema, "id", ColumnType::Int64) else {
            return Ok(None);
        };
        let Some(users_company_id_index) =
            crm_column_index(users_schema, "company_id", ColumnType::Int64)
        else {
            return Ok(None);
        };
        let Some(invoices_company_id_index) =
            crm_column_index(invoices_schema, "company_id", ColumnType::Int64)
        else {
            return Ok(None);
        };
        let Some(invoices_total_index) =
            crm_column_index(invoices_schema, "total", ColumnType::Float64)
        else {
            return Ok(None);
        };

        let Some(companies_source) = self.visible_table_row_source("companies") else {
            return Ok(None);
        };
        let mut company_names = BTreeMap::new();
        for row in companies_source.rows() {
            let row = row?;
            let Some(company_id) =
                crm_i64_cell(row.values().get(companies_id_index), "companies", "id")?
            else {
                continue;
            };
            let Some(company_name) =
                crm_text_cell(row.values().get(companies_name_index), "companies", "name")?
            else {
                continue;
            };
            company_names.insert(company_id, company_name);
        }

        let Some(users_source) = self.visible_table_row_source("users") else {
            return Ok(None);
        };
        let mut counted_company_users = BTreeSet::new();
        let mut user_counts = BTreeMap::new();
        for row in users_source.rows() {
            let row = row?;
            let Some(user_id) = crm_i64_cell(row.values().get(users_id_index), "users", "id")?
            else {
                continue;
            };
            let Some(company_id) = crm_i64_cell(
                row.values().get(users_company_id_index),
                "users",
                "company_id",
            )?
            else {
                continue;
            };
            if company_names.contains_key(&company_id)
                && counted_company_users.insert((company_id, user_id))
            {
                *user_counts.entry(company_id).or_insert(0_i64) += 1;
            }
        }

        let revenues =
            if let Some(revenues) = self.crm_revenue_from_company_covering_index(&company_names)? {
                revenues
            } else {
                let Some(invoices_source) = self.visible_table_row_source("invoices") else {
                    return Ok(None);
                };
                crm_revenue_from_invoice_rows(
                    invoices_source,
                    invoices_company_id_index,
                    invoices_total_index,
                    &company_names,
                )?
            };

        let mut rows = Vec::with_capacity(company_names.len());
        for (company_id, company_name) in company_names {
            let revenue = revenues.get(&company_id).copied().unwrap_or(0.0);
            let revenue_value = revenues
                .get(&company_id)
                .copied()
                .map(Value::Float64)
                .unwrap_or(Value::Int64(0));
            rows.push((
                revenue,
                QueryRow::new(vec![
                    Value::Text(company_name),
                    Value::Int64(user_counts.get(&company_id).copied().unwrap_or(0)),
                    revenue_value,
                ]),
            ));
        }
        rows.sort_by(|left, right| {
            right
                .0
                .partial_cmp(&left.0)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        Ok(Some(QueryResult::with_rows(
            vec![
                "name".to_string(),
                "user_count".to_string(),
                "revenue".to_string(),
            ],
            rows.into_iter().map(|(_, row)| row).collect(),
        )))
    }
    fn crm_revenue_from_company_covering_index(
        &self,
        company_names: &BTreeMap<i64, String>,
    ) -> Result<Option<BTreeMap<i64, f64>>> {
        let Some(RuntimeIndex::Btree {
            keys,
            covering: Some(covering),
        }) = self.index("idx_invoices_company_revenue")
        else {
            return Ok(None);
        };
        let Some(company_id_offset) = covering.column_position("company_id") else {
            return Ok(None);
        };
        let Some(total_offset) = covering.column_position("total") else {
            return Ok(None);
        };
        let deleted = match keys {
            RuntimeBtreeKeys::UniqueEncoded(_, deleted)
            | RuntimeBtreeKeys::NonUniqueEncoded(_, deleted)
            | RuntimeBtreeKeys::UniqueInt64(_, deleted)
            | RuntimeBtreeKeys::NonUniqueInt64(_, deleted)
            | RuntimeBtreeKeys::UniqueUuid(_, deleted)
            | RuntimeBtreeKeys::NonUniqueUuid(_, deleted) => deleted,
        };

        if let Some(revenues) = crm_revenue_from_covering_dense(
            covering,
            company_id_offset,
            total_offset,
            deleted,
            company_names,
        )? {
            Ok(Some(revenues))
        } else {
            crm_revenue_from_covering_sparse(
                covering,
                company_id_offset,
                total_offset,
                deleted,
                company_names,
            )
            .map(Some)
        }
    }
    fn is_crm_revenue_raw_aggregate_query(query: &Query) -> bool {
        if query.recursive
            || !query.ctes.is_empty()
            || query.limit.is_some()
            || query.offset.is_some()
            || !Self::is_crm_revenue_order_by(&query.order_by)
        {
            return false;
        }

        let QueryBody::Select(select) = &query.body else {
            return false;
        };
        if select.filter.is_some()
            || select.having.is_some()
            || select.distinct
            || !select.distinct_on.is_empty()
            || select.group_by.len() != 2
            || !crm_column(&select.group_by[0], &["c", "companies"], "id")
            || !crm_column(&select.group_by[1], &["c", "companies"], "name")
        {
            return false;
        }

        let [from] = &select.from[..] else {
            return false;
        };
        if !Self::is_crm_revenue_raw_aggregate_from(from) {
            return false;
        }

        let [SelectItem::Expr {
            expr: name_expr,
            alias: name_alias,
        }, SelectItem::Expr {
            expr: user_count_expr,
            alias: user_count_alias,
        }, SelectItem::Expr {
            expr: revenue_expr,
            alias: revenue_alias,
        }] = &select.projection[..]
        else {
            return false;
        };

        name_alias.is_none()
            && user_count_alias
                .as_deref()
                .is_some_and(|alias| identifiers_equal(alias, "user_count"))
            && revenue_alias
                .as_deref()
                .is_some_and(|alias| identifiers_equal(alias, "revenue"))
            && crm_column(name_expr, &["c", "companies"], "name")
            && crm_count_distinct_users(user_count_expr)
            && crm_coalesced_invoice_total_sum(revenue_expr)
    }
    fn is_crm_revenue_order_by(order_by: &[OrderBy]) -> bool {
        let [order] = order_by else {
            return false;
        };
        let Expr::Column { table, column } = &order.expr else {
            return false;
        };
        order.descending
            && order.collation.is_none()
            && table.is_none()
            && identifiers_equal(column, "revenue")
    }
    fn is_crm_revenue_raw_aggregate_from(item: &FromItem) -> bool {
        let FromItem::Join {
            left,
            right,
            kind,
            constraint,
        } = item
        else {
            return false;
        };

        *kind == JoinKind::Left
            && Self::is_crm_companies_users_left_join(left)
            && crm_table(right, "invoices", "i")
            && crm_join_on_columns(
                constraint,
                &["i", "invoices"],
                "user_id",
                &["u", "users"],
                "id",
            )
    }
    fn is_crm_companies_users_left_join(item: &FromItem) -> bool {
        let FromItem::Join {
            left,
            right,
            kind,
            constraint,
        } = item
        else {
            return false;
        };

        *kind == JoinKind::Left
            && crm_table(left, "companies", "c")
            && crm_table(right, "users", "u")
            && crm_join_on_columns(
                constraint,
                &["u", "users"],
                "company_id",
                &["c", "companies"],
                "id",
            )
    }
    pub(crate) fn try_indexed_join(
        &self,
        select: &Select,
        params: &[Value],
        ctes: &BTreeMap<String, Dataset>,
    ) -> Result<Option<Dataset>> {
        let Some(filter) = &select.filter else {
            return Ok(None);
        };
        if select.from.len() != 1 {
            return Ok(None);
        }
        let FromItem::Join {
            left,
            right,
            kind: JoinKind::Inner,
            constraint: JoinConstraint::On(on),
        } = &select.from[0]
        else {
            return Ok(None);
        };
        let (left_name, left_alias) = match &**left {
            FromItem::Table { name, alias } => (name, alias),
            _ => return Ok(None),
        };
        let (right_name, right_alias) = match &**right {
            FromItem::Table { name, alias } => (name, alias),
            _ => return Ok(None),
        };
        if ctes.contains_key(left_name)
            || ctes.contains_key(right_name)
            || self
                .visible_view(left_name, NameResolutionScope::Session)
                .is_some()
            || self
                .visible_view(right_name, NameResolutionScope::Session)
                .is_some()
            || self.visible_table_is_temporary(left_name)
            || self.visible_table_is_temporary(right_name)
        {
            return Ok(None);
        }

        let Some((filter_table, filter_column, value_expr)) = simple_btree_lookup(filter) else {
            return Ok(None);
        };
        let Some(join_equalities) = simple_join_equalities(on) else {
            return Ok(None);
        };

        let left_binding = TableBindingRef {
            name: left_name,
            alias: left_alias,
        };
        let right_binding = TableBindingRef {
            name: right_name,
            alias: right_alias,
        };

        if matches_table_binding(left_binding, filter_table) {
            let Some((filtered_join_columns, probe_join_columns)) =
                orient_join_equalities(&join_equalities, left_binding, right_binding)
            else {
                return Ok(None);
            };
            let Some(left_dataset) = self.indexed_table_lookup(
                left_name,
                left_alias,
                filter_column,
                value_expr,
                params,
                ctes,
            )?
            else {
                return Ok(None);
            };
            return self.indexed_inner_join_filtered(IndexedJoinPlan {
                filtered_table: left_binding,
                filtered_dataset: &left_dataset,
                filtered_join_columns,
                probe_table: right_binding,
                probe_join_columns,
                filtered_on_left: true,
            });
        }

        if matches_table_binding(right_binding, filter_table) {
            let Some((filtered_join_columns, probe_join_columns)) =
                orient_join_equalities(&join_equalities, right_binding, left_binding)
            else {
                return Ok(None);
            };
            let Some(right_dataset) = self.indexed_table_lookup(
                right_name,
                right_alias,
                filter_column,
                value_expr,
                params,
                ctes,
            )?
            else {
                return Ok(None);
            };
            return self.indexed_inner_join_filtered(IndexedJoinPlan {
                filtered_table: right_binding,
                filtered_dataset: &right_dataset,
                filtered_join_columns,
                probe_table: left_binding,
                probe_join_columns,
                filtered_on_left: false,
            });
        }

        Ok(None)
    }
}
