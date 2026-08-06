//! Thematic extraction (mechanical split; no behavior change).

use super::*;

pub(crate) fn aggregate_matches_single_binding_column(
    expr: &Expr,
    aggregate_name: &str,
    binding: TableBindingRef<'_>,
    column: &str,
) -> bool {
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
    if !name.eq_ignore_ascii_case(aggregate_name)
        || *distinct
        || *star
        || !order_by.is_empty()
        || *within_group
        || args.len() != 1
    {
        return false;
    }
    expr_matches_binding_column(&args[0], binding, column)
}

pub(crate) fn aggregate_matches_count_star(expr: &Expr) -> bool {
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
    name.eq_ignore_ascii_case("count")
        && *star
        && args.is_empty()
        && !*distinct
        && order_by.is_empty()
        && !*within_group
}

pub(crate) fn aggregate_matches_single_binding_column_or_unqualified(
    expr: &Expr,
    aggregate_name: &str,
    binding: TableBindingRef<'_>,
    column: &str,
) -> bool {
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
    if !name.eq_ignore_ascii_case(aggregate_name)
        || *distinct
        || *star
        || !order_by.is_empty()
        || *within_group
        || args.len() != 1
    {
        return false;
    }
    expr_matches_binding_column_or_unqualified(&args[0], binding, column)
}

pub(crate) fn aggregate_matches_status_case_sum(
    expr: &Expr,
    aggregate_name: &str,
    binding: TableBindingRef<'_>,
    status_column: &str,
    status_value: &str,
) -> bool {
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
    if !name.eq_ignore_ascii_case(aggregate_name)
        || *distinct
        || *star
        || !order_by.is_empty()
        || *within_group
        || args.len() != 1
    {
        return false;
    }
    let Expr::Case {
        operand: None,
        branches,
        else_expr: Some(else_expr),
    } = &args[0]
    else {
        return false;
    };
    if branches.len() != 1
        || !matches!(&branches[0].1, Expr::Literal(Value::Int64(1)))
        || !matches!(&**else_expr, Expr::Literal(Value::Int64(0)))
    {
        return false;
    }
    status_case_condition_matches(&branches[0].0, binding, status_column, status_value)
}

pub(crate) fn aggregate_matches_binding_product(
    expr: &Expr,
    aggregate_name: &str,
    left_binding: TableBindingRef<'_>,
    left_column: &str,
    right_binding: TableBindingRef<'_>,
    right_column: &str,
) -> bool {
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
    if !name.eq_ignore_ascii_case(aggregate_name)
        || *distinct
        || *star
        || !order_by.is_empty()
        || *within_group
        || args.len() != 1
    {
        return false;
    }
    let Expr::Binary { left, op, right } = &args[0] else {
        return false;
    };
    if *op != BinaryOp::Mul {
        return false;
    }
    (expr_matches_binding_column(left, left_binding, left_column)
        && expr_matches_binding_column(right, right_binding, right_column))
        || (expr_matches_binding_column(left, right_binding, right_column)
            && expr_matches_binding_column(right, left_binding, left_column))
}

impl EngineRuntime {
    pub(crate) fn runtime_index_key_values_to_group_values<'a>(
        key: &'a [u8],
        row_id: Option<i64>,
        row_source: Option<VisibleTableRowSource<'a>>,
        projection_indexes: &[usize],
    ) -> Result<Option<Vec<Value>>> {
        if let Some(value) = Self::decode_runtime_index_group_key(key) {
            return Ok(Some(vec![value]));
        }

        let (row_source, row_id) = match (row_source, row_id) {
            (Some(row_source), Some(row_id)) => (row_source, row_id),
            _ => return Ok(None),
        };
        row_source.projected_values_by_id(row_id, projection_indexes)
    }
    pub(crate) fn try_execute_general_grouped_query(
        &self,
        query: &Query,
        params: &[Value],
    ) -> Result<Option<QueryResult>> {
        let Some(plan) = self.analyze_general_grouped_single_table_query(query) else {
            return Ok(None);
        };
        let Some(source) = self.visible_table_row_source(plan.table_name) else {
            return Ok(None);
        };
        Ok(Some(self.execute_general_grouped_from_source(
            source, &plan, params,
        )?))
    }
    fn analyze_general_grouped_single_table_query<'a>(
        &self,
        query: &'a Query,
    ) -> Option<GeneralGroupedSingleTablePlan<'a>> {
        if !query.ctes.is_empty() || query.recursive {
            return None;
        }
        let QueryBody::Select(select) = &query.body else {
            return None;
        };
        if select.group_by.is_empty() && !projection_has_aggregate_items(&select.projection) {
            return None;
        }
        if select.from.len() != 1 {
            return None;
        }
        let FromItem::Table { name, alias } = &select.from[0] else {
            return None;
        };
        if self
            .visible_view(name, NameResolutionScope::Session)
            .is_some()
        {
            return None;
        }
        self.table_schema(name)?;
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
        if select.having.as_ref().is_some_and(expr_contains_window) {
            return None;
        }
        for order in &query.order_by {
            if expr_contains_window(&order.expr) {
                return None;
            }
        }
        if select.filter.as_ref().is_some_and(expr_contains_window) {
            return None;
        }
        for gb in &select.group_by {
            if expr_contains_window(gb) {
                return None;
            }
        }
        Some(GeneralGroupedSingleTablePlan {
            table_name: name,
            table_alias: alias.as_deref(),
            group_by: &select.group_by,
            filter: select.filter.as_ref(),
            projection: &select.projection,
            having: select.having.as_ref(),
            order_by: &query.order_by,
            distinct: select.distinct,
            limit: query.limit.as_ref(),
            offset: query.offset.as_ref(),
        })
    }
    fn execute_general_grouped_from_source(
        &self,
        source: VisibleTableRowSource<'_>,
        plan: &GeneralGroupedSingleTablePlan<'_>,
        params: &[Value],
    ) -> Result<QueryResult> {
        let table = self.table_schema(plan.table_name).ok_or_else(|| {
            DbError::internal(format!(
                "table {} not found for general grouped query",
                plan.table_name
            ))
        })?;

        let binding_name = plan.table_alias.unwrap_or(plan.table_name);
        let columns: Vec<ColumnBinding> = table
            .columns
            .iter()
            .map(|c| ColumnBinding::visible(Some(binding_name.to_string()), c.name.clone()))
            .collect();

        let empty_dataset = Dataset::with_rows(columns.clone(), Vec::new());
        let ctes = BTreeMap::new();
        let needs_virtual_generated = !generated_columns_are_stored(table);

        // Materialize filtered rows into a single flat buffer and store only
        // row indices per group. Previously this path stored full row clones
        // per group and then cloned them again into a per-group `Dataset`;
        // that doubled the copy cost and regressed aggregate-over-full-table
        // queries by ~2x versus the prior `evaluate_grouped_select` path.
        // Indexing into one shared Dataset matches that older path's
        // efficiency while keeping the streaming scan from `source.rows()`.
        let mut all_rows: Vec<Vec<Value>> = Vec::new();
        let mut groups: BTreeMap<Vec<u8>, Vec<usize>> = BTreeMap::new();

        for row_result in source.rows() {
            let row_ref = row_result?;
            let mut values = row_ref.values().to_vec();
            if needs_virtual_generated {
                self.apply_virtual_generated_columns(table, &mut values)?;
            }

            if let Some(filter) = plan.filter {
                let val = self.eval_expr(filter, &empty_dataset, &values, params, &ctes, None)?;
                if !matches!(val, Value::Bool(true)) {
                    continue;
                }
            }

            let key_values: Vec<Value> = plan
                .group_by
                .iter()
                .map(|expr| self.eval_expr(expr, &empty_dataset, &values, params, &ctes, None))
                .collect::<Result<Vec<_>>>()?;
            let key = row_identity(&key_values)?;

            let row_index = all_rows.len();
            all_rows.push(values);
            groups.entry(key).or_default().push(row_index);
        }

        if groups.is_empty() && plan.group_by.is_empty() {
            groups.insert(Vec::new(), Vec::new());
        }

        let result_columns: Vec<ColumnBinding> = plan
            .projection
            .iter()
            .enumerate()
            .map(|(index, item)| match item {
                SelectItem::Expr { expr, alias } => ColumnBinding::visible(
                    None,
                    alias
                        .clone()
                        .unwrap_or_else(|| infer_expr_name(expr, index + 1)),
                ),
                _ => ColumnBinding::visible(None, format!("col{}", index + 1)),
            })
            .collect();

        let has_order_by = !plan.order_by.is_empty();
        let projection_order_plan = projection_order_by_plan(plan.order_by, plan.projection);
        let mut output_with_order: Vec<(Vec<Value>, Vec<Value>)> = Vec::new();

        // Build a single shared dataset that every group indexes into. This
        // replaces the per-group `Dataset::with_rows(columns.clone(),
        // group_rows.clone())` that previously cloned every matching row
        // twice.
        let group_dataset = Dataset::with_rows(columns.clone(), all_rows);

        for group_row_indexes in groups.values() {
            if let Some(having) = plan.having {
                let val =
                    self.eval_group_expr(having, &group_dataset, group_row_indexes, params, &ctes)?;
                if !matches!(val, Value::Bool(true)) {
                    continue;
                }
            }

            let mut output = Vec::with_capacity(plan.projection.len());
            for item in plan.projection {
                let SelectItem::Expr { expr, .. } = item else {
                    return Err(DbError::sql(
                        "wildcards not supported in grouped SELECT output",
                    ));
                };
                output.push(self.eval_group_expr(
                    expr,
                    &group_dataset,
                    group_row_indexes,
                    params,
                    &ctes,
                )?);
            }

            let order_values = if let Some(order_plan) = &projection_order_plan {
                order_plan
                    .iter()
                    .map(|plan| output[plan.projection_index].clone())
                    .collect()
            } else if has_order_by {
                plan.order_by
                    .iter()
                    .map(|order| {
                        self.eval_group_expr(
                            &order.expr,
                            &group_dataset,
                            group_row_indexes,
                            params,
                            &ctes,
                        )
                    })
                    .collect::<Result<Vec<_>>>()?
            } else {
                Vec::new()
            };

            output_with_order.push((output, order_values));
        }

        if has_order_by {
            let mut sort_error = None;
            output_with_order.sort_by(|(_, left_order), (_, right_order)| {
                if let Some(order_plan) = &projection_order_plan {
                    let mut ord = std::cmp::Ordering::Equal;
                    for (i, plan) in order_plan.iter().enumerate() {
                        match compare_values_with_runtime_collation(
                            Some(self),
                            &left_order[i],
                            &right_order[i],
                            plan.collation.clone(),
                        ) {
                            Ok(std::cmp::Ordering::Equal) => continue,
                            Ok(o) => {
                                ord = if plan.descending { o.reverse() } else { o };
                                break;
                            }
                            Err(error) => {
                                if sort_error.is_none() {
                                    sort_error = Some(error);
                                }
                                break;
                            }
                        }
                    }
                    ord
                } else {
                    match compare_query_row_order_values(
                        Some(self),
                        left_order,
                        right_order,
                        plan.order_by,
                    ) {
                        Ok(ordering) => ordering,
                        Err(error) => {
                            if sort_error.is_none() {
                                sort_error = Some(error);
                            }
                            std::cmp::Ordering::Equal
                        }
                    }
                }
            });
            if let Some(error) = sort_error {
                return Err(error);
            }
        }

        let mut rows: Vec<QueryRow> = if plan.distinct {
            let mut seen = BTreeSet::new();
            let mut distinct_rows = Vec::new();
            for (output, _) in output_with_order {
                if seen.insert(row_identity(&output)?) {
                    distinct_rows.push(QueryRow::new(output));
                }
            }
            distinct_rows
        } else {
            output_with_order
                .into_iter()
                .map(|(output, _)| QueryRow::new(output))
                .collect()
        };

        let offset_val = plan
            .offset
            .map(|expr| self.eval_constant_i64(expr, params, &ctes))
            .transpose()?
            .unwrap_or(0);
        let limit_val = plan
            .limit
            .map(|expr| self.eval_constant_i64(expr, params, &ctes))
            .transpose()?;

        let start = usize::try_from(offset_val.max(0)).unwrap_or(usize::MAX);
        if start > 0 || limit_val.is_some() {
            let take = limit_val
                .map(|l| usize::try_from(l.max(0)).unwrap_or(0))
                .unwrap_or(usize::MAX);
            rows = rows.into_iter().skip(start).take(take).collect();
        }

        let column_names: Vec<String> = result_columns.into_iter().map(|c| c.name).collect();
        Ok(QueryResult::with_rows(column_names, rows))
    }
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn apply_simple_grouped_postprocessing<I>(
        &self,
        rows: I,
        column_names: Vec<String>,
        having_bindings: &[ColumnBinding],
        having: Option<&Expr>,
        params: &[Value],
        order_by: Option<&[SimpleOrderByPlan]>,
        limit: Option<usize>,
        offset: usize,
    ) -> Result<QueryResult>
    where
        I: IntoIterator<Item = QueryRow>,
    {
        let bounded_order = order_by
            .and_then(|order_by| limit.map(|limit| (order_by, offset.saturating_add(limit))));
        let mut rows_out = if let Some((_, bounded_row_count)) = bounded_order {
            if bounded_row_count == 0 {
                return Ok(QueryResult::with_rows(column_names, Vec::new()));
            }
            Vec::with_capacity(bounded_row_count)
        } else {
            Vec::new()
        };

        let mut push_row = |row: QueryRow| -> Result<()> {
            if let Some((order_by, bounded_row_count)) = bounded_order {
                push_bounded_projection_ordered_query_row(
                    Some(self),
                    &mut rows_out,
                    row,
                    order_by,
                    bounded_row_count,
                )
            } else {
                rows_out.push(row);
                Ok(())
            }
        };

        if let Some(having) = having {
            let having_dataset = Dataset::with_rows(having_bindings.to_vec(), Vec::new());
            let ctes = BTreeMap::new();
            for row in rows {
                if matches!(
                    self.eval_expr(having, &having_dataset, row.values(), params, &ctes, None)?,
                    Value::Bool(true)
                ) {
                    push_row(row)?;
                }
            }
        } else {
            for row in rows {
                push_row(row)?;
            }
        }

        if let Some((order_by, _)) = bounded_order {
            sort_query_rows_by_projection_order(Some(self), &mut rows_out, order_by)?;
            let rows = rows_out
                .into_iter()
                .skip(offset)
                .take(limit.unwrap_or(usize::MAX))
                .collect();
            return Ok(QueryResult::with_rows(column_names, rows));
        }

        if let Some(order_by) = order_by {
            sort_query_rows_by_projection_order(Some(self), &mut rows_out, order_by)?;
        }

        let rows = rows_out
            .into_iter()
            .skip(offset)
            .take(limit.unwrap_or(usize::MAX))
            .collect();
        Ok(QueryResult::with_rows(column_names, rows))
    }
}
