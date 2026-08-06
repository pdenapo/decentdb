//! Thematic extraction (mechanical split; no behavior change).

use super::*;

impl EngineRuntime {
    pub(crate) fn clear_fts_eval_context(&self) -> Result<()> {
        self.fts_eval_context
            .lock()
            .map_err(|_| DbError::internal("FTS eval context lock poisoned"))?
            .scores
            .clear();
        Ok(())
    }
    pub(crate) fn apply_select_distinct(
        &self,
        select: &Select,
        dataset: Dataset,
        params: &[Value],
        ctes: &BTreeMap<String, Dataset>,
    ) -> Result<Dataset> {
        if !select.distinct {
            return Ok(dataset);
        }
        if select.distinct_on.iter().any(expr_contains_collation)
            || select.projection.iter().any(select_item_contains_collation)
        {
            return Err(DbError::sql(
                "COLLATE in DISTINCT keys is not supported in this compatibility slice",
            ));
        }

        let Dataset { columns, rows } = dataset;
        let rows = if select.distinct_on.is_empty() {
            deduplicate_rows_stable(Arc::unwrap_or_clone(rows))?
        } else {
            let key_dataset = Dataset::with_rows(columns.clone(), Vec::new());
            let mut seen = BTreeSet::new();
            let mut distinct_rows = Vec::new();
            for row in Arc::unwrap_or_clone(rows) {
                let key = select
                    .distinct_on
                    .iter()
                    .map(|expr| self.eval_expr(expr, &key_dataset, &row, params, ctes, None))
                    .collect::<Result<Vec<_>>>()?;
                if seen.insert(row_identity(&key)?) {
                    distinct_rows.push(row);
                }
            }
            distinct_rows
        };

        Ok(Dataset::with_rows(columns, rows))
    }
    pub(crate) fn try_view_filter_pushdown(
        &self,
        select: &Select,
        params: &[Value],
        ctes: &BTreeMap<String, Dataset>,
    ) -> Result<Option<Dataset>> {
        let Some(filter) = select.filter.as_ref() else {
            return Ok(None);
        };
        if select.from.len() != 1 {
            return Ok(None);
        }
        let FromItem::Table { name, alias } = &select.from[0] else {
            return Ok(None);
        };
        if ctes.contains_key(name) {
            return Ok(None);
        }
        let Some(view) = self.visible_view(name, NameResolutionScope::Session) else {
            return Ok(None);
        };
        let Some((table_qualifier, filter_column, value_expr)) = simple_btree_lookup(filter) else {
            return Ok(None);
        };
        let view_binding = alias.as_deref().unwrap_or(name.as_str());
        if table_qualifier.is_some_and(|table| !identifiers_equal(table, view_binding)) {
            return Ok(None);
        }

        let mut query = (*self.cached_view_query(view)?).clone();
        if query.recursive
            || !query.ctes.is_empty()
            || !query.order_by.is_empty()
            || query.limit.is_some()
            || query.offset.is_some()
        {
            return Ok(None);
        }
        let QueryBody::Select(view_select) = &mut query.body else {
            return Ok(None);
        };
        if view_select.distinct
            || !view_select.distinct_on.is_empty()
            || !view_select.group_by.is_empty()
            || view_select.having.is_some()
            || projection_has_aggregate_items(&view_select.projection)
        {
            return Ok(None);
        }
        let Some(view_expr) =
            view_projection_expr_for_output_column(&view_select.projection, filter_column)
        else {
            return Ok(None);
        };
        let pushed_filter = Expr::Binary {
            left: Box::new(view_expr),
            op: BinaryOp::Eq,
            right: Box::new(value_expr.clone()),
        };
        view_select.filter = match view_select.filter.take() {
            Some(existing) => Some(Expr::Binary {
                left: Box::new(existing),
                op: BinaryOp::And,
                right: Box::new(pushed_filter),
            }),
            None => Some(pushed_filter),
        };

        let mut dataset = if view.temporary {
            self.evaluate_query(&query, params, ctes)?
        } else {
            let persistent_runtime = self.persistent_resolution_runtime();
            persistent_runtime.evaluate_query(&query, params, ctes)?
        };
        if let Some(alias) = alias {
            for column in &mut dataset.columns {
                column.table = Some(alias.clone());
            }
        } else {
            for column in &mut dataset.columns {
                column.table = Some(view.name.clone());
            }
        }
        Ok(Some(dataset))
    }
    pub(crate) fn evaluate_values_body(
        &self,
        rows: &[Vec<Expr>],
        params: &[Value],
        ctes: &BTreeMap<String, Dataset>,
    ) -> Result<Dataset> {
        self.evaluate_values_body_inner(rows, params, ctes, &Dataset::empty(), &[])
    }
    pub(crate) fn evaluate_values_body_with_outer(
        &self,
        rows: &[Vec<Expr>],
        params: &[Value],
        ctes: &BTreeMap<String, Dataset>,
        outer_dataset: &Dataset,
        outer_row: &[Value],
    ) -> Result<Dataset> {
        self.evaluate_values_body_inner(rows, params, ctes, outer_dataset, outer_row)
    }
    fn evaluate_values_body_inner(
        &self,
        rows: &[Vec<Expr>],
        params: &[Value],
        ctes: &BTreeMap<String, Dataset>,
        scope_dataset: &Dataset,
        scope_row: &[Value],
    ) -> Result<Dataset> {
        let width = rows.first().map_or(0, Vec::len);
        let mut columns = Vec::with_capacity(width);
        if let Some(first_row) = rows.first() {
            for (index, expr) in first_row.iter().enumerate() {
                columns.push(ColumnBinding::visible(
                    None,
                    infer_expr_name(expr, index + 1),
                ));
            }
        }

        let mut result_rows = Vec::with_capacity(rows.len());
        for row in rows {
            if row.len() != width {
                return Err(DbError::sql(
                    "VALUES rows must all have the same number of columns",
                ));
            }
            let values = row
                .iter()
                .map(|expr| self.eval_expr(expr, scope_dataset, scope_row, params, ctes, None))
                .collect::<Result<Vec<_>>>()?;
            result_rows.push(values);
        }
        Ok(Dataset::with_rows(columns, result_rows))
    }
    pub(crate) fn evaluate_from_item(
        &self,
        item: &FromItem,
        params: &[Value],
        ctes: &BTreeMap<String, Dataset>,
    ) -> Result<Dataset> {
        self.evaluate_from_item_in_scope(item, params, ctes, &Dataset::empty(), &[])
    }
    pub(crate) fn evaluate_from_item_in_scope(
        &self,
        item: &FromItem,
        params: &[Value],
        ctes: &BTreeMap<String, Dataset>,
        scope_dataset: &Dataset,
        scope_row: &[Value],
    ) -> Result<Dataset> {
        match item {
            FromItem::Table { name, alias } => {
                if let Some(dataset) = ctes.get(name) {
                    let mut columns = dataset.columns.clone();
                    if let Some(alias) = alias {
                        for column in &mut columns {
                            column.table = Some(alias.clone());
                        }
                    }
                    return Ok(dataset.share_rows(columns));
                }
                if let Some(mut dataset) = self.compatibility_virtual_table(name)? {
                    if let Some(alias) = alias {
                        for column in &mut dataset.columns {
                            column.table = Some(alias.clone());
                        }
                    }
                    return Ok(dataset);
                }
                if let Some(view) = self.visible_view(name, NameResolutionScope::Session) {
                    let query = self.cached_view_query(view)?;
                    let mut dataset = if view.temporary {
                        self.evaluate_query(query.as_ref(), params, ctes)?
                    } else {
                        let persistent_runtime = self.persistent_resolution_runtime();
                        persistent_runtime.evaluate_query(query.as_ref(), params, ctes)?
                    };
                    if let Some(alias) = alias {
                        for column in &mut dataset.columns {
                            column.table = Some(alias.clone());
                        }
                    } else {
                        for column in &mut dataset.columns {
                            column.table = Some(view.name.clone());
                        }
                    }
                    return Ok(dataset);
                }
                let table = self
                    .table_schema(name)
                    .ok_or_else(|| DbError::sql(format!("unknown table or view {name}")))?;
                let row_source = self.visible_table_row_source(name).ok_or_else(|| {
                    DbError::internal(format!(
                        "table row source for {name} was not loaded before FROM evaluation"
                    ))
                })?;
                self.dataset_from_visible_row_source(table, row_source, alias)
            }
            FromItem::Subquery {
                query,
                alias,
                column_names,
                lateral,
            } => {
                let mut dataset = if *lateral || query_references_outer_scope(query, scope_dataset)
                {
                    self.evaluate_query_with_outer(query, params, ctes, scope_dataset, scope_row)?
                } else {
                    self.evaluate_query(query, params, ctes)?
                };
                if !column_names.is_empty() {
                    if column_names.len() != dataset.columns.len() {
                        return Err(DbError::sql(format!(
                            "subquery alias {} expected {} column names but produced {} columns",
                            alias,
                            column_names.len(),
                            dataset.columns.len()
                        )));
                    }
                    for (binding, column_name) in dataset.columns.iter_mut().zip(column_names) {
                        binding.name = column_name.clone();
                    }
                }
                for column in &mut dataset.columns {
                    column.table = Some(alias.clone());
                }
                Ok(dataset)
            }
            FromItem::Function {
                name,
                args,
                alias,
                lateral,
            } => {
                let eval_dataset = if *lateral {
                    scope_dataset
                } else {
                    &Dataset::empty()
                };
                let eval_row = if *lateral { scope_row } else { &[] };
                let values = args
                    .iter()
                    .map(|expr| self.eval_expr(expr, eval_dataset, eval_row, params, ctes, None))
                    .collect::<Result<Vec<_>>>()?;
                self.evaluate_table_function(name, values, alias)
            }
            FromItem::Join {
                left,
                right,
                kind,
                constraint,
            } => {
                let left =
                    self.evaluate_from_item_in_scope(left, params, ctes, scope_dataset, scope_row)?;
                if from_item_is_lateral(right) {
                    return self.evaluate_join_with_lateral_right(
                        left,
                        right,
                        *kind,
                        constraint,
                        params,
                        ctes,
                        scope_dataset,
                        scope_row,
                    );
                }
                if matches!(
                    kind,
                    JoinKind::Inner | JoinKind::Left | JoinKind::Right | JoinKind::Full
                ) {
                    if let Some(dataset) = self.try_indexed_equi_join_with_right_table(
                        &left, right, *kind, constraint, ctes,
                    )? {
                        return Ok(dataset);
                    }
                    if let Some(dataset) = self.try_indexed_equi_join_with_right_cte(
                        &left, right, constraint, *kind, ctes,
                    )? {
                        return Ok(dataset);
                    }
                }
                let right = self.evaluate_from_item_in_scope(
                    right,
                    params,
                    ctes,
                    scope_dataset,
                    scope_row,
                )?;
                nested_loop_join(left, right, *kind, constraint, self, params, ctes)
            }
        }
    }
    fn evaluate_table_function(
        &self,
        name: &str,
        values: Vec<Value>,
        alias: &Option<String>,
    ) -> Result<Dataset> {
        let table_name = alias.clone().unwrap_or_else(|| name.to_string());
        match name {
            "json_each" | "pg_catalog.json_each" => {
                self.evaluate_json_table_function(table_name, values, false)
            }
            "json_tree" | "pg_catalog.json_tree" => {
                self.evaluate_json_table_function(table_name, values, true)
            }
            "generate_series" | "pg_catalog.generate_series" => {
                self.evaluate_generate_series(table_name, values)
            }
            "pragma_table_info" | "main.pragma_table_info" | "temp.pragma_table_info" => {
                self.evaluate_pragma_table_info_function(table_name, values, false)
            }
            "pragma_table_xinfo" | "main.pragma_table_xinfo" | "temp.pragma_table_xinfo" => {
                self.evaluate_pragma_table_info_function(table_name, values, true)
            }
            "pragma_table_list" | "main.pragma_table_list" | "temp.pragma_table_list" => {
                self.evaluate_pragma_table_list_function(table_name, values)
            }
            "pragma_index_list" | "main.pragma_index_list" | "temp.pragma_index_list" => {
                self.evaluate_pragma_index_list_function(table_name, values)
            }
            "pragma_index_info" | "main.pragma_index_info" | "temp.pragma_index_info" => {
                self.evaluate_pragma_index_info_function(table_name, values, false)
            }
            "pragma_index_xinfo" | "main.pragma_index_xinfo" | "temp.pragma_index_xinfo" => {
                self.evaluate_pragma_index_info_function(table_name, values, true)
            }
            "pragma_foreign_key_list"
            | "main.pragma_foreign_key_list"
            | "temp.pragma_foreign_key_list" => {
                self.evaluate_pragma_foreign_key_list_function(table_name, values)
            }
            "pragma_database_list" | "main.pragma_database_list" | "temp.pragma_database_list" => {
                self.evaluate_pragma_database_list_function(table_name, values)
            }
            other => {
                if let Some(dataset) = crate::extensions::evaluate_table_function_from_runtime(
                    self, other, values, table_name,
                )? {
                    return Ok(dataset);
                }
                Err(DbError::sql(format!("unsupported table function {other}")))
            }
        }
    }
    fn compatibility_virtual_table(&self, name: &str) -> Result<Option<Dataset>> {
        let normalized = name.to_ascii_lowercase();
        let table_name = match normalized.as_str() {
            "sqlite_schema" | "sqlite_master" | "main.sqlite_schema" | "main.sqlite_master" => {
                return Ok(Some(self.sqlite_schema_dataset("sqlite_schema", false)));
            }
            "sqlite_temp_schema" | "sqlite_temp_master" | "temp.sqlite_schema"
            | "temp.sqlite_master" => {
                return Ok(Some(self.sqlite_schema_dataset("sqlite_temp_schema", true)));
            }
            "information_schema.schemata" => {
                return Ok(Some(self.information_schema_schemata_dataset()));
            }
            "information_schema.tables" => {
                return Ok(Some(self.information_schema_tables_dataset()));
            }
            "information_schema.columns" => {
                return Ok(Some(self.information_schema_columns_dataset()));
            }
            "sys_audit_context" => {
                return self.sys_audit_context_dataset().map(Some);
            }
            _ => name,
        };
        let _ = table_name;
        Ok(None)
    }
    fn sys_audit_context_dataset(&self) -> Result<Dataset> {
        let context = self
            .audit_context
            .lock()
            .map_err(|_| DbError::internal("audit context lock poisoned"))?
            .snapshot();
        let rows = context
            .into_iter()
            .map(|(key, value)| vec![Value::Text(key), value])
            .collect::<Vec<_>>();
        Ok(Dataset::with_rows(
            visible_columns("sys_audit_context", &["key", "value"]),
            rows,
        ))
    }
    fn evaluate_generate_series(&self, table_name: String, values: Vec<Value>) -> Result<Dataset> {
        if !(values.len() == 2 || values.len() == 3) {
            return Err(DbError::sql("generate_series expects 2 or 3 arguments"));
        }
        let rows = generate_series_rows(&values)?;
        Ok(Dataset::with_rows(
            vec![ColumnBinding::visible(
                Some(table_name),
                "value".to_string(),
            )],
            rows.into_iter().map(|value| vec![value]).collect(),
        ))
    }
    fn evaluate_pragma_table_info_function(
        &self,
        table_name: String,
        values: Vec<Value>,
        extended: bool,
    ) -> Result<Dataset> {
        let target = one_text_arg("pragma_table_info", values)?;
        let Some(table) = self.table_schema(&target) else {
            return Ok(pragma_table_info_dataset(table_name, &[], extended));
        };
        Ok(pragma_table_info_dataset(
            table_name,
            &table.columns,
            extended,
        ))
    }
    fn evaluate_pragma_table_list_function(
        &self,
        table_name: String,
        values: Vec<Value>,
    ) -> Result<Dataset> {
        if !values.is_empty() {
            return Err(DbError::sql("pragma_table_list expects no arguments"));
        }
        Ok(self.pragma_table_list_dataset(table_name))
    }
    fn evaluate_pragma_index_list_function(
        &self,
        table_name: String,
        values: Vec<Value>,
    ) -> Result<Dataset> {
        let target = one_text_arg("pragma_index_list", values)?;
        let mut rows = Vec::new();
        for (seq, index) in self.indexes_for_table(&target).into_iter().enumerate() {
            rows.push(vec![
                Value::Int64(seq as i64),
                Value::Text(index.name.clone()),
                Value::Int64(i64::from(index.unique)),
                Value::Text(if index.unique { "u" } else { "c" }.to_string()),
                Value::Int64(i64::from(index.predicate_sql.is_some())),
            ]);
        }
        Ok(Dataset::with_rows(
            visible_columns(&table_name, &["seq", "name", "unique", "origin", "partial"]),
            rows,
        ))
    }
    fn evaluate_pragma_index_info_function(
        &self,
        table_name: String,
        values: Vec<Value>,
        extended: bool,
    ) -> Result<Dataset> {
        let target = one_text_arg("pragma_index_info", values)?;
        let rows = self
            .index_by_name(&target)
            .map(|index| index_info_rows(index, extended))
            .unwrap_or_default();
        let columns = if extended {
            visible_columns(
                &table_name,
                &["seqno", "cid", "name", "desc", "coll", "key"],
            )
        } else {
            visible_columns(&table_name, &["seqno", "cid", "name"])
        };
        Ok(Dataset::with_rows(columns, rows))
    }
    fn evaluate_pragma_foreign_key_list_function(
        &self,
        table_name: String,
        values: Vec<Value>,
    ) -> Result<Dataset> {
        let target = one_text_arg("pragma_foreign_key_list", values)?;
        let mut rows = Vec::new();
        if let Some(table) = self.table_schema(&target) {
            rows.extend(foreign_key_rows(table));
        }
        Ok(Dataset::with_rows(
            visible_columns(
                &table_name,
                &[
                    "id",
                    "seq",
                    "table",
                    "from",
                    "to",
                    "on_update",
                    "on_delete",
                    "match",
                ],
            ),
            rows,
        ))
    }
    fn evaluate_pragma_database_list_function(
        &self,
        table_name: String,
        values: Vec<Value>,
    ) -> Result<Dataset> {
        if !values.is_empty() {
            return Err(DbError::sql("pragma_database_list expects no arguments"));
        }
        Ok(Dataset::with_rows(
            visible_columns(&table_name, &["seq", "name", "file"]),
            vec![vec![
                Value::Int64(0),
                Value::Text("main".to_string()),
                Value::Text("main".to_string()),
            ]],
        ))
    }
    fn pragma_table_list_dataset(&self, table_name: String) -> Dataset {
        let mut rows = Vec::new();
        for table in self.catalog.tables.values() {
            if !compat_catalog_object_is_visible(&table.name) {
                continue;
            }
            rows.push(table_list_row(
                "main",
                &table.name,
                "table",
                table.columns.len(),
            ));
        }
        for view in self.catalog.views.values() {
            rows.push(table_list_row(
                "main",
                &view.name,
                "view",
                view.column_names.len(),
            ));
        }
        for table in self.temp_tables.values() {
            rows.push(table_list_row(
                "temp",
                &table.name,
                "table",
                table.columns.len(),
            ));
        }
        for view in self.temp_views.values() {
            rows.push(table_list_row(
                "temp",
                &view.name,
                "view",
                view.column_names.len(),
            ));
        }
        Dataset::with_rows(
            visible_columns(
                &table_name,
                &["schema", "name", "type", "ncol", "wr", "strict"],
            ),
            rows,
        )
    }
    fn sqlite_schema_dataset(&self, table_name: &str, temporary: bool) -> Dataset {
        let mut rows = Vec::new();
        if temporary {
            for table in self.temp_tables.values() {
                if !compat_catalog_object_is_visible(&table.name) {
                    continue;
                }
                rows.push(sqlite_schema_row(
                    "table",
                    &table.name,
                    &table.name,
                    Some(render_compat_create_table(table)),
                ));
            }
            for view in self.temp_views.values() {
                rows.push(sqlite_schema_row(
                    "view",
                    &view.name,
                    &view.name,
                    Some(render_compat_create_view(view)),
                ));
            }
            for index in self.temp_indexes.values() {
                if !compat_catalog_object_is_visible(&index.name)
                    || !compat_catalog_object_is_visible(&index.table_name)
                {
                    continue;
                }
                rows.push(sqlite_schema_row(
                    "index",
                    &index.name,
                    &index.table_name,
                    Some(render_compat_create_index(index)),
                ));
            }
        } else {
            for table in self.catalog.tables.values() {
                if !compat_catalog_object_is_visible(&table.name) {
                    continue;
                }
                rows.push(sqlite_schema_row(
                    "table",
                    &table.name,
                    &table.name,
                    Some(render_compat_create_table(table)),
                ));
            }
            for view in self.catalog.views.values() {
                rows.push(sqlite_schema_row(
                    "view",
                    &view.name,
                    &view.name,
                    Some(render_compat_create_view(view)),
                ));
            }
            for index in self.catalog.indexes.values() {
                if !compat_catalog_object_is_visible(&index.name)
                    || !compat_catalog_object_is_visible(&index.table_name)
                {
                    continue;
                }
                rows.push(sqlite_schema_row(
                    "index",
                    &index.name,
                    &index.table_name,
                    Some(render_compat_create_index(index)),
                ));
            }
            for trigger in self.catalog.triggers.values() {
                rows.push(sqlite_schema_row(
                    "trigger",
                    &trigger.name,
                    &trigger.target_name,
                    Some(render_compat_create_trigger(trigger)),
                ));
            }
        }
        Dataset::with_rows(
            visible_columns(table_name, &["type", "name", "tbl_name", "rootpage", "sql"]),
            rows,
        )
    }
    fn information_schema_schemata_dataset(&self) -> Dataset {
        let table_name = "schemata";
        let mut rows = vec![
            information_schema_schemata_row("main"),
            information_schema_schemata_row("temp"),
        ];
        for schema in self.catalog.schemas.values() {
            if !identifiers_equal(&schema.name, "main") && !identifiers_equal(&schema.name, "temp")
            {
                rows.push(information_schema_schemata_row(&schema.name));
            }
        }
        Dataset::with_rows(
            visible_columns(
                table_name,
                &[
                    "catalog_name",
                    "schema_name",
                    "schema_owner",
                    "default_character_set_catalog",
                    "default_character_set_schema",
                    "default_character_set_name",
                ],
            ),
            rows,
        )
    }
    fn information_schema_tables_dataset(&self) -> Dataset {
        let table_name = "tables";
        let mut rows = Vec::new();
        for table in self.catalog.tables.values() {
            if !compat_catalog_object_is_visible(&table.name) {
                continue;
            }
            rows.push(information_schema_table_row(
                "main",
                &table.name,
                "BASE TABLE",
            ));
        }
        for view in self.catalog.views.values() {
            rows.push(information_schema_table_row("main", &view.name, "VIEW"));
        }
        for table in self.temp_tables.values() {
            if !compat_catalog_object_is_visible(&table.name) {
                continue;
            }
            rows.push(information_schema_table_row(
                "temp",
                &table.name,
                "LOCAL TEMPORARY",
            ));
        }
        for view in self.temp_views.values() {
            rows.push(information_schema_table_row(
                "temp",
                &view.name,
                "LOCAL TEMPORARY",
            ));
        }
        Dataset::with_rows(
            visible_columns(
                table_name,
                &["table_catalog", "table_schema", "table_name", "table_type"],
            ),
            rows,
        )
    }
    fn information_schema_columns_dataset(&self) -> Dataset {
        let table_name = "columns";
        let mut rows = Vec::new();
        for table in self.catalog.tables.values() {
            if !compat_catalog_object_is_visible(&table.name) {
                continue;
            }
            rows.extend(information_schema_column_rows(
                "main",
                &table.name,
                &table.columns,
            ));
        }
        for table in self.temp_tables.values() {
            if !compat_catalog_object_is_visible(&table.name) {
                continue;
            }
            rows.extend(information_schema_column_rows(
                "temp",
                &table.name,
                &table.columns,
            ));
        }
        Dataset::with_rows(
            visible_columns(
                table_name,
                &[
                    "table_catalog",
                    "table_schema",
                    "table_name",
                    "column_name",
                    "ordinal_position",
                    "column_default",
                    "is_nullable",
                    "data_type",
                ],
            ),
            rows,
        )
    }
    fn evaluate_json_table_function(
        &self,
        table_name: String,
        values: Vec<Value>,
        recursive: bool,
    ) -> Result<Dataset> {
        if values.len() != 1 {
            return Err(DbError::sql(if recursive {
                "json_tree expects 1 argument"
            } else {
                "json_each expects 1 argument"
            }));
        }
        let rows = if recursive {
            expand_json_tree_rows(&values[0])?
        } else {
            expand_json_each_rows(&values[0])?
        };
        let mut columns = vec![
            ColumnBinding::visible(Some(table_name.clone()), "key".to_string()),
            ColumnBinding::visible(Some(table_name.clone()), "value".to_string()),
            ColumnBinding::visible(Some(table_name.clone()), "type".to_string()),
        ];
        if recursive {
            columns.push(ColumnBinding::visible(Some(table_name), "path".to_string()));
        }
        Ok(Dataset::with_rows(columns, rows))
    }
    pub(crate) fn dataset_from_row_id_set(
        &self,
        table: &TableSchema,
        row_source: Option<&TableRowSource>,
        alias: &Option<String>,
        row_ids: RuntimeRowIdSet<'_>,
        include_hidden_row_id: bool,
    ) -> Result<Dataset> {
        let table_name = alias.clone().unwrap_or_else(|| table.name.clone());
        let mut rows = Vec::with_capacity(row_ids.len());
        let mut row_lookup_error = None;
        row_ids.for_each(|row_id| {
            if row_lookup_error.is_some() {
                return;
            }
            match row_source
                .map(|source| source.row_by_id(row_id))
                .transpose()
            {
                Ok(Some(Some(row))) => {
                    let mut values = row.values().to_vec();
                    if include_hidden_row_id {
                        values.push(Value::Int64(row.row_id()));
                    }
                    rows.push(values);
                }
                Ok(Some(None)) | Ok(None) => {}
                Err(error) => row_lookup_error = Some(error),
            }
        });
        if let Some(error) = row_lookup_error {
            return Err(error);
        }
        let mut columns = table
            .columns
            .iter()
            .map(|column| {
                ColumnBinding::visible_source(
                    Some(table_name.clone()),
                    Some(table.name.clone()),
                    column.name.clone(),
                )
            })
            .collect::<Vec<_>>();
        if include_hidden_row_id {
            columns.push(ColumnBinding::hidden_source(
                Some(table_name),
                Some(table.name.clone()),
                FTS_HIDDEN_ROW_ID_COLUMN.to_string(),
            ));
        }
        Ok(Dataset::with_rows(columns, rows))
    }
    fn dataset_from_visible_row_source(
        &self,
        table: &TableSchema,
        row_source: VisibleTableRowSource<'_>,
        alias: &Option<String>,
    ) -> Result<Dataset> {
        let table_name = alias.clone().unwrap_or_else(|| table.name.clone());
        let mut rows = Vec::with_capacity(row_source.row_count());
        for row in row_source.rows() {
            let row = row?;
            let mut values = row.values().to_vec();
            if !generated_columns_are_stored(table) {
                self.apply_virtual_generated_columns(table, &mut values)?;
            }
            rows.push(values);
        }
        let columns = table
            .columns
            .iter()
            .map(|column| {
                ColumnBinding::visible_source(
                    Some(table_name.clone()),
                    Some(table.name.clone()),
                    column.name.clone(),
                )
            })
            .collect::<Vec<_>>();
        let mut dataset = Dataset::with_rows(columns, rows);
        self.apply_row_policies(table, &mut dataset)?;
        Ok(dataset)
    }
    fn apply_row_policies(&self, table: &TableSchema, dataset: &mut Dataset) -> Result<()> {
        if table.temporary || crate::security::is_security_internal_table(&table.name) {
            return Ok(());
        }
        let policies = self.active_row_policies_for_table(&table.name)?;
        if policies.is_empty() {
            return Ok(());
        }
        let filter_dataset = Dataset::with_rows(dataset.columns.clone(), Vec::new());
        let mut kept = Vec::with_capacity(dataset.rows.len());
        for row in dataset.rows.iter() {
            let mut visible = true;
            for policy in &policies {
                match self.eval_expr(
                    &policy.expr,
                    &filter_dataset,
                    row,
                    &[],
                    &BTreeMap::new(),
                    None,
                )? {
                    Value::Bool(true) => {}
                    Value::Bool(false) | Value::Null => {
                        visible = false;
                        break;
                    }
                    other => {
                        return Err(DbError::sql(format!(
                            "policy {} did not evaluate to BOOL: {other:?}",
                            policy.name
                        )))
                    }
                }
            }
            if visible {
                kept.push(row.clone());
            }
        }
        dataset.set_rows(kept);
        Ok(())
    }
    fn active_row_policies_for_table(&self, table_name: &str) -> Result<Vec<ActiveRowPolicy>> {
        let Some(row_source) = self.visible_table_row_source(crate::security::POLICIES_TABLE)
        else {
            return Ok(Vec::new());
        };
        let mut policies = Vec::new();
        for row in row_source.rows() {
            let row = row?;
            let values = row.values();
            let enabled = matches!(values.get(3), Some(Value::Bool(true)));
            if !enabled {
                continue;
            }
            let Some(Value::Text(policy_name)) = values.first() else {
                continue;
            };
            let Some(Value::Text(policy_table)) = values.get(1) else {
                continue;
            };
            if !identifiers_equal(policy_table, table_name) {
                continue;
            }
            let Some(Value::Text(using_sql)) = values.get(2) else {
                continue;
            };
            policies.push(ActiveRowPolicy {
                name: policy_name.clone(),
                expr: parse_sql_statement(&format!("SELECT {using_sql}")).and_then(
                    |statement| {
                        let Statement::Query(query) = statement else {
                            return Err(DbError::sql("policy expression did not parse as SELECT"));
                        };
                        let QueryBody::Select(select) = query.body else {
                            return Err(DbError::sql("policy expression did not parse as SELECT"));
                        };
                        let Some(SelectItem::Expr { expr, .. }) = select.projection.first() else {
                            return Err(DbError::sql("policy expression is not scalar"));
                        };
                        Ok(expr.clone())
                    },
                )?,
            });
        }
        Ok(policies)
    }
    fn active_column_masks(&self) -> Result<Vec<ActiveColumnMask>> {
        let Some(row_source) = self.visible_table_row_source(crate::security::MASKS_TABLE) else {
            return Ok(Vec::new());
        };
        let mut masks = Vec::new();
        for row in row_source.rows() {
            let row = row?;
            let values = row.values();
            if !matches!(values.get(4), Some(Value::Bool(true))) {
                continue;
            }
            let (
                Some(Value::Text(_mask_name)),
                Some(Value::Text(table_name)),
                Some(Value::Text(column_name)),
                Some(Value::Text(expression_sql)),
            ) = (values.first(), values.get(1), values.get(2), values.get(3))
            else {
                continue;
            };
            masks.push(ActiveColumnMask {
                table_name: table_name.clone(),
                column_name: column_name.clone(),
                expr: parse_sql_statement(&format!("SELECT {expression_sql}")).and_then(
                    |statement| {
                        let Statement::Query(query) = statement else {
                            return Err(DbError::sql("mask expression did not parse as SELECT"));
                        };
                        let QueryBody::Select(select) = query.body else {
                            return Err(DbError::sql("mask expression did not parse as SELECT"));
                        };
                        let Some(SelectItem::Expr { expr, .. }) = select.projection.first() else {
                            return Err(DbError::sql("mask expression is not scalar"));
                        };
                        Ok(expr.clone())
                    },
                )?,
            });
        }
        Ok(masks)
    }
    pub(crate) fn security_rules_active(&self) -> Result<bool> {
        if let Some(row_source) = self.visible_table_row_source(crate::security::POLICIES_TABLE) {
            for row in row_source.rows() {
                if matches!(row?.values().get(3), Some(Value::Bool(true))) {
                    return Ok(true);
                }
            }
        }
        self.security_masks_active()
    }
    pub(crate) fn security_masks_active(&self) -> Result<bool> {
        let Some(row_source) = self.visible_table_row_source(crate::security::MASKS_TABLE) else {
            return Ok(false);
        };
        for row in row_source.rows() {
            if matches!(row?.values().get(4), Some(Value::Bool(true))) {
                return Ok(true);
            }
        }
        Ok(false)
    }
    pub(crate) fn masked_output_value(
        &self,
        binding: &ColumnBinding,
        value: &Value,
        dataset: &Dataset,
        row: &[Value],
        params: &[Value],
        ctes: &BTreeMap<String, Dataset>,
    ) -> Result<Value> {
        let masks = self.active_column_masks()?;
        let exact = masks.iter().find(|mask| {
            identifiers_equal(&mask.column_name, &binding.name)
                && binding
                    .source_table
                    .as_deref()
                    .or(binding.table.as_deref())
                    .is_some_and(|table| identifiers_equal(table, &mask.table_name))
        });
        let display_table = if exact.is_none() {
            binding.table.as_deref().filter(|table| {
                binding
                    .source_table
                    .as_deref()
                    .is_none_or(|source| !identifiers_equal(source, table))
            })
        } else {
            None
        };
        let alias_match = display_table.and_then(|table| {
            let matches = masks
                .iter()
                .filter(|mask| {
                    identifiers_equal(&mask.column_name, &binding.name)
                        && identifiers_equal(&mask.table_name, table)
                })
                .collect::<Vec<_>>();
            if matches.len() == 1 {
                matches.first().copied()
            } else {
                None
            }
        });
        let fallback = if exact.is_none() && alias_match.is_none() {
            let matches = masks
                .iter()
                .filter(|mask| identifiers_equal(&mask.column_name, &binding.name))
                .collect::<Vec<_>>();
            if matches.len() == 1 {
                matches.first().copied()
            } else {
                None
            }
        } else {
            None
        };
        if let Some(mask) = exact.or(alias_match).or(fallback) {
            self.eval_expr(&mask.expr, dataset, row, params, ctes, None)
        } else {
            Ok(value.clone())
        }
    }
}
