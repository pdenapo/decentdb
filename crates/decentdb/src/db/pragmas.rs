//! Thematic extraction (mechanical split; no behavior change).

use super::*;

impl Db {
    pub(super) fn execute_pragma_command(&self, command: PragmaCommand) -> Result<QueryResult> {
        match command {
            PragmaCommand::Query(target) => self.execute_pragma_query(target),
            PragmaCommand::Call { target, argument } => self.execute_pragma_call(target, argument),
            PragmaCommand::Set(target, value) => self.execute_pragma_set(target, value),
        }
    }
    fn execute_pragma_query(&self, target: PragmaTarget) -> Result<QueryResult> {
        match target.name {
            PragmaName::PageSize => Ok(QueryResult::with_rows(
                vec!["page_size".to_string()],
                vec![QueryRow::new(vec![Value::Int64(i64::from(
                    self.inner.config.page_size,
                ))])],
            )),
            PragmaName::CacheSize => Ok(QueryResult::with_rows(
                vec!["cache_size".to_string()],
                vec![QueryRow::new(vec![Value::Int64(cache_size_pages(
                    &self.inner.config,
                ))])],
            )),
            PragmaName::DatabaseList => {
                let file_name = if is_memory_path(&self.inner.path) {
                    ":memory:".to_string()
                } else {
                    self.inner.path.display().to_string()
                };
                Ok(QueryResult::with_rows(
                    vec!["seq".to_string(), "name".to_string(), "file".to_string()],
                    vec![QueryRow::new(vec![
                        Value::Int64(0),
                        Value::Text("main".to_string()),
                        Value::Text(file_name),
                    ])],
                ))
            }
            PragmaName::TableInfo => Err(DbError::sql(
                "PRAGMA table_info(table_name) requires a table name argument",
            )),
            PragmaName::TableXInfo => Err(DbError::sql(
                "PRAGMA table_xinfo(table_name) requires a table name argument",
            )),
            PragmaName::IndexList => Err(DbError::sql(
                "PRAGMA index_list(table_name) requires a table name argument",
            )),
            PragmaName::IndexInfo => Err(DbError::sql(
                "PRAGMA index_info(index_name) requires an index name argument",
            )),
            PragmaName::IndexXInfo => Err(DbError::sql(
                "PRAGMA index_xinfo(index_name) requires an index name argument",
            )),
            PragmaName::ForeignKeyList => Err(DbError::sql(
                "PRAGMA foreign_key_list(table_name) requires a table name argument",
            )),
            PragmaName::TableList => self.execute_compatibility_select(&format!(
                "SELECT * FROM {}pragma_table_list()",
                pragma_schema_function_prefix(target.schema)
            )),
            PragmaName::IntegrityCheck | PragmaName::QuickCheck => self.integrity_check_results(),
            PragmaName::ForeignKeys => Ok(QueryResult::with_rows(
                vec!["foreign_keys".to_string()],
                vec![QueryRow::new(vec![Value::Int64(1)])],
            )),
            PragmaName::JournalMode => Ok(QueryResult::with_rows(
                vec!["journal_mode".to_string()],
                vec![QueryRow::new(vec![Value::Text("wal".to_string())])],
            )),
            PragmaName::Synchronous => Ok(QueryResult::with_rows(
                vec!["synchronous".to_string()],
                vec![QueryRow::new(vec![Value::Int64(
                    pragma_synchronous_mode_value(self.inner.config.wal_sync_mode),
                )])],
            )),
            PragmaName::WalCheckpoint => self.execute_pragma_wal_checkpoint(None),
            PragmaName::SchemaVersion => {
                let runtime = self.runtime_for_metadata_inspection()?;
                let version = match target.schema {
                    Some(PragmaSchema::Temp) => runtime.temp_schema_cookie,
                    _ => runtime.catalog.schema_cookie,
                };
                Ok(QueryResult::with_rows(
                    vec!["schema_version".to_string()],
                    vec![QueryRow::new(vec![Value::Int64(i64::from(version))])],
                ))
            }
            PragmaName::UserVersion => self.execute_application_pragma_query("user_version"),
            PragmaName::ApplicationId => self.execute_application_pragma_query("application_id"),
            PragmaName::Encoding => Ok(QueryResult::with_rows(
                vec!["encoding".to_string()],
                vec![QueryRow::new(vec![Value::Text("UTF-8".to_string())])],
            )),
            PragmaName::LockingMode => Ok(QueryResult::with_rows(
                vec!["locking_mode".to_string()],
                vec![QueryRow::new(vec![Value::Text("normal".to_string())])],
            )),
            PragmaName::TempStore => Ok(QueryResult::with_rows(
                vec!["temp_store".to_string()],
                vec![QueryRow::new(vec![Value::Int64(1)])],
            )),
            PragmaName::BusyTimeout => Ok(QueryResult::with_rows(
                vec!["busy_timeout".to_string()],
                vec![QueryRow::new(vec![Value::Int64(
                    i64::try_from(self.inner.busy_timeout_ms.load(Ordering::Acquire))
                        .unwrap_or(i64::MAX),
                )])],
            )),
            PragmaName::FlushPlanCache => {
                self.flush_plan_cache()?;
                Ok(QueryResult::with_affected_rows(0))
            }
        }
    }
    fn execute_pragma_call(
        &self,
        target: PragmaTarget,
        argument: Option<String>,
    ) -> Result<QueryResult> {
        match target.name {
            PragmaName::TableInfo => {
                let table_name = pragma_required_argument(&target, argument)?;
                self.execute_pragma_table_info(&table_name, target.schema, false)
            }
            PragmaName::TableXInfo => {
                let table_name = pragma_required_argument(&target, argument)?;
                self.execute_compatibility_select(&format!(
                    "SELECT * FROM {}pragma_table_xinfo({})",
                    pragma_schema_function_prefix(target.schema),
                    sql_string_literal(&table_name)
                ))
            }
            PragmaName::IndexList => {
                let table_name = pragma_required_argument(&target, argument)?;
                self.execute_compatibility_select(&format!(
                    "SELECT * FROM {}pragma_index_list({})",
                    pragma_schema_function_prefix(target.schema),
                    sql_string_literal(&table_name)
                ))
            }
            PragmaName::IndexInfo => {
                let index_name = pragma_required_argument(&target, argument)?;
                self.execute_compatibility_select(&format!(
                    "SELECT * FROM {}pragma_index_info({})",
                    pragma_schema_function_prefix(target.schema),
                    sql_string_literal(&index_name)
                ))
            }
            PragmaName::IndexXInfo => {
                let index_name = pragma_required_argument(&target, argument)?;
                self.execute_compatibility_select(&format!(
                    "SELECT * FROM {}pragma_index_xinfo({})",
                    pragma_schema_function_prefix(target.schema),
                    sql_string_literal(&index_name)
                ))
            }
            PragmaName::ForeignKeyList => {
                let table_name = pragma_required_argument(&target, argument)?;
                self.execute_compatibility_select(&format!(
                    "SELECT * FROM {}pragma_foreign_key_list({})",
                    pragma_schema_function_prefix(target.schema),
                    sql_string_literal(&table_name)
                ))
            }
            PragmaName::FlushPlanCache => {
                self.flush_plan_cache()?;
                Ok(QueryResult::with_affected_rows(0))
            }
            PragmaName::WalCheckpoint => self.execute_pragma_wal_checkpoint(argument.as_deref()),
            other => Err(DbError::sql(format!(
                "PRAGMA {} does not accept call syntax",
                pragma_name_sql(&other)
            ))),
        }
    }
    fn execute_pragma_table_info(
        &self,
        table_name: &str,
        schema: Option<PragmaSchema>,
        extended: bool,
    ) -> Result<QueryResult> {
        let runtime = self.runtime_for_metadata_inspection()?;
        let table = match schema {
            Some(PragmaSchema::Temp) => runtime
                .temp_table_schema(table_name)
                .ok_or_else(|| DbError::sql(format!("unknown temporary table {table_name}")))?,
            Some(PragmaSchema::Main) => runtime
                .catalog
                .table(table_name)
                .ok_or_else(|| DbError::sql(format!("unknown table {table_name}")))?,
            None => runtime
                .table_schema(table_name)
                .ok_or_else(|| DbError::sql(format!("unknown table {table_name}")))?,
        };
        let rows = table
            .columns
            .iter()
            .enumerate()
            .map(|(cid, column)| {
                let mut values = vec![
                    Value::Int64(i64::try_from(cid).unwrap_or(i64::MAX)),
                    Value::Text(column.name.clone()),
                    Value::Text(column.column_type.as_str().to_string()),
                    Value::Int64(if column.nullable { 0 } else { 1 }),
                    column.default_sql.clone().map_or(Value::Null, Value::Text),
                    Value::Int64(if column.primary_key { 1 } else { 0 }),
                ];
                if extended {
                    let hidden = if column.generated_sql.is_none() {
                        0
                    } else if column.generated_stored {
                        3
                    } else {
                        2
                    };
                    values.push(Value::Int64(hidden));
                }
                QueryRow::new(values)
            })
            .collect();
        let mut columns = vec![
            "cid".to_string(),
            "name".to_string(),
            "type".to_string(),
            "notnull".to_string(),
            "dflt_value".to_string(),
            "pk".to_string(),
        ];
        if extended {
            columns.push("hidden".to_string());
        }
        Ok(QueryResult::with_rows(columns, rows))
    }
    fn execute_pragma_wal_checkpoint(&self, mode: Option<&str>) -> Result<QueryResult> {
        if let Some(mode) = mode {
            match mode.trim().to_ascii_uppercase().as_str() {
                "PASSIVE" | "FULL" | "RESTART" | "TRUNCATE" => {}
                other => {
                    return Err(DbError::sql(format!(
                        "PRAGMA wal_checkpoint mode {other} is not supported; expected PASSIVE, FULL, RESTART, or TRUNCATE"
                    )))
                }
            }
        }
        let active_readers = self.inner.wal.active_reader_count()?;
        let retained_snapshot = self.inner.wal.retained_snapshot_lsn().is_some();
        let before_versions = self.inner.wal.version_count()?;
        self.prepare_resident_payload_offset_caches_for_wal_checkpoint()?;
        self.checkpoint_wal()?;
        let after_versions = self.inner.wal.version_count()?;
        let checkpointed = before_versions.saturating_sub(after_versions);
        Ok(QueryResult::with_rows(
            vec![
                "busy".to_string(),
                "log".to_string(),
                "checkpointed".to_string(),
            ],
            vec![QueryRow::new(vec![
                Value::Int64(i64::from(active_readers > 0 || retained_snapshot)),
                Value::Int64(i64::try_from(before_versions).unwrap_or(i64::MAX)),
                Value::Int64(i64::try_from(checkpointed).unwrap_or(i64::MAX)),
            ])],
        ))
    }
    fn execute_pragma_set(&self, target: PragmaTarget, value: PragmaValue) -> Result<QueryResult> {
        match target.name {
            PragmaName::PageSize => {
                let value = pragma_value_i64(&value)?;
                if value == i64::from(self.inner.config.page_size) {
                    Ok(QueryResult::with_affected_rows(0))
                } else {
                    Err(DbError::sql(
                        "PRAGMA page_size cannot be changed on an open database; reopen with DbConfig::page_size",
                    ))
                }
            }
            PragmaName::CacheSize => {
                if pragma_value_i64(&value)? == cache_size_pages(&self.inner.config) {
                    Ok(QueryResult::with_affected_rows(0))
                } else {
                    Err(DbError::sql(
                        "PRAGMA cache_size cannot be changed on an open connection; reopen with DbConfig::cache_size_mb",
                    ))
                }
            }
            PragmaName::IntegrityCheck
            | PragmaName::DatabaseList
            | PragmaName::TableInfo
            | PragmaName::TableXInfo
            | PragmaName::TableList
            | PragmaName::IndexList
            | PragmaName::IndexInfo
            | PragmaName::IndexXInfo
            | PragmaName::ForeignKeyList
            | PragmaName::WalCheckpoint
            | PragmaName::QuickCheck => Err(DbError::sql(format!(
                "PRAGMA {} does not support assignment",
                pragma_name_sql(&target.name)
            ))),
            PragmaName::FlushPlanCache => {
                let value = parse_pragma_text_or_mode(&value, "PRAGMA flush_plan_cache")?;
                if value == "LOCAL" {
                    self.flush_plan_cache()?;
                    Ok(QueryResult::with_affected_rows(0))
                } else {
                    Err(DbError::sql(
                        "PRAGMA flush_plan_cache accepts only local in this release",
                    ))
                }
            }
            PragmaName::ForeignKeys => {
                let value = parse_pragma_bool_value(&value, "PRAGMA foreign_keys")?;
                if value {
                    Ok(QueryResult::with_affected_rows(0))
                } else {
                    Err(DbError::sql(
                        "PRAGMA foreign_keys cannot disable foreign key enforcement in DecentDB",
                    ))
                }
            }
            PragmaName::JournalMode => {
                let mode = parse_pragma_text_or_mode(&value, "PRAGMA journal_mode")?;
                if mode == "WAL" {
                    Ok(QueryResult::with_rows(
                        vec!["journal_mode".to_string()],
                        vec![QueryRow::new(vec![Value::Text("wal".to_string())])],
                    ))
                } else {
                    Err(DbError::sql(
                        "PRAGMA journal_mode supports only WAL in this compatibility slice",
                    ))
                }
            }
            PragmaName::Synchronous => {
                let requested = parse_pragma_synchronous_request(&value, "PRAGMA synchronous")?;
                let current = self.inner.config.wal_sync_mode;
                match requested {
                    SynchronousRequest::Full => {
                        if current == WalSyncMode::Full {
                            Ok(QueryResult::with_affected_rows(0))
                        } else {
                            Err(DbError::sql(
                                "PRAGMA synchronous = FULL requires reopening with DbConfig::wal_sync_mode = Full",
                            ))
                        }
                    }
                    SynchronousRequest::Normal => {
                        if current == WalSyncMode::Normal
                            || matches!(current, WalSyncMode::AsyncCommit { .. })
                        {
                            Ok(QueryResult::with_affected_rows(0))
                        } else {
                            Err(DbError::sql(
                                "PRAGMA synchronous = NORMAL requires reopening with DbConfig::wal_sync_mode = Normal or AsyncCommit",
                            ))
                        }
                    }
                    SynchronousRequest::Off => {
                        if current == WalSyncMode::TestingOnlyUnsafeNoSync {
                            Ok(QueryResult::with_affected_rows(0))
                        } else {
                            Err(DbError::sql(
                                "PRAGMA synchronous = OFF requires reopening with DbConfig::wal_sync_mode = TestingOnlyUnsafeNoSync",
                            ))
                        }
                    }
                    SynchronousRequest::Extra => Err(DbError::sql(
                        "PRAGMA synchronous = EXTRA is not supported by DecentDB",
                    )),
                }
            }
            PragmaName::SchemaVersion => Err(DbError::sql(
                "PRAGMA schema_version does not support assignment",
            )),
            PragmaName::UserVersion => self.execute_application_pragma_set("user_version", &value),
            PragmaName::ApplicationId => {
                self.execute_application_pragma_set("application_id", &value)
            }
            PragmaName::Encoding => {
                let mode = parse_pragma_text_or_mode(&value, "PRAGMA encoding")?;
                if mode == "UTF-8" || mode == "UTF8" {
                    Ok(QueryResult::with_affected_rows(0))
                } else {
                    Err(DbError::sql(
                        "PRAGMA encoding can only be set to UTF-8 in this compatibility slice",
                    ))
                }
            }
            PragmaName::LockingMode => {
                let mode = parse_pragma_text_or_mode(&value, "PRAGMA locking_mode")?;
                if mode == "NORMAL" {
                    Ok(QueryResult::with_affected_rows(0))
                } else {
                    Err(DbError::sql(
                        "PRAGMA locking_mode supports only NORMAL in this compatibility slice",
                    ))
                }
            }
            PragmaName::TempStore => {
                let value = parse_pragma_text_or_mode(&value, "PRAGMA temp_store")?;
                if matches!(value.as_str(), "DEFAULT" | "FILE" | "0" | "1") {
                    Ok(QueryResult::with_affected_rows(0))
                } else if value == "MEMORY" {
                    Err(DbError::sql(
                        "PRAGMA temp_store = MEMORY is not supported in this compatibility slice",
                    ))
                } else {
                    Err(DbError::sql(
                        "PRAGMA temp_store accepts 0, 1, 'DEFAULT', or 'FILE' only",
                    ))
                }
            }
            PragmaName::BusyTimeout => {
                let value = pragma_value_i64(&value)?;
                let value = u64::try_from(value).map_err(|_| {
                    DbError::sql("PRAGMA busy_timeout requires a non-negative integer")
                })?;
                self.inner.busy_timeout_ms.store(value, Ordering::Release);
                Ok(QueryResult::with_affected_rows(0))
            }
        }
    }
}
