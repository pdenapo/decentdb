//! Thematic extraction (mechanical split; no behavior change).

use super::*;

impl Db {
    pub(crate) fn sync_temp_state_from_runtime(&self, runtime: &EngineRuntime) -> Result<()> {
        if runtime.temp_schema_cookie == 0
            && runtime.temp_tables.is_empty()
            && runtime.temp_table_data.is_empty()
            && runtime.temp_views.is_empty()
            && runtime.temp_indexes.is_empty()
        {
            return Ok(());
        }
        let changed = {
            let mut state = self
                .inner
                .temp_state
                .lock()
                .map_err(|_| DbError::internal("temp schema lock poisoned"))?;
            let before = state.schema_cookie;
            state.update_from_runtime(runtime);
            before != state.schema_cookie
        };
        if changed {
            crate::plan_cache::PlanCacheInvalidator::on_temp_schema_change(&*self.inner);
        }
        Ok(())
    }
    pub fn sync_init_replica(&self, replica_id: &str) -> Result<()> {
        self.ensure_sync_tables()?;
        self.sync_upsert_metadata("replica_id", replica_id)?;
        self.sync_upsert_metadata("enabled", "true")?;
        self.sync_upsert_metadata("next_sequence", "1")?;
        self.inner.sync_ctx.set_replica_id(replica_id);
        self.inner.sync_ctx.set_enabled(true);
        self.inner.sync_ctx.set_next_sequence(1);
        self.inner.sync_ctx.ensure_journal_open(&self.inner.vfs)?;
        Ok(())
    }
    pub fn sync_create_scope(
        &self,
        name: &str,
        include_tables: &[&str],
        row_filter: Option<&str>,
    ) -> Result<()> {
        self.ensure_sync_tables()?;
        let runtime = self.runtime_for_metadata_inspection()?;
        let validation =
            validate_sync_scope_definition(&runtime, name, include_tables, row_filter)?;
        let created_at_micros = self
            .sync_scope(name)?
            .map(|scope| scope.created_at_micros)
            .unwrap_or_else(current_time_micros);
        let updated_at_micros = current_time_micros();
        let sql = format!(
            "INSERT INTO {table} (name, include_tables_json, row_filter, filter_columns_json, created_at_micros, updated_at_micros) VALUES ({name}, {include_tables_json}, {row_filter}, {filter_columns_json}, {created_at_micros}, {updated_at_micros}) ON CONFLICT (name) DO UPDATE SET include_tables_json = {include_tables_json}, row_filter = {row_filter}, filter_columns_json = {filter_columns_json}, updated_at_micros = {updated_at_micros}",
            table = crate::sync::SCOPES_TABLE,
            name = sql_text_literal(&validation.name),
            include_tables_json = sql_text_literal(
                &serde_json::to_string(&validation.include_tables)
                    .map_err(|error| DbError::internal(format!("failed to encode scope tables: {error}")))?,
            ),
            row_filter = sql_nullable_text_literal(validation.row_filter.as_deref()),
            filter_columns_json = sql_text_literal(
                &serde_json::to_string(&validation.filter_columns)
                    .map_err(|error| DbError::internal(format!("failed to encode scope columns: {error}")))?,
            ),
            created_at_micros = created_at_micros,
            updated_at_micros = updated_at_micros,
        );
        let _ = self.execute(&sql)?;
        Ok(())
    }
    pub fn sync_drop_scope(&self, name: &str) -> Result<bool> {
        self.ensure_sync_tables()?;
        let scope_name = name.trim();
        if scope_name.is_empty() {
            return Err(DbError::sql("sync scope name must not be empty"));
        }
        if self
            .sync_peer_scope_bindings()?
            .iter()
            .any(|binding| binding.scope_name.eq_ignore_ascii_case(scope_name))
        {
            return Err(DbError::sql(format!(
                "cannot drop sync scope '{scope_name}' while peer bindings exist"
            )));
        }
        let sql = format!(
            "DELETE FROM {} WHERE name = {}",
            crate::sync::SCOPES_TABLE,
            sql_text_literal(scope_name)
        );
        let result = self.execute(&sql)?;
        Ok(result.affected_rows() > 0)
    }
    pub fn sync_scope(&self, name: &str) -> Result<Option<SyncScope>> {
        self.ensure_sync_tables()?;
        let scope_name = name.trim();
        if scope_name.is_empty() {
            return Err(DbError::sql("sync scope name must not be empty"));
        }
        let sql = format!(
            "SELECT name, include_tables_json, row_filter, filter_columns_json, created_at_micros, updated_at_micros FROM {} WHERE name = {}",
            crate::sync::SCOPES_TABLE,
            sql_text_literal(scope_name)
        );
        match self.execute(&sql) {
            Ok(result) => Ok(result.rows().first().map(sync_scope_from_row).transpose()?),
            Err(error) => {
                let message = error.to_string();
                if message.contains("no such table") || message.contains("unknown table") {
                    Ok(None)
                } else {
                    Err(error)
                }
            }
        }
    }
    pub fn sync_scopes(&self) -> Result<Vec<SyncScope>> {
        self.ensure_sync_tables()?;
        let sql = format!(
            "SELECT name, include_tables_json, row_filter, filter_columns_json, created_at_micros, updated_at_micros FROM {} ORDER BY name",
            crate::sync::SCOPES_TABLE
        );
        match self.execute(&sql) {
            Ok(result) => result.rows().iter().map(sync_scope_from_row).collect(),
            Err(error) => {
                let message = error.to_string();
                if message.contains("no such table") || message.contains("unknown table") {
                    Ok(Vec::new())
                } else {
                    Err(error)
                }
            }
        }
    }
    pub fn sync_bind_peer_scope(&self, peer_name: &str, scope_name: &str) -> Result<()> {
        self.ensure_sync_tables()?;
        let peer_name = peer_name.trim();
        if peer_name.is_empty() {
            return Err(DbError::sql("sync peer name must not be empty"));
        }
        if self.sync_peer(peer_name)?.is_none() {
            return Err(DbError::sql(format!("sync peer '{peer_name}' not found")));
        }
        let scope = self
            .sync_scope(scope_name)?
            .ok_or_else(|| DbError::sql(format!("sync scope '{scope_name}' not found")))?;
        let existing = self.sync_peer_scope_binding_row(peer_name)?;
        let created_at_micros = existing
            .as_ref()
            .map(|binding| binding.created_at_micros)
            .unwrap_or_else(current_time_micros);
        let updated_at_micros = current_time_micros();
        let sql = format!(
            "INSERT INTO {table} (peer_name, scope_name, created_at_micros, updated_at_micros) VALUES ({peer_name}, {scope_name}, {created_at_micros}, {updated_at_micros}) ON CONFLICT (peer_name) DO UPDATE SET scope_name = {scope_name}, updated_at_micros = {updated_at_micros}",
            table = crate::sync::PEER_SCOPES_TABLE,
            peer_name = sql_text_literal(peer_name),
            scope_name = sql_text_literal(&scope.name),
            created_at_micros = created_at_micros,
            updated_at_micros = updated_at_micros,
        );
        let _ = self.execute(&sql)?;
        Ok(())
    }
    pub fn sync_unbind_peer_scope(&self, peer_name: &str) -> Result<bool> {
        self.ensure_sync_tables()?;
        let peer_name = peer_name.trim();
        if peer_name.is_empty() {
            return Err(DbError::sql("sync peer name must not be empty"));
        }
        let sql = format!(
            "DELETE FROM {} WHERE peer_name = {}",
            crate::sync::PEER_SCOPES_TABLE,
            sql_text_literal(peer_name)
        );
        let result = self.execute(&sql)?;
        Ok(result.affected_rows() > 0)
    }
    pub fn sync_peer_scope(&self, peer_name: &str) -> Result<Option<SyncPeerScopeBinding>> {
        self.ensure_sync_tables()?;
        let peer_name = peer_name.trim();
        if peer_name.is_empty() {
            return Err(DbError::sql("sync peer name must not be empty"));
        }
        self.sync_peer_scope_binding_row(peer_name)
    }
    pub fn sync_peer_scope_definition(&self, peer_name: &str) -> Result<Option<SyncScope>> {
        let binding = match self.sync_peer_scope(peer_name)? {
            Some(binding) => binding,
            None => return Ok(None),
        };
        match self.sync_scope(&binding.scope_name)? {
            Some(scope) => Ok(Some(scope)),
            None => Err(DbError::sql(format!(
                "sync scope '{}' bound to peer '{}' was not found",
                binding.scope_name, binding.peer_name
            ))),
        }
    }
    pub fn sync_peer_scope_bindings(&self) -> Result<Vec<SyncPeerScopeBinding>> {
        self.ensure_sync_tables()?;
        let sql = format!(
            "SELECT peer_name, scope_name, created_at_micros, updated_at_micros FROM {} ORDER BY peer_name",
            crate::sync::PEER_SCOPES_TABLE
        );
        match self.execute(&sql) {
            Ok(result) => result
                .rows()
                .iter()
                .map(sync_peer_scope_binding_from_row)
                .collect(),
            Err(error) => {
                let message = error.to_string();
                if message.contains("no such table") || message.contains("unknown table") {
                    Ok(Vec::new())
                } else {
                    Err(error)
                }
            }
        }
    }
    pub fn sync_export_batch_for_scope(
        &self,
        scope_name: &str,
        since_seq: u64,
        limit: usize,
    ) -> Result<SyncChangeBatch> {
        let scope = self
            .sync_scope(scope_name)?
            .ok_or_else(|| DbError::sql(format!("sync scope '{scope_name}' not found")))?;
        let records = self.sync_pending_changes(since_seq, limit)?;
        let source_replica_id = records.first().map(|record| record.replica_id.clone());
        let source_high_watermark = records.last().map(|record| record.sequence);
        let filtered = self.sync_filter_records_for_scope(&scope, records)?;
        SyncChangeBatch::scoped_from_records(filtered, source_replica_id, source_high_watermark)
    }
    pub fn sync_import_batch_for_scope(
        &self,
        scope_name: &str,
        batch: &SyncChangeBatch,
    ) -> Result<SyncImportSummary> {
        batch.validate()?;
        let scope = self
            .sync_scope(scope_name)?
            .ok_or_else(|| DbError::sql(format!("sync scope '{scope_name}' not found")))?;
        self.sync_validate_batch_for_scope(&scope, batch)?;
        self.sync_import_batch(batch)
    }
    pub fn sync_import_batch_for_scope_with_policy(
        &self,
        scope_name: &str,
        batch: &SyncChangeBatch,
        policy: SyncConflictPolicy,
    ) -> Result<SyncImportSummary> {
        batch.validate()?;
        let scope = self
            .sync_scope(scope_name)?
            .ok_or_else(|| DbError::sql(format!("sync scope '{scope_name}' not found")))?;
        self.sync_validate_batch_for_scope(&scope, batch)?;
        self.sync_import_batch_with_policy(batch, policy)
    }
    pub fn sync_add_peer(&self, name: &str, endpoint: &str, token_env: Option<&str>) -> Result<()> {
        let name = name.trim();
        if name.is_empty() {
            return Err(DbError::sql("sync peer name must not be empty"));
        }
        if !(endpoint.starts_with("http://") || endpoint.starts_with("https://")) {
            return Err(DbError::sql(
                "sync peer endpoint must start with http:// or https://",
            ));
        }
        if token_env.is_some_and(|value| value.trim().is_empty()) {
            return Err(DbError::sql("sync peer token_env must not be empty"));
        }

        self.ensure_sync_tables()?;
        let now = current_time_micros();
        let token_sql = token_env
            .map(sql_text_literal)
            .unwrap_or_else(|| "NULL".to_string());
        let sql = format!(
            "INSERT INTO {table} (name, endpoint, token_env, created_at_micros, updated_at_micros) VALUES ({name}, {endpoint}, {token_env}, {now}, {now}) ON CONFLICT (name) DO UPDATE SET endpoint = {endpoint}, token_env = {token_env}, updated_at_micros = {now}",
            table = crate::sync::PEERS_TABLE,
            name = sql_text_literal(name),
            endpoint = sql_text_literal(endpoint),
            token_env = token_sql,
            now = now,
        );
        let _ = self.execute(&sql)?;
        Ok(())
    }
    pub fn sync_remove_peer(&self, name: &str) -> Result<bool> {
        self.ensure_sync_tables()?;
        let sql = format!(
            "DELETE FROM {} WHERE name = {}",
            crate::sync::PEERS_TABLE,
            sql_text_literal(name)
        );
        let result = self.execute(&sql)?;
        Ok(result.affected_rows() > 0)
    }
    pub fn sync_peers(&self) -> Result<Vec<SyncPeer>> {
        self.ensure_sync_tables()?;
        let sql = format!(
            "SELECT name, endpoint, token_env, created_at_micros, updated_at_micros FROM {} ORDER BY name",
            crate::sync::PEERS_TABLE
        );
        match self.execute(&sql) {
            Ok(result) => result.rows().iter().map(sync_peer_from_row).collect(),
            Err(error) => {
                let message = error.to_string();
                if message.contains("no such table") || message.contains("unknown table") {
                    Ok(Vec::new())
                } else {
                    Err(error)
                }
            }
        }
    }
    pub fn sync_peer(&self, name: &str) -> Result<Option<SyncPeer>> {
        self.ensure_sync_tables()?;
        let sql = format!(
            "SELECT name, endpoint, token_env, created_at_micros, updated_at_micros FROM {} WHERE name = {}",
            crate::sync::PEERS_TABLE,
            sql_text_literal(name)
        );
        match self.execute(&sql) {
            Ok(result) => Ok(result.rows().first().map(sync_peer_from_row).transpose()?),
            Err(error) => {
                let message = error.to_string();
                if message.contains("no such table") || message.contains("unknown table") {
                    Ok(None)
                } else {
                    Err(error)
                }
            }
        }
    }
    fn sync_peer_scope_binding_row(&self, peer_name: &str) -> Result<Option<SyncPeerScopeBinding>> {
        let sql = format!(
            "SELECT peer_name, scope_name, created_at_micros, updated_at_micros FROM {} WHERE peer_name = {}",
            crate::sync::PEER_SCOPES_TABLE,
            sql_text_literal(peer_name)
        );
        match self.execute(&sql) {
            Ok(result) => Ok(result
                .rows()
                .first()
                .map(sync_peer_scope_binding_from_row)
                .transpose()?),
            Err(error) => {
                let message = error.to_string();
                if message.contains("no such table") || message.contains("unknown table") {
                    Ok(None)
                } else {
                    Err(error)
                }
            }
        }
    }
    pub fn sync_sessions(&self) -> Result<Vec<SyncSession>> {
        self.ensure_sync_tables()?;
        let sql = format!(
            "SELECT session_id, peer_name, direction, remote_replica_id, started_at_micros, ended_at_micros, status, error, pushed_batch_id, pulled_batch_id, pushed_seen, pushed_applied, pushed_skipped, pushed_conflicted, pulled_seen, pulled_applied, pulled_skipped, pulled_conflicted, retry_count FROM {} ORDER BY session_id",
            crate::sync::SESSIONS_TABLE
        );
        match self.execute(&sql) {
            Ok(result) => result.rows().iter().map(sync_session_from_row).collect(),
            Err(error) => {
                let message = error.to_string();
                if message.contains("no such table") || message.contains("unknown table") {
                    Ok(Vec::new())
                } else {
                    Err(error)
                }
            }
        }
    }
    pub fn sync_start_session(
        &self,
        peer_name: &str,
        direction: SyncRunDirection,
        remote_replica_id: Option<&str>,
    ) -> Result<i64> {
        self.ensure_sync_tables()?;
        let session_id = self.next_sync_session_id()?;
        let started_at_micros = current_time_micros();
        let sql = format!(
            "INSERT INTO {table} (session_id, peer_name, direction, remote_replica_id, started_at_micros, ended_at_micros, status, error, pushed_batch_id, pulled_batch_id, pushed_seen, pushed_applied, pushed_skipped, pushed_conflicted, pulled_seen, pulled_applied, pulled_skipped, pulled_conflicted, retry_count) VALUES ({session_id}, {peer_name}, {direction}, {remote_replica_id}, {started_at_micros}, NULL, 'started', NULL, NULL, NULL, 0, 0, 0, 0, 0, 0, 0, 0, 0)",
            table = crate::sync::SESSIONS_TABLE,
            session_id = session_id,
            peer_name = sql_text_literal(peer_name),
            direction = sql_text_literal(direction.as_str()),
            remote_replica_id = remote_replica_id
                .map(sql_text_literal)
                .unwrap_or_else(|| "NULL".to_string()),
            started_at_micros = started_at_micros,
        );
        let _ = self.execute(&sql)?;
        Ok(session_id)
    }
    pub fn sync_finish_session_success(
        &self,
        session_id: i64,
        summary: &SyncRunSummary,
    ) -> Result<()> {
        self.sync_update_session(session_id, summary, "success", None, current_time_micros())
    }
    pub fn sync_finish_session_failed(
        &self,
        session_id: i64,
        summary: &SyncRunSummary,
        error: &str,
    ) -> Result<()> {
        self.sync_update_session(
            session_id,
            summary,
            "failed",
            Some(error),
            current_time_micros(),
        )
    }
    pub fn sync_integrity_report(&self) -> Result<SyncJournalIntegrityReport> {
        let local_replica_id = self.sync_read_metadata("replica_id").ok().flatten();
        crate::sync::inspect_journal_integrity(
            self.inner.sync_ctx.journal_path(),
            &self.inner.vfs,
            local_replica_id.as_deref(),
        )
    }
    pub fn sync_peer_lag_report(&self) -> Result<Vec<SyncPeerLag>> {
        self.ensure_sync_tables()?;
        let local_high_watermark = self.sync_integrity_report()?.last_sequence;
        let peers = self.sync_peers()?;
        let sessions = self.sync_sessions()?;
        let mut latest_successful_remote_replica_ids: HashMap<String, Option<String>> =
            HashMap::new();
        for session in sessions.iter().rev() {
            if session.status == "success" {
                latest_successful_remote_replica_ids
                    .entry(session.peer_name.clone())
                    .or_insert_with(|| session.remote_replica_id.clone());
            }
        }

        peers
            .into_iter()
            .map(|peer| {
                let remote_replica_id = latest_successful_remote_replica_ids
                    .get(&peer.name)
                    .cloned()
                    .flatten();
                let in_watermark = match remote_replica_id.as_deref() {
                    Some(replica_id) => self.sync_peer_watermark(replica_id)?,
                    None => None,
                };
                let out_watermark = self.sync_peer_out_watermark(&peer.name)?;
                let in_lag = match (local_high_watermark, in_watermark) {
                    (Some(local_high), Some(in_watermark)) if local_high >= in_watermark => {
                        Some(local_high - in_watermark)
                    }
                    _ => None,
                };
                let out_lag = match (local_high_watermark, out_watermark) {
                    (Some(local_high), Some(out_watermark)) if local_high >= out_watermark => {
                        Some(local_high - out_watermark)
                    }
                    _ => None,
                };
                Ok(SyncPeerLag {
                    peer_name: peer.name,
                    remote_replica_id,
                    in_watermark,
                    out_watermark,
                    local_high_watermark,
                    in_lag,
                    out_lag,
                })
            })
            .collect()
    }
    pub fn sync_retention_report(&self) -> Result<SyncRetentionReport> {
        let integrity = self.sync_integrity_report()?;
        let peer_lag = self.sync_peer_lag_report()?;
        let journal_size_bytes = self.sync_status()?.journal_size_bytes;
        let mut watermark_entries = peer_lag
            .iter()
            .flat_map(|peer| {
                let inbound = peer.remote_replica_id.as_ref().zip(peer.in_watermark).map(
                    |(remote_replica_id, watermark)| {
                        (format!("remote:{remote_replica_id}"), watermark)
                    },
                );
                let outbound = peer
                    .out_watermark
                    .map(|watermark| (peer.peer_name.clone(), watermark));
                inbound.into_iter().chain(outbound)
            })
            .collect::<Vec<_>>();
        watermark_entries.extend(self.sync_peer_watermark_entries()?);
        watermark_entries.extend(
            self.sync_shape_clients()?
                .into_iter()
                .filter(|client| client.retention_blocking)
                .map(|client| {
                    (
                        format!(
                            "shape:{}:client:{}",
                            client.shape_id, client.client_replica_id
                        ),
                        client.last_ack_watermark,
                    )
                }),
        );
        watermark_entries.sort_by(|left, right| left.0.cmp(&right.0).then(left.1.cmp(&right.1)));
        watermark_entries.dedup();
        let lowest_watermark = watermark_entries
            .iter()
            .map(|(_, watermark)| *watermark)
            .min();
        let safe_prune_through = if integrity.total_records == 0 {
            None
        } else {
            lowest_watermark.and_then(|watermark| watermark.checked_sub(1))
        };
        let blocked_by = if integrity.total_records == 0 {
            Vec::new()
        } else if let Some(lowest_watermark) = lowest_watermark {
            if integrity
                .last_sequence
                .is_some_and(|local_high| lowest_watermark <= local_high)
            {
                watermark_entries
                    .iter()
                    .filter(|(_, watermark)| *watermark == lowest_watermark)
                    .map(|(label, _)| label.clone())
                    .collect::<Vec<_>>()
            } else {
                Vec::new()
            }
        } else {
            Vec::new()
        };
        let prunable_records = match safe_prune_through {
            Some(safe_through) => {
                match crate::sync::read_journal_records(
                    self.inner.sync_ctx.journal_path(),
                    &self.inner.vfs,
                    0,
                    usize::MAX,
                ) {
                    Ok(records) => records
                        .into_iter()
                        .filter(|record| record.sequence <= safe_through)
                        .count(),
                    Err(_) => 0,
                }
            }
            None => 0,
        };

        Ok(SyncRetentionReport {
            journal_records: integrity.total_records,
            first_sequence: integrity.first_sequence,
            last_sequence: integrity.last_sequence,
            safe_prune_through,
            prunable_records,
            blocked_by,
            journal_size_bytes,
        })
    }
    pub fn sync_operational_doctor_report(&self) -> Result<SyncOperationalDoctorReport> {
        let status = self.sync_status()?;
        let integrity = self.sync_integrity_report()?;
        let retention = self.sync_retention_report()?;
        let peer_lag = self.sync_peer_lag_report()?;
        let unresolved_conflicts = self.sync_conflicts()?.len();
        let mut recent_sessions = self.sync_sessions()?;
        if recent_sessions.len() > 5 {
            recent_sessions = recent_sessions.split_off(recent_sessions.len() - 5);
        }
        let mut issues = integrity.issues.clone();
        let mut guidance = Vec::new();
        let mut highest_severity = integrity.highest_severity;

        if !status.enabled {
            highest_severity = highest_severity.max(SyncDoctorSeverity::Warning);
            guidance.push(
                "sync is disabled; enable it before expecting journal growth or peer watermarks"
                    .to_string(),
            );
        }

        if unresolved_conflicts > 0 {
            highest_severity = highest_severity.max(SyncDoctorSeverity::Warning);
            let message = format!("{unresolved_conflicts} unresolved conflict(s) need attention");
            issues.push(SyncJournalIssue {
                line_number: 0,
                sequence: None,
                severity: SyncDoctorSeverity::Warning,
                code: "unresolved_conflicts".to_string(),
                message: message.clone(),
            });
            guidance.push(message);
        }

        if retention.journal_records > 0 && retention.safe_prune_through.is_none() {
            highest_severity = highest_severity.max(SyncDoctorSeverity::Warning);
            let message = if retention.blocked_by.is_empty() {
                "safe prune is unavailable because no peer watermarks are known".to_string()
            } else {
                format!(
                    "safe prune is blocked by {}",
                    retention.blocked_by.join(", ")
                )
            };
            issues.push(SyncJournalIssue {
                line_number: 0,
                sequence: None,
                severity: SyncDoctorSeverity::Warning,
                code: "retention_blocked".to_string(),
                message: message.clone(),
            });
            guidance.push(message);
        } else if let Some(safe_through) = retention.safe_prune_through {
            guidance.push(format!(
                "safe prune is available through sequence {safe_through}"
            ));
        }

        if peer_lag.iter().any(|peer| {
            peer.in_lag.is_some_and(|lag| lag > 0) || peer.out_lag.is_some_and(|lag| lag > 0)
        }) {
            highest_severity = highest_severity.max(SyncDoctorSeverity::Warning);
            guidance.push("peer lag exists; inspect sys_sync_peer_lag before pruning".to_string());
        }

        if integrity.highest_severity == SyncDoctorSeverity::Error {
            highest_severity = SyncDoctorSeverity::Error;
        }

        if issues.is_empty() {
            guidance.push("journal integrity is clean".to_string());
        }

        Ok(SyncOperationalDoctorReport {
            status,
            integrity,
            retention,
            peer_lag,
            unresolved_conflicts,
            recent_sessions,
            highest_severity,
            issues,
            guidance,
        })
    }
    pub fn sync_status(&self) -> Result<SyncStatus> {
        if self.inner.sync_ctx.is_enabled() {
            return Ok(SyncStatus {
                enabled: true,
                replica_id: self.inner.sync_ctx.replica_id(),
                next_sequence: self.inner.sync_ctx.next_sequence(),
                journal_path: Some(
                    self.inner
                        .sync_ctx
                        .journal_path()
                        .to_string_lossy()
                        .to_string(),
                ),
                journal_size_bytes: self.inner.sync_ctx.journal_size_bytes(),
            });
        }
        self.load_sync_status_from_db()
    }
    pub fn sync_pending_changes(
        &self,
        since_seq: u64,
        limit: usize,
    ) -> Result<Vec<SyncJournalRecord>> {
        if !self.inner.sync_ctx.is_enabled() {
            let loaded = self.load_sync_status_from_db()?;
            if !loaded.enabled {
                return Ok(Vec::new());
            }
        }
        crate::sync::read_journal_records(
            self.inner.sync_ctx.journal_path(),
            &self.inner.vfs,
            since_seq,
            limit,
        )
    }
    fn sync_scope_row_filter_expr(scope: &SyncScope) -> Result<Option<crate::sql::ast::Expr>> {
        match scope.row_filter.as_deref() {
            Some(filter_sql) => Ok(Some(parse_expression_sql(filter_sql).map_err(|error| {
                DbError::sql(format!(
                    "invalid row filter for sync scope '{}': {error}",
                    scope.name
                ))
            })?)),
            None => Ok(None),
        }
    }
    fn sync_scope_record_matches(
        &self,
        scope: &SyncScope,
        record: &SyncJournalRecord,
    ) -> Result<bool> {
        if !scope
            .include_tables
            .iter()
            .any(|table_name| table_name.eq_ignore_ascii_case(&record.table))
        {
            return Ok(false);
        }
        let Some(expr) = Self::sync_scope_row_filter_expr(scope)? else {
            return Ok(true);
        };
        let runtime = self.runtime_for_metadata_inspection()?;
        let table = runtime
            .catalog
            .table(&record.table)
            .ok_or_else(|| DbError::sql(format!("unknown table '{}'", record.table)))?;
        let payload = match record.operation.as_str() {
            "delete" => &record.primary_key,
            "insert" | "update" => record
                .after
                .as_ref()
                .ok_or_else(|| DbError::sql("sync record missing after payload"))?,
            other => {
                return Err(DbError::sql(format!("unsupported operation '{other}'")));
            }
        };
        let payload = payload.as_object().ok_or_else(|| {
            DbError::sql(format!(
                "sync record for table '{}' must use an object payload",
                record.table
            ))
        })?;

        let mut values = Vec::with_capacity(scope.filter_columns.len());
        for column_name in &scope.filter_columns {
            let column = table
                .columns
                .iter()
                .find(|candidate| candidate.name.eq_ignore_ascii_case(column_name))
                .ok_or_else(|| {
                    DbError::sql(format!(
                        "sync scope column '{column_name}' is missing from table '{}'",
                        table.name
                    ))
                })?;
            let json_value = payload.get(&column.name).ok_or_else(|| {
                DbError::sql(format!(
                    "sync record for table '{}' is missing scoped column '{}'",
                    table.name, column.name
                ))
            })?;
            values.push(json_to_column_value(&table.name, column, json_value)?);
        }

        row_satisfies_expression(&runtime, &table.name, &scope.filter_columns, &values, &expr)
    }
    fn sync_filter_records_for_scope(
        &self,
        scope: &SyncScope,
        records: Vec<SyncJournalRecord>,
    ) -> Result<Vec<SyncJournalRecord>> {
        let mut filtered = Vec::new();
        for record in records {
            if self.sync_scope_record_matches(scope, &record)? {
                filtered.push(record);
            }
        }
        Ok(filtered)
    }
    fn sync_validate_batch_for_scope(
        &self,
        scope: &SyncScope,
        batch: &SyncChangeBatch,
    ) -> Result<()> {
        let runtime = self.runtime_for_metadata_inspection()?;
        for record in &batch.records {
            if !scope
                .include_tables
                .iter()
                .any(|table_name| table_name.eq_ignore_ascii_case(&record.table))
            {
                return Err(DbError::sql(format!(
                    "sync batch contains table '{}' which is outside scope '{}'",
                    record.table, scope.name
                )));
            }
            if scope.row_filter.is_some() {
                let table = runtime
                    .catalog
                    .table(&record.table)
                    .ok_or_else(|| DbError::sql(format!("unknown table '{}'", record.table)))?;
                let payload = match record.operation.as_str() {
                    "delete" => &record.primary_key,
                    "insert" | "update" => record
                        .after
                        .as_ref()
                        .ok_or_else(|| DbError::sql("sync record missing after payload"))?,
                    other => {
                        return Err(DbError::sql(format!("unsupported operation '{other}'")));
                    }
                };
                let payload = payload.as_object().ok_or_else(|| {
                    DbError::sql(format!(
                        "sync record for table '{}' must use an object payload",
                        record.table
                    ))
                })?;
                let mut values = Vec::with_capacity(scope.filter_columns.len());
                for column_name in &scope.filter_columns {
                    let column = table
                        .columns
                        .iter()
                        .find(|candidate| candidate.name.eq_ignore_ascii_case(column_name))
                        .ok_or_else(|| {
                            DbError::sql(format!(
                                "sync scope column '{column_name}' is missing from table '{}'",
                                table.name
                            ))
                        })?;
                    let json_value = payload.get(&column.name).ok_or_else(|| {
                        DbError::sql(format!(
                            "sync record for table '{}' is missing scoped column '{}'",
                            table.name, column.name
                        ))
                    })?;
                    values.push(json_to_column_value(&table.name, column, json_value)?);
                }
                if !row_satisfies_expression(
                    &runtime,
                    &table.name,
                    &scope.filter_columns,
                    &values,
                    &Self::sync_scope_row_filter_expr(scope)?
                        .ok_or_else(|| DbError::internal("scope row filter expression missing"))?,
                )? {
                    return Err(DbError::sql(format!(
                        "sync batch contains record for table '{}' that does not match scope '{}'",
                        record.table, scope.name
                    )));
                }
            }
        }
        Ok(())
    }
    pub fn sync_export_batch(&self, since_seq: u64, limit: usize) -> Result<SyncChangeBatch> {
        let records = self.sync_pending_changes(since_seq, limit)?;
        SyncChangeBatch::from_records(records)
    }
    pub fn sync_create_changeset(
        &self,
        mut options: CreateChangesetOptions,
    ) -> Result<SyncChangeset> {
        self.ensure_sync_tables()?;
        let mut scoped_from_shape = false;
        if let Some(principal) = options.principal.as_ref() {
            principal.validate()?;
        }
        if let Some(shape_id) = options.shape_id.as_deref() {
            let shape = self.sync_shape(shape_id)?.ok_or_else(|| {
                DbError::sql(format!(
                    "SHAPE_NOT_FOUND: sync shape '{shape_id}' not found"
                ))
            })?;
            self.sync_authorize_shape(options.principal.as_ref(), &shape)?;
            options.scope_name = Some(shape.scope_name);
            scoped_from_shape = true;
        }
        if let Some(scope_name) = options.scope_name.as_deref() {
            if !scoped_from_shape {
                self.sync_authorize_scope(options.principal.as_ref(), scope_name)?;
            }
        }

        let max_records = options
            .max_records
            .map(usize::try_from)
            .transpose()
            .map_err(|_| DbError::sql("max_records is too large"))?
            .unwrap_or(usize::MAX);
        let created_at_micros = current_time_micros();
        let tooling = self.get_tooling_metadata()?;
        let runtime = self.runtime_for_metadata_inspection()?;
        let schema_cookie = runtime.catalog.schema_cookie;
        let tenant_id = options
            .principal
            .as_ref()
            .map(|principal| principal.tenant_id.clone())
            .or_else(|| {
                options
                    .shape_id
                    .as_deref()
                    .and_then(|shape_id| self.sync_shape(shape_id).ok().flatten())
                    .map(|shape| shape.tenant_id)
            });

        let mut changeset = match &options.source {
            SyncChangesetSource::Checkpoint {
                peer,
                since_sequence,
            } => {
                let batch = match options.scope_name.as_deref() {
                    Some(scope_name) => {
                        self.sync_export_batch_for_scope(scope_name, *since_sequence, max_records)?
                    }
                    None => self.sync_export_batch(*since_sequence, max_records)?,
                };
                let source_replica_id = batch
                    .source_replica_id
                    .clone()
                    .or_else(|| self.sync_status().ok().and_then(|status| status.replica_id))
                    .unwrap_or_else(|| "unknown".to_string());
                let records = batch
                    .records
                    .iter()
                    .map(sync_changeset_record_from_journal_record)
                    .collect::<Vec<_>>();
                let start_checkpoint = batch.first_sequence;
                let end_checkpoint = batch.last_sequence;
                let source_high_watermark = batch
                    .source_high_watermark
                    .or(batch.last_sequence)
                    .or_else(|| {
                        self.sync_integrity_report()
                            .ok()
                            .and_then(|report| report.last_sequence)
                    });
                let changeset_id = sync_changeset_id(
                    "checkpoint",
                    &source_replica_id,
                    created_at_micros,
                    records.len(),
                );
                SyncChangeset {
                    changeset_version: crate::sync::SYNC_CHANGESET_VERSION,
                    changeset_id,
                    source_replica_id,
                    source_kind: options.source.kind(),
                    tenant_id,
                    scope_name: options.scope_name.clone(),
                    shape_id: options.shape_id.clone(),
                    base_kind: "checkpoint".to_string(),
                    base_checkpoint: Some(SyncChangesetCheckpoint {
                        peer: peer.clone(),
                        sequence: *since_sequence,
                    }),
                    base_branch: None,
                    base_snapshot: None,
                    start_checkpoint,
                    end_checkpoint,
                    source_high_watermark,
                    schema_fingerprint: tooling.schema_fingerprint.clone(),
                    schema_cookie,
                    sync_contract_version: crate::sync::SYNC_CONTRACT_VERSION,
                    query_contract_fingerprint: None,
                    producer_capabilities: SyncChangesetCapabilities::default(),
                    limits: SyncChangesetLimits::default(),
                    records,
                    conflict_policy_hint: None,
                    created_at_micros,
                    integrity_hash: None,
                }
            }
            SyncChangesetSource::Branch { from, to } => {
                self.sync_create_diff_changeset(SyncDiffChangesetContext {
                    base_kind: "branch",
                    from_ref: from,
                    to_ref: to,
                    scope_name: options.scope_name.as_deref(),
                    shape_id: options.shape_id.as_deref(),
                    tenant_id: tenant_id.as_deref(),
                    schema_fingerprint: &tooling.schema_fingerprint,
                    schema_cookie,
                    created_at_micros,
                    max_records,
                })?
            }
            SyncChangesetSource::Snapshot { from, to } => {
                self.sync_create_diff_changeset(SyncDiffChangesetContext {
                    base_kind: "snapshot",
                    from_ref: from,
                    to_ref: to,
                    scope_name: options.scope_name.as_deref(),
                    shape_id: options.shape_id.as_deref(),
                    tenant_id: tenant_id.as_deref(),
                    schema_fingerprint: &tooling.schema_fingerprint,
                    schema_cookie,
                    created_at_micros,
                    max_records,
                })?
            }
        };
        self.sync_finalize_changeset(&mut changeset, options.max_bytes)?;
        self.sync_record_changeset_history(&changeset, "created", None)?;
        Ok(changeset)
    }
    fn sync_create_diff_changeset(
        &self,
        ctx: SyncDiffChangesetContext<'_>,
    ) -> Result<SyncChangeset> {
        let diff = self.branch_diff(ctx.from_ref, ctx.to_ref)?;
        let table_infos = self
            .list_tables()?
            .into_iter()
            .map(|table| (table.name.clone(), table))
            .collect::<BTreeMap<_, _>>();
        let mut records = Vec::new();
        let source_replica_id = format!("{}:{}:{}", ctx.base_kind, ctx.from_ref, ctx.to_ref);
        let mut sequence = 1u64;
        for table_diff in &diff.tables {
            if matches!(
                table_diff.status,
                crate::branch::BranchTableDiffStatus::Unsupported
            ) {
                return Err(DbError::sql(format!(
                    "CHANGESET_UNSUPPORTED: branch/snapshot diff for table '{}' is unsupported: {}",
                    table_diff.table,
                    table_diff
                        .message
                        .clone()
                        .unwrap_or_else(|| "unsupported row diff".to_string())
                )));
            }
            if table_diff.schema_changed {
                return Err(DbError::sql(format!(
                    "SCHEMA_INCOMPATIBLE: changeset diff for table '{}' changes schema",
                    table_diff.table
                )));
            }
            let Some(table) = table_infos.get(&table_diff.table) else {
                continue;
            };
            let pk_names = &table.primary_key_columns;
            let column_names = table
                .columns
                .iter()
                .map(|column| column.name.clone())
                .collect::<Vec<_>>();
            let table_context = BranchChangesetTableContext {
                source_replica_id: &source_replica_id,
                table_name: &table.name,
                primary_key_columns: pk_names,
                column_names: &column_names,
                schema_cookie: ctx.schema_cookie,
                created_at_micros: ctx.created_at_micros,
            };
            for row in &table_diff.added {
                records.push(sync_changeset_record_from_branch_row(
                    &table_context,
                    sequence,
                    "insert",
                    row,
                )?);
                sequence += 1;
            }
            for row in &table_diff.updated {
                records.push(sync_changeset_record_from_branch_row(
                    &table_context,
                    sequence,
                    "update",
                    row,
                )?);
                sequence += 1;
            }
            for row in &table_diff.deleted {
                records.push(sync_changeset_record_from_branch_row(
                    &table_context,
                    sequence,
                    "delete",
                    row,
                )?);
                sequence += 1;
            }
            if records.len() > ctx.max_records {
                return Err(DbError::sql(
                    "BATCH_TOO_LARGE: changeset exceeds max_records",
                ));
            }
        }

        let source_kind = if ctx.base_kind == "branch" {
            crate::sync::SyncChangesetSourceKind::Branch
        } else {
            crate::sync::SyncChangesetSourceKind::Snapshot
        };
        let changeset_id = sync_changeset_id(
            ctx.base_kind,
            &source_replica_id,
            ctx.created_at_micros,
            records.len(),
        );
        Ok(SyncChangeset {
            changeset_version: crate::sync::SYNC_CHANGESET_VERSION,
            changeset_id,
            source_replica_id,
            source_kind,
            tenant_id: ctx.tenant_id.map(str::to_string),
            scope_name: ctx.scope_name.map(str::to_string),
            shape_id: ctx.shape_id.map(str::to_string),
            base_kind: ctx.base_kind.to_string(),
            base_checkpoint: None,
            base_branch: (ctx.base_kind == "branch").then(|| ctx.from_ref.to_string()),
            base_snapshot: (ctx.base_kind == "snapshot").then(|| ctx.from_ref.to_string()),
            start_checkpoint: records.first().map(|record| record.origin_sequence),
            end_checkpoint: records.last().map(|record| record.origin_sequence),
            source_high_watermark: records.last().map(|record| record.origin_sequence),
            schema_fingerprint: ctx.schema_fingerprint.to_string(),
            schema_cookie: ctx.schema_cookie,
            sync_contract_version: crate::sync::SYNC_CONTRACT_VERSION,
            query_contract_fingerprint: None,
            producer_capabilities: SyncChangesetCapabilities {
                before_images: true,
                ..SyncChangesetCapabilities::default()
            },
            limits: SyncChangesetLimits::default(),
            records,
            conflict_policy_hint: None,
            created_at_micros: ctx.created_at_micros,
            integrity_hash: None,
        })
    }
    pub fn sync_inspect_changeset(
        &self,
        changeset: &SyncChangeset,
        options: InspectChangesetOptions,
    ) -> Result<SyncChangesetInspection> {
        self.sync_validate_changeset_envelope(changeset)?;
        let bytes = serde_json::to_vec(changeset)
            .map_err(|error| DbError::internal(format!("failed to serialize changeset: {error}")))?
            .len() as u64;
        let mut tables = BTreeSet::new();
        let mut operations = BTreeMap::new();
        let mut warnings = Vec::new();
        for record in &changeset.records {
            tables.insert(record.table.clone());
            *operations.entry(record.operation.clone()).or_insert(0u64) += 1;
            if record.operation == "delete" && record.before.is_none() {
                warnings.push(format!(
                    "delete record for table '{}' cannot be inverted without before image",
                    record.table
                ));
            }
        }
        let compatibility = if options.check_local_compatibility {
            match self.sync_check_changeset_compatibility(changeset) {
                Ok(()) => SyncChangesetCompatibility {
                    checked_against_local_db: true,
                    status: "compatible".to_string(),
                    message: None,
                },
                Err(error) => SyncChangesetCompatibility {
                    checked_against_local_db: true,
                    status: "incompatible".to_string(),
                    message: Some(error.to_string()),
                },
            }
        } else {
            SyncChangesetCompatibility {
                checked_against_local_db: false,
                status: "not_checked".to_string(),
                message: None,
            }
        };
        Ok(SyncChangesetInspection {
            changeset_id: changeset.changeset_id.clone(),
            valid_envelope: true,
            source_kind: changeset.source_kind.clone(),
            scope_name: changeset.scope_name.clone(),
            shape_id: changeset.shape_id.clone(),
            record_count: changeset.records.len() as u64,
            bytes,
            tables: tables.into_iter().collect(),
            operations,
            start_checkpoint: changeset.start_checkpoint,
            end_checkpoint: changeset.end_checkpoint,
            schema_fingerprint: changeset.schema_fingerprint.clone(),
            compatibility,
            warnings,
        })
    }
    pub fn sync_apply_changeset(
        &self,
        changeset: &SyncChangeset,
        options: ApplyChangesetOptions,
    ) -> Result<SyncChangesetApplyResult> {
        self.ensure_sync_tables()?;
        self.sync_validate_changeset_envelope(changeset)?;
        if !options.atomic {
            return Err(DbError::sql(
                "CHANGESET_UNSUPPORTED: non-atomic changeset apply is not supported",
            ));
        }
        if let Some(principal) = options.principal.as_ref() {
            principal.validate()?;
        }
        let mut scope_authorized_via_shape = false;
        if let Some(shape_id) = changeset.shape_id.as_deref() {
            let shape = self.sync_shape(shape_id)?.ok_or_else(|| {
                DbError::sql(format!(
                    "SHAPE_NOT_FOUND: sync shape '{shape_id}' not found"
                ))
            })?;
            scope_authorized_via_shape = true;
            self.sync_authorize_shape(options.principal.as_ref(), &shape)?;
        }
        if let Some(scope_name) = changeset.scope_name.as_deref() {
            if !scope_authorized_via_shape {
                self.sync_authorize_scope(options.principal.as_ref(), scope_name)?;
            }
        }
        if matches!(
            options.compatibility_mode,
            crate::sync::SyncCompatibilityMode::Strict
        ) {
            self.sync_check_changeset_compatibility(changeset)?;
        }
        let integrity_hash = self.sync_changeset_integrity_hash(changeset)?;
        if let Some(existing) =
            self.sync_read_metadata(&changeset_applied_key(&changeset.changeset_id))?
        {
            if existing != integrity_hash {
                return Err(DbError::sql(format!(
                    "CHANGESET_ID_COLLISION: changeset '{}' was already applied with a different integrity hash",
                    changeset.changeset_id
                )));
            }
            return Ok(SyncChangesetApplyResult {
                outcome: "already_applied".to_string(),
                changeset_id: changeset.changeset_id.clone(),
                rows_seen: changeset.records.len() as u64,
                rows_applied: 0,
                rows_skipped: changeset.records.len() as u64,
                rows_conflicted: 0,
                checkpoint_after: changeset.source_high_watermark.or(changeset.end_checkpoint),
            });
        }

        let journal_records = changeset
            .records
            .iter()
            .map(sync_journal_record_from_changeset_record)
            .collect::<Result<Vec<_>>>()?;
        let batch = SyncChangeBatch::scoped_from_records(
            journal_records,
            Some(changeset.source_replica_id.clone()),
            changeset.source_high_watermark.or(changeset.end_checkpoint),
        )?;
        let summary = match (changeset.scope_name.as_deref(), options.conflict_policy) {
            (Some(scope_name), Some(policy)) => {
                self.sync_import_batch_for_scope_with_policy(scope_name, &batch, policy)?
            }
            (Some(scope_name), None) => self.sync_import_batch_for_scope(scope_name, &batch)?,
            (None, Some(policy)) => self.sync_import_batch_with_policy(&batch, policy)?,
            (None, None) => self.sync_import_batch(&batch)?,
        };
        self.sync_upsert_metadata(
            &changeset_applied_key(&changeset.changeset_id),
            &integrity_hash,
        )?;
        self.sync_record_changeset_history(changeset, "applied", Some(current_time_micros()))?;
        Ok(SyncChangesetApplyResult {
            outcome: if summary.conflicted > 0 {
                "conflict_recorded".to_string()
            } else {
                "applied".to_string()
            },
            changeset_id: changeset.changeset_id.clone(),
            rows_seen: summary.seen as u64,
            rows_applied: summary.applied as u64,
            rows_skipped: summary.skipped as u64,
            rows_conflicted: summary.conflicted as u64,
            checkpoint_after: changeset.source_high_watermark.or(changeset.end_checkpoint),
        })
    }
    pub fn sync_invert_changeset(
        &self,
        changeset: &SyncChangeset,
        _options: InvertChangesetOptions,
    ) -> Result<SyncChangeset> {
        self.sync_validate_changeset_envelope(changeset)?;
        let created_at_micros = current_time_micros();
        let mut inverse_records = Vec::with_capacity(changeset.records.len());
        for (index, record) in changeset.records.iter().enumerate() {
            let operation = match record.operation.as_str() {
                "insert" => "delete",
                "delete" if record.before.is_some() => "insert",
                "update" if record.before.is_some() => "update",
                "delete" | "update" => {
                    return Err(DbError::sql(format!(
                        "CHANGESET_INVERSION_UNSUPPORTED: record {index} lacks before image"
                    )));
                }
                other => {
                    return Err(DbError::sql(format!(
                        "CHANGESET_INVALID: unsupported record operation '{other}'"
                    )));
                }
            };
            inverse_records.push(SyncChangesetRecord {
                record_version: record.record_version,
                table: record.table.clone(),
                operation: operation.to_string(),
                primary_key: record.primary_key.clone(),
                origin_replica_id: format!("inverse:{}", changeset.changeset_id),
                origin_sequence: (index as u64) + 1,
                transaction_id: format!("txn:inverse:{}", changeset.changeset_id),
                transaction_lsn: (index as u64) + 1,
                schema_cookie: record.schema_cookie,
                before_hash: None,
                before: record.after.clone(),
                after: if operation == "delete" {
                    None
                } else {
                    record.before.clone()
                },
                column_mask: record.column_mask.clone(),
                tombstone: operation == "delete",
                conflict_metadata: None,
            });
        }
        let source_replica_id = format!("inverse:{}", changeset.source_replica_id);
        let mut inverse = SyncChangeset {
            changeset_version: crate::sync::SYNC_CHANGESET_VERSION,
            changeset_id: sync_changeset_id(
                "inverse",
                &source_replica_id,
                created_at_micros,
                inverse_records.len(),
            ),
            source_replica_id,
            source_kind: changeset.source_kind.clone(),
            tenant_id: changeset.tenant_id.clone(),
            scope_name: changeset.scope_name.clone(),
            shape_id: changeset.shape_id.clone(),
            base_kind: format!("inverse:{}", changeset.base_kind),
            base_checkpoint: changeset.base_checkpoint.clone(),
            base_branch: changeset.base_branch.clone(),
            base_snapshot: changeset.base_snapshot.clone(),
            start_checkpoint: inverse_records.first().map(|record| record.origin_sequence),
            end_checkpoint: inverse_records.last().map(|record| record.origin_sequence),
            source_high_watermark: inverse_records.last().map(|record| record.origin_sequence),
            schema_fingerprint: changeset.schema_fingerprint.clone(),
            schema_cookie: changeset.schema_cookie,
            sync_contract_version: changeset.sync_contract_version,
            query_contract_fingerprint: changeset.query_contract_fingerprint.clone(),
            producer_capabilities: SyncChangesetCapabilities {
                before_images: true,
                ..SyncChangesetCapabilities::default()
            },
            limits: SyncChangesetLimits::default(),
            records: inverse_records,
            conflict_policy_hint: changeset.conflict_policy_hint.clone(),
            created_at_micros,
            integrity_hash: None,
        };
        self.sync_finalize_changeset(&mut inverse, None)?;
        Ok(inverse)
    }
    pub fn sync_create_shape(&self, options: CreateShapeOptions) -> Result<SyncShape> {
        self.ensure_sync_tables()?;
        let shape_id = options.shape_id.trim();
        let scope_name = options.scope_name.trim();
        let tenant_id = options.tenant_id.trim();
        if shape_id.is_empty() {
            return Err(DbError::sql("sync shape_id must not be empty"));
        }
        if scope_name.is_empty() {
            return Err(DbError::sql("sync shape scope_name must not be empty"));
        }
        if tenant_id.is_empty() {
            return Err(DbError::sql(
                "TENANT_REQUIRED: sync shape tenant_id is required",
            ));
        }
        let scope = self
            .sync_scope(scope_name)?
            .ok_or_else(|| DbError::sql(format!("sync scope '{scope_name}' not found")))?;
        if scope.include_tables.is_empty() {
            return Err(DbError::sql(format!(
                "sync scope '{scope_name}' has no included tables"
            )));
        }
        let name = options.name.as_deref().unwrap_or(shape_id).trim();
        if name.is_empty() {
            return Err(DbError::sql("sync shape name must not be empty"));
        }
        let now = current_time_micros();
        let existing = self.sync_shape(shape_id)?;
        let created_at_micros = existing
            .as_ref()
            .map(|shape| shape.created_at_micros)
            .unwrap_or(now);
        let retention_ttl_micros = options
            .retention_ttl_micros
            .unwrap_or(30 * 24 * 60 * 60 * 1_000_000);
        let max_records = options.max_records.unwrap_or(50_000);
        let ack_deadline_micros = options.ack_deadline_micros.unwrap_or(30_000_000);
        let heartbeat_micros = options.heartbeat_micros.unwrap_or(20_000_000);
        let allowed_roles_json =
            serde_json::to_string(&options.allowed_roles).map_err(|error| {
                DbError::internal(format!("failed to encode shape allowed roles: {error}"))
            })?;
        let allowed_subjects_json =
            serde_json::to_string(&options.allowed_subjects).map_err(|error| {
                DbError::internal(format!("failed to encode shape allowed subjects: {error}"))
            })?;
        let sql = format!(
            "INSERT INTO {table} (shape_id, name, scope_name, tenant_id, allowed_roles_json, allowed_subjects_json, created_at_micros, updated_at_micros, retention_ttl_micros, max_records, ack_deadline_micros, heartbeat_micros) VALUES ({shape_id}, {name}, {scope_name}, {tenant_id}, {allowed_roles_json}, {allowed_subjects_json}, {created_at_micros}, {updated_at_micros}, {retention_ttl_micros}, {max_records}, {ack_deadline_micros}, {heartbeat_micros}) ON CONFLICT (shape_id) DO UPDATE SET name = {name}, scope_name = {scope_name}, tenant_id = {tenant_id}, allowed_roles_json = {allowed_roles_json}, allowed_subjects_json = {allowed_subjects_json}, updated_at_micros = {updated_at_micros}, retention_ttl_micros = {retention_ttl_micros}, max_records = {max_records}, ack_deadline_micros = {ack_deadline_micros}, heartbeat_micros = {heartbeat_micros}",
            table = crate::sync::SHAPES_TABLE,
            shape_id = sql_text_literal(shape_id),
            name = sql_text_literal(name),
            scope_name = sql_text_literal(&scope.name),
            tenant_id = sql_text_literal(tenant_id),
            allowed_roles_json = sql_text_literal(&allowed_roles_json),
            allowed_subjects_json = sql_text_literal(&allowed_subjects_json),
            created_at_micros = created_at_micros,
            updated_at_micros = now,
            retention_ttl_micros = retention_ttl_micros,
            max_records = max_records,
            ack_deadline_micros = ack_deadline_micros,
            heartbeat_micros = heartbeat_micros,
        );
        let _ = self.execute(&sql)?;
        self.sync_shape(shape_id)?
            .ok_or_else(|| DbError::internal("sync shape missing after create/update"))
    }
    pub fn sync_drop_shape(&self, shape_id: &str) -> Result<bool> {
        self.ensure_sync_tables()?;
        let shape_id = shape_id.trim();
        if shape_id.is_empty() {
            return Err(DbError::sql("sync shape_id must not be empty"));
        }
        let _ = self.execute(&format!(
            "DELETE FROM {} WHERE shape_id = {}",
            crate::sync::SHAPE_CLIENTS_TABLE,
            sql_text_literal(shape_id)
        ))?;
        let result = self.execute(&format!(
            "DELETE FROM {} WHERE shape_id = {}",
            crate::sync::SHAPES_TABLE,
            sql_text_literal(shape_id)
        ))?;
        Ok(result.affected_rows() > 0)
    }
    pub fn sync_shape(&self, shape_id: &str) -> Result<Option<SyncShape>> {
        self.ensure_sync_tables()?;
        let sql = format!(
            "SELECT shape_id, name, scope_name, tenant_id, allowed_roles_json, allowed_subjects_json, created_at_micros, updated_at_micros, retention_ttl_micros, max_records, ack_deadline_micros, heartbeat_micros FROM {} WHERE shape_id = {}",
            crate::sync::SHAPES_TABLE,
            sql_text_literal(shape_id)
        );
        match self.execute(&sql) {
            Ok(result) => result.rows().first().map(sync_shape_from_row).transpose(),
            Err(error) => {
                let message = error.to_string();
                if message.contains("no such table") || message.contains("unknown table") {
                    Ok(None)
                } else {
                    Err(error)
                }
            }
        }
    }
    pub fn sync_shapes(&self) -> Result<Vec<SyncShape>> {
        self.ensure_sync_tables()?;
        let sql = format!(
            "SELECT shape_id, name, scope_name, tenant_id, allowed_roles_json, allowed_subjects_json, created_at_micros, updated_at_micros, retention_ttl_micros, max_records, ack_deadline_micros, heartbeat_micros FROM {} ORDER BY shape_id",
            crate::sync::SHAPES_TABLE,
        );
        match self.execute(&sql) {
            Ok(result) => result.rows().iter().map(sync_shape_from_row).collect(),
            Err(error) => {
                let message = error.to_string();
                if message.contains("no such table") || message.contains("unknown table") {
                    Ok(Vec::new())
                } else {
                    Err(error)
                }
            }
        }
    }
    pub fn sync_shape_clients(&self) -> Result<Vec<SyncShapeClient>> {
        self.ensure_sync_tables()?;
        let sql = format!(
            "SELECT shape_id, tenant_id, client_replica_id, subject_id, session_id, last_ack_sequence, last_ack_watermark, last_changeset_id, last_seen_at_micros, retention_blocking, status FROM {} ORDER BY shape_id, client_replica_id",
            crate::sync::SHAPE_CLIENTS_TABLE,
        );
        match self.execute(&sql) {
            Ok(result) => result
                .rows()
                .iter()
                .map(sync_shape_client_from_row)
                .collect(),
            Err(error) => {
                let message = error.to_string();
                if message.contains("no such table") || message.contains("unknown table") {
                    Ok(Vec::new())
                } else {
                    Err(error)
                }
            }
        }
    }
    pub fn sync_shape_snapshot(
        &self,
        shape_id: &str,
        _client_replica_id: &str,
        principal: Option<SyncPrincipal>,
    ) -> Result<SyncShapeDelivery> {
        self.ensure_sync_tables()?;
        let shape = self.sync_shape(shape_id)?.ok_or_else(|| {
            DbError::sql(format!(
                "SHAPE_NOT_FOUND: sync shape '{shape_id}' not found"
            ))
        })?;
        self.sync_authorize_shape(principal.as_ref(), &shape)?;
        let scope = self
            .sync_scope(&shape.scope_name)?
            .ok_or_else(|| DbError::sql(format!("sync scope '{}' not found", shape.scope_name)))?;
        let changeset =
            self.sync_create_shape_snapshot_changeset(&shape, &scope, principal.as_ref())?;
        let shape_sequence = changeset
            .source_high_watermark
            .or(changeset.end_checkpoint)
            .unwrap_or(0);
        Ok(SyncShapeDelivery {
            message_type: "snapshot".to_string(),
            shape_id: shape.shape_id,
            shape_sequence,
            ack_deadline_micros: current_time_micros() + shape.ack_deadline_micros,
            checkpoint: SyncShapeCheckpoint {
                shape_sequence,
                source_high_watermark: changeset.source_high_watermark.unwrap_or(shape_sequence),
            },
            changeset,
        })
    }
    pub fn sync_shape_changes(
        &self,
        shape_id: &str,
        since_watermark: u64,
        principal: Option<SyncPrincipal>,
    ) -> Result<SyncShapeDelivery> {
        let shape = self.sync_shape(shape_id)?.ok_or_else(|| {
            DbError::sql(format!(
                "SHAPE_NOT_FOUND: sync shape '{shape_id}' not found"
            ))
        })?;
        self.sync_authorize_shape(principal.as_ref(), &shape)?;
        let retention = self.sync_retention_report()?;
        if let Some(first_sequence) = retention.first_sequence {
            if since_watermark > 0 && since_watermark < first_sequence {
                return Err(DbError::sql(format!(
                    "SHAPE_RESYNC_REQUIRED: since checkpoint {since_watermark} is below retained first sequence {first_sequence}"
                )));
            }
        }
        let changeset = self.sync_create_changeset(CreateChangesetOptions {
            source: SyncChangesetSource::Checkpoint {
                peer: shape_id.to_string(),
                since_sequence: since_watermark,
            },
            scope_name: Some(shape.scope_name.clone()),
            shape_id: Some(shape.shape_id.clone()),
            max_records: Some(shape.max_records),
            max_bytes: None,
            principal,
        })?;
        let shape_sequence = changeset
            .source_high_watermark
            .or(changeset.end_checkpoint)
            .unwrap_or(since_watermark);
        Ok(SyncShapeDelivery {
            message_type: "changeset".to_string(),
            shape_id: shape.shape_id,
            shape_sequence,
            ack_deadline_micros: current_time_micros() + shape.ack_deadline_micros,
            checkpoint: SyncShapeCheckpoint {
                shape_sequence,
                source_high_watermark: changeset.source_high_watermark.unwrap_or(shape_sequence),
            },
            changeset,
        })
    }
    pub fn sync_ack_shape(&self, ack: ShapeAckOptions) -> Result<SyncShapeClient> {
        self.sync_ack_shape_with_principal(ack, None)
    }
    pub fn sync_ack_shape_with_principal(
        &self,
        ack: ShapeAckOptions,
        principal: Option<&SyncPrincipal>,
    ) -> Result<SyncShapeClient> {
        self.ensure_sync_tables()?;
        let shape = self.sync_shape(&ack.shape_id)?.ok_or_else(|| {
            DbError::sql(format!(
                "SHAPE_NOT_FOUND: sync shape '{}' not found",
                ack.shape_id
            ))
        })?;
        self.sync_authorize_shape(principal, &shape)?;
        if !shape.tenant_id.eq_ignore_ascii_case(&ack.tenant_id) {
            return Err(DbError::sql(format!(
                "AUTH_FORBIDDEN: shape '{}' belongs to tenant '{}'",
                shape.shape_id, shape.tenant_id
            )));
        }
        let now = current_time_micros();
        let sql = format!(
            "INSERT INTO {table} (shape_id, tenant_id, client_replica_id, subject_id, session_id, last_ack_sequence, last_ack_watermark, last_changeset_id, last_seen_at_micros, retention_blocking, status) VALUES ({shape_id}, {tenant_id}, {client_replica_id}, {subject_id}, {session_id}, {last_ack_sequence}, {last_ack_watermark}, {last_changeset_id}, {last_seen_at_micros}, 1, 'active') ON CONFLICT (shape_id, client_replica_id) DO UPDATE SET tenant_id = {tenant_id}, subject_id = {subject_id}, session_id = {session_id}, last_ack_sequence = {last_ack_sequence}, last_ack_watermark = {last_ack_watermark}, last_changeset_id = {last_changeset_id}, last_seen_at_micros = {last_seen_at_micros}, retention_blocking = 1, status = 'active'",
            table = crate::sync::SHAPE_CLIENTS_TABLE,
            shape_id = sql_text_literal(&shape.shape_id),
            tenant_id = sql_text_literal(&ack.tenant_id),
            client_replica_id = sql_text_literal(&ack.client_replica_id),
            subject_id = sql_text_literal(&ack.subject_id),
            session_id = sql_nullable_text_literal(ack.session_id.as_deref()),
            last_ack_sequence = ack.shape_sequence,
            last_ack_watermark = ack.source_high_watermark,
            last_changeset_id = sql_nullable_text_literal(ack.changeset_id.as_deref()),
            last_seen_at_micros = now,
        );
        let _ = self.execute(&sql)?;
        self.sync_shape_clients()?
            .into_iter()
            .find(|client| {
                client.shape_id == shape.shape_id
                    && client.client_replica_id == ack.client_replica_id
            })
            .ok_or_else(|| DbError::internal("sync shape client missing after ack"))
    }
    pub fn sync_relay_status(
        &self,
        relay_id: Option<&str>,
        production_mode: bool,
        secure_transport_required: bool,
        insecure_override_enabled: bool,
        started_at_micros: Option<i64>,
    ) -> Result<SyncRelayStatus> {
        let status = self.sync_status()?;
        let active_sessions = self
            .sync_relay_sessions()?
            .into_iter()
            .filter(|session| session.ended_at_micros.is_none() && session.status == "started")
            .count() as u64;
        Ok(SyncRelayStatus {
            relay_id: relay_id.unwrap_or("relay-local").to_string(),
            protocol_version: crate::sync::SYNC_RELAY_PROTOCOL_VERSION,
            database_replica_id: status.replica_id,
            production_mode,
            secure_transport_required,
            insecure_override_enabled,
            active_sessions,
            active_streams: 0,
            started_at_micros: started_at_micros.unwrap_or_else(current_time_micros),
        })
    }
    pub fn sync_relay_sessions(&self) -> Result<Vec<SyncRelaySession>> {
        self.ensure_sync_tables()?;
        let sql = format!(
            "SELECT session_id, tenant_id, subject_id, subject_kind, request_id, operation, scope_name, shape_id, started_at_micros, ended_at_micros, status, error, rows_seen, bytes_seen FROM {} ORDER BY started_at_micros, session_id",
            crate::sync::RELAY_SESSIONS_TABLE,
        );
        match self.execute(&sql) {
            Ok(result) => result
                .rows()
                .iter()
                .map(sync_relay_session_from_row)
                .collect(),
            Err(error) => {
                let message = error.to_string();
                if message.contains("no such table") || message.contains("unknown table") {
                    Ok(Vec::new())
                } else {
                    Err(error)
                }
            }
        }
    }
    pub fn sync_start_relay_session(
        &self,
        principal: &SyncPrincipal,
        operation: &str,
        scope_name: Option<&str>,
        shape_id: Option<&str>,
    ) -> Result<SyncRelaySession> {
        self.ensure_sync_tables()?;
        principal.validate()?;
        let started_at_micros = current_time_micros();
        let session_id = principal.session_id.clone();
        let sql = format!(
            "INSERT INTO {table} (session_id, tenant_id, subject_id, subject_kind, request_id, operation, scope_name, shape_id, started_at_micros, ended_at_micros, status, error, rows_seen, bytes_seen) VALUES ({session_id}, {tenant_id}, {subject_id}, {subject_kind}, {request_id}, {operation}, {scope_name}, {shape_id}, {started_at_micros}, NULL, 'started', NULL, 0, 0) ON CONFLICT (session_id) DO UPDATE SET tenant_id = {tenant_id}, subject_id = {subject_id}, subject_kind = {subject_kind}, request_id = {request_id}, operation = {operation}, scope_name = {scope_name}, shape_id = {shape_id}, started_at_micros = {started_at_micros}, ended_at_micros = NULL, status = 'started', error = NULL",
            table = crate::sync::RELAY_SESSIONS_TABLE,
            session_id = sql_text_literal(&session_id),
            tenant_id = sql_text_literal(&principal.tenant_id),
            subject_id = sql_text_literal(&principal.subject_id),
            subject_kind = sql_text_literal(principal.subject_kind.as_str()),
            request_id = sql_text_literal(&principal.request_id),
            operation = sql_text_literal(operation),
            scope_name = sql_nullable_text_literal(scope_name),
            shape_id = sql_nullable_text_literal(shape_id),
            started_at_micros = started_at_micros,
        );
        let _ = self.execute(&sql)?;
        self.sync_relay_sessions()?
            .into_iter()
            .find(|session| session.session_id == session_id)
            .ok_or_else(|| DbError::internal("sync relay session missing after start"))
    }
    pub fn sync_finish_relay_session(
        &self,
        session_id: &str,
        status: &str,
        error: Option<&str>,
        rows_seen: u64,
        bytes_seen: u64,
    ) -> Result<()> {
        self.ensure_sync_tables()?;
        let sql = format!(
            "UPDATE {table} SET ended_at_micros = {ended_at_micros}, status = {status}, error = {error}, rows_seen = {rows_seen}, bytes_seen = {bytes_seen} WHERE session_id = {session_id}",
            table = crate::sync::RELAY_SESSIONS_TABLE,
            ended_at_micros = current_time_micros(),
            status = sql_text_literal(status),
            error = sql_nullable_text_literal(error),
            rows_seen = rows_seen,
            bytes_seen = bytes_seen,
            session_id = sql_text_literal(session_id),
        );
        let _ = self.execute(&sql)?;
        Ok(())
    }
    pub fn sync_changeset_history(&self) -> Result<Vec<SyncChangesetHistory>> {
        self.ensure_sync_tables()?;
        let sql = format!(
            "SELECT changeset_id, source_replica_id, source_kind, scope_name, shape_id, record_count, bytes, created_at_micros, applied_at_micros, outcome, integrity_hash FROM {} ORDER BY created_at_micros, changeset_id",
            crate::sync::CHANGESET_HISTORY_TABLE,
        );
        match self.execute(&sql) {
            Ok(result) => result
                .rows()
                .iter()
                .map(sync_changeset_history_from_row)
                .collect(),
            Err(error) => {
                let message = error.to_string();
                if message.contains("no such table") || message.contains("unknown table") {
                    Ok(Vec::new())
                } else {
                    Err(error)
                }
            }
        }
    }
    fn sync_authorize_scope(
        &self,
        principal: Option<&SyncPrincipal>,
        scope_name: &str,
    ) -> Result<()> {
        if let Some(principal) = principal {
            if !principal.allows_scope(scope_name) {
                return Err(DbError::sql(format!(
                    "SCOPE_UNAUTHORIZED: principal '{}' cannot access scope '{}'",
                    principal.subject_id, scope_name
                )));
            }
        }
        Ok(())
    }
    fn sync_authorize_shape(
        &self,
        principal: Option<&SyncPrincipal>,
        shape: &SyncShape,
    ) -> Result<()> {
        let Some(principal) = principal else {
            return Ok(());
        };
        if !principal.tenant_id.eq_ignore_ascii_case(&shape.tenant_id) {
            return Err(DbError::sql(format!(
                "AUTH_FORBIDDEN: shape '{}' belongs to tenant '{}'",
                shape.shape_id, shape.tenant_id
            )));
        }
        if !principal.allows_shape(&shape.shape_id) {
            return Err(DbError::sql(format!(
                "AUTH_FORBIDDEN: principal '{}' cannot access shape '{}'",
                principal.subject_id, shape.shape_id
            )));
        }
        if !shape.allowed_subjects.is_empty()
            && !shape
                .allowed_subjects
                .iter()
                .any(|subject| subject == "*" || subject == &principal.subject_id)
        {
            return Err(DbError::sql(format!(
                "AUTH_FORBIDDEN: subject '{}' is not allowed for shape '{}'",
                principal.subject_id, shape.shape_id
            )));
        }
        if !shape.allowed_roles.is_empty()
            && !principal.roles.iter().any(|role| {
                shape
                    .allowed_roles
                    .iter()
                    .any(|allowed| allowed == "*" || allowed == role)
            })
        {
            return Err(DbError::sql(format!(
                "AUTH_FORBIDDEN: principal '{}' lacks a role for shape '{}'",
                principal.subject_id, shape.shape_id
            )));
        }
        Ok(())
    }
    fn sync_create_shape_snapshot_changeset(
        &self,
        shape: &SyncShape,
        scope: &SyncScope,
        principal: Option<&SyncPrincipal>,
    ) -> Result<SyncChangeset> {
        let created_at_micros = current_time_micros();
        let tooling = self.get_tooling_metadata()?;
        let runtime = self.runtime_for_metadata_inspection()?;
        let schema_cookie = runtime.catalog.schema_cookie;
        let source_replica_id = self
            .sync_status()
            .ok()
            .and_then(|status| status.replica_id)
            .unwrap_or_else(|| "snapshot".to_string());
        let mut records = Vec::new();
        let mut origin_sequence = 1u64;
        for table_name in &scope.include_tables {
            let table = runtime.catalog.table(table_name).ok_or_else(|| {
                DbError::sql(format!("sync scope table '{table_name}' does not exist"))
            })?;
            let column_sql = table
                .columns
                .iter()
                .map(|column| sql_identifier(&column.name))
                .collect::<Vec<_>>()
                .join(", ");
            let order_by = table
                .primary_key_columns
                .iter()
                .map(|column| sql_identifier(column))
                .collect::<Vec<_>>()
                .join(", ");
            let where_sql = scope
                .row_filter
                .as_ref()
                .map(|filter| format!(" WHERE {filter}"))
                .unwrap_or_default();
            let sql = format!(
                "SELECT {column_sql} FROM {}{where_sql} ORDER BY {order_by}",
                sql_identifier(&table.name)
            );
            let result = self.execute(&sql)?;
            for row in result.rows() {
                let after = crate::sync::build_after_json(table, row.values());
                let primary_key = crate::sync::build_primary_key_json(table, row.values());
                records.push(SyncChangesetRecord {
                    record_version: 1,
                    table: table.name.clone(),
                    operation: "insert".to_string(),
                    primary_key,
                    origin_replica_id: source_replica_id.clone(),
                    origin_sequence,
                    transaction_id: format!("shape-snapshot:{}:{origin_sequence}", shape.shape_id),
                    transaction_lsn: origin_sequence,
                    schema_cookie,
                    before_hash: None,
                    before: None,
                    after: Some(after),
                    column_mask: table
                        .columns
                        .iter()
                        .map(|column| column.name.clone())
                        .collect(),
                    tombstone: false,
                    conflict_metadata: None,
                });
                origin_sequence += 1;
                if records.len() as u64 > shape.max_records {
                    return Err(DbError::sql(
                        "BATCH_TOO_LARGE: shape snapshot exceeds max_records",
                    ));
                }
            }
        }
        let high_watermark = self.sync_integrity_report()?.last_sequence.unwrap_or(0);
        let mut changeset = SyncChangeset {
            changeset_version: crate::sync::SYNC_CHANGESET_VERSION,
            changeset_id: sync_changeset_id(
                "shape_snapshot",
                &source_replica_id,
                created_at_micros,
                records.len(),
            ),
            source_replica_id,
            source_kind: crate::sync::SyncChangesetSourceKind::Snapshot,
            tenant_id: Some(
                principal
                    .map(|principal| principal.tenant_id.clone())
                    .unwrap_or_else(|| shape.tenant_id.clone()),
            ),
            scope_name: Some(scope.name.clone()),
            shape_id: Some(shape.shape_id.clone()),
            base_kind: "snapshot".to_string(),
            base_checkpoint: None,
            base_branch: None,
            base_snapshot: Some(format!("shape:{}", shape.shape_id)),
            start_checkpoint: records.first().map(|record| record.origin_sequence),
            end_checkpoint: records.last().map(|record| record.origin_sequence),
            source_high_watermark: Some(high_watermark),
            schema_fingerprint: tooling.schema_fingerprint,
            schema_cookie,
            sync_contract_version: crate::sync::SYNC_CONTRACT_VERSION,
            query_contract_fingerprint: None,
            producer_capabilities: SyncChangesetCapabilities::default(),
            limits: SyncChangesetLimits::default(),
            records,
            conflict_policy_hint: None,
            created_at_micros,
            integrity_hash: None,
        };
        self.sync_finalize_changeset(&mut changeset, None)?;
        self.sync_record_changeset_history(&changeset, "created", None)?;
        Ok(changeset)
    }
    fn sync_finalize_changeset(
        &self,
        changeset: &mut SyncChangeset,
        max_bytes: Option<u64>,
    ) -> Result<()> {
        changeset.limits.record_count = changeset.records.len() as u64;
        changeset.limits.uncompressed_bytes = 0;
        changeset.integrity_hash = None;
        let bytes = serde_json::to_vec(changeset)
            .map_err(|error| DbError::internal(format!("failed to serialize changeset: {error}")))?
            .len() as u64;
        if max_bytes.is_some_and(|limit| bytes > limit) {
            return Err(DbError::sql(format!(
                "BATCH_TOO_LARGE: changeset is {bytes} bytes"
            )));
        }
        changeset.limits.uncompressed_bytes = bytes;
        let hash = self.sync_changeset_integrity_hash(changeset)?;
        changeset.integrity_hash = Some(hash);
        Ok(())
    }
    fn sync_validate_changeset_envelope(&self, changeset: &SyncChangeset) -> Result<()> {
        if changeset.changeset_version != crate::sync::SYNC_CHANGESET_VERSION {
            return Err(DbError::sql(format!(
                "CHANGESET_UNSUPPORTED: unsupported changeset version {}",
                changeset.changeset_version
            )));
        }
        if changeset.sync_contract_version != crate::sync::SYNC_CONTRACT_VERSION {
            return Err(DbError::sql(format!(
                "CHANGESET_UNSUPPORTED: unsupported sync contract version {}",
                changeset.sync_contract_version
            )));
        }
        if changeset.changeset_id.trim().is_empty() {
            return Err(DbError::sql("CHANGESET_INVALID: changeset_id is required"));
        }
        let Some(expected_hash) = changeset.integrity_hash.as_deref() else {
            return Err(DbError::sql(
                "CHANGESET_INVALID: integrity_hash is required",
            ));
        };
        let actual_hash = self.sync_changeset_integrity_hash(changeset)?;
        if expected_hash != actual_hash {
            return Err(DbError::sql(
                "CHANGESET_INVALID: integrity_hash does not match payload",
            ));
        }
        for (index, record) in changeset.records.iter().enumerate() {
            if record.record_version != 1 {
                return Err(DbError::sql(format!(
                    "CHANGESET_UNSUPPORTED: record {index} uses version {}",
                    record.record_version
                )));
            }
            match record.operation.as_str() {
                "insert" | "update" if record.after.is_none() => {
                    return Err(DbError::sql(format!(
                        "CHANGESET_INVALID: record {index} operation '{}' requires after image",
                        record.operation
                    )));
                }
                "insert" | "update" | "delete" => {}
                other => {
                    return Err(DbError::sql(format!(
                        "CHANGESET_INVALID: unsupported record operation '{other}'"
                    )));
                }
            }
        }
        Ok(())
    }
    fn sync_check_changeset_compatibility(&self, changeset: &SyncChangeset) -> Result<()> {
        let tooling = self.get_tooling_metadata()?;
        if tooling.schema_fingerprint != changeset.schema_fingerprint {
            return Err(DbError::sql(format!(
                "SCHEMA_INCOMPATIBLE: local schema fingerprint {} does not match changeset {}",
                tooling.schema_fingerprint, changeset.schema_fingerprint
            )));
        }
        let runtime = self.runtime_for_metadata_inspection()?;
        if runtime.catalog.schema_cookie != changeset.schema_cookie {
            return Err(DbError::sql(format!(
                "SCHEMA_INCOMPATIBLE: local schema_cookie {} does not match changeset {}",
                runtime.catalog.schema_cookie, changeset.schema_cookie
            )));
        }
        Ok(())
    }
    fn sync_changeset_integrity_hash(&self, changeset: &SyncChangeset) -> Result<String> {
        let mut clone = changeset.clone();
        clone.integrity_hash = None;
        let bytes = serde_json::to_vec(&clone).map_err(|error| {
            DbError::internal(format!("failed to serialize changeset: {error}"))
        })?;
        let digest = Sha256::digest(&bytes);
        Ok(format!("sha256:{}", hex_encode(&digest)))
    }
    fn sync_record_changeset_history(
        &self,
        changeset: &SyncChangeset,
        outcome: &str,
        applied_at_micros: Option<i64>,
    ) -> Result<()> {
        self.ensure_sync_tables()?;
        let sql = format!(
            "INSERT INTO {table} (changeset_id, source_replica_id, source_kind, scope_name, shape_id, record_count, bytes, created_at_micros, applied_at_micros, outcome, integrity_hash) VALUES ({changeset_id}, {source_replica_id}, {source_kind}, {scope_name}, {shape_id}, {record_count}, {bytes}, {created_at_micros}, {applied_at_micros}, {outcome}, {integrity_hash}) ON CONFLICT (changeset_id) DO UPDATE SET applied_at_micros = COALESCE({applied_at_micros}, applied_at_micros), outcome = {outcome}, integrity_hash = {integrity_hash}",
            table = crate::sync::CHANGESET_HISTORY_TABLE,
            changeset_id = sql_text_literal(&changeset.changeset_id),
            source_replica_id = sql_text_literal(&changeset.source_replica_id),
            source_kind = sql_text_literal(changeset.source_kind.as_str()),
            scope_name = sql_nullable_text_literal(changeset.scope_name.as_deref()),
            shape_id = sql_nullable_text_literal(changeset.shape_id.as_deref()),
            record_count = changeset.records.len(),
            bytes = changeset.limits.uncompressed_bytes,
            created_at_micros = changeset.created_at_micros,
            applied_at_micros = applied_at_micros
                .map(|value| value.to_string())
                .unwrap_or_else(|| "NULL".to_string()),
            outcome = sql_text_literal(outcome),
            integrity_hash = sql_nullable_text_literal(changeset.integrity_hash.as_deref()),
        );
        let _ = self.execute(&sql)?;
        Ok(())
    }
    pub fn sync_conflict_policy(&self) -> Result<SyncConflictPolicyConfig> {
        let default_policy = self
            .sync_read_metadata("conflict_policy")?
            .map(|value| SyncConflictPolicy::from_str(&value))
            .transpose()?
            .unwrap_or_default();
        let origin_priority = match self.sync_read_metadata("conflict_origin_priority")? {
            Some(value) => serde_json::from_str::<Vec<String>>(&value).map_err(|error| {
                DbError::sql(format!(
                    "invalid sync conflict origin priority metadata: {error}"
                ))
            })?,
            None => Vec::new(),
        };
        Ok(SyncConflictPolicyConfig {
            default_policy,
            origin_priority,
        })
    }
    pub fn sync_set_conflict_policy(
        &self,
        policy: SyncConflictPolicy,
        origin_priority: &[&str],
    ) -> Result<()> {
        self.ensure_sync_tables()?;
        let origin_priority = origin_priority
            .iter()
            .map(|value| value.trim())
            .map(|value| {
                if value.is_empty() {
                    Err(DbError::sql(
                        "sync conflict origin priority entries must not be empty",
                    ))
                } else {
                    Ok(value.to_string())
                }
            })
            .collect::<Result<Vec<_>>>()?;
        self.sync_upsert_metadata("conflict_policy", policy.as_str())?;
        self.sync_upsert_metadata(
            "conflict_origin_priority",
            &serde_json::to_string(&origin_priority).map_err(|error| {
                DbError::internal(format!(
                    "failed to serialize sync conflict origin priority: {error}"
                ))
            })?,
        )?;
        Ok(())
    }
    pub fn sync_import_batch(&self, batch: &SyncChangeBatch) -> Result<SyncImportSummary> {
        let policy = self.sync_conflict_policy()?.default_policy;
        self.sync_import_batch_with_policy(batch, policy)
    }
    pub fn sync_import_batch_with_policy(
        &self,
        batch: &SyncChangeBatch,
        policy: SyncConflictPolicy,
    ) -> Result<SyncImportSummary> {
        batch.validate()?;
        self.ensure_sync_tables()?;

        let runtime = self.runtime_for_metadata_inspection()?;
        let schema_cookie = runtime.catalog.schema_cookie;
        let local_replica_id = self
            .inner
            .sync_ctx
            .replica_id()
            .or_else(|| self.sync_read_metadata("replica_id").ok().flatten());
        let batch_source_replica_id = batch.source_replica_id.as_deref();
        let batch_watermark = batch.source_high_watermark.or(batch.last_sequence);
        let current_peer_watermark = match batch_source_replica_id {
            Some(replica_id) => self.sync_peer_watermark(replica_id)?,
            None => None,
        };

        if let Some(local_replica_id) = local_replica_id.as_deref() {
            if batch_source_replica_id == Some(local_replica_id) {
                return Err(DbError::sql(format!(
                    "cannot import batch from same replica '{}'",
                    local_replica_id
                )));
            }
        }

        let _suppress_capture = self.inner.sync_ctx.suppress_capture();
        struct SyncImportTransaction<'a>(&'a Db, bool);
        impl<'a> SyncImportTransaction<'a> {
            fn new(db: &'a Db) -> Result<Self> {
                db.begin_transaction()?;
                Ok(Self(db, true))
            }
            fn commit(mut self) -> Result<()> {
                self.1 = false;
                self.0.commit_transaction()?;
                Ok(())
            }
        }
        impl Drop for SyncImportTransaction<'_> {
            fn drop(&mut self) {
                if self.1 {
                    let _ = self.0.rollback_transaction();
                }
            }
        }

        if let Some(batch_watermark) = batch_watermark {
            if current_peer_watermark.is_some_and(|watermark| batch_watermark <= watermark) {
                if let Some(replica_id) = batch_source_replica_id {
                    let watermark = current_peer_watermark
                        .map_or(batch_watermark, |current| current.max(batch_watermark));
                    self.sync_upsert_metadata(
                        &peer_watermark_key(replica_id),
                        &watermark.to_string(),
                    )?;
                }
                return Ok(SyncImportSummary {
                    seen: batch.record_count,
                    applied: 0,
                    skipped: batch.record_count,
                    conflicted: 0,
                });
            }
        }

        let tx = SyncImportTransaction::new(self)?;
        let mut applied = 0usize;
        let mut skipped = 0usize;
        let mut conflicted = 0usize;
        let mut stop_conflict: Option<(SyncJournalRecord, SyncConflictRecordData)> = None;

        for record in &batch.records {
            if let Some(local_replica_id) = local_replica_id.as_deref() {
                if record.replica_id == local_replica_id {
                    return Err(DbError::sql(format!(
                        "cannot import record from same replica '{}'",
                        local_replica_id
                    )));
                }
            }

            if let Some(watermark) = current_peer_watermark {
                if record.sequence <= watermark {
                    skipped += 1;
                    continue;
                }
            }

            if record.schema_version != 1 {
                return Err(DbError::sql(format!(
                    "unsupported sync record schema version {}",
                    record.schema_version
                )));
            }

            if record.schema_cookie != schema_cookie {
                return Err(DbError::sql(format!(
                    "schema mismatch for table '{}': record has schema_cookie {} but local schema is {}",
                    record.table, record.schema_cookie, schema_cookie
                )));
            }

            let table = runtime
                .catalog
                .table(&record.table)
                .ok_or_else(|| DbError::sql(format!("unknown table '{}'", record.table)))?;
            if crate::sync::is_internal_table_name(&table.name) {
                return Err(DbError::sql(format!(
                    "cannot import into internal table '{}'",
                    table.name
                )));
            }
            let marker_key = imported_record_key(&record.replica_id, record.sequence);
            if self.sync_read_metadata(&marker_key)?.is_some() {
                skipped += 1;
                continue;
            }

            let outcome = self.sync_apply_import_record(batch, record, table, &policy)?;
            match outcome {
                SyncImportRecordOutcome::Applied => {
                    self.sync_upsert_metadata(&marker_key, "applied")?;
                    applied += 1;
                }
                SyncImportRecordOutcome::Conflict(conflict) => {
                    if matches!(policy, SyncConflictPolicy::Stop) {
                        stop_conflict = Some((record.clone(), conflict));
                        break;
                    }
                    self.record_sync_conflict_with_data(batch, record, &conflict)?;
                    conflicted += 1;
                }
                SyncImportRecordOutcome::Resolved(conflict) => {
                    self.sync_upsert_metadata(&marker_key, "applied")?;
                    self.record_sync_conflict_with_data(batch, record, &conflict)?;
                    applied += 1;
                    conflicted += 1;
                }
            }
        }

        if let Some((record, conflict)) = stop_conflict {
            drop(tx);
            let conflict_id = self.record_sync_conflict_with_data(batch, &record, &conflict)?;
            return Err(DbError::sql(format!(
                "sync import stopped on conflict {}",
                conflict_id
            )));
        }

        if let (Some(replica_id), Some(batch_watermark)) =
            (batch_source_replica_id, batch_watermark)
        {
            let watermark = current_peer_watermark
                .map_or(batch_watermark, |current| current.max(batch_watermark));
            self.sync_upsert_metadata(&peer_watermark_key(replica_id), &watermark.to_string())?;
        }

        crate::reactive::with_change_source(ChangeSource::SyncApply, || tx.commit())?;
        Ok(SyncImportSummary {
            seen: batch.record_count,
            applied,
            skipped,
            conflicted,
        })
    }
    pub fn sync_import_records(&self, records: &[SyncJournalRecord]) -> Result<SyncImportSummary> {
        let batch = SyncChangeBatch::from_records(records.to_vec())?;
        self.sync_import_batch(&batch)
    }
    pub fn sync_peer_watermark(&self, replica_id: &str) -> Result<Option<u64>> {
        match self.sync_read_metadata(&peer_watermark_key(replica_id))? {
            Some(value) => value
                .parse::<u64>()
                .map(Some)
                .map_err(|error| DbError::sql(format!("invalid peer watermark value: {error}"))),
            None => Ok(None),
        }
    }
    pub fn sync_peer_out_watermark(&self, peer_name: &str) -> Result<Option<u64>> {
        match self.sync_read_metadata(&peer_out_watermark_key(peer_name))? {
            Some(value) => value.parse::<u64>().map(Some).map_err(|error| {
                DbError::sql(format!("invalid peer outbound watermark value: {error}"))
            }),
            None => Ok(None),
        }
    }
    pub fn sync_set_peer_out_watermark(&self, peer_name: &str, watermark: u64) -> Result<()> {
        self.ensure_sync_tables()?;
        self.sync_upsert_metadata(&peer_out_watermark_key(peer_name), &watermark.to_string())
    }
    pub fn sync_conflicts(&self) -> Result<Vec<SyncConflict>> {
        self.ensure_sync_tables()?;
        let sql = format!(
            "SELECT * FROM {} WHERE resolved = 0 ORDER BY conflict_id",
            crate::sync::CONFLICTS_TABLE
        );
        match self.execute(&sql) {
            Ok(result) => result.rows().iter().map(sync_conflict_from_row).collect(),
            Err(error) => {
                let message = error.to_string();
                if message.contains("no such table") || message.contains("unknown table") {
                    Ok(Vec::new())
                } else {
                    Err(error)
                }
            }
        }
    }
    pub fn sync_conflicts_all(&self) -> Result<Vec<SyncConflict>> {
        self.ensure_sync_tables()?;
        let sql = format!(
            "SELECT * FROM {} ORDER BY conflict_id",
            crate::sync::CONFLICTS_TABLE
        );
        match self.execute(&sql) {
            Ok(result) => result.rows().iter().map(sync_conflict_from_row).collect(),
            Err(error) => {
                let message = error.to_string();
                if message.contains("no such table") || message.contains("unknown table") {
                    Ok(Vec::new())
                } else {
                    Err(error)
                }
            }
        }
    }
    pub fn sync_conflict(&self, conflict_id: i64) -> Result<Option<SyncConflict>> {
        self.ensure_sync_tables()?;
        let sql = format!(
            "SELECT * FROM {} WHERE conflict_id = {}",
            crate::sync::CONFLICTS_TABLE,
            conflict_id
        );
        match self.execute(&sql) {
            Ok(result) => Ok(result
                .rows()
                .first()
                .map(sync_conflict_from_row)
                .transpose()?),
            Err(error) => {
                let message = error.to_string();
                if message.contains("no such table") || message.contains("unknown table") {
                    Ok(None)
                } else {
                    Err(error)
                }
            }
        }
    }
    pub fn sync_resolve_conflict_keep_local(
        &self,
        conflict_id: i64,
        resolved_by: Option<&str>,
        note: Option<&str>,
    ) -> Result<bool> {
        self.sync_update_conflict_resolution(
            conflict_id,
            Some("keep_local"),
            resolved_by,
            note,
            Some(current_time_micros()),
        )
    }
    pub fn sync_resolve_conflict_apply_remote(
        &self,
        conflict_id: i64,
        resolved_by: Option<&str>,
        note: Option<&str>,
    ) -> Result<bool> {
        let Some(conflict) = self.sync_conflict(conflict_id)? else {
            return Ok(false);
        };
        let record: SyncJournalRecord = serde_json::from_value(conflict.remote_record_json.clone())
            .map_err(|error| {
                DbError::corruption(format!(
                    "malformed sync conflict remote_record_json: {error}"
                ))
            })?;
        let batch = SyncChangeBatch::from_records(vec![record.clone()])?;
        let policy = SyncConflictPolicy::LastWriterWins;
        let _suppress_capture = self.inner.sync_ctx.suppress_capture();
        let tx = {
            self.begin_transaction()?;
            struct Tx<'a>(&'a Db, bool);
            impl<'a> Drop for Tx<'a> {
                fn drop(&mut self) {
                    if self.1 {
                        let _ = self.0.rollback_transaction();
                    }
                }
            }
            impl<'a> Tx<'a> {
                fn commit(mut self) -> Result<()> {
                    self.1 = false;
                    self.0.commit_transaction()?;
                    Ok(())
                }
            }
            Tx(self, true)
        };
        let runtime = self.runtime_for_metadata_inspection()?;
        let table = runtime
            .catalog
            .table(&record.table)
            .ok_or_else(|| DbError::sql(format!("unknown table '{}'", record.table)))?;
        match self.sync_apply_import_record(&batch, &record, table, &policy)? {
            SyncImportRecordOutcome::Applied => {
                self.sync_upsert_metadata(
                    &imported_record_key(&record.replica_id, record.sequence),
                    "applied",
                )?;
                self.sync_update_conflict_resolution(
                    conflict_id,
                    Some("apply_remote"),
                    resolved_by,
                    note,
                    Some(current_time_micros()),
                )?;
                tx.commit()?;
                Ok(true)
            }
            SyncImportRecordOutcome::Resolved(_) => {
                self.sync_upsert_metadata(
                    &imported_record_key(&record.replica_id, record.sequence),
                    "applied",
                )?;
                self.sync_update_conflict_resolution(
                    conflict_id,
                    Some("apply_remote"),
                    resolved_by,
                    note,
                    Some(current_time_micros()),
                )?;
                tx.commit()?;
                Ok(true)
            }
            SyncImportRecordOutcome::Conflict(conflict) => {
                let _ = conflict;
                Err(DbError::sql(format!(
                    "cannot apply remote conflict {} because replay now fails",
                    conflict_id
                )))
            }
        }
    }
    pub fn sync_reopen_conflict(&self, conflict_id: i64) -> Result<bool> {
        self.sync_update_conflict_resolution(conflict_id, None, None, None, None)
    }
    pub fn sync_prune_journal_through(&self, sequence: u64) -> Result<usize> {
        self.sync_prune_journal(sequence, false, false)
            .map(|summary| summary.pruned)
    }
    pub fn sync_prune_journal(
        &self,
        through: u64,
        dry_run: bool,
        allow_data_loss: bool,
    ) -> Result<SyncPruneSummary> {
        let retention = self.sync_retention_report()?;
        let requested_through = through;
        if !allow_data_loss && through > retention.safe_prune_through.unwrap_or(0) {
            let message = if let Some(lowest_watermark) =
                retention.safe_prune_through.map(|value| value + 1)
            {
                format!(
                    "cannot prune through {through}; lowest peer watermark is {lowest_watermark}"
                )
            } else if retention.blocked_by.is_empty() {
                format!("cannot prune through {through}; no peer watermarks are known")
            } else {
                format!("cannot prune through {through}; lowest peer watermark is 0")
            };
            return Err(DbError::sql(message));
        }

        let records = crate::sync::read_journal_records(
            self.inner.sync_ctx.journal_path(),
            &self.inner.vfs,
            0,
            usize::MAX,
        )?;
        if records.is_empty() {
            return Ok(SyncPruneSummary {
                requested_through,
                effective_through: 0,
                pruned: 0,
                dry_run,
                allow_data_loss,
                blocked_by: retention.blocked_by,
            });
        }

        let effective_through = records
            .last()
            .map(|record| record.sequence.min(through))
            .unwrap_or(0);
        let total_records = records.len();
        let retained = records
            .into_iter()
            .filter(|record| record.sequence > through)
            .collect::<Vec<_>>();
        let pruned = total_records.saturating_sub(retained.len());

        if dry_run || pruned == 0 {
            return Ok(SyncPruneSummary {
                requested_through,
                effective_through,
                pruned,
                dry_run,
                allow_data_loss,
                blocked_by: retention.blocked_by,
            });
        }

        let mut buffer = Vec::new();
        for record in &retained {
            serde_json::to_writer(&mut buffer, record).map_err(|error| {
                DbError::internal(format!("failed to serialize sync journal record: {error}"))
            })?;
            buffer.push(b'\n');
        }

        if self
            .inner
            .vfs
            .file_exists(self.inner.sync_ctx.journal_path())?
        {
            let journal_file = self.inner.sync_ctx.journal_file_handle()?;
            let journal_file = match journal_file {
                Some(file) => file,
                None => self.inner.vfs.open(
                    self.inner.sync_ctx.journal_path(),
                    OpenMode::OpenExisting,
                    FileKind::SyncJournal,
                )?,
            };

            journal_file.set_len(0)?;
            write_all_at(journal_file.as_ref(), 0, &buffer)?;
            journal_file.sync_data()?;
            self.inner
                .sync_ctx
                .set_journal_write_offset(buffer.len() as u64)?;
        }

        Ok(SyncPruneSummary {
            requested_through,
            effective_through,
            pruned,
            dry_run,
            allow_data_loss,
            blocked_by: retention.blocked_by,
        })
    }
    pub fn sync_set_enabled(&self, enabled: bool) -> Result<()> {
        self.ensure_sync_tables()?;
        self.sync_upsert_metadata("enabled", if enabled { "true" } else { "false" })?;
        self.inner.sync_ctx.set_enabled(enabled);
        if enabled {
            self.inner.sync_ctx.ensure_journal_open(&self.inner.vfs)?;
        }
        Ok(())
    }
    pub fn sync_is_enabled(&self) -> Result<bool> {
        if self.inner.sync_ctx.is_enabled() {
            return Ok(true);
        }
        let status = self.load_sync_status_from_db()?;
        if status.enabled {
            self.inner.sync_ctx.set_enabled(true);
            self.inner
                .sync_ctx
                .set_replica_id(&status.replica_id.unwrap_or_default());
            self.inner.sync_ctx.set_next_sequence(status.next_sequence);
        }
        Ok(status.enabled)
    }
    fn sync_upsert_metadata(&self, key: &str, value: &str) -> Result<()> {
        let sql = format!(
            "INSERT INTO {table} (key, value) VALUES ('{k}', '{v}') ON CONFLICT (key) DO UPDATE SET value = '{v}'",
            table = crate::sync::METADATA_TABLE,
            k = key.replace('\'', "''"),
            v = value.replace('\'', "''"),
        );
        let _ = self.execute(&sql)?;
        Ok(())
    }
    pub(crate) fn sync_read_metadata(&self, key: &str) -> Result<Option<String>> {
        let sql = format!(
            "SELECT value FROM {} WHERE key = '{}'",
            crate::sync::METADATA_TABLE,
            key.replace('\'', "''"),
        );
        match self.execute(&sql) {
            Ok(result) => {
                if let Some(row) = result.rows().first() {
                    if let Some(val) = row.values().first() {
                        match val {
                            Value::Text(s) => return Ok(Some(s.clone())),
                            _ => return Ok(None),
                        }
                    }
                }
                Ok(None)
            }
            Err(e) => {
                let msg = e.to_string();
                if msg.contains("no such table") || msg.contains("unknown table") {
                    return Ok(None);
                }
                Err(e)
            }
        }
    }
    fn sync_metadata_entries(&self) -> Result<Vec<(String, String)>> {
        let sql = format!("SELECT key, value FROM {}", crate::sync::METADATA_TABLE);
        match self.execute(&sql) {
            Ok(result) => result
                .rows()
                .iter()
                .map(|row| {
                    let key = row
                        .values()
                        .first()
                        .and_then(|value| match value {
                            Value::Text(text) => Some(text.clone()),
                            _ => None,
                        })
                        .ok_or_else(|| DbError::corruption("malformed sync metadata row"))?;
                    let value = row
                        .values()
                        .get(1)
                        .and_then(|value| match value {
                            Value::Text(text) => Some(text.clone()),
                            _ => None,
                        })
                        .ok_or_else(|| DbError::corruption("malformed sync metadata row"))?;
                    Ok((key, value))
                })
                .collect(),
            Err(error) => {
                let message = error.to_string();
                if message.contains("no such table") || message.contains("unknown table") {
                    Ok(Vec::new())
                } else {
                    Err(error)
                }
            }
        }
    }
    fn sync_peer_watermark_entries(&self) -> Result<Vec<(String, u64)>> {
        self.sync_metadata_entries()?
            .into_iter()
            .filter_map(|(key, value)| {
                key.strip_prefix("peer_watermark:")
                    .map(|replica_id| (replica_id.to_string(), value))
            })
            .map(|(replica_id, value)| {
                let watermark = value.parse::<u64>().map_err(|error| {
                    DbError::sql(format!(
                        "invalid peer watermark value for replica '{}': {}",
                        replica_id, error
                    ))
                })?;
                Ok((format!("remote:{replica_id}"), watermark))
            })
            .collect()
    }
    fn sync_capture_local_row_json(
        &self,
        table: &TableSchema,
        primary_key: &serde_json::Map<String, JsonValue>,
    ) -> Result<Option<serde_json::Value>> {
        let (mut runtime, snapshot_lsn) = self.runtime_for_targeted_row_source_inspection()?;
        if let Some(snapshot_lsn) = snapshot_lsn {
            self.load_runtime_table_row_sources_at_snapshot(
                &mut runtime,
                &[table.name.as_str()],
                snapshot_lsn,
            )?;
        }
        let Some(source) = runtime.table_row_source(&table.name) else {
            return Ok(None);
        };
        for row in source.rows() {
            let row = row?;
            let values = row.values();
            let mut matches = true;
            for pk_col in &table.primary_key_columns {
                let column = table
                    .columns
                    .iter()
                    .find(|column| column.name == *pk_col)
                    .ok_or_else(|| {
                        DbError::sql(format!(
                            "table '{}' missing primary key column '{}'",
                            table.name, pk_col
                        ))
                    })?;
                let json_value = primary_key.get(pk_col).ok_or_else(|| {
                    DbError::sql(format!(
                        "missing primary key column '{pk_col}' in record for table '{}'",
                        table.name
                    ))
                })?;
                let expected = json_to_column_value(&table.name, column, json_value)?;
                let Some(actual) = values.get(
                    table
                        .columns
                        .iter()
                        .position(|candidate| candidate.name == *pk_col)
                        .ok_or_else(|| {
                            DbError::sql(format!(
                                "table '{}' missing primary key column '{}'",
                                table.name, pk_col
                            ))
                        })?,
                ) else {
                    matches = false;
                    break;
                };
                if actual != &expected {
                    matches = false;
                    break;
                }
            }
            if matches {
                return Ok(Some(crate::sync::build_after_json(table, values)));
            }
        }
        Ok(None)
    }
    fn sync_apply_import_record(
        &self,
        _batch: &SyncChangeBatch,
        record: &SyncJournalRecord,
        table: &TableSchema,
        policy: &SyncConflictPolicy,
    ) -> Result<SyncImportRecordOutcome> {
        let primary_key = record
            .primary_key
            .as_object()
            .ok_or_else(|| DbError::sql("primary_key must be an object"))?;
        let local_row_json = self.sync_capture_local_row_json(table, primary_key)?;
        let operation = match record.operation.as_str() {
            "insert" => SyncOperation::Insert,
            "update" => SyncOperation::Update,
            "delete" => SyncOperation::Delete,
            other => return Err(DbError::sql(format!("unsupported operation '{other}'"))),
        };

        let remote_wins = match policy {
            SyncConflictPolicy::Record | SyncConflictPolicy::Stop => false,
            SyncConflictPolicy::LastWriterWins => true,
            SyncConflictPolicy::OriginPriority => {
                let config = self.sync_conflict_policy()?;
                match self
                    .inner
                    .sync_ctx
                    .replica_id()
                    .or_else(|| self.sync_read_metadata("replica_id").ok().flatten())
                {
                    Some(local_replica_id) => {
                        let remote_index = config
                            .origin_priority
                            .iter()
                            .position(|replica| replica == &record.replica_id);
                        let local_index = config
                            .origin_priority
                            .iter()
                            .position(|replica| replica == &local_replica_id);
                        matches!((remote_index, local_index), (Some(remote), Some(local)) if remote < local)
                    }
                    None => false,
                }
            }
        };

        let apply_remote_replace = |operation: SyncOperation| -> Result<()> {
            let sql = format!(
                "DELETE FROM {} WHERE {}",
                sql_identifier(&table.name),
                table
                    .primary_key_columns
                    .iter()
                    .enumerate()
                    .map(|(idx, pk_col)| format!("{} = ${}", sql_identifier(pk_col), idx + 1))
                    .collect::<Vec<_>>()
                    .join(" AND ")
            );
            let mut where_values = Vec::with_capacity(table.primary_key_columns.len());
            for pk_col in &table.primary_key_columns {
                let column = table
                    .columns
                    .iter()
                    .find(|column| column.name == *pk_col)
                    .ok_or_else(|| {
                        DbError::sql(format!(
                            "table '{}' missing primary key column '{}'",
                            table.name, pk_col
                        ))
                    })?;
                let json_value = primary_key.get(pk_col).ok_or_else(|| {
                    DbError::sql(format!(
                        "missing primary key column '{pk_col}' in record for table '{}'",
                        table.name
                    ))
                })?;
                where_values.push(json_to_column_value(&table.name, column, json_value)?);
            }
            let _ = self.execute_with_params(&sql, &where_values)?;

            if matches!(operation, SyncOperation::Delete) {
                return Ok(());
            }

            let after = record
                .after
                .as_ref()
                .ok_or_else(|| DbError::sql("remote record missing after payload"))?
                .as_object()
                .ok_or_else(|| DbError::sql("remote record after payload must be an object"))?;
            let mut columns = Vec::with_capacity(table.columns.len());
            let mut values = Vec::with_capacity(table.columns.len());
            for column in &table.columns {
                let json_value = after.get(&column.name).ok_or_else(|| {
                    DbError::sql(format!(
                        "missing column '{}' in after payload for table '{}'",
                        column.name, table.name
                    ))
                })?;
                columns.push(sql_identifier(&column.name));
                values.push(json_to_column_value(&table.name, column, json_value)?);
            }
            let sql = format!(
                "INSERT INTO {} ({}) VALUES ({})",
                sql_identifier(&table.name),
                columns.join(", "),
                (1..=values.len())
                    .map(|idx| format!("${idx}"))
                    .collect::<Vec<_>>()
                    .join(", ")
            );
            let _ = self.execute_with_params(&sql, &values)?;
            Ok(())
        };

        match operation {
            SyncOperation::Insert => {
                let after = record
                    .after
                    .as_ref()
                    .ok_or_else(|| DbError::sql("insert record missing after payload"))?
                    .as_object()
                    .ok_or_else(|| DbError::sql("after must be an object for insert"))?;
                let mut columns = Vec::with_capacity(table.columns.len());
                let mut values = Vec::with_capacity(table.columns.len());
                for column in &table.columns {
                    let json_value = after.get(&column.name).ok_or_else(|| {
                        DbError::sql(format!(
                            "missing column '{}' in after payload for table '{}'",
                            column.name, table.name
                        ))
                    })?;
                    columns.push(sql_identifier(&column.name));
                    values.push(json_to_column_value(&table.name, column, json_value)?);
                }
                let sql = format!(
                    "INSERT INTO {} ({}) VALUES ({})",
                    sql_identifier(&table.name),
                    columns.join(", "),
                    (1..=values.len())
                        .map(|idx| format!("${idx}"))
                        .collect::<Vec<_>>()
                        .join(", ")
                );
                match self.execute_with_params(&sql, &values) {
                    Ok(_) => Ok(SyncImportRecordOutcome::Applied),
                    Err(DbError::Constraint { message }) if remote_wins => {
                        apply_remote_replace(SyncOperation::Insert)?;
                        Ok(SyncImportRecordOutcome::Resolved(SyncConflictRecordData {
                            conflict_type: "insert_insert".to_string(),
                            message,
                            local_row_json,
                            resolution: Some("remote_applied".to_string()),
                            resolved_at_micros: Some(current_time_micros()),
                            resolved_by: Some("sync_policy".to_string()),
                            resolution_note: None,
                            policy_name: Some(policy.as_str().to_string()),
                        }))
                    }
                    Err(DbError::Constraint { message }) => {
                        Ok(SyncImportRecordOutcome::Conflict(SyncConflictRecordData {
                            conflict_type: if local_row_json.is_some() {
                                "insert_insert".to_string()
                            } else {
                                "constraint_error".to_string()
                            },
                            message,
                            local_row_json,
                            resolution: None,
                            resolved_at_micros: None,
                            resolved_by: None,
                            resolution_note: None,
                            policy_name: Some(policy.as_str().to_string()),
                        }))
                    }
                    Err(error) => Ok(SyncImportRecordOutcome::Conflict(SyncConflictRecordData {
                        conflict_type: "apply_error".to_string(),
                        message: error.to_string(),
                        local_row_json,
                        resolution: None,
                        resolved_at_micros: None,
                        resolved_by: None,
                        resolution_note: None,
                        policy_name: Some(policy.as_str().to_string()),
                    })),
                }
            }
            SyncOperation::Update => {
                let after = record
                    .after
                    .as_ref()
                    .ok_or_else(|| DbError::sql("update record missing after payload"))?
                    .as_object()
                    .ok_or_else(|| DbError::sql("after must be an object for update"))?;
                let mut params =
                    Vec::with_capacity(table.columns.len() + table.primary_key_columns.len());
                let mut expressions = Vec::with_capacity(table.columns.len());
                for column in &table.columns {
                    let json_value = after.get(&column.name).ok_or_else(|| {
                        DbError::sql(format!(
                            "missing column '{}' in update payload for table '{}'",
                            column.name, table.name
                        ))
                    })?;
                    params.push(json_to_column_value(&table.name, column, json_value)?);
                    expressions.push(format!(
                        "{} = ${}",
                        sql_identifier(&column.name),
                        params.len()
                    ));
                }
                for pk_col in &table.primary_key_columns {
                    let column = table
                        .columns
                        .iter()
                        .find(|column| column.name == *pk_col)
                        .ok_or_else(|| {
                            DbError::sql(format!(
                                "table '{}' missing primary key column '{}'",
                                table.name, pk_col
                            ))
                        })?;
                    let json_value = primary_key.get(pk_col).ok_or_else(|| {
                        DbError::sql(format!(
                            "missing primary key column '{pk_col}' in record for table '{}'",
                            table.name
                        ))
                    })?;
                    params.push(json_to_column_value(&table.name, column, json_value)?);
                }
                let sql = format!(
                    "UPDATE {} SET {} WHERE {}",
                    sql_identifier(&table.name),
                    expressions.join(", "),
                    table
                        .primary_key_columns
                        .iter()
                        .enumerate()
                        .map(|(idx, pk_col)| format!(
                            "{} = ${}",
                            sql_identifier(pk_col),
                            table.columns.len() + idx + 1
                        ))
                        .collect::<Vec<_>>()
                        .join(" AND ")
                );
                match self.execute_with_params(&sql, &params) {
                    Ok(result) if result.affected_rows() == 0 => {
                        Ok(SyncImportRecordOutcome::Conflict(SyncConflictRecordData {
                            conflict_type: "missing_target".to_string(),
                            message: "update affected no rows".to_string(),
                            local_row_json,
                            resolution: None,
                            resolved_at_micros: None,
                            resolved_by: None,
                            resolution_note: None,
                            policy_name: Some(policy.as_str().to_string()),
                        }))
                    }
                    Ok(_) => Ok(SyncImportRecordOutcome::Applied),
                    Err(DbError::Constraint { message }) if remote_wins => {
                        apply_remote_replace(SyncOperation::Update)?;
                        Ok(SyncImportRecordOutcome::Resolved(SyncConflictRecordData {
                            conflict_type: "update_update".to_string(),
                            message,
                            local_row_json,
                            resolution: Some("remote_applied".to_string()),
                            resolved_at_micros: Some(current_time_micros()),
                            resolved_by: Some("sync_policy".to_string()),
                            resolution_note: None,
                            policy_name: Some(policy.as_str().to_string()),
                        }))
                    }
                    Err(DbError::Constraint { message }) => {
                        Ok(SyncImportRecordOutcome::Conflict(SyncConflictRecordData {
                            conflict_type: if local_row_json.is_some() {
                                "update_update".to_string()
                            } else {
                                "constraint_error".to_string()
                            },
                            message,
                            local_row_json,
                            resolution: None,
                            resolved_at_micros: None,
                            resolved_by: None,
                            resolution_note: None,
                            policy_name: Some(policy.as_str().to_string()),
                        }))
                    }
                    Err(error) => Ok(SyncImportRecordOutcome::Conflict(SyncConflictRecordData {
                        conflict_type: "apply_error".to_string(),
                        message: error.to_string(),
                        local_row_json,
                        resolution: None,
                        resolved_at_micros: None,
                        resolved_by: None,
                        resolution_note: None,
                        policy_name: Some(policy.as_str().to_string()),
                    })),
                }
            }
            SyncOperation::Delete => {
                let mut where_values = Vec::with_capacity(table.primary_key_columns.len());
                let mut where_parts = Vec::with_capacity(table.primary_key_columns.len());
                for pk_col in &table.primary_key_columns {
                    let column = table
                        .columns
                        .iter()
                        .find(|column| column.name == *pk_col)
                        .ok_or_else(|| {
                            DbError::sql(format!(
                                "table '{}' missing primary key column '{}'",
                                table.name, pk_col
                            ))
                        })?;
                    let json_value = primary_key.get(pk_col).ok_or_else(|| {
                        DbError::sql(format!(
                            "missing primary key column '{pk_col}' in record for table '{}'",
                            table.name
                        ))
                    })?;
                    where_values.push(json_to_column_value(&table.name, column, json_value)?);
                    where_parts.push(format!(
                        "{} = ${}",
                        sql_identifier(pk_col),
                        where_values.len()
                    ));
                }
                let sql = format!(
                    "DELETE FROM {} WHERE {}",
                    sql_identifier(&table.name),
                    where_parts.join(" AND ")
                );
                match self.execute_with_params(&sql, &where_values) {
                    Ok(result) if result.affected_rows() == 0 => {
                        Ok(SyncImportRecordOutcome::Conflict(SyncConflictRecordData {
                            conflict_type: "missing_target".to_string(),
                            message: "delete affected no rows".to_string(),
                            local_row_json,
                            resolution: None,
                            resolved_at_micros: None,
                            resolved_by: None,
                            resolution_note: None,
                            policy_name: Some(policy.as_str().to_string()),
                        }))
                    }
                    Ok(_) => Ok(SyncImportRecordOutcome::Applied),
                    Err(DbError::Constraint { message }) if remote_wins => {
                        apply_remote_replace(SyncOperation::Delete)?;
                        Ok(SyncImportRecordOutcome::Resolved(SyncConflictRecordData {
                            conflict_type: "delete_update".to_string(),
                            message,
                            local_row_json,
                            resolution: Some("remote_applied".to_string()),
                            resolved_at_micros: Some(current_time_micros()),
                            resolved_by: Some("sync_policy".to_string()),
                            resolution_note: None,
                            policy_name: Some(policy.as_str().to_string()),
                        }))
                    }
                    Err(DbError::Constraint { message }) => {
                        Ok(SyncImportRecordOutcome::Conflict(SyncConflictRecordData {
                            conflict_type: if local_row_json.is_some() {
                                "delete_update".to_string()
                            } else {
                                "constraint_error".to_string()
                            },
                            message,
                            local_row_json,
                            resolution: None,
                            resolved_at_micros: None,
                            resolved_by: None,
                            resolution_note: None,
                            policy_name: Some(policy.as_str().to_string()),
                        }))
                    }
                    Err(error) => Ok(SyncImportRecordOutcome::Conflict(SyncConflictRecordData {
                        conflict_type: "apply_error".to_string(),
                        message: error.to_string(),
                        local_row_json,
                        resolution: None,
                        resolved_at_micros: None,
                        resolved_by: None,
                        resolution_note: None,
                        policy_name: Some(policy.as_str().to_string()),
                    })),
                }
            }
        }
    }
    fn sync_update_conflict_resolution(
        &self,
        conflict_id: i64,
        resolution: Option<&str>,
        resolved_by: Option<&str>,
        note: Option<&str>,
        resolved_at_micros: Option<i64>,
    ) -> Result<bool> {
        self.ensure_sync_tables()?;
        let Some(_) = self.sync_conflict(conflict_id)? else {
            return Ok(false);
        };
        let sql = format!(
            "UPDATE {table} SET resolved = {resolved}, resolution = {resolution}, resolved_at_micros = {resolved_at_micros}, resolved_by = {resolved_by}, resolution_note = {resolution_note} WHERE conflict_id = {conflict_id}",
            table = crate::sync::CONFLICTS_TABLE,
            resolved = if resolution.is_some() { 1 } else { 0 },
            resolution = resolution
                .map(sql_text_literal)
                .unwrap_or_else(|| "NULL".to_string()),
            resolved_at_micros = resolved_at_micros
                .map(|value| value.to_string())
                .unwrap_or_else(|| "NULL".to_string()),
            resolved_by = resolved_by
                .map(sql_text_literal)
                .unwrap_or_else(|| "NULL".to_string()),
            resolution_note = note
                .map(sql_text_literal)
                .unwrap_or_else(|| "NULL".to_string()),
            conflict_id = conflict_id,
        );
        let _ = self.execute(&sql)?;
        Ok(true)
    }
    fn sync_update_session(
        &self,
        session_id: i64,
        summary: &SyncRunSummary,
        status: &str,
        error: Option<&str>,
        ended_at_micros: i64,
    ) -> Result<()> {
        self.ensure_sync_tables()?;
        let (
            pushed_seen,
            pushed_applied,
            pushed_skipped,
            pushed_conflicted,
            pulled_seen,
            pulled_applied,
            pulled_skipped,
            pulled_conflicted,
        ) = sync_session_summary_counts(summary);
        let sql = format!(
            "UPDATE {table} SET remote_replica_id = {remote_replica_id}, ended_at_micros = {ended_at_micros}, status = {status}, error = {error}, pushed_batch_id = {pushed_batch_id}, pulled_batch_id = {pulled_batch_id}, pushed_seen = {pushed_seen}, pushed_applied = {pushed_applied}, pushed_skipped = {pushed_skipped}, pushed_conflicted = {pushed_conflicted}, pulled_seen = {pulled_seen}, pulled_applied = {pulled_applied}, pulled_skipped = {pulled_skipped}, pulled_conflicted = {pulled_conflicted}, retry_count = {retry_count} WHERE session_id = {session_id}",
            table = crate::sync::SESSIONS_TABLE,
            remote_replica_id = sql_nullable_text_literal(summary.remote_replica_id.as_deref()),
            ended_at_micros = ended_at_micros,
            status = sql_text_literal(status),
            error = sql_nullable_text_literal(error),
            pushed_batch_id = sql_nullable_text_literal(summary.pushed_batch_id.as_deref()),
            pulled_batch_id = sql_nullable_text_literal(summary.pulled_batch_id.as_deref()),
            pushed_seen = pushed_seen,
            pushed_applied = pushed_applied,
            pushed_skipped = pushed_skipped,
            pushed_conflicted = pushed_conflicted,
            pulled_seen = pulled_seen,
            pulled_applied = pulled_applied,
            pulled_skipped = pulled_skipped,
            pulled_conflicted = pulled_conflicted,
            retry_count = summary.retry_count as i64,
            session_id = session_id,
        );
        let _ = self.execute(&sql)?;
        Ok(())
    }
    pub(crate) fn sync_table_columns(&self, table_name: &str) -> Result<Vec<String>> {
        let sql = format!("PRAGMA table_info({})", sql_identifier(table_name));
        match self.execute(&sql) {
            Ok(result) => Ok(result
                .rows()
                .iter()
                .filter_map(|row| match row.values().get(1) {
                    Some(Value::Text(value)) => Some(value.clone()),
                    _ => None,
                })
                .collect()),
            Err(error) => {
                let message = error.to_string();
                if message.contains("no such table") || message.contains("unknown table") {
                    Ok(Vec::new())
                } else {
                    Err(error)
                }
            }
        }
    }
    pub(crate) fn sync_status_query_result(&self) -> Result<QueryResult> {
        let status = self.sync_status()?;
        Ok(QueryResult::with_rows(
            vec![
                "enabled".to_string(),
                "replica_id".to_string(),
                "next_sequence".to_string(),
                "journal_path".to_string(),
                "journal_size_bytes".to_string(),
            ],
            vec![QueryRow::new(vec![
                Value::Bool(status.enabled),
                status.replica_id.map_or(Value::Null, Value::Text),
                sync_u64_to_i64(status.next_sequence, "next_sequence")?,
                status.journal_path.map_or(Value::Null, Value::Text),
                sync_u64_to_i64(status.journal_size_bytes, "journal_size_bytes")?,
            ])],
        ))
    }
    pub(crate) fn sync_journal_query_result(&self, since_sequence: u64) -> Result<QueryResult> {
        let records = self.sync_pending_changes(since_sequence, usize::MAX)?;
        let rows = records
            .into_iter()
            .map(|record| {
                Ok(QueryRow::new(vec![
                    sync_u64_to_i64(record.sequence, "sequence")?,
                    Value::Text(record.replica_id),
                    sync_u64_to_i64(record.transaction_lsn, "transaction_lsn")?,
                    Value::Text(record.table),
                    Value::Text(record.operation),
                    Value::Text(record.primary_key.to_string()),
                    record
                        .after
                        .map_or(Value::Null, |value| Value::Text(value.to_string())),
                    Value::Int64(i64::from(record.schema_cookie)),
                    Value::Int64(record.committed_at_micros),
                ]))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(QueryResult::with_rows(
            vec![
                "sequence".to_string(),
                "replica_id".to_string(),
                "transaction_lsn".to_string(),
                "table_name".to_string(),
                "operation".to_string(),
                "primary_key_json".to_string(),
                "after_json".to_string(),
                "schema_cookie".to_string(),
                "committed_at_micros".to_string(),
            ],
            rows,
        ))
    }
    pub(crate) fn sync_peers_query_result(&self) -> Result<QueryResult> {
        let peers = self.sync_peers()?;
        let rows = peers
            .into_iter()
            .map(|peer| {
                Ok(QueryRow::new(vec![
                    Value::Text(peer.name),
                    Value::Text(peer.endpoint),
                    peer.token_env.map_or(Value::Null, Value::Text),
                    Value::Int64(peer.created_at_micros),
                    Value::Int64(peer.updated_at_micros),
                ]))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(QueryResult::with_rows(
            vec![
                "name".to_string(),
                "endpoint".to_string(),
                "token_env".to_string(),
                "created_at_micros".to_string(),
                "updated_at_micros".to_string(),
            ],
            rows,
        ))
    }
    pub(crate) fn sync_retention_query_result(&self) -> Result<QueryResult> {
        let retention = self.sync_retention_report()?;
        let first_sequence = match retention.first_sequence {
            Some(value) => sync_u64_to_i64(value, "first_sequence")?,
            None => Value::Null,
        };
        let last_sequence = match retention.last_sequence {
            Some(value) => sync_u64_to_i64(value, "last_sequence")?,
            None => Value::Null,
        };
        let safe_prune_through = match retention.safe_prune_through {
            Some(value) => sync_u64_to_i64(value, "safe_prune_through")?,
            None => Value::Null,
        };

        Ok(QueryResult::with_rows(
            vec![
                "journal_records".to_string(),
                "first_sequence".to_string(),
                "last_sequence".to_string(),
                "safe_prune_through".to_string(),
                "prunable_records".to_string(),
                "blocked_by_json".to_string(),
                "journal_size_bytes".to_string(),
            ],
            vec![QueryRow::new(vec![
                sync_u64_to_i64(retention.journal_records as u64, "journal_records")?,
                first_sequence,
                last_sequence,
                safe_prune_through,
                sync_u64_to_i64(retention.prunable_records as u64, "prunable_records")?,
                Value::Text(
                    serde_json::to_string(&retention.blocked_by).map_err(|error| {
                        DbError::internal(format!(
                            "failed to encode sync retention blocked_by: {error}"
                        ))
                    })?,
                ),
                sync_u64_to_i64(retention.journal_size_bytes, "journal_size_bytes")?,
            ])],
        ))
    }
    pub(crate) fn sync_peer_lag_query_result(&self) -> Result<QueryResult> {
        let peer_lag = self.sync_peer_lag_report()?;
        let rows = peer_lag
            .into_iter()
            .map(|lag| {
                let in_watermark = match lag.in_watermark {
                    Some(value) => sync_u64_to_i64(value, "in_watermark")?,
                    None => Value::Null,
                };
                let out_watermark = match lag.out_watermark {
                    Some(value) => sync_u64_to_i64(value, "out_watermark")?,
                    None => Value::Null,
                };
                let local_high_watermark = match lag.local_high_watermark {
                    Some(value) => sync_u64_to_i64(value, "local_high_watermark")?,
                    None => Value::Null,
                };
                let in_lag = match lag.in_lag {
                    Some(value) => sync_u64_to_i64(value, "in_lag")?,
                    None => Value::Null,
                };
                let out_lag = match lag.out_lag {
                    Some(value) => sync_u64_to_i64(value, "out_lag")?,
                    None => Value::Null,
                };
                Ok(QueryRow::new(vec![
                    Value::Text(lag.peer_name),
                    lag.remote_replica_id.map_or(Value::Null, Value::Text),
                    in_watermark,
                    out_watermark,
                    local_high_watermark,
                    in_lag,
                    out_lag,
                ]))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(QueryResult::with_rows(
            vec![
                "peer_name".to_string(),
                "remote_replica_id".to_string(),
                "in_watermark".to_string(),
                "out_watermark".to_string(),
                "local_high_watermark".to_string(),
                "in_lag".to_string(),
                "out_lag".to_string(),
            ],
            rows,
        ))
    }
    pub(crate) fn sync_doctor_query_result(&self) -> Result<QueryResult> {
        let report = self.sync_operational_doctor_report()?;
        Ok(QueryResult::with_rows(
            vec![
                "enabled".to_string(),
                "replica_id".to_string(),
                "highest_severity".to_string(),
                "journal_records".to_string(),
                "journal_size_bytes".to_string(),
                "unresolved_conflicts".to_string(),
                "guidance_json".to_string(),
            ],
            vec![QueryRow::new(vec![
                Value::Bool(report.status.enabled),
                report.status.replica_id.map_or(Value::Null, Value::Text),
                Value::Text(report.highest_severity.to_string()),
                sync_u64_to_i64(report.integrity.total_records as u64, "journal_records")?,
                sync_u64_to_i64(report.retention.journal_size_bytes, "journal_size_bytes")?,
                sync_u64_to_i64(report.unresolved_conflicts as u64, "unresolved_conflicts")?,
                Value::Text(serde_json::to_string(&report.guidance).map_err(|error| {
                    DbError::internal(format!("failed to encode sync doctor guidance: {error}"))
                })?),
            ])],
        ))
    }
    pub(crate) fn sync_scopes_query_result(&self) -> Result<QueryResult> {
        let scopes = self.sync_scopes()?;
        let rows = scopes
            .into_iter()
            .map(|scope| {
                let SyncScope {
                    name,
                    include_tables,
                    row_filter,
                    filter_columns,
                    created_at_micros,
                    updated_at_micros,
                } = scope;
                Ok(QueryRow::new(vec![
                    Value::Text(name),
                    Value::Text(serde_json::to_string(&include_tables).map_err(|error| {
                        DbError::internal(format!(
                            "failed to encode sync scope include tables: {error}"
                        ))
                    })?),
                    row_filter.map_or(Value::Null, Value::Text),
                    Value::Text(serde_json::to_string(&filter_columns).map_err(|error| {
                        DbError::internal(format!(
                            "failed to encode sync scope filter columns: {error}"
                        ))
                    })?),
                    Value::Int64(created_at_micros),
                    Value::Int64(updated_at_micros),
                ]))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(QueryResult::with_rows(
            vec![
                "name".to_string(),
                "include_tables_json".to_string(),
                "row_filter".to_string(),
                "filter_columns_json".to_string(),
                "created_at_micros".to_string(),
                "updated_at_micros".to_string(),
            ],
            rows,
        ))
    }
    pub(crate) fn sync_scope_tables_query_result(&self) -> Result<QueryResult> {
        let mut rows = Vec::new();
        for scope in self.sync_scopes()? {
            let scope_name = scope.name;
            for table_name in scope.include_tables {
                rows.push(QueryRow::new(vec![
                    Value::Text(scope_name.clone()),
                    Value::Text(table_name),
                ]));
            }
        }
        Ok(QueryResult::with_rows(
            vec!["scope_name".to_string(), "table_name".to_string()],
            rows,
        ))
    }
    pub(crate) fn sync_peer_scopes_query_result(&self) -> Result<QueryResult> {
        let bindings = self.sync_peer_scope_bindings()?;
        let rows = bindings
            .into_iter()
            .map(|binding| {
                Ok(QueryRow::new(vec![
                    Value::Text(binding.peer_name),
                    Value::Text(binding.scope_name),
                    Value::Int64(binding.created_at_micros),
                    Value::Int64(binding.updated_at_micros),
                ]))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(QueryResult::with_rows(
            vec![
                "peer_name".to_string(),
                "scope_name".to_string(),
                "created_at_micros".to_string(),
                "updated_at_micros".to_string(),
            ],
            rows,
        ))
    }
    pub(crate) fn sync_sessions_query_result(&self) -> Result<QueryResult> {
        let sessions = self.sync_sessions()?;
        let rows = sessions
            .into_iter()
            .map(|session| {
                Ok(QueryRow::new(vec![
                    Value::Int64(session.session_id),
                    Value::Text(session.peer_name),
                    Value::Text(session.direction.to_string()),
                    session.remote_replica_id.map_or(Value::Null, Value::Text),
                    Value::Int64(session.started_at_micros),
                    session.ended_at_micros.map_or(Value::Null, Value::Int64),
                    Value::Text(session.status),
                    session.error.map_or(Value::Null, Value::Text),
                    session.pushed_batch_id.map_or(Value::Null, Value::Text),
                    session.pulled_batch_id.map_or(Value::Null, Value::Text),
                    Value::Int64(session.pushed_seen),
                    Value::Int64(session.pushed_applied),
                    Value::Int64(session.pushed_skipped),
                    Value::Int64(session.pushed_conflicted),
                    Value::Int64(session.pulled_seen),
                    Value::Int64(session.pulled_applied),
                    Value::Int64(session.pulled_skipped),
                    Value::Int64(session.pulled_conflicted),
                    Value::Int64(session.retry_count),
                ]))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(QueryResult::with_rows(
            vec![
                "session_id".to_string(),
                "peer_name".to_string(),
                "direction".to_string(),
                "remote_replica_id".to_string(),
                "started_at_micros".to_string(),
                "ended_at_micros".to_string(),
                "status".to_string(),
                "error".to_string(),
                "pushed_batch_id".to_string(),
                "pulled_batch_id".to_string(),
                "pushed_seen".to_string(),
                "pushed_applied".to_string(),
                "pushed_skipped".to_string(),
                "pushed_conflicted".to_string(),
                "pulled_seen".to_string(),
                "pulled_applied".to_string(),
                "pulled_skipped".to_string(),
                "pulled_conflicted".to_string(),
                "retry_count".to_string(),
            ],
            rows,
        ))
    }
    pub(crate) fn sync_conflict_policy_query_result(&self) -> Result<QueryResult> {
        let policy = self.sync_conflict_policy()?;
        Ok(QueryResult::with_rows(
            vec![
                "default_policy".to_string(),
                "origin_priority_json".to_string(),
            ],
            vec![QueryRow::new(vec![
                Value::Text(policy.default_policy.to_string()),
                Value::Text(
                    serde_json::to_string(&policy.origin_priority).map_err(|error| {
                        DbError::internal(format!(
                            "failed to encode sync conflict policy origin priority: {error}"
                        ))
                    })?,
                ),
            ])],
        ))
    }
    pub(crate) fn sync_conflicts_query_result(&self) -> Result<QueryResult> {
        let sql = format!(
            "SELECT * FROM {} WHERE resolved = 0 ORDER BY conflict_id",
            crate::sync::CONFLICTS_TABLE
        );
        match self.execute(&sql) {
            Ok(result) => Ok(result),
            Err(error) => {
                let message = error.to_string();
                if message.contains("no such table") || message.contains("unknown table") {
                    Ok(QueryResult::with_rows(
                        vec![
                            "conflict_id".to_string(),
                            "batch_id".to_string(),
                            "remote_replica_id".to_string(),
                            "remote_sequence".to_string(),
                            "table_name".to_string(),
                            "operation".to_string(),
                            "conflict_type".to_string(),
                            "message".to_string(),
                            "primary_key_json".to_string(),
                            "remote_record_json".to_string(),
                            "local_row_json".to_string(),
                            "created_at_micros".to_string(),
                            "resolved".to_string(),
                            "resolution".to_string(),
                            "resolved_at_micros".to_string(),
                            "resolved_by".to_string(),
                            "resolution_note".to_string(),
                            "policy_name".to_string(),
                            "local_record_json".to_string(),
                        ],
                        Vec::new(),
                    ))
                } else {
                    Err(error)
                }
            }
        }
    }
    pub(crate) fn sync_relay_status_query_result(&self) -> Result<QueryResult> {
        let status = self.sync_relay_status(None, false, false, false, None)?;
        Ok(QueryResult::with_rows(
            vec![
                "relay_id".to_string(),
                "protocol_version".to_string(),
                "database_replica_id".to_string(),
                "production_mode".to_string(),
                "secure_transport_required".to_string(),
                "insecure_override_enabled".to_string(),
                "active_sessions".to_string(),
                "active_streams".to_string(),
                "started_at_micros".to_string(),
            ],
            vec![QueryRow::new(vec![
                Value::Text(status.relay_id),
                Value::Int64(i64::from(status.protocol_version)),
                status
                    .database_replica_id
                    .map(Value::Text)
                    .unwrap_or(Value::Null),
                Value::Bool(status.production_mode),
                Value::Bool(status.secure_transport_required),
                Value::Bool(status.insecure_override_enabled),
                sync_u64_to_i64(status.active_sessions, "active_sessions")?,
                sync_u64_to_i64(status.active_streams, "active_streams")?,
                Value::Int64(status.started_at_micros),
            ])],
        ))
    }
    pub(crate) fn sync_relay_sessions_query_result(&self) -> Result<QueryResult> {
        self.query_table_or_empty(
            crate::sync::RELAY_SESSIONS_TABLE,
            &[
                "session_id",
                "tenant_id",
                "subject_id",
                "subject_kind",
                "request_id",
                "operation",
                "scope_name",
                "shape_id",
                "started_at_micros",
                "ended_at_micros",
                "status",
                "error",
                "rows_seen",
                "bytes_seen",
            ],
            "started_at_micros, session_id",
        )
    }
    pub(crate) fn sync_shapes_query_result(&self) -> Result<QueryResult> {
        self.query_table_or_empty(
            crate::sync::SHAPES_TABLE,
            &[
                "shape_id",
                "name",
                "scope_name",
                "tenant_id",
                "allowed_roles_json",
                "allowed_subjects_json",
                "created_at_micros",
                "updated_at_micros",
                "retention_ttl_micros",
                "max_records",
                "ack_deadline_micros",
                "heartbeat_micros",
            ],
            "shape_id",
        )
    }
    pub(crate) fn sync_shape_clients_query_result(&self) -> Result<QueryResult> {
        self.query_table_or_empty(
            crate::sync::SHAPE_CLIENTS_TABLE,
            &[
                "shape_id",
                "tenant_id",
                "client_replica_id",
                "subject_id",
                "session_id",
                "last_ack_sequence",
                "last_ack_watermark",
                "last_changeset_id",
                "last_seen_at_micros",
                "retention_blocking",
                "status",
            ],
            "shape_id, client_replica_id",
        )
    }
    pub(crate) fn sync_changeset_history_query_result(&self) -> Result<QueryResult> {
        self.query_table_or_empty(
            crate::sync::CHANGESET_HISTORY_TABLE,
            &[
                "changeset_id",
                "source_replica_id",
                "source_kind",
                "scope_name",
                "shape_id",
                "record_count",
                "bytes",
                "created_at_micros",
                "applied_at_micros",
                "outcome",
                "integrity_hash",
            ],
            "created_at_micros, changeset_id",
        )
    }
    pub(crate) fn sync_post_commit(
        &self,
        runtime: &mut EngineRuntime,
        committed_lsn: u64,
    ) -> Result<()> {
        let mutations = runtime.take_sync_mutations();
        if mutations.is_empty() {
            return Ok(());
        }
        if !self.inner.sync_ctx.capture_enabled() {
            return Ok(());
        }
        let enabled = if self.inner.sync_ctx.is_enabled() {
            true
        } else {
            let status = self.load_sync_status_from_runtime(runtime)?;
            if status.enabled {
                self.inner.sync_ctx.set_enabled(true);
                if let Some(replica_id) = status.replica_id.as_deref() {
                    self.inner.sync_ctx.set_replica_id(replica_id);
                }
                self.inner.sync_ctx.set_next_sequence(status.next_sequence);
            }
            status.enabled
        };
        if !enabled {
            return Ok(());
        }
        self.inner
            .sync_ctx
            .pending_mutations
            .lock()
            .map_err(|_| DbError::internal("sync pending mutations lock poisoned"))?
            .extend(mutations);
        self.inner
            .sync_ctx
            .flush_journal(&self.inner.vfs, committed_lsn)?;
        Ok(())
    }
}
