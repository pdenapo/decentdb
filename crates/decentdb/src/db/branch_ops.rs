//! Thematic extraction (mechanical split; no behavior change).

use super::*;

impl Db {
    /// Compares two refs (`main`, branch name, named snapshot, or branch head ID).
    pub fn branch_diff(
        &self,
        left_ref: &str,
        right_ref: &str,
    ) -> Result<crate::branch::BranchDiffReport> {
        let left_db = self.materialize_ref_db(left_ref)?;
        let right_db = self.materialize_ref_db(right_ref)?;
        diff_materialized_refs(left_ref, right_ref, &left_db, &right_db)
    }
    /// Restores a non-main branch head to another branch, named snapshot, or head ID.
    pub fn branch_restore(
        &self,
        branch_name: &str,
        target_ref: &str,
        dry_run: bool,
    ) -> Result<crate::branch::BranchRestoreReport> {
        if self.inner.sql_txn_active.load(Ordering::Acquire) {
            return Err(DbError::transaction(
                "cannot restore a branch while a SQL transaction is active",
            ));
        }
        if branch_name == crate::branch::DEFAULT_BRANCH_NAME {
            return Err(DbError::transaction(
                "restore currently targets non-main branches; create a branch from the restore point to inspect main rollback candidates",
            ));
        }
        let branch = crate::branch::branch_by_name(self, branch_name)?
            .ok_or_else(|| DbError::transaction(format!("unknown branch '{branch_name}'")))?;
        let target_head = self.resolve_branch_target_head(target_ref)?;
        let diff = self.branch_diff(branch_name, target_ref)?;
        if dry_run {
            return Ok(crate::branch::BranchRestoreReport {
                branch: branch_name.to_string(),
                target_ref: target_ref.to_string(),
                dry_run: true,
                previous_head_id: branch.current_head_id,
                target_head_id: target_head.head_id,
                new_head_id: None,
                changed_table_count: diff.changed_table_count,
                added_row_count: diff.added_row_count,
                updated_row_count: diff.updated_row_count,
                deleted_row_count: diff.deleted_row_count,
            });
        }
        let new_head = crate::branch::restore_branch_head(self, &branch, &target_head, target_ref)?;
        self.refresh_named_snapshot_retention()?;
        Ok(crate::branch::BranchRestoreReport {
            branch: branch_name.to_string(),
            target_ref: target_ref.to_string(),
            dry_run: false,
            previous_head_id: branch.current_head_id,
            target_head_id: target_head.head_id,
            new_head_id: Some(new_head.head_id),
            changed_table_count: diff.changed_table_count,
            added_row_count: diff.added_row_count,
            updated_row_count: diff.updated_row_count,
            deleted_row_count: diff.deleted_row_count,
        })
    }
    /// Merges clean primary-key row changes from a source branch into a target ref.
    pub fn branch_merge(
        &self,
        source_branch: &str,
        target_ref: &str,
        dry_run: bool,
    ) -> Result<crate::branch::BranchMergeReport> {
        if self.inner.sql_txn_active.load(Ordering::Acquire) {
            return Err(DbError::transaction(
                "cannot merge a branch while a SQL transaction is active",
            ));
        }
        if source_branch == crate::branch::DEFAULT_BRANCH_NAME {
            return Err(DbError::transaction(
                "merge source must be a non-main branch",
            ));
        }
        let source = crate::branch::branch_by_name(self, source_branch)?
            .ok_or_else(|| DbError::transaction(format!("unknown branch '{source_branch}'")))?;
        let base_head_id = source.base_head_id.clone().ok_or_else(|| {
            DbError::transaction(format!("branch '{source_branch}' has no merge base"))
        })?;
        if target_ref != crate::branch::DEFAULT_BRANCH_NAME
            && crate::branch::branch_by_name(self, target_ref)?.is_none()
        {
            return Err(DbError::transaction(format!(
                "merge target must be 'main' or a branch; got '{target_ref}'"
            )));
        }

        let base_db = self.materialize_branch_head_db(&base_head_id)?;
        let source_db = self.materialize_branch_db(&source)?;
        let target_db = self.materialize_ref_db(target_ref)?;
        let plan = build_merge_plan(
            source_branch,
            target_ref,
            &base_head_id,
            &base_db,
            &source_db,
            &target_db,
        )?;
        if dry_run || !plan.conflicts.is_empty() {
            return Ok(plan.into_report(dry_run));
        }
        let sql = plan
            .changes
            .iter()
            .map(|change| change.sql.as_str())
            .collect::<Vec<_>>()
            .join("\n");
        if !sql.trim().is_empty() {
            if target_ref == crate::branch::DEFAULT_BRANCH_NAME {
                crate::reactive::with_change_source(ChangeSource::BranchMerge, || {
                    self.execute_batch(&sql)
                })?;
            } else {
                self.execute_batch_on_branch(&sql, target_ref)?;
            }
        }
        Ok(plan.into_report(false))
    }
    /// Creates a branch from `main`, another branch, a named snapshot, or a branch head.
    pub fn branch_create(
        &self,
        name: &str,
        from: Option<&str>,
    ) -> Result<crate::branch::BranchInfo> {
        let source = from.unwrap_or(crate::branch::DEFAULT_BRANCH_NAME);
        let (source_lsn, parent_head_id) = if source == crate::branch::DEFAULT_BRANCH_NAME {
            let initial_lsn = self.inner.wal.latest_snapshot();
            self.inner.wal.set_retained_snapshot_lsn(Some(initial_lsn));
            self.checkpoint_wal()?;
            let source_lsn = self.inner.wal.latest_snapshot();
            let parent_head_id = crate::branch::main_branch_head(self)?.map(|head| head.head_id);
            (source_lsn, parent_head_id)
        } else if let Some(branch) = crate::branch::branch_by_name(self, source)? {
            let source_lsn = self.branch_lsn(source)?.ok_or_else(|| {
                DbError::transaction(format!("branch '{source}' has no current head"))
            })?;
            (source_lsn, branch.current_head_id)
        } else if let Some(snapshot) = self.snapshot_get(source)? {
            (snapshot.snapshot_lsn, Some(snapshot.head_id))
        } else if let Some(source_lsn) = crate::branch::branch_head_lsn_by_id(self, source)? {
            (source_lsn, Some(source.to_string()))
        } else {
            return Err(DbError::transaction(format!(
                "unknown branch, snapshot, or head '{source}'"
            )));
        };
        self.inner.wal.set_retained_snapshot_lsn(Some(source_lsn));
        let schema_cookie = self.current_schema_cookie_at_snapshot(source_lsn)?;
        let result = crate::branch::create_branch(
            self,
            name,
            source_lsn,
            schema_cookie,
            parent_head_id.as_deref(),
        );
        self.refresh_named_snapshot_retention()?;
        result
    }
    /// Lists branches.
    pub fn branch_list(&self) -> Result<Vec<crate::branch::BranchInfo>> {
        crate::branch::list_branches(self)
    }
    /// Deletes a non-main branch.
    pub fn branch_delete(&self, name: &str) -> Result<bool> {
        let deleted = crate::branch::delete_branch(self, name)?;
        if deleted {
            self.refresh_named_snapshot_retention()?;
        }
        Ok(deleted)
    }
    /// Renames a non-main branch.
    pub fn branch_rename(&self, old_name: &str, new_name: &str) -> Result<bool> {
        crate::branch::rename_branch(self, old_name, new_name)
    }
    /// Resolves a branch name to its current retained WAL LSN.
    pub fn branch_lsn(&self, name: &str) -> Result<Option<u64>> {
        crate::branch::branch_lsn_by_name(self, name)
    }
    /// Adds a named commit marker to a non-main branch.
    pub fn branch_commit(
        &self,
        name: &str,
        message: &str,
    ) -> Result<crate::branch::BranchLogEntry> {
        if self.inner.sql_txn_active.load(Ordering::Acquire) {
            return Err(DbError::transaction(
                "cannot create a branch commit marker while a SQL transaction is active",
            ));
        }
        if name == crate::branch::DEFAULT_BRANCH_NAME {
            return Err(DbError::transaction(
                "branch commit markers are only supported on non-main branches",
            ));
        }
        let branch = crate::branch::branch_by_name(self, name)?
            .ok_or_else(|| DbError::transaction(format!("unknown branch '{name}'")))?;
        let head = crate::branch::commit_branch(self, &branch, message)?;
        self.refresh_named_snapshot_retention()?;
        Ok(crate::branch::BranchLogEntry {
            head_id: head.head_id,
            branch_id: head.branch_id,
            parent_head_id: head.parent_head_id,
            message: head.message,
            created_at_micros: head.created_at_micros,
            sql: None,
        })
    }
    /// Returns branch head history newest first.
    pub fn branch_log(&self, name: &str) -> Result<Vec<crate::branch::BranchLogEntry>> {
        crate::branch::branch_log(self, name)
    }
}
