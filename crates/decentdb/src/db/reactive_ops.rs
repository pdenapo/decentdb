//! Thematic extraction (mechanical split; no behavior change).

use super::*;

impl Db {
    /// Subscribes to committed changes for one or more persistent user tables.
    pub fn watch_table(&self, options: TableWatchOptions) -> Result<WatchHandle> {
        let tables = self.validate_watch_tables(&options.tables)?;
        self.reactive_hub().watch_table(
            tables,
            options.queue_capacity,
            self.inner.wal.latest_snapshot(),
            self.schema_cookie()?,
        )
    }
    /// Subscribes to committed changes intersecting a primary-key range.
    pub fn watch_range(&self, mut options: RangeWatchOptions) -> Result<WatchHandle> {
        let canonical = self.validate_watch_range_table(&options.table)?;
        options.table = canonical;
        self.reactive_hub().watch_range(
            options,
            self.inner.wal.latest_snapshot(),
            self.schema_cookie()?,
        )
    }
    /// Executes a SELECT and subscribes to invalidations for its dependencies.
    pub fn watch_query(
        &self,
        sql: &str,
        params: &[Value],
        options: QueryWatchOptions,
    ) -> Result<WatchHandle> {
        let statement = self.parsed_statement(sql)?;
        if !statement_is_read_only(&statement) {
            return Err(DbError::sql(
                "query subscriptions require a read-only SELECT",
            ));
        }
        let dependencies = self.query_watch_dependencies(&statement)?;
        let result = self.execute_with_params(sql, params)?;
        self.reactive_hub().watch_query(
            dependencies,
            options.queue_capacity,
            self.inner.wal.latest_snapshot(),
            self.schema_cookie()?,
            result,
        )
    }
    /// Returns current reactive subscription counters.
    #[must_use]
    pub fn reactive_metrics(&self) -> ReactiveMetricsSnapshot {
        self.reactive_hub_if_initialized()
            .map_or_else(ReactiveMetricsSnapshot::default, |hub| {
                hub.metrics_snapshot()
            })
    }
    /// Returns current reactive subscription details.
    #[must_use]
    pub fn reactive_subscriptions(&self) -> Vec<ReactiveSubscriptionSnapshot> {
        self.reactive_hub_if_initialized()
            .map_or_else(Vec::new, |hub| hub.subscription_snapshots())
    }
    pub(crate) fn reactive_hub(&self) -> Arc<ReactiveHub> {
        Arc::clone(self.inner.reactive_hub.get_or_init(|| {
            crate::reactive::acquire_hub(
                self.inner.reactive_registry_key.clone(),
                &self.inner.config,
            )
        }))
    }
    fn reactive_hub_if_initialized(&self) -> Option<&Arc<ReactiveHub>> {
        self.inner.reactive_hub.get()
    }
    pub(crate) fn reactive_hub_if_available(&self) -> Option<Arc<ReactiveHub>> {
        self.reactive_hub_if_initialized()
            .map(Arc::clone)
            .or_else(|| crate::reactive::existing_hub(self.inner.reactive_registry_key.as_ref()))
    }
    pub(crate) fn reactive_has_watchers(&self) -> bool {
        self.reactive_hub_if_available()
            .is_some_and(|hub| hub.has_watchers())
    }
    pub(crate) fn reactive_metrics_query_result(&self) -> Result<QueryResult> {
        let metrics = self.reactive_metrics();
        Ok(QueryResult::with_rows(
            vec![
                "active_watch_count".to_string(),
                "table_watch_count".to_string(),
                "range_watch_count".to_string(),
                "query_watch_count".to_string(),
                "change_stream_count".to_string(),
                "events_published".to_string(),
                "events_delivered".to_string(),
                "events_dropped".to_string(),
                "lagged_watch_count".to_string(),
                "row_change_events_truncated".to_string(),
            ],
            vec![QueryRow::new(vec![
                sync_usize_to_i64(metrics.active_watch_count, "active_watch_count")?,
                sync_usize_to_i64(metrics.table_watch_count, "table_watch_count")?,
                sync_usize_to_i64(metrics.range_watch_count, "range_watch_count")?,
                sync_usize_to_i64(metrics.query_watch_count, "query_watch_count")?,
                sync_usize_to_i64(metrics.change_stream_count, "change_stream_count")?,
                sync_u64_to_i64(metrics.events_published, "events_published")?,
                sync_u64_to_i64(metrics.events_delivered, "events_delivered")?,
                sync_u64_to_i64(metrics.events_dropped, "events_dropped")?,
                sync_usize_to_i64(metrics.lagged_watch_count, "lagged_watch_count")?,
                sync_u64_to_i64(
                    metrics.row_change_events_truncated,
                    "row_change_events_truncated",
                )?,
            ])],
        ))
    }
    pub(crate) fn reactive_subscriptions_query_result(&self) -> Result<QueryResult> {
        let rows = self
            .reactive_subscriptions()
            .into_iter()
            .map(|subscription| {
                Ok(QueryRow::new(vec![
                    sync_u64_to_i64(subscription.watch_id, "watch_id")?,
                    Value::Text(subscription.kind.as_str().to_string()),
                    Value::Int64(subscription.created_at_micros),
                    sync_usize_to_i64(subscription.queue_capacity, "queue_capacity")?,
                    sync_usize_to_i64(subscription.queue_depth, "queue_depth")?,
                    sync_u64_to_i64(
                        subscription.last_delivered_event_id,
                        "last_delivered_event_id",
                    )?,
                    sync_u64_to_i64(subscription.dropped_events, "dropped_events")?,
                    Value::Bool(subscription.lagged),
                    Value::Text(subscription.dependencies_json),
                ]))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(QueryResult::with_rows(
            vec![
                "watch_id".to_string(),
                "kind".to_string(),
                "created_at_micros".to_string(),
                "queue_capacity".to_string(),
                "queue_depth".to_string(),
                "last_delivered_event_id".to_string(),
                "dropped_events".to_string(),
                "lagged".to_string(),
                "dependencies_json".to_string(),
            ],
            rows,
        ))
    }
}
