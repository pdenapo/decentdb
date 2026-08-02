#![allow(dead_code)]

use std::cell::RefCell;
use std::collections::HashMap;

use crate::record::value::Value;
use crate::tracing::buffer::BoundedRingBuffer;
use crate::tracing::config::RuntimeTracingConfig;

/// How an index was used in a traced operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IndexUsageKind {
    Read,
    Write,
}

thread_local! {
    static LOCAL_INDEX_USAGE: RefCell<Vec<(String, String, String, IndexUsageKind)>> = const { RefCell::new(Vec::new()) };
}

/// Push a usage event into the thread-local accumulator.
pub fn record_index_usage_local(
    table_name: &str,
    index_name: &str,
    index_kind: &str,
    kind: IndexUsageKind,
) {
    LOCAL_INDEX_USAGE.with(|buf| {
        buf.borrow_mut().push((
            table_name.to_string(),
            index_name.to_string(),
            index_kind.to_string(),
            kind,
        ));
    });
}

/// Drain the thread-local buffer and return its contents.
pub fn drain_local_index_usage() -> Vec<(String, String, String, IndexUsageKind)> {
    LOCAL_INDEX_USAGE.with(|buf| std::mem::take(&mut *buf.borrow_mut()))
}

/// Per-index usage aggregate maintained by `IndexUsageStore`.
#[derive(Clone, Debug)]
pub struct IndexUsageRow {
    pub table_name: String,
    pub index_name: String,
    pub index_kind: String,
    pub read_count: u64,
    pub write_count: u64,
}

impl IndexUsageRow {
    pub fn to_query_row(&self) -> Vec<Value> {
        vec![
            Value::Text(self.table_name.clone()),
            Value::Text(self.index_name.clone()),
            Value::Text(self.index_kind.clone()),
            Value::Int64(i64::try_from(self.read_count).unwrap_or(-1)),
            Value::Int64(i64::try_from(self.write_count).unwrap_or(-1)),
        ]
    }
}

fn index_key(table_name: &str, index_name: &str) -> String {
    format!("{table_name}:{index_name}")
}

#[derive(Debug)]
pub(crate) struct IndexUsageStore {
    config: RuntimeTracingConfig,
    rows: HashMap<String, IndexUsageRow>,
    order: BoundedRingBuffer<String>,
}

impl IndexUsageStore {
    pub(crate) fn new(config: &RuntimeTracingConfig) -> Self {
        let capacity = if config.enabled && config.index_usage.enabled {
            config.index_usage.max_rows.clamp(1, 65_536)
        } else {
            0
        };
        Self {
            config: config.clone(),
            rows: HashMap::with_capacity(capacity),
            order: BoundedRingBuffer::with_capacity(capacity),
        }
    }

    #[inline]
    pub(crate) fn record(
        &mut self,
        table_name: &str,
        index_name: &str,
        index_kind: &str,
        kind: IndexUsageKind,
    ) {
        if !self.config.enabled || !self.config.index_usage.enabled {
            return;
        }
        let key = index_key(table_name, index_name);
        if let Some(existing) = self.rows.get_mut(&key) {
            match kind {
                IndexUsageKind::Read => existing.read_count += 1,
                IndexUsageKind::Write => existing.write_count += 1,
            }
        } else {
            if self.rows.len() >= self.config.index_usage.max_rows {
                // Evict oldest entry
                if let Some(oldest_key) = self.order.oldest() {
                    self.rows.remove(oldest_key);
                    self.order.evict_oldest();
                }
            }
            self.rows.insert(
                key.clone(),
                IndexUsageRow {
                    table_name: table_name.to_string(),
                    index_name: index_name.to_string(),
                    index_kind: index_kind.to_string(),
                    read_count: match kind {
                        IndexUsageKind::Read => 1,
                        IndexUsageKind::Write => 0,
                    },
                    write_count: match kind {
                        IndexUsageKind::Read => 0,
                        IndexUsageKind::Write => 1,
                    },
                },
            );
            self.order.push_back(key);
        }
    }

    pub(crate) fn snapshot(&self) -> Vec<IndexUsageRow> {
        self.rows.values().cloned().collect()
    }

    pub(crate) fn reset(&mut self) {
        self.rows.clear();
        self.order.reset();
    }

    #[cfg(test)]
    pub(crate) fn allocated_capacities(&self) -> (usize, usize) {
        (self.rows.capacity(), self.order.capacity())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tracing::config::IndexUsageTraceConfig;

    #[test]
    fn disabled_store_does_not_allocate() {
        let config = RuntimeTracingConfig {
            enabled: true,
            index_usage: IndexUsageTraceConfig {
                enabled: false,
                ..Default::default()
            },
            ..Default::default()
        };
        let store = IndexUsageStore::new(&config);
        assert_eq!(store.allocated_capacities(), (0, 0));
    }

    #[test]
    fn enabled_store_reserves_configured_capacity() {
        let config = RuntimeTracingConfig {
            enabled: true,
            index_usage: IndexUsageTraceConfig {
                enabled: true,
                max_rows: 7,
            },
            ..Default::default()
        };
        let store = IndexUsageStore::new(&config);
        let (rows_capacity, order_capacity) = store.allocated_capacities();
        assert!(rows_capacity >= 7);
        assert_eq!(order_capacity, 7);
    }
}
