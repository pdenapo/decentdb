#![deny(unsafe_op_in_unsafe_fn)]
#![deny(unused_must_use)]
//! DecentDB core engine.
//!
//! Phase 0 establishes the stable top-level API surface and the bootstrap
//! database file format entry points used by later storage slices.

#[cfg(feature = "bench-internals")]
pub mod benchmark;
mod branch;
#[cfg(any(all(target_arch = "wasm32", target_os = "unknown"), test))]
mod browser_result;
mod btree;
mod c_api;
mod catalog;
mod config;
mod db;
mod doctor;
mod error;
mod exec;
mod extensions;
mod json;
#[cfg(test)]
mod json_tests;
mod metadata;
pub(crate) mod plan_cache;
mod planner;
mod reactive;
mod record;
mod search;
mod security;
pub(crate) mod spatial;
mod sql;
mod storage;
mod sync;
mod tooling;
mod tracing;
mod vfs;
mod wal;
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
mod wasm;
mod write_queue;

/// Internal entry points for coverage-guided fuzz targets (`fuzz/` crate).
///
/// Only available with the `fuzz-internals` feature; not part of the stable
/// API surface. Every function here must tolerate arbitrary malformed input
/// and return a typed error instead of panicking.
#[cfg(feature = "fuzz-internals")]
#[doc(hidden)]
pub mod fuzzing {
    use crate::error::Result;
    use crate::record::row::Row;

    /// Decode a row from raw bytes, returning the column count on success.
    pub fn row_decode(bytes: &[u8]) -> Result<usize> {
        Row::decode(bytes).map(|row| row.values().len())
    }

    /// Decode the `INT64` value at `column_index` from raw row bytes.
    pub fn row_decode_int64_at(bytes: &[u8], column_index: usize) -> Result<Option<i64>> {
        Row::decode_int64_at(bytes, column_index)
    }

    /// Decode a varint-encoded `u64` from raw bytes.
    pub fn decode_varint_u64(bytes: &[u8]) -> Result<(u64, usize)> {
        crate::record::decode_varint_u64(bytes)
    }
}

pub use crate::branch::{
    BranchDiffReport, BranchInfo, BranchLogEntry, BranchMergeChange, BranchMergeConflict,
    BranchMergeOperation, BranchMergeReport, BranchRestoreReport, BranchRowDiff, BranchTableDiff,
    BranchTableDiffStatus, NamedSnapshot,
};
pub use crate::config::{
    DbConfig, DbEncryptionConfig, EncryptionKey, ProcessCoordinationMode, WalSyncMode,
};
pub use crate::db::{
    evict_shared_wal, Db, PreparedStatement, PreparedStatementBatch, SqlTransaction,
};
pub use crate::doctor::{
    render_markdown, run_doctor, sort_findings, DoctorCategory, DoctorCheckSelection,
    DoctorCollectedFacts, DoctorDatabaseSummary, DoctorEvidence, DoctorEvidenceValue,
    DoctorFinding, DoctorFix, DoctorFixStatus, DoctorHighestSeverity, DoctorIndexVerification,
    DoctorMode, DoctorOptions, DoctorPathMode, DoctorRecommendation, DoctorReport, DoctorSeverity,
    DoctorStatus, DoctorSummary,
};
pub use crate::error::{
    DbDiagnostic, DbDiagnosticAuditContext, DbDiagnosticContext, DbDiagnosticOpenOptions,
    DbDiagnosticParameter, DbDiagnosticPath, DbDiagnosticPathKind, DbDiagnosticRedaction,
    DbDiagnosticSyncToken, DbDoctorHandoff, DbDoctorHandoffKind, DbError, DbErrorCode, Result,
    DIAGNOSTIC_VERSION,
};
pub use crate::exec::{BulkLoadOptions, QueryResult, QueryRow};
pub use crate::extensions::{
    validate_extension_package, Ed25519SignatureVerifier, ExtensionDependencyRecord,
    ExtensionFunctionManifest, ExtensionManager, ExtensionManifest, ExtensionNullHandling,
    ExtensionPackageDependency, ExtensionPackageFile, ExtensionPermissions, ExtensionRuntimeLimits,
    ExtensionSignature, ExtensionSignatureVerifier, ExtensionSqlType, ExtensionTrustAnchor,
    ExtensionValidationOptions, ExtensionValidationReport, InstalledExtensionPackage,
    SUPPORTED_EXTENSION_API_VERSION,
};
pub use crate::metadata::{
    CheckConstraintInfo, ColumnInfo, ForeignKeyInfo, HeaderInfo, IndexInfo, IndexVerification,
    QueryContract, QueryParameterInfo, QueryResultColumnInfo, SchemaColumnInfo, SchemaIndexInfo,
    SchemaSnapshot, SchemaTableInfo, SchemaTriggerInfo, SchemaViewInfo, StorageInfo, TableInfo,
    ToolingCapabilities, ToolingColumnTypeMetadata, ToolingMetadata, ToolingSpatialTypeInfo,
    ToolingTypeInfo, TriggerInfo, ViewInfo,
};
pub use crate::plan_cache::{PlanCacheConfig, PlanCacheSummary};
pub use crate::reactive::{
    ChangeSource, ChangeStreamEvent, ChangeStreamOptions, InitialWatchEvent, InvalidationEvent,
    LaggedWatchEvent, QueryWatchOptions, RangeWatchOptions, ReactiveMetricsSnapshot,
    ReactiveSubscriptionSnapshot, RowChange, RowChangeDetail, RowOperation, TableChange,
    TableWatchOptions, WatchEvent, WatchHandle, WatchKind,
};
pub use crate::record::value::Value;
pub use crate::storage::DB_FORMAT_VERSION;
pub use crate::sync::{
    ApplyChangesetOptions, CreateChangesetOptions, CreateShapeOptions, InspectChangesetOptions,
    InvertChangesetOptions, ShapeAckOptions, SyncChangeBatch, SyncChangeset,
    SyncChangesetApplyResult, SyncChangesetCapabilities, SyncChangesetCheckpoint,
    SyncChangesetCompatibility, SyncChangesetHistory, SyncChangesetInspection, SyncChangesetLimits,
    SyncChangesetRecord, SyncChangesetSource, SyncChangesetSourceKind, SyncCompatibilityMode,
    SyncConflict, SyncConflictPolicy, SyncConflictPolicyConfig, SyncDoctorSeverity, SyncHandshake,
    SyncImportSummary, SyncJournalIntegrityReport, SyncJournalIssue, SyncJournalRecord,
    SyncOperationalDoctorReport, SyncPeer, SyncPeerLag, SyncPeerScopeBinding, SyncPrincipal,
    SyncPruneSummary, SyncRelayHello, SyncRelaySession, SyncRelayStatus, SyncRetentionReport,
    SyncRunDirection, SyncRunSummary, SyncScope, SyncSession, SyncShape, SyncShapeCheckpoint,
    SyncShapeClient, SyncShapeDelivery, SyncStatus, SyncSubjectKind, SYNC_CHANGESET_VERSION,
    SYNC_CONTRACT_VERSION, SYNC_RELAY_PROTOCOL_VERSION, SYNC_SHAPE_STREAM_VERSION,
};
pub use crate::tracing::config::SqlTextMode;
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
pub use crate::wasm::WebDb;
pub use crate::write_queue::{QueuedWriteOptions, WriteQueueMetricsSnapshot};

/// Returns the DecentDB crate version.
#[must_use]
pub fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

#[cfg(test)]
mod tests {
    use super::version;

    #[test]
    fn test_version() {
        assert!(!version().is_empty());
    }
}
