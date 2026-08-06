//! Thematic extraction (mechanical split; no behavior change).

use super::*;

pub(crate) fn decode_root_header(page_bytes: &[u8]) -> Result<Option<RootHeader>> {
    if page_bytes.iter().all(|byte| *byte == 0) {
        return Ok(None);
    }
    if page_bytes.len() < ENGINE_ROOT_HEADER_SIZE {
        return Err(DbError::corruption("catalog root page is truncated"));
    }
    if page_bytes[0..ENGINE_ROOT_MAGIC.len()] != ENGINE_ROOT_MAGIC {
        return Err(DbError::corruption("catalog root page magic is invalid"));
    }
    let version = u32::from_le_bytes(page_bytes[8..12].try_into().expect("version"));
    if version != ENGINE_ROOT_VERSION {
        return Err(DbError::corruption(format!(
            "unsupported catalog root version {version}"
        )));
    }
    Ok(Some(RootHeader {
        schema_cookie: u32::from_le_bytes(page_bytes[12..16].try_into().expect("cookie")),
        payload_checksum: u32::from_le_bytes(page_bytes[16..20].try_into().expect("checksum")),
        pointer: OverflowPointer {
            head_page_id: u32::from_le_bytes(page_bytes[20..24].try_into().expect("head page")),
            logical_len: u32::from_le_bytes(page_bytes[24..28].try_into().expect("logical len")),
            flags: page_bytes[28],
        },
    }))
}

pub(crate) fn encode_root_header(page_size: u32, header: RootHeader) -> Vec<u8> {
    let mut page = vec![0_u8; page_size as usize];
    page[0..8].copy_from_slice(&ENGINE_ROOT_MAGIC);
    page[8..12].copy_from_slice(&ENGINE_ROOT_VERSION.to_le_bytes());
    page[12..16].copy_from_slice(&header.schema_cookie.to_le_bytes());
    page[16..20].copy_from_slice(&header.payload_checksum.to_le_bytes());
    page[20..24].copy_from_slice(&header.pointer.head_page_id.to_le_bytes());
    page[24..28].copy_from_slice(&header.pointer.logical_len.to_le_bytes());
    page[28] = header.pointer.flags;
    page
}

#[cfg(test)]
pub(crate) fn encode_runtime_payload(runtime: &EngineRuntime) -> Result<Vec<u8>> {
    let mut output = Vec::new();
    output.extend_from_slice(LEGACY_RUNTIME_PAYLOAD_MAGIC);
    encode_u32(&mut output, runtime.catalog.schema_cookie);
    encode_u32(&mut output, runtime.catalog.tables.len() as u32);
    for table in runtime.catalog.tables.values() {
        encode_string(&mut output, &table.name)?;
        encode_u32(&mut output, table.columns.len() as u32);
        for column in &table.columns {
            encode_string(&mut output, &column.name)?;
            output.push(encode_column_type(column.column_type));
            output.push(u8::from(column.nullable));
            encode_optional_string(&mut output, column.default_sql.as_deref())?;
            output.push(u8::from(column.primary_key));
            output.push(u8::from(column.unique));
            output.push(u8::from(column.auto_increment));
            encode_u32(&mut output, column.checks.len() as u32);
            for check in &column.checks {
                encode_optional_string(&mut output, check.name.as_deref())?;
                encode_string(&mut output, &check.expression_sql)?;
            }
            output.push(u8::from(column.foreign_key.is_some()));
            if let Some(foreign_key) = &column.foreign_key {
                encode_foreign_key(&mut output, foreign_key)?;
            }
        }
        encode_u32(&mut output, table.checks.len() as u32);
        for check in &table.checks {
            encode_optional_string(&mut output, check.name.as_deref())?;
            encode_string(&mut output, &check.expression_sql)?;
        }
        encode_u32(&mut output, table.foreign_keys.len() as u32);
        for foreign_key in &table.foreign_keys {
            encode_foreign_key(&mut output, foreign_key)?;
        }
        encode_strings(&mut output, &table.primary_key_columns)?;
        encode_i64(&mut output, table.next_row_id);
        let data = runtime
            .tables
            .get(&table.name)
            .map(|source| source.resident_data().clone())
            .unwrap_or_default();
        encode_u32(&mut output, data.row_count() as u32);
        for row in data.visible_rows() {
            encode_i64(&mut output, row.row_id);
            let encoded = Row::new(row.values.clone()).encode()?;
            encode_bytes(&mut output, &encoded)?;
        }
    }

    encode_u32(&mut output, runtime.catalog.indexes.len() as u32);
    for index in runtime.catalog.indexes.values() {
        encode_string(&mut output, &index.name)?;
        encode_string(&mut output, &index.table_name)?;
        output.push(index.kind as u8);
        output.push(u8::from(index.unique));
        encode_u32(&mut output, index.columns.len() as u32);
        for column in &index.columns {
            encode_optional_string(&mut output, column.column_name.as_deref())?;
            encode_optional_string(&mut output, column.expression_sql.as_deref())?;
        }
        encode_optional_string(&mut output, index.predicate_sql.as_deref())?;
        output.push(u8::from(index.fresh));
    }
    encode_u32(&mut output, runtime.catalog.views.len() as u32);
    for view in runtime.catalog.views.values() {
        encode_string(&mut output, &view.name)?;
        encode_string(&mut output, &view.sql_text)?;
        encode_strings(&mut output, &view.column_names)?;
        encode_strings(&mut output, &view.dependencies)?;
    }

    encode_u32(&mut output, runtime.catalog.triggers.len() as u32);
    for trigger in runtime.catalog.triggers.values() {
        encode_string(&mut output, &trigger.name)?;
        encode_string(&mut output, &trigger.target_name)?;
        output.push(trigger.kind as u8);
        output.push(trigger.event as u8);
        output.push(u8::from(trigger.on_view));
        encode_string(&mut output, &trigger.action_sql)?;
    }
    encode_schemas_section(&mut output, &runtime.catalog.schemas)?;
    encode_index_include_columns_section(&mut output, &runtime.catalog.indexes)?;
    encode_full_text_options_section(&mut output, &runtime.catalog.indexes)?;
    encode_generated_columns_section(&mut output, &runtime.catalog.tables)?;
    encode_spatial_columns_section(&mut output, &runtime.catalog.tables)?;
    encode_enum_columns_section(&mut output, &runtime.catalog.tables)?;
    Ok(output)
}

pub(crate) fn decode_runtime_payload(bytes: &[u8]) -> Result<EngineRuntime> {
    let mut cursor = Cursor::new(bytes);
    let magic = cursor.read_slice(9)?;
    if magic != LEGACY_RUNTIME_PAYLOAD_MAGIC {
        return Err(DbError::corruption("catalog state magic is invalid"));
    }
    let mut runtime = EngineRuntime::empty(cursor.read_u32()?);
    let table_count = cursor.read_u32()?;
    for _ in 0..table_count {
        let table_name = cursor.read_string()?;
        let column_count = cursor.read_u32()?;
        let mut table = TableSchema {
            name: table_name.clone(),
            temporary: false,
            columns: Vec::with_capacity(column_count as usize),
            checks: Vec::new(),
            foreign_keys: Vec::new(),
            primary_key_columns: Vec::new(),
            next_row_id: 1,
            pk_index_root: None,
        };
        for _ in 0..column_count {
            let name = cursor.read_string()?;
            let column_type = decode_column_type(cursor.read_u8()?)?;
            let nullable = cursor.read_bool()?;
            let default_sql = cursor.read_optional_string()?;
            let primary_key = cursor.read_bool()?;
            let unique = cursor.read_bool()?;
            let auto_increment = cursor.read_bool()?;
            let check_count = cursor.read_u32()?;
            let mut checks = Vec::with_capacity(check_count as usize);
            for _ in 0..check_count {
                checks.push(crate::catalog::CheckConstraint {
                    name: cursor.read_optional_string()?,
                    expression_sql: cursor.read_string()?,
                });
            }
            let has_fk = cursor.read_bool()?;
            let foreign_key = if has_fk {
                Some(decode_foreign_key(&mut cursor)?)
            } else {
                None
            };
            table.columns.push(crate::catalog::ColumnSchema {
                name,
                column_type,
                spatial_type: None,
                enum_type: None,
                nullable,
                default_sql,
                generated_sql: None,
                generated_stored: true,
                primary_key,
                unique,
                auto_increment,
                checks,
                foreign_key,
            });
        }
        let table_check_count = cursor.read_u32()?;
        for _ in 0..table_check_count {
            table.checks.push(crate::catalog::CheckConstraint {
                name: cursor.read_optional_string()?,
                expression_sql: cursor.read_string()?,
            });
        }
        let fk_count = cursor.read_u32()?;
        for _ in 0..fk_count {
            table.foreign_keys.push(decode_foreign_key(&mut cursor)?);
        }
        table.primary_key_columns = cursor.read_strings()?;
        table.next_row_id = cursor.read_i64()?;
        let row_count = cursor.read_u32()?;
        let mut data = TableData::default();
        for _ in 0..row_count {
            let row_id = cursor.read_i64()?;
            let row_bytes_len = cursor.read_u32()? as usize;
            let row_bytes = cursor.read_slice(row_bytes_len)?;
            let row = Row::decode(row_bytes)?;
            data.push_row(StoredRow {
                row_id,
                values: row.into_values(),
            });
        }
        runtime
            .catalog_mut()
            .tables
            .insert(table_name.clone(), table);
        runtime.tables_mut().insert(table_name, data.into());
    }

    let index_count = cursor.read_u32()?;
    for _ in 0..index_count {
        let name = cursor.read_string()?;
        let table_name = cursor.read_string()?;
        let kind = decode_index_kind(cursor.read_u8()?)?;
        let unique = cursor.read_bool()?;
        let column_count = cursor.read_u32()?;
        let mut columns = Vec::with_capacity(column_count as usize);
        for _ in 0..column_count {
            columns.push(crate::catalog::IndexColumn {
                column_name: cursor.read_optional_string()?,
                expression_sql: cursor.read_optional_string()?,
            });
        }
        let predicate_sql = cursor.read_optional_string()?;
        let fresh = cursor.read_bool()?;
        runtime.catalog_mut().indexes.insert(
            name.clone(),
            crate::catalog::IndexSchema {
                name,
                table_name,
                kind,
                unique,
                columns,
                include_columns: Vec::new(),
                predicate_sql,
                full_text: None,
                fresh,
            },
        );
    }
    let view_count = cursor.read_u32()?;
    for _ in 0..view_count {
        let view = crate::catalog::ViewSchema {
            name: cursor.read_string()?,
            temporary: false,
            sql_text: cursor.read_string()?,
            column_names: cursor.read_strings()?,
            dependencies: cursor.read_strings()?,
        };
        runtime.catalog_mut().views.insert(view.name.clone(), view);
    }

    let trigger_count = cursor.read_u32()?;
    for _ in 0..trigger_count {
        let trigger = crate::catalog::TriggerSchema {
            name: cursor.read_string()?,
            target_name: cursor.read_string()?,
            kind: decode_trigger_kind(cursor.read_u8()?)?,
            event: decode_trigger_event(cursor.read_u8()?)?,
            on_view: cursor.read_bool()?,
            action_sql: cursor.read_string()?,
        };
        runtime
            .catalog_mut()
            .triggers
            .insert(trigger.name.clone(), trigger);
    }
    if cursor.offset < cursor.bytes.len() {
        decode_schemas_section(&mut cursor, &mut runtime.catalog_mut().schemas)?;
    }
    if cursor.offset < cursor.bytes.len() {
        decode_index_include_columns_section(&mut cursor, &mut runtime.catalog_mut().indexes)?;
    }
    if cursor.offset < cursor.bytes.len() {
        decode_full_text_options_section(&mut cursor, &mut runtime.catalog_mut().indexes)?;
    }
    if cursor.offset < cursor.bytes.len() {
        decode_generated_columns_section(&mut cursor, &mut runtime.catalog_mut().tables)?;
    }
    if cursor.offset < cursor.bytes.len() {
        decode_spatial_columns_section(&mut cursor, &mut runtime.catalog_mut().tables)?;
    }
    if cursor.offset < cursor.bytes.len() {
        decode_enum_columns_section(&mut cursor, &mut runtime.catalog_mut().tables)?;
    }
    if cursor.offset < cursor.bytes.len() {
        decode_pk_index_roots_section(&mut cursor, &mut runtime.catalog_mut().tables)?;
    }
    Ok(runtime)
}

#[cfg(test)]
pub(crate) fn encode_manifest_payload(
    runtime: &EngineRuntime,
    table_states: &BTreeMap<String, PersistedTableState>,
) -> Result<Vec<u8>> {
    Ok(encode_manifest_payload_with_offsets(runtime, table_states)?.bytes)
}

pub(crate) fn encode_manifest_payload_with_offsets(
    runtime: &EngineRuntime,
    table_states: &BTreeMap<String, PersistedTableState>,
) -> Result<ManifestEncoding> {
    let mut output = Vec::new();
    let mut table_next_row_id_offsets = BTreeMap::new();
    let mut table_state_offsets = BTreeMap::new();
    let mut table_pk_index_root_offsets = BTreeMap::new();
    output.extend_from_slice(MANIFEST_PAYLOAD_MAGIC);
    encode_u32(&mut output, runtime.catalog.schema_cookie);
    encode_u32(&mut output, runtime.catalog.tables.len() as u32);
    for table in runtime.catalog.tables.values() {
        encode_string(&mut output, &table.name)?;
        encode_u32(&mut output, table.columns.len() as u32);
        for column in &table.columns {
            encode_string(&mut output, &column.name)?;
            output.push(encode_column_type(column.column_type));
            output.push(u8::from(column.nullable));
            encode_optional_string(&mut output, column.default_sql.as_deref())?;
            output.push(u8::from(column.primary_key));
            output.push(u8::from(column.unique));
            output.push(u8::from(column.auto_increment));
            encode_u32(&mut output, column.checks.len() as u32);
            for check in &column.checks {
                encode_optional_string(&mut output, check.name.as_deref())?;
                encode_string(&mut output, &check.expression_sql)?;
            }
            output.push(u8::from(column.foreign_key.is_some()));
            if let Some(foreign_key) = &column.foreign_key {
                encode_foreign_key(&mut output, foreign_key)?;
            }
        }
        encode_u32(&mut output, table.checks.len() as u32);
        for check in &table.checks {
            encode_optional_string(&mut output, check.name.as_deref())?;
            encode_string(&mut output, &check.expression_sql)?;
        }
        encode_u32(&mut output, table.foreign_keys.len() as u32);
        for foreign_key in &table.foreign_keys {
            encode_foreign_key(&mut output, foreign_key)?;
        }
        encode_strings(&mut output, &table.primary_key_columns)?;
        table_next_row_id_offsets.insert(table.name.clone(), output.len());
        encode_i64(&mut output, table.next_row_id);
        table_state_offsets.insert(table.name.clone(), output.len());
        let state = table_states.get(&table.name).copied().unwrap_or_default();
        encode_u32(&mut output, state.checksum);
        encode_u32(&mut output, state.pointer.head_page_id);
        encode_u32(&mut output, state.pointer.logical_len);
        output.push(state.pointer.flags);
    }

    encode_u32(&mut output, runtime.catalog.indexes.len() as u32);
    for index in runtime.catalog.indexes.values() {
        encode_string(&mut output, &index.name)?;
        encode_string(&mut output, &index.table_name)?;
        output.push(index.kind as u8);
        output.push(u8::from(index.unique));
        encode_u32(&mut output, index.columns.len() as u32);
        for column in &index.columns {
            encode_optional_string(&mut output, column.column_name.as_deref())?;
            encode_optional_string(&mut output, column.expression_sql.as_deref())?;
        }
        encode_optional_string(&mut output, index.predicate_sql.as_deref())?;
        output.push(u8::from(index.fresh));
    }
    encode_u32(&mut output, runtime.catalog.views.len() as u32);
    for view in runtime.catalog.views.values() {
        encode_string(&mut output, &view.name)?;
        encode_string(&mut output, &view.sql_text)?;
        encode_strings(&mut output, &view.column_names)?;
        encode_strings(&mut output, &view.dependencies)?;
    }

    encode_u32(&mut output, runtime.catalog.triggers.len() as u32);
    for trigger in runtime.catalog.triggers.values() {
        encode_string(&mut output, &trigger.name)?;
        encode_string(&mut output, &trigger.target_name)?;
        output.push(trigger.kind as u8);
        output.push(trigger.event as u8);
        output.push(u8::from(trigger.on_view));
        encode_string(&mut output, &trigger.action_sql)?;
    }

    let table_stats = runtime
        .catalog
        .table_stats
        .iter()
        .filter(|(name, _)| runtime.catalog.tables.contains_key(*name))
        .collect::<Vec<_>>();
    encode_u32(&mut output, table_stats.len() as u32);
    for (name, stats) in table_stats {
        encode_string(&mut output, name)?;
        encode_i64(&mut output, stats.row_count);
    }

    let index_stats = runtime
        .catalog
        .index_stats
        .iter()
        .filter(|(name, _)| runtime.catalog.indexes.contains_key(*name))
        .collect::<Vec<_>>();
    encode_u32(&mut output, index_stats.len() as u32);
    for (name, stats) in index_stats {
        encode_string(&mut output, name)?;
        encode_i64(&mut output, stats.entry_count);
        encode_i64(&mut output, stats.distinct_key_count);
    }
    encode_schemas_section(&mut output, &runtime.catalog.schemas)?;
    encode_index_include_columns_section(&mut output, &runtime.catalog.indexes)?;
    encode_full_text_options_section(&mut output, &runtime.catalog.indexes)?;
    encode_generated_columns_section(&mut output, &runtime.catalog.tables)?;
    encode_spatial_columns_section(&mut output, &runtime.catalog.tables)?;
    encode_enum_columns_section(&mut output, &runtime.catalog.tables)?;
    encode_pk_index_roots_section(
        &mut output,
        &runtime.catalog.tables,
        Some(&mut table_pk_index_root_offsets),
    )?;
    Ok(ManifestEncoding {
        bytes: output,
        table_next_row_id_offsets,
        table_state_offsets,
        table_pk_index_root_offsets,
    })
}

pub(crate) fn decode_manifest_payload<S: PageStore>(
    _store: &S,
    bytes: &[u8],
) -> Result<EngineRuntime> {
    let mut cursor = Cursor::new(bytes);
    let magic = cursor.read_slice(MANIFEST_PAYLOAD_MAGIC.len())?;
    if magic != MANIFEST_PAYLOAD_MAGIC {
        return Err(DbError::corruption("catalog manifest magic is invalid"));
    }
    let mut runtime = EngineRuntime::empty(cursor.read_u32()?);
    let table_count = cursor.read_u32()?;
    for _ in 0..table_count {
        let table_name = cursor.read_string()?;
        let column_count = cursor.read_u32()?;
        let mut table = TableSchema {
            name: table_name.clone(),
            temporary: false,
            columns: Vec::with_capacity(column_count as usize),
            checks: Vec::new(),
            foreign_keys: Vec::new(),
            primary_key_columns: Vec::new(),
            next_row_id: 1,
            pk_index_root: None,
        };
        for _ in 0..column_count {
            let name = cursor.read_string()?;
            let column_type = decode_column_type(cursor.read_u8()?)?;
            let nullable = cursor.read_bool()?;
            let default_sql = cursor.read_optional_string()?;
            let primary_key = cursor.read_bool()?;
            let unique = cursor.read_bool()?;
            let auto_increment = cursor.read_bool()?;
            let check_count = cursor.read_u32()?;
            let mut checks = Vec::with_capacity(check_count as usize);
            for _ in 0..check_count {
                checks.push(crate::catalog::CheckConstraint {
                    name: cursor.read_optional_string()?,
                    expression_sql: cursor.read_string()?,
                });
            }
            let has_fk = cursor.read_bool()?;
            let foreign_key = if has_fk {
                Some(decode_foreign_key(&mut cursor)?)
            } else {
                None
            };
            table.columns.push(crate::catalog::ColumnSchema {
                name,
                column_type,
                spatial_type: None,
                enum_type: None,
                nullable,
                default_sql,
                generated_sql: None,
                generated_stored: true,
                primary_key,
                unique,
                auto_increment,
                checks,
                foreign_key,
            });
        }
        let table_check_count = cursor.read_u32()?;
        for _ in 0..table_check_count {
            table.checks.push(crate::catalog::CheckConstraint {
                name: cursor.read_optional_string()?,
                expression_sql: cursor.read_string()?,
            });
        }
        let fk_count = cursor.read_u32()?;
        for _ in 0..fk_count {
            table.foreign_keys.push(decode_foreign_key(&mut cursor)?);
        }
        table.primary_key_columns = cursor.read_strings()?;
        table.next_row_id = cursor.read_i64()?;
        let state = PersistedTableState {
            checksum: cursor.read_u32()?,
            pointer: OverflowPointer {
                head_page_id: cursor.read_u32()?,
                logical_len: cursor.read_u32()?,
                flags: cursor.read_u8()?,
            },
            row_count: 0,
            tail: OverflowTailInfo::default(),
            pk_index_root: None,
        };
        runtime
            .catalog_mut()
            .tables
            .insert(table_name.clone(), table);
        let has_data = state.pointer.head_page_id != 0 && state.pointer.logical_len != 0;
        if has_data {
            // Defer row data loading to first statement execution.
            runtime.deferred_tables_mut().insert(table_name.clone());
        }
        runtime
            .persisted_tables_mut()
            .insert(table_name.clone(), state);
        if !has_data {
            // Empty tables are immediately available.
            runtime
                .tables_mut()
                .insert(table_name, TableData::default().into());
        }
    }

    let index_count = cursor.read_u32()?;
    for _ in 0..index_count {
        let name = cursor.read_string()?;
        let table_name = cursor.read_string()?;
        let kind = decode_index_kind(cursor.read_u8()?)?;
        let unique = cursor.read_bool()?;
        let column_count = cursor.read_u32()?;
        let mut columns = Vec::with_capacity(column_count as usize);
        for _ in 0..column_count {
            columns.push(crate::catalog::IndexColumn {
                column_name: cursor.read_optional_string()?,
                expression_sql: cursor.read_optional_string()?,
            });
        }
        let predicate_sql = cursor.read_optional_string()?;
        let fresh = cursor.read_bool()?;
        runtime.catalog_mut().indexes.insert(
            name.clone(),
            crate::catalog::IndexSchema {
                name,
                table_name,
                kind,
                unique,
                columns,
                include_columns: Vec::new(),
                predicate_sql,
                full_text: None,
                fresh,
            },
        );
    }
    let view_count = cursor.read_u32()?;
    for _ in 0..view_count {
        let view = crate::catalog::ViewSchema {
            name: cursor.read_string()?,
            temporary: false,
            sql_text: cursor.read_string()?,
            column_names: cursor.read_strings()?,
            dependencies: cursor.read_strings()?,
        };
        runtime.catalog_mut().views.insert(view.name.clone(), view);
    }

    let trigger_count = cursor.read_u32()?;
    for _ in 0..trigger_count {
        let trigger = crate::catalog::TriggerSchema {
            name: cursor.read_string()?,
            target_name: cursor.read_string()?,
            kind: decode_trigger_kind(cursor.read_u8()?)?,
            event: decode_trigger_event(cursor.read_u8()?)?,
            on_view: cursor.read_bool()?,
            action_sql: cursor.read_string()?,
        };
        runtime
            .catalog_mut()
            .triggers
            .insert(trigger.name.clone(), trigger);
    }
    if cursor.offset < cursor.bytes.len() {
        let table_stats_count = cursor.read_u32()?;
        for _ in 0..table_stats_count {
            let name = cursor.read_string()?;
            let stats = crate::catalog::TableStats {
                row_count: cursor.read_i64()?,
            };
            if let Some(state) = runtime.persisted_tables_mut().get_mut(&name) {
                state.row_count = usize::try_from(stats.row_count.max(0)).unwrap_or(usize::MAX);
            }
            runtime.catalog_mut().table_stats.insert(name, stats);
        }
    }
    if cursor.offset < cursor.bytes.len() {
        let index_stats_count = cursor.read_u32()?;
        for _ in 0..index_stats_count {
            let name = cursor.read_string()?;
            let stats = crate::catalog::IndexStats {
                entry_count: cursor.read_i64()?,
                distinct_key_count: cursor.read_i64()?,
            };
            runtime.catalog_mut().index_stats.insert(name, stats);
        }
    }
    if cursor.offset < cursor.bytes.len() {
        decode_schemas_section(&mut cursor, &mut runtime.catalog_mut().schemas)?;
    }
    if cursor.offset < cursor.bytes.len() {
        decode_index_include_columns_section(&mut cursor, &mut runtime.catalog_mut().indexes)?;
    }
    if cursor.offset < cursor.bytes.len() {
        decode_full_text_options_section(&mut cursor, &mut runtime.catalog_mut().indexes)?;
    }
    if cursor.offset < cursor.bytes.len() {
        decode_generated_columns_section(&mut cursor, &mut runtime.catalog_mut().tables)?;
    }
    if cursor.offset < cursor.bytes.len() {
        decode_spatial_columns_section(&mut cursor, &mut runtime.catalog_mut().tables)?;
    }
    if cursor.offset < cursor.bytes.len() {
        decode_enum_columns_section(&mut cursor, &mut runtime.catalog_mut().tables)?;
    }
    if cursor.offset < cursor.bytes.len() {
        decode_pk_index_roots_section(&mut cursor, &mut runtime.catalog_mut().tables)?;
    }
    let table_pk_roots = runtime
        .catalog
        .tables
        .iter()
        .map(|(table_name, table)| (table_name.clone(), table.pk_index_root))
        .collect::<Vec<_>>();
    for (table_name, pk_index_root) in table_pk_roots {
        if let Some(state) = runtime.persisted_tables_mut().get_mut(&table_name) {
            state.pk_index_root = pk_index_root;
        }
    }
    Ok(runtime)
}

pub(crate) fn encode_table_payload(data: &TableData) -> Result<Vec<u8>> {
    encode_table_payload_with_tombstone_locators(data).map(|(payload, _)| payload)
}

pub(crate) fn encode_table_payload_with_tombstone_locators(
    data: &TableData,
) -> Result<(Vec<u8>, Int64Map<u32>)> {
    let row_count = data.row_count();
    if row_count == 0 {
        return Ok((
            Vec::new(),
            Int64Map::with_hasher(Int64HashBuilder::default()),
        ));
    }
    let mut output = Vec::with_capacity(TABLE_PAYLOAD_MAGIC.len() + 4 + row_count * 32);
    let mut locators = Int64Map::with_capacity_and_hasher(row_count, Int64HashBuilder::default());
    output.extend_from_slice(TABLE_PAYLOAD_MAGIC);
    encode_u32(&mut output, row_count as u32);
    let mut encoded_row = Vec::with_capacity(64);
    for row in data.visible_rows() {
        encode_i64(&mut output, row.row_id);
        Row::encode_values_into(&row.values, &mut encoded_row)?;
        let row_body_len = encoded_row
            .len()
            .saturating_add(TABLE_PAYLOAD_ROW_BODY_PADDING_BYTES);
        encode_u32(
            &mut output,
            u32::try_from(row_body_len)
                .ok()
                .filter(|len| *len < TABLE_PAYLOAD_ROW_TOMBSTONE_FLAG)
                .ok_or_else(|| DbError::constraint("table row body length exceeds u32"))?,
        );
        locators.insert(
            row.row_id,
            u32::try_from(output.len().saturating_sub(4))
                .map_err(|_| DbError::constraint("resident tombstone locator exceeds u32"))?,
        );
        output.extend_from_slice(&encoded_row);
        output.extend(std::iter::repeat_n(
            0u8,
            row_body_len.saturating_sub(encoded_row.len()),
        ));
    }
    Ok((output, locators))
}

pub(crate) fn encode_paged_table_chunks_from_rows(
    rows: &[StoredRow],
    page_size: u32,
) -> Result<Vec<EncodedPagedTableChunk>> {
    if rows.is_empty() {
        return Ok(Vec::new());
    }

    let target_chunk_bytes = paged_table_target_chunk_bytes(page_size);
    let mut chunks = Vec::new();
    let mut chunk = Vec::with_capacity(target_chunk_bytes);
    chunk.extend_from_slice(TABLE_PAYLOAD_MAGIC);
    chunk.extend_from_slice(&0_u32.to_le_bytes());
    let mut chunk_row_count = 0usize;
    let mut encoded_row = Vec::with_capacity(64);

    for row in rows {
        encoded_row.clear();
        Row::encode_values_into(&row.values, &mut encoded_row)?;
        let encoded_row_len = 8usize.saturating_add(4).saturating_add(encoded_row.len());
        if chunk_row_count > 0 && chunk.len().saturating_add(encoded_row_len) > target_chunk_bytes {
            chunks.push(finalize_encoded_paged_table_chunk(chunk, chunk_row_count)?);
            chunk = Vec::with_capacity(target_chunk_bytes);
            chunk.extend_from_slice(TABLE_PAYLOAD_MAGIC);
            chunk.extend_from_slice(&0_u32.to_le_bytes());
            chunk_row_count = 0;
        }
        encode_i64(&mut chunk, row.row_id);
        encode_bytes(&mut chunk, &encoded_row)?;
        chunk_row_count += 1;
    }

    if chunk_row_count > 0 {
        chunks.push(finalize_encoded_paged_table_chunk(chunk, chunk_row_count)?);
    }
    Ok(chunks)
}

pub(crate) fn encode_paged_table_chunks(
    data: &TableData,
    page_size: u32,
) -> Result<Vec<EncodedPagedTableChunk>> {
    if !data.has_tombstoned_rows() {
        return encode_paged_table_chunks_from_rows(&data.rows, page_size);
    }
    let rows = data.visible_rows().cloned().collect::<Vec<_>>();
    encode_paged_table_chunks_from_rows(&rows, page_size)
}

pub(crate) fn encode_paged_table_manifest_payload(
    manifest: &PersistedPagedTableManifest,
) -> Result<Vec<u8>> {
    let mut output = Vec::with_capacity(
        TABLE_PAGED_MANIFEST_MAGIC.len() + 4 + manifest.chunks.len().saturating_mul(30),
    );
    output.extend_from_slice(TABLE_PAGED_MANIFEST_MAGIC);
    encode_u32(
        &mut output,
        u32::try_from(manifest.chunks.len())
            .map_err(|_| DbError::constraint("paged table chunk count exceeds u32"))?,
    );
    for chunk in &manifest.chunks {
        encode_u32(&mut output, chunk.checksum);
        encode_u32(&mut output, chunk.pointer.head_page_id);
        encode_u32(&mut output, chunk.pointer.logical_len);
        output.push(chunk.pointer.flags);
        encode_u32(
            &mut output,
            u32::try_from(chunk.row_count)
                .map_err(|_| DbError::constraint("paged table chunk row count exceeds u32"))?,
        );
        encode_u32(
            &mut output,
            u32::try_from(chunk.tombstoned_row_ids.len()).map_err(|_| {
                DbError::constraint("paged table chunk tombstone count exceeds u32")
            })?,
        );
        for row_id in &chunk.tombstoned_row_ids {
            encode_i64(&mut output, *row_id);
        }
        output.push(if chunk.overlay_pointer.is_some() {
            1
        } else {
            0
        });
        if let Some(overlay_pointer) = chunk.overlay_pointer {
            encode_u32(&mut output, overlay_pointer.head_page_id);
            encode_u32(&mut output, overlay_pointer.logical_len);
            output.push(overlay_pointer.flags);
            encode_u32(
                &mut output,
                chunk.overlay_checksum.ok_or_else(|| {
                    DbError::internal("paged table chunk overlay checksum missing")
                })?,
            );
        }
    }
    Ok(output)
}

pub(crate) fn decode_paged_table_manifest_payload(
    bytes: &[u8],
) -> Result<PersistedPagedTableManifest> {
    let mut cursor = Cursor::new(bytes);
    let magic = cursor.read_slice(TABLE_PAGED_MANIFEST_MAGIC.len())?;
    if magic != TABLE_PAGED_MANIFEST_MAGIC {
        return Err(DbError::corruption("paged table manifest magic is invalid"));
    }
    let chunk_count = cursor.read_u32()? as usize;
    let mut chunks = Vec::with_capacity(chunk_count);
    for _ in 0..chunk_count {
        let checksum = cursor.read_u32()?;
        let pointer = OverflowPointer {
            head_page_id: cursor.read_u32()?,
            logical_len: cursor.read_u32()?,
            flags: cursor.read_u8()?,
        };
        let row_count = cursor.read_u32()? as usize;
        let tombstoned_row_ids_len = cursor.read_u32()? as usize;
        let mut tombstoned_row_ids = Vec::with_capacity(tombstoned_row_ids_len);
        for _ in 0..tombstoned_row_ids_len {
            tombstoned_row_ids.push(cursor.read_i64()?);
        }
        let has_overlay = cursor.read_bool()?;
        let mut overlay_pointer = None;
        let mut overlay_checksum = None;
        if has_overlay {
            overlay_pointer = Some(OverflowPointer {
                head_page_id: cursor.read_u32()?,
                logical_len: cursor.read_u32()?,
                flags: cursor.read_u8()?,
            });
            overlay_checksum = Some(cursor.read_u32()?);
        }
        chunks.push(PersistedTableChunkState {
            checksum,
            pointer,
            row_count,
            tombstoned_row_ids,
            overlay_pointer,
            overlay_checksum,
        });
    }
    if cursor.offset != cursor.bytes.len() {
        return Err(DbError::corruption(
            "paged table manifest payload had trailing bytes",
        ));
    }
    Ok(PersistedPagedTableManifest { chunks })
}

pub(crate) fn decode_table_payload_rows(bytes: &[u8]) -> Result<Vec<StoredRow>> {
    let mut rows = Vec::new();
    visit_table_payload_rows_from_bytes(bytes, &mut |row_id, values| {
        rows.push(StoredRow {
            row_id,
            values: values.to_vec(),
        });
        Ok(())
    })?;
    Ok(rows)
}

pub(crate) fn encode_legacy_table_payload_from_manifest(
    manifest: &TablePageManifest,
) -> Result<Vec<u8>> {
    if manifest.row_count() == 0 {
        return Ok(Vec::new());
    }
    let mut rows = Vec::with_capacity(manifest.row_count());
    for row in manifest.rows() {
        let row = row?;
        rows.push(StoredRow {
            row_id: row.row_id(),
            values: row.values().to_vec(),
        });
    }
    encode_table_payload(&TableData::from_rows(rows))
}

pub(crate) fn decode_persisted_table_data<S: PageStore>(
    store: &S,
    state: PersistedTableState,
) -> Result<TableData> {
    let manifest = read_table_page_manifest_from_state(store, state)?;
    let mut rows = Vec::with_capacity(manifest.row_count());
    for row in manifest.rows() {
        let row = row?;
        rows.push(StoredRow {
            row_id: row.row_id(),
            values: row.values().to_vec(),
        });
    }
    Ok(TableData::from_rows(rows))
}

pub(crate) fn encode_appended_table_rows(
    data: &TableData,
    existing_count: usize,
) -> Result<Vec<u8>> {
    if existing_count > data.rows.len() {
        return Err(DbError::internal(
            "append-only table payload rewrite saw fewer rows than the previous persisted payload",
        ));
    }
    if existing_count == data.rows.len() {
        return Ok(Vec::new());
    }

    let mut appended = Vec::with_capacity((data.rows.len() - existing_count) * 32);
    let mut encoded_row = Vec::with_capacity(64);
    for row in data.rows.iter().skip(existing_count) {
        encode_i64(&mut appended, row.row_id);
        Row::encode_values_into(&row.values, &mut encoded_row)?;
        let row_body_len = encoded_row
            .len()
            .saturating_add(TABLE_PAYLOAD_ROW_BODY_PADDING_BYTES);
        encode_u32(
            &mut appended,
            u32::try_from(row_body_len)
                .map_err(|_| DbError::constraint("table row body length exceeds u32"))?,
        );
        appended.extend_from_slice(&encoded_row);
        appended.extend(std::iter::repeat_n(
            0u8,
            row_body_len.saturating_sub(encoded_row.len()),
        ));
        encoded_row.clear();
    }
    Ok(appended)
}

#[cfg(test)]
pub(crate) fn decode_table_payload(bytes: &[u8]) -> Result<TableData> {
    if bytes.is_empty() {
        return Ok(TableData::default());
    }
    let mut cursor = Cursor::new(bytes);
    let magic = cursor.read_slice(TABLE_PAYLOAD_MAGIC.len())?;
    if magic != TABLE_PAYLOAD_MAGIC {
        return Err(DbError::corruption("table payload magic is invalid"));
    }
    let row_count = cursor.read_u32()? as usize;
    let mut data = TableData::default();
    data.reserve_rows(row_count);
    let mut slots = 0usize;
    while cursor.offset < cursor.bytes.len() {
        let row_id = cursor.read_i64()?;
        let (is_tombstone, row_bytes_len) = split_table_payload_row_len(cursor.read_u32()?);
        let row_bytes = cursor.read_slice(row_bytes_len)?;
        slots += 1;
        if is_tombstone {
            continue;
        }
        let row = Row::decode(row_bytes)?;
        data.push_row(StoredRow {
            row_id,
            values: row.into_values(),
        });
    }
    if slots < row_count {
        return Err(DbError::corruption(
            "table payload row count exceeded decoded row content",
        ));
    }
    Ok(data)
}

pub(crate) fn encode_u32(output: &mut Vec<u8>, value: u32) {
    output.extend_from_slice(&value.to_le_bytes());
}

pub(crate) fn encode_u64(output: &mut Vec<u8>, value: u64) {
    output.extend_from_slice(&value.to_le_bytes());
}

pub(crate) fn encode_i64(output: &mut Vec<u8>, value: i64) {
    output.extend_from_slice(&value.to_le_bytes());
}

pub(crate) fn encode_string(output: &mut Vec<u8>, value: &str) -> Result<()> {
    encode_u32(
        output,
        u32::try_from(value.len()).map_err(|_| DbError::constraint("string length exceeds u32"))?,
    );
    output.extend_from_slice(value.as_bytes());
    Ok(())
}

pub(crate) fn encode_optional_string(output: &mut Vec<u8>, value: Option<&str>) -> Result<()> {
    output.push(u8::from(value.is_some()));
    if let Some(value) = value {
        encode_string(output, value)?;
    }
    Ok(())
}

pub(crate) fn encode_strings(output: &mut Vec<u8>, values: &[String]) -> Result<()> {
    encode_u32(
        output,
        u32::try_from(values.len())
            .map_err(|_| DbError::constraint("string list length exceeds u32"))?,
    );
    for value in values {
        encode_string(output, value)?;
    }
    Ok(())
}

pub(crate) fn encode_bytes(output: &mut Vec<u8>, bytes: &[u8]) -> Result<()> {
    encode_u32(
        output,
        u32::try_from(bytes.len())
            .map_err(|_| DbError::constraint("byte vector length exceeds u32"))?,
    );
    output.extend_from_slice(bytes);
    Ok(())
}

pub(crate) fn encode_foreign_key(
    output: &mut Vec<u8>,
    foreign_key: &crate::catalog::ForeignKeyConstraint,
) -> Result<()> {
    encode_optional_string(output, foreign_key.name.as_deref())?;
    encode_strings(output, &foreign_key.columns)?;
    encode_string(output, &foreign_key.referenced_table)?;
    encode_strings(output, &foreign_key.referenced_columns)?;
    output.push(foreign_key.on_delete as u8);
    output.push(foreign_key.on_update as u8);
    Ok(())
}

pub(crate) fn encode_generated_columns_section(
    output: &mut Vec<u8>,
    tables: &BTreeMap<String, TableSchema>,
) -> Result<()> {
    let generated_columns = tables
        .values()
        .flat_map(|table| {
            table.columns.iter().filter_map(move |column| {
                column.generated_sql.as_ref().map(|generated_sql| {
                    (
                        table.name.as_str(),
                        column.name.as_str(),
                        generated_sql.as_str(),
                        column.generated_stored,
                    )
                })
            })
        })
        .collect::<Vec<_>>();
    output.extend_from_slice(GENERATED_COLUMNS_SECTION_MAGIC);
    output.push(1);
    encode_u32(
        output,
        u32::try_from(generated_columns.len())
            .map_err(|_| DbError::constraint("generated column count exceeds u32"))?,
    );
    for (table_name, column_name, generated_sql, generated_stored) in generated_columns {
        encode_string(output, table_name)?;
        encode_string(output, column_name)?;
        encode_string(output, generated_sql)?;
        output.push(u8::from(generated_stored));
    }
    Ok(())
}

pub(crate) fn encode_spatial_columns_section(
    output: &mut Vec<u8>,
    tables: &BTreeMap<String, TableSchema>,
) -> Result<()> {
    let spatial_columns = tables
        .values()
        .flat_map(|table| {
            table.columns.iter().filter_map(move |column| {
                column
                    .spatial_type
                    .map(|spatial_type| (table.name.as_str(), column.name.as_str(), spatial_type))
            })
        })
        .collect::<Vec<_>>();
    output.extend_from_slice(SPATIAL_COLUMNS_SECTION_MAGIC);
    output.push(1);
    encode_u32(
        output,
        u32::try_from(spatial_columns.len())
            .map_err(|_| DbError::constraint("spatial column count exceeds u32"))?,
    );
    for (table_name, column_name, spatial_type) in spatial_columns {
        encode_string(output, table_name)?;
        encode_string(output, column_name)?;
        output.push(encode_spatial_subtype_tag(spatial_type.subtype));
        output.push(encode_spatial_dimensions_tag(spatial_type.dimensions));
        let srid = u32::try_from(spatial_type.srid)
            .map_err(|_| DbError::constraint("spatial SRID must be non-negative"))?;
        encode_u32(output, srid);
    }
    Ok(())
}

pub(crate) fn encode_enum_columns_section(
    output: &mut Vec<u8>,
    tables: &BTreeMap<String, TableSchema>,
) -> Result<()> {
    let enum_columns = tables
        .values()
        .flat_map(|table| {
            table.columns.iter().filter_map(move |column| {
                column
                    .enum_type
                    .as_ref()
                    .map(|enum_type| (table.name.as_str(), column.name.as_str(), enum_type))
            })
        })
        .collect::<Vec<_>>();
    output.extend_from_slice(ENUM_COLUMNS_SECTION_MAGIC);
    output.push(1);
    encode_u32(
        output,
        u32::try_from(enum_columns.len())
            .map_err(|_| DbError::constraint("enum column count exceeds u32"))?,
    );
    for (table_name, column_name, enum_type) in enum_columns {
        encode_string(output, table_name)?;
        encode_string(output, column_name)?;
        encode_u64(output, enum_type.type_id);
        encode_u32(
            output,
            u32::try_from(enum_type.labels.len())
                .map_err(|_| DbError::constraint("enum label count exceeds u32"))?,
        );
        for label in &enum_type.labels {
            encode_u64(output, label.id);
            encode_string(output, &label.label)?;
        }
    }
    Ok(())
}

pub(crate) fn encode_index_include_columns_section(
    output: &mut Vec<u8>,
    indexes: &BTreeMap<String, IndexSchema>,
) -> Result<()> {
    let include_entries = indexes
        .iter()
        .filter(|(_, index)| !index.include_columns.is_empty())
        .collect::<Vec<_>>();
    output.extend_from_slice(INDEX_INCLUDE_COLUMNS_SECTION_MAGIC);
    output.push(1);
    encode_u32(
        output,
        u32::try_from(include_entries.len())
            .map_err(|_| DbError::constraint("index include entry count exceeds u32"))?,
    );
    for (index_name, index) in include_entries {
        encode_string(output, index_name)?;
        encode_strings(output, &index.include_columns)?;
    }
    Ok(())
}

pub(crate) fn encode_full_text_options_section(
    output: &mut Vec<u8>,
    indexes: &BTreeMap<String, IndexSchema>,
) -> Result<()> {
    let entries = indexes
        .iter()
        .filter_map(|(name, index)| index.full_text.as_ref().map(|config| (name, config)))
        .collect::<Vec<_>>();
    output.extend_from_slice(FULL_TEXT_OPTIONS_SECTION_MAGIC);
    output.push(1);
    encode_u32(
        output,
        u32::try_from(entries.len())
            .map_err(|_| DbError::constraint("fulltext option entry count exceeds u32"))?,
    );
    for (index_name, config) in entries {
        encode_string(output, index_name)?;
        let bytes = config
            .to_json()
            .map_err(|error| DbError::internal(error.message))?;
        encode_bytes(output, &bytes)?;
    }
    Ok(())
}

pub(crate) fn encode_schemas_section(
    output: &mut Vec<u8>,
    schemas: &BTreeMap<String, SchemaInfo>,
) -> Result<()> {
    output.extend_from_slice(SCHEMAS_SECTION_MAGIC);
    output.push(1);
    encode_u32(
        output,
        u32::try_from(schemas.len())
            .map_err(|_| DbError::constraint("schema count exceeds u32"))?,
    );
    for schema in schemas.values() {
        encode_string(output, &schema.name)?;
    }
    Ok(())
}

pub(crate) fn encode_pk_index_roots_section(
    output: &mut Vec<u8>,
    tables: &BTreeMap<String, TableSchema>,
    mut offsets: Option<&mut BTreeMap<String, usize>>,
) -> Result<()> {
    output.extend_from_slice(PK_INDEX_ROOTS_SECTION_MAGIC);
    output.push(1);
    encode_u32(
        output,
        u32::try_from(tables.len())
            .map_err(|_| DbError::constraint("pk index root entry count exceeds u32"))?,
    );
    for table in tables.values() {
        encode_string(output, &table.name)?;
        if let Some(offsets) = offsets.as_deref_mut() {
            offsets.insert(table.name.clone(), output.len());
        }
        encode_u32(output, table.pk_index_root.unwrap_or(0));
    }
    Ok(())
}

pub(crate) fn decode_schemas_section(
    cursor: &mut Cursor<'_>,
    schemas: &mut BTreeMap<String, SchemaInfo>,
) -> Result<()> {
    let section_is_present = cursor
        .bytes
        .get(cursor.offset..cursor.offset + SCHEMAS_SECTION_MAGIC.len())
        .is_some_and(|magic| magic == SCHEMAS_SECTION_MAGIC);
    if !section_is_present {
        return Ok(());
    }
    cursor.offset += SCHEMAS_SECTION_MAGIC.len();
    let version = cursor.read_u8()?;
    if version != 1 {
        return Err(DbError::corruption(format!(
            "unknown schemas section version {version}"
        )));
    }
    let schema_count = cursor.read_u32()?;
    for _ in 0..schema_count {
        let name = cursor.read_string()?;
        schemas.insert(name.clone(), SchemaInfo { name });
    }
    Ok(())
}

pub(crate) fn decode_index_include_columns_section(
    cursor: &mut Cursor<'_>,
    indexes: &mut BTreeMap<String, IndexSchema>,
) -> Result<()> {
    let section_is_present = cursor
        .bytes
        .get(cursor.offset..cursor.offset + INDEX_INCLUDE_COLUMNS_SECTION_MAGIC.len())
        .is_some_and(|magic| magic == INDEX_INCLUDE_COLUMNS_SECTION_MAGIC);
    if !section_is_present {
        return Ok(());
    }
    cursor.offset += INDEX_INCLUDE_COLUMNS_SECTION_MAGIC.len();
    let version = cursor.read_u8()?;
    if version != 1 {
        return Err(DbError::corruption(format!(
            "unknown index include columns section version {version}"
        )));
    }
    let entry_count = cursor.read_u32()?;
    for _ in 0..entry_count {
        let index_name = cursor.read_string()?;
        let include_columns = cursor.read_strings()?;
        let index = indexes.get_mut(&index_name).ok_or_else(|| {
            DbError::corruption(format!(
                "index include metadata referenced unknown index {index_name}"
            ))
        })?;
        index.include_columns = include_columns;
    }
    Ok(())
}

pub(crate) fn decode_full_text_options_section(
    cursor: &mut Cursor<'_>,
    indexes: &mut BTreeMap<String, IndexSchema>,
) -> Result<()> {
    let section_is_present = cursor
        .bytes
        .get(cursor.offset..cursor.offset + FULL_TEXT_OPTIONS_SECTION_MAGIC.len())
        .is_some_and(|magic| magic == FULL_TEXT_OPTIONS_SECTION_MAGIC);
    if !section_is_present {
        return Ok(());
    }
    cursor.offset += FULL_TEXT_OPTIONS_SECTION_MAGIC.len();
    let version = cursor.read_u8()?;
    if version != 1 {
        return Err(DbError::corruption(format!(
            "unknown fulltext options section version {version}"
        )));
    }
    let entry_count = cursor.read_u32()?;
    for _ in 0..entry_count {
        let index_name = cursor.read_string()?;
        let config_bytes_len = cursor.read_u32()? as usize;
        let config_bytes = cursor.read_slice(config_bytes_len)?;
        let config = AnalyzerConfig::from_json(config_bytes)
            .map_err(|error| DbError::corruption(error.message))?;
        let index = indexes.get_mut(&index_name).ok_or_else(|| {
            DbError::corruption(format!(
                "fulltext options metadata referenced unknown index {index_name}"
            ))
        })?;
        if index.kind != IndexKind::FullText {
            return Err(DbError::corruption(format!(
                "fulltext options metadata referenced non-fulltext index {index_name}"
            )));
        }
        index.full_text = Some(config);
    }
    Ok(())
}

pub(crate) fn decode_generated_columns_section(
    cursor: &mut Cursor<'_>,
    tables: &mut BTreeMap<String, TableSchema>,
) -> Result<()> {
    let section_is_versioned = cursor
        .bytes
        .get(cursor.offset..cursor.offset + GENERATED_COLUMNS_SECTION_MAGIC.len())
        .is_some_and(|magic| magic == GENERATED_COLUMNS_SECTION_MAGIC);
    if section_is_versioned {
        cursor.offset += GENERATED_COLUMNS_SECTION_MAGIC.len();
        let version = cursor.read_u8()?;
        if version != 1 {
            return Err(DbError::corruption(format!(
                "unknown generated columns section version {version}"
            )));
        }
    }
    let generated_column_count = cursor.read_u32()?;
    for _ in 0..generated_column_count {
        let table_name = cursor.read_string()?;
        let column_name = cursor.read_string()?;
        let generated_sql = cursor.read_string()?;
        let generated_stored = if section_is_versioned {
            cursor.read_bool()?
        } else {
            true
        };
        let table = tables.get_mut(&table_name).ok_or_else(|| {
            DbError::corruption(format!(
                "generated column metadata referenced unknown table {table_name}"
            ))
        })?;
        let column = table
            .columns
            .iter_mut()
            .find(|column| identifiers_equal(&column.name, &column_name))
            .ok_or_else(|| {
                DbError::corruption(format!(
                    "generated column metadata referenced unknown column {}.{}",
                    table_name, column_name
                ))
            })?;
        column.generated_sql = Some(generated_sql);
        column.generated_stored = generated_stored;
    }
    Ok(())
}

pub(crate) fn decode_spatial_columns_section(
    cursor: &mut Cursor<'_>,
    tables: &mut BTreeMap<String, TableSchema>,
) -> Result<()> {
    let section_is_present = cursor
        .bytes
        .get(cursor.offset..cursor.offset + SPATIAL_COLUMNS_SECTION_MAGIC.len())
        .is_some_and(|magic| magic == SPATIAL_COLUMNS_SECTION_MAGIC);
    if !section_is_present {
        return Ok(());
    }
    cursor.offset += SPATIAL_COLUMNS_SECTION_MAGIC.len();
    let version = cursor.read_u8()?;
    if version != 1 {
        return Err(DbError::corruption(format!(
            "unknown spatial columns section version {version}"
        )));
    }
    let entry_count = cursor.read_u32()?;
    for _ in 0..entry_count {
        let table_name = cursor.read_string()?;
        let column_name = cursor.read_string()?;
        let subtype = decode_spatial_subtype_tag(cursor.read_u8()?)?;
        let dimensions = decode_spatial_dimensions_tag(cursor.read_u8()?)?;
        let srid = i32::try_from(cursor.read_u32()?)
            .map_err(|_| DbError::corruption("spatial SRID exceeds i32"))?;
        let table = tables.get_mut(&table_name).ok_or_else(|| {
            DbError::corruption(format!(
                "spatial column metadata referenced unknown table {table_name}"
            ))
        })?;
        let column = table
            .columns
            .iter_mut()
            .find(|column| identifiers_equal(&column.name, &column_name))
            .ok_or_else(|| {
                DbError::corruption(format!(
                    "spatial column metadata referenced unknown column {}.{}",
                    table_name, column_name
                ))
            })?;
        column.spatial_type = Some(crate::catalog::SpatialTypeInfo {
            subtype,
            dimensions,
            srid,
        });
    }
    Ok(())
}

pub(crate) fn decode_enum_columns_section(
    cursor: &mut Cursor<'_>,
    tables: &mut BTreeMap<String, TableSchema>,
) -> Result<()> {
    let section_is_present = cursor
        .bytes
        .get(cursor.offset..cursor.offset + ENUM_COLUMNS_SECTION_MAGIC.len())
        .is_some_and(|magic| magic == ENUM_COLUMNS_SECTION_MAGIC);
    if !section_is_present {
        return Ok(());
    }
    cursor.offset += ENUM_COLUMNS_SECTION_MAGIC.len();
    let version = cursor.read_u8()?;
    if version != 1 {
        return Err(DbError::corruption(format!(
            "unknown enum columns section version {version}"
        )));
    }
    let entry_count = cursor.read_u32()?;
    for _ in 0..entry_count {
        let table_name = cursor.read_string()?;
        let column_name = cursor.read_string()?;
        let type_id = cursor.read_u64()?;
        let label_count = cursor.read_u32()?;
        let mut labels = Vec::with_capacity(label_count as usize);
        for _ in 0..label_count {
            labels.push(EnumLabel {
                id: cursor.read_u64()?,
                label: cursor.read_string()?,
            });
        }
        let table = tables.get_mut(&table_name).ok_or_else(|| {
            DbError::corruption(format!(
                "enum column metadata referenced unknown table {table_name}"
            ))
        })?;
        let column = table
            .columns
            .iter_mut()
            .find(|column| identifiers_equal(&column.name, &column_name))
            .ok_or_else(|| {
                DbError::corruption(format!(
                    "enum column metadata referenced unknown column {}.{}",
                    table_name, column_name
                ))
            })?;
        column.enum_type = Some(EnumTypeInfo { type_id, labels });
    }
    Ok(())
}

pub(crate) fn decode_pk_index_roots_section(
    cursor: &mut Cursor<'_>,
    tables: &mut BTreeMap<String, TableSchema>,
) -> Result<()> {
    let section_is_present = cursor
        .bytes
        .get(cursor.offset..cursor.offset + PK_INDEX_ROOTS_SECTION_MAGIC.len())
        .is_some_and(|magic| magic == PK_INDEX_ROOTS_SECTION_MAGIC);
    if !section_is_present {
        return Ok(());
    }
    cursor.offset += PK_INDEX_ROOTS_SECTION_MAGIC.len();
    let version = cursor.read_u8()?;
    if version != 1 {
        return Err(DbError::corruption(format!(
            "unknown pk index roots section version {version}"
        )));
    }
    let entry_count = cursor.read_u32()?;
    for _ in 0..entry_count {
        let table_name = cursor.read_string()?;
        let pk_index_root = match cursor.read_u32()? {
            0 => None,
            page_id => Some(page_id),
        };
        let table = tables.get_mut(&table_name).ok_or_else(|| {
            DbError::corruption(format!(
                "pk index root metadata referenced unknown table {table_name}"
            ))
        })?;
        table.pk_index_root = pk_index_root;
    }
    Ok(())
}

pub(crate) fn encode_spatial_subtype_tag(subtype: crate::catalog::SpatialSubtype) -> u8 {
    match subtype {
        crate::catalog::SpatialSubtype::Any => 0,
        crate::catalog::SpatialSubtype::Point => 1,
        crate::catalog::SpatialSubtype::LineString => 2,
        crate::catalog::SpatialSubtype::Polygon => 3,
        crate::catalog::SpatialSubtype::MultiPoint => 4,
        crate::catalog::SpatialSubtype::MultiLineString => 5,
        crate::catalog::SpatialSubtype::MultiPolygon => 6,
    }
}

pub(crate) fn decode_spatial_subtype_tag(tag: u8) -> Result<crate::catalog::SpatialSubtype> {
    match tag {
        0 => Ok(crate::catalog::SpatialSubtype::Any),
        1 => Ok(crate::catalog::SpatialSubtype::Point),
        2 => Ok(crate::catalog::SpatialSubtype::LineString),
        3 => Ok(crate::catalog::SpatialSubtype::Polygon),
        4 => Ok(crate::catalog::SpatialSubtype::MultiPoint),
        5 => Ok(crate::catalog::SpatialSubtype::MultiLineString),
        6 => Ok(crate::catalog::SpatialSubtype::MultiPolygon),
        _ => Err(DbError::corruption("unknown spatial subtype tag")),
    }
}

pub(crate) fn encode_spatial_dimensions_tag(dimensions: crate::catalog::SpatialDimensions) -> u8 {
    match dimensions {
        crate::catalog::SpatialDimensions::Any => 0,
        crate::catalog::SpatialDimensions::Xy => 1,
        crate::catalog::SpatialDimensions::Xyz => 2,
        crate::catalog::SpatialDimensions::Xym => 3,
        crate::catalog::SpatialDimensions::Xyzm => 4,
    }
}

pub(crate) fn decode_spatial_dimensions_tag(tag: u8) -> Result<crate::catalog::SpatialDimensions> {
    match tag {
        0 => Ok(crate::catalog::SpatialDimensions::Any),
        1 => Ok(crate::catalog::SpatialDimensions::Xy),
        2 => Ok(crate::catalog::SpatialDimensions::Xyz),
        3 => Ok(crate::catalog::SpatialDimensions::Xym),
        4 => Ok(crate::catalog::SpatialDimensions::Xyzm),
        _ => Err(DbError::corruption("unknown spatial dimensions tag")),
    }
}

pub(crate) fn encode_column_type(column_type: crate::catalog::ColumnType) -> u8 {
    match column_type {
        crate::catalog::ColumnType::Int64 => 0,
        crate::catalog::ColumnType::Float64 => 1,
        crate::catalog::ColumnType::Text => 2,
        crate::catalog::ColumnType::Bool => 3,
        crate::catalog::ColumnType::Blob => 4,
        crate::catalog::ColumnType::Decimal => 5,
        crate::catalog::ColumnType::Uuid => 6,
        crate::catalog::ColumnType::Timestamp => 7,
        crate::catalog::ColumnType::Geometry => 8,
        crate::catalog::ColumnType::Geography => 9,
        crate::catalog::ColumnType::Enum => 10,
        crate::catalog::ColumnType::IpAddr => 11,
        crate::catalog::ColumnType::Cidr => 12,
        crate::catalog::ColumnType::Date => 13,
        crate::catalog::ColumnType::Time => 14,
        crate::catalog::ColumnType::TimestampTz => 15,
        crate::catalog::ColumnType::Interval => 16,
        crate::catalog::ColumnType::MacAddr => 17,
    }
}

pub(crate) fn decode_column_type(tag: u8) -> Result<crate::catalog::ColumnType> {
    match tag {
        0 => Ok(crate::catalog::ColumnType::Int64),
        1 => Ok(crate::catalog::ColumnType::Float64),
        2 => Ok(crate::catalog::ColumnType::Text),
        3 => Ok(crate::catalog::ColumnType::Bool),
        4 => Ok(crate::catalog::ColumnType::Blob),
        5 => Ok(crate::catalog::ColumnType::Decimal),
        6 => Ok(crate::catalog::ColumnType::Uuid),
        7 => Ok(crate::catalog::ColumnType::Timestamp),
        8 => Ok(crate::catalog::ColumnType::Geometry),
        9 => Ok(crate::catalog::ColumnType::Geography),
        10 => Ok(crate::catalog::ColumnType::Enum),
        11 => Ok(crate::catalog::ColumnType::IpAddr),
        12 => Ok(crate::catalog::ColumnType::Cidr),
        13 => Ok(crate::catalog::ColumnType::Date),
        14 => Ok(crate::catalog::ColumnType::Time),
        15 => Ok(crate::catalog::ColumnType::TimestampTz),
        16 => Ok(crate::catalog::ColumnType::Interval),
        17 => Ok(crate::catalog::ColumnType::MacAddr),
        _ => Err(DbError::corruption("unknown column type tag")),
    }
}

pub(crate) fn decode_index_kind(tag: u8) -> Result<crate::catalog::IndexKind> {
    match tag {
        0 => Ok(crate::catalog::IndexKind::Btree),
        1 => Ok(crate::catalog::IndexKind::Trigram),
        2 => Ok(crate::catalog::IndexKind::Spatial),
        3 => Ok(crate::catalog::IndexKind::FullText),
        _ => Err(DbError::corruption("unknown index kind tag")),
    }
}

pub(crate) fn decode_trigger_kind(tag: u8) -> Result<crate::catalog::TriggerKind> {
    match tag {
        0 => Ok(crate::catalog::TriggerKind::After),
        1 => Ok(crate::catalog::TriggerKind::InsteadOf),
        _ => Err(DbError::corruption("unknown trigger kind tag")),
    }
}

pub(crate) fn decode_trigger_event(tag: u8) -> Result<crate::catalog::TriggerEvent> {
    match tag {
        0 => Ok(crate::catalog::TriggerEvent::Insert),
        1 => Ok(crate::catalog::TriggerEvent::Update),
        2 => Ok(crate::catalog::TriggerEvent::Delete),
        _ => Err(DbError::corruption("unknown trigger event tag")),
    }
}

pub(crate) fn decode_fk_action(tag: u8) -> Result<crate::catalog::ForeignKeyAction> {
    match tag {
        0 => Ok(crate::catalog::ForeignKeyAction::NoAction),
        1 => Ok(crate::catalog::ForeignKeyAction::Restrict),
        2 => Ok(crate::catalog::ForeignKeyAction::Cascade),
        3 => Ok(crate::catalog::ForeignKeyAction::SetNull),
        _ => Err(DbError::corruption("unknown foreign-key action tag")),
    }
}

pub(crate) fn decode_foreign_key(
    cursor: &mut Cursor<'_>,
) -> Result<crate::catalog::ForeignKeyConstraint> {
    Ok(crate::catalog::ForeignKeyConstraint {
        name: cursor.read_optional_string()?,
        columns: cursor.read_strings()?,
        referenced_table: cursor.read_string()?,
        referenced_columns: cursor.read_strings()?,
        on_delete: decode_fk_action(cursor.read_u8()?)?,
        on_update: decode_fk_action(cursor.read_u8()?)?,
    })
}

pub(crate) fn encode_row_id_locator_key(row_id: i64) -> u64 {
    (row_id as u64) ^ SIGNED_ROW_ID_BIAS
}

pub(crate) fn decode_row_id_locator_key(key: u64) -> i64 {
    (key ^ SIGNED_ROW_ID_BIAS) as i64
}

pub(crate) fn encode_row_locator(locator: RowLocatorV1) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(8);
    bytes.extend_from_slice(&locator.byte_offset.to_le_bytes());
    bytes.extend_from_slice(&locator.byte_len.to_le_bytes());
    bytes
}

pub(crate) fn encode_paged_row_locator(locator: RowLocatorV2) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(13);
    bytes.extend_from_slice(&locator.chunk_index.to_le_bytes());
    bytes.extend_from_slice(&locator.byte_offset.to_le_bytes());
    bytes.extend_from_slice(&locator.byte_len.to_le_bytes());
    bytes.push(if locator.is_overlay { 1 } else { 0 });
    bytes
}

pub(crate) fn decode_row_locator(bytes: &[u8]) -> Result<DecodedRowLocator> {
    match bytes.len() {
        8 => Ok(DecodedRowLocator::V1(RowLocatorV1 {
            byte_offset: u32::from_le_bytes(bytes[0..4].try_into().expect("row locator offset")),
            byte_len: u32::from_le_bytes(bytes[4..8].try_into().expect("row locator len")),
        })),
        12 => Ok(DecodedRowLocator::V2(RowLocatorV2 {
            chunk_index: u32::from_le_bytes(bytes[0..4].try_into().expect("row locator chunk")),
            byte_offset: u32::from_le_bytes(bytes[4..8].try_into().expect("row locator offset")),
            byte_len: u32::from_le_bytes(bytes[8..12].try_into().expect("row locator len")),
            is_overlay: false,
        })),
        13 => Ok(DecodedRowLocator::V2(RowLocatorV2 {
            chunk_index: u32::from_le_bytes(bytes[0..4].try_into().expect("row locator chunk")),
            byte_offset: u32::from_le_bytes(bytes[4..8].try_into().expect("row locator offset")),
            byte_len: u32::from_le_bytes(bytes[8..12].try_into().expect("row locator len")),
            is_overlay: bytes[12] != 0,
        })),
        _ => Err(DbError::corruption("row locator payload length is invalid")),
    }
}

pub(crate) fn decode_compressed_table_payload_lookup_entry(
    payload: Arc<Vec<u8>>,
) -> Result<DeferredCompressedLookupCacheEntry> {
    let mut cursor = Cursor::new(payload.as_slice());
    let magic = cursor.read_slice(TABLE_PAYLOAD_MAGIC.len())?;
    if magic != TABLE_PAYLOAD_MAGIC {
        return Err(DbError::corruption("table payload magic is invalid"));
    }
    let row_count = cursor.read_u32()? as usize;
    let mut row_locators = HashMap::with_capacity(row_count);
    for _ in 0..row_count {
        let row_id = cursor.read_i64()?;
        let (is_tombstone, row_bytes_len) = split_table_payload_row_len(cursor.read_u32()?);
        let row_bytes_offset = cursor.offset;
        let _ = cursor.read_slice(row_bytes_len)?;
        if is_tombstone {
            continue;
        }
        row_locators.insert(
            row_id,
            RowLocatorV1 {
                byte_offset: u32::try_from(row_bytes_offset)
                    .map_err(|_| DbError::constraint("row locator offset exceeds u32"))?,
                byte_len: u32::try_from(row_bytes_len)
                    .map_err(|_| DbError::constraint("row locator length exceeds u32"))?,
            },
        );
    }
    Ok(DeferredCompressedLookupCacheEntry {
        payload,
        row_locators,
    })
}

pub(crate) fn decode_row_by_locator_from_payload(
    payload: &[u8],
    row_id: i64,
    locator: RowLocatorV1,
) -> Result<StoredRow> {
    let start = locator.byte_offset as usize;
    let end = start
        .checked_add(locator.byte_len as usize)
        .ok_or_else(|| DbError::corruption("row locator exceeded payload length"))?;
    let row_bytes = payload
        .get(start..end)
        .ok_or_else(|| DbError::corruption("row locator exceeded payload length"))?;
    let row = Row::decode(row_bytes)?;
    Ok(StoredRow {
        row_id,
        values: row.into_values(),
    })
}

pub(crate) fn decode_projected_values_by_locator_from_payload<S: PageStore>(
    store: Option<&S>,
    payload: &[u8],
    locator: RowLocatorV1,
    projection_indexes: &[usize],
) -> Result<Vec<Value>> {
    let start = locator.byte_offset as usize;
    let end = start
        .checked_add(locator.byte_len as usize)
        .ok_or_else(|| DbError::corruption("row locator exceeded payload length"))?;
    let row_bytes = payload
        .get(start..end)
        .ok_or_else(|| DbError::corruption("row locator exceeded payload length"))?;
    Row::decode_projection_sorted_unique_with_overflow(row_bytes, store, projection_indexes)
}

impl EngineRuntime {
    pub(crate) fn decode_runtime_index_group_key(key: &[u8]) -> Option<Value> {
        let (tag, payload) = key.split_first()?;
        match *tag {
            0 if payload.is_empty() => Some(Value::Null),
            1 if payload.len() == 1 => match payload[0] {
                0 => Some(Value::Bool(false)),
                1 => Some(Value::Bool(true)),
                _ => None,
            },
            2 if payload.len() == 8 => {
                let mut bytes = [0_u8; 8];
                bytes.copy_from_slice(payload);
                let bits = u64::from_be_bytes(bytes) ^ 0x8000_0000_0000_0000;
                Some(Value::Int64(i64::from_be_bytes(bits.to_be_bytes())))
            }
            3 if payload.len() == 8 => {
                let mut bytes = [0_u8; 8];
                bytes.copy_from_slice(payload);
                let sortable = u64::from_be_bytes(bytes);
                let bits = if sortable & (1_u64 << 63) != 0 {
                    sortable ^ (1_u64 << 63)
                } else {
                    !sortable
                };
                Some(Value::Float64(f64::from_bits(bits)))
            }
            6 if payload.len() == 16 => {
                let mut bytes = [0_u8; 16];
                bytes.copy_from_slice(payload);
                Some(Value::Uuid(bytes))
            }
            7 => {
                let text = String::from_utf8(payload.to_vec()).ok()?;
                Some(Value::Text(text))
            }
            8 => Some(Value::Blob(payload.to_vec())),
            9 if payload.len() == 16 => {
                let mut enum_type_id = [0_u8; 8];
                enum_type_id.copy_from_slice(&payload[..8]);
                let mut label_id = [0_u8; 8];
                label_id.copy_from_slice(&payload[8..16]);
                Some(Value::Enum {
                    enum_type_id: u64::from_be_bytes(enum_type_id),
                    label_id: u64::from_be_bytes(label_id),
                })
            }
            10 if payload.len() == 17 => {
                let family = *payload.last()?;
                match family {
                    4 => {
                        let mut addr = [0_u8; 16];
                        addr[..4].copy_from_slice(&payload[12..16]);
                        Some(Value::IpAddr { family, addr })
                    }
                    6 => {
                        let mut addr = [0_u8; 16];
                        addr.copy_from_slice(&payload[..16]);
                        Some(Value::IpAddr { family, addr })
                    }
                    _ => None,
                }
            }
            11 if matches!(payload.len(), 6 | 18) => {
                let family = payload[0];
                let prefix_len = payload[1];
                if family != 4 && family != 6 {
                    return None;
                }
                let mut network = [0_u8; 16];
                if family == 4 {
                    if payload.len() != 6 {
                        return None;
                    }
                    network[..4].copy_from_slice(&payload[2..6]);
                } else {
                    network.copy_from_slice(&payload[2..18]);
                }
                Some(Value::Cidr {
                    family,
                    prefix_len,
                    network,
                })
            }
            12 if payload.len() == 4 => {
                let mut bytes = [0_u8; 4];
                bytes.copy_from_slice(payload);
                let raw = u32::from_be_bytes(bytes) ^ 0x8000_0000;
                Some(Value::DateDays(i32::from_be_bytes(raw.to_be_bytes())))
            }
            13 if payload.len() == 8 => {
                let mut bytes = [0_u8; 8];
                bytes.copy_from_slice(payload);
                let bits = u64::from_be_bytes(bytes) ^ 0x8000_0000_0000_0000;
                Some(Value::TimeMicros(i64::from_be_bytes(bits.to_be_bytes())))
            }
            14 if payload.len() == 8 => {
                let mut bytes = [0_u8; 8];
                bytes.copy_from_slice(payload);
                let bits = u64::from_be_bytes(bytes) ^ 0x8000_0000_0000_0000;
                Some(Value::TimestampTzMicros(i64::from_be_bytes(
                    bits.to_be_bytes(),
                )))
            }
            15 if payload.len() == 16 => {
                let mut months = [0_u8; 4];
                months.copy_from_slice(&payload[..4]);
                let mut days = [0_u8; 4];
                days.copy_from_slice(&payload[4..8]);
                let mut micros = [0_u8; 8];
                micros.copy_from_slice(&payload[8..16]);
                Some(Value::Interval {
                    months: {
                        let raw = u32::from_be_bytes(months) ^ 0x8000_0000;
                        i32::from_be_bytes(raw.to_be_bytes())
                    },
                    days: {
                        let raw = u32::from_be_bytes(days) ^ 0x8000_0000;
                        i32::from_be_bytes(raw.to_be_bytes())
                    },
                    micros: {
                        let bits = u64::from_be_bytes(micros) ^ 0x8000_0000_0000_0000;
                        i64::from_be_bytes(bits.to_be_bytes())
                    },
                })
            }
            16 if !payload.is_empty() => {
                let len = *payload.last()?;
                if len > 8 {
                    return None;
                }
                let len_usize = usize::from(len);
                if payload.len() != len_usize + 1 {
                    return None;
                }
                let mut bytes = [0_u8; 8];
                bytes[..len_usize].copy_from_slice(&payload[..len_usize]);
                Some(Value::MacAddr { len, bytes })
            }
            5 => None,
            _ => None,
        }
    }
}
