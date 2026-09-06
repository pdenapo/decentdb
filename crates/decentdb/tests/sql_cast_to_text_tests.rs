//! CAST(... AS TEXT) coverage for types that should stringify.

use decentdb::{Db, DbConfig, QueryResult, Value};

fn mem_db() -> Db {
    Db::open_or_create(":memory:", DbConfig::default()).unwrap()
}

fn exec(db: &Db, sql: &str) -> QueryResult {
    db.execute(sql).unwrap()
}

fn text(db: &Db, sql: &str) -> String {
    match &exec(db, sql).rows()[0].values()[0] {
        Value::Text(s) => s.clone(),
        other => panic!("expected TEXT, got {other:?} for SQL: {sql}"),
    }
}

#[test]
fn cast_decimal_to_text() {
    let db = mem_db();
    /* Note: in Postgresql 18.6 this gives 10.20, change it would requiere to change the function value_to_text */
    assert_eq!(
        text(&db, "SELECT CAST(CAST(10.2 AS DECIMAL(10,2)) AS TEXT)"),
        "10.2"
    );
    assert_eq!(
        text(&db, "SELECT CAST(CAST(19.99 AS DECIMAL(10,2)) AS TEXT)"),
        "19.99"
    );
}

#[test]
fn cast_uuid_to_text() {
    let db = mem_db();
    let s = text(
        &db,
        "SELECT CAST(CAST('550e8400-e29b-41d4-a716-446655440000' AS UUID) AS TEXT)",
    );
    assert_eq!(s, "550e8400-e29b-41d4-a716-446655440000");
}

#[test]
fn cast_timestamp_to_text() {
    let db = mem_db();
    let s = text(
        &db,
        "SELECT CAST(CAST('2024-03-15 14:30:00' AS TIMESTAMP) AS TEXT)",
    );
    assert_eq!(s, "2024-03-15 14:30:00");
}

#[test]
fn cast_basic_types_to_text_still_work() {
    let db = mem_db();
    assert_eq!(text(&db, "SELECT CAST(42 AS TEXT)"), "42");
    assert_eq!(text(&db, "SELECT CAST(true AS TEXT)"), "true");
    assert_eq!(text(&db, "SELECT CAST(1.5 AS TEXT)"), "1.5");
}

#[test]
fn cast_null_to_text() {
    let db = mem_db();
    let r = exec(&db, "SELECT CAST(NULL AS TEXT)");
    assert_eq!(r.rows()[0].values()[0], Value::Null);
}
