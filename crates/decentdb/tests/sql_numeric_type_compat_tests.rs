use decentdb::{Db, DbConfig, Value};

fn db() -> Db {
    Db::open_or_create(":memory:", DbConfig::default()).unwrap()
}

fn values(db: &Db, sql: &str) -> Vec<Value> {
    db.execute(sql).unwrap().rows()[0].values().to_vec()
}

#[test]
fn issue_67_decimal_arithmetic_is_exact() {
    let db = db();
    assert_eq!(
        values(
            &db,
            "SELECT CAST(0.1 AS NUMERIC) + CAST(0.2 AS NUMERIC) = CAST(0.3 AS NUMERIC)"
        ),
        vec![Value::Bool(true)]
    );
    assert_eq!(
        values(
            &db,
            "SELECT
        CAST('1.20' AS NUMERIC) + CAST('0.003' AS NUMERIC),
        CAST('1.20' AS NUMERIC) - CAST('0.003' AS NUMERIC),
        CAST('1.20' AS NUMERIC) * CAST('0.003' AS NUMERIC),
        CAST('1' AS NUMERIC) / CAST('3' AS NUMERIC),
        CAST('-1' AS NUMERIC) / CAST('3' AS NUMERIC),
        CAST('5.5' AS NUMERIC) % CAST('2' AS NUMERIC)"
        ),
        vec![
            Value::Decimal {
                scaled: 1203,
                scale: 3
            },
            Value::Decimal {
                scaled: 1197,
                scale: 3
            },
            Value::Decimal {
                scaled: 36,
                scale: 4
            },
            Value::Decimal {
                scaled: 333333,
                scale: 6
            },
            Value::Decimal {
                scaled: -333333,
                scale: 6
            },
            Value::Decimal {
                scaled: 15,
                scale: 1
            },
        ]
    );
}

#[test]
fn decimal_mixed_operands_negation_and_rounding() {
    let db = db();
    assert_eq!(
        values(
            &db,
            "SELECT
        CAST('1.25' AS NUMERIC) + 2,
        2 - CAST('1.25' AS NUMERIC),
        -CAST('1.25' AS NUMERIC),
        CAST('1' AS NUMERIC) / CAST('128' AS NUMERIC),
        CAST('-1' AS NUMERIC) / CAST('128' AS NUMERIC),
        CAST('1.25' AS NUMERIC) + CAST(0.5 AS FLOAT64)"
        ),
        vec![
            Value::Decimal {
                scaled: 325,
                scale: 2
            },
            Value::Decimal {
                scaled: 75,
                scale: 2
            },
            Value::Decimal {
                scaled: -125,
                scale: 2
            },
            Value::Decimal {
                scaled: 7813,
                scale: 6
            },
            Value::Decimal {
                scaled: -7813,
                scale: 6
            },
            Value::Float64(1.75),
        ]
    );
    assert_eq!(values(&db, "SELECT CAST('1.25' AS NUMERIC) + NULL, CAST('1' AS NUMERIC) / 0, CAST('1' AS NUMERIC) % 0"), vec![Value::Null; 3]);
}

#[test]
fn decimal_overflow_is_a_sql_error() {
    let db = db();
    for sql in [
        "SELECT CAST('9223372036854775807' AS NUMERIC) + 1",
        "SELECT CAST('9223372036854775807' AS NUMERIC) * 2",
        "SELECT -CAST('-9223372036854775808' AS NUMERIC)",
    ] {
        assert!(db
            .execute(sql)
            .unwrap_err()
            .to_string()
            .contains("DECIMAL arithmetic overflow"));
    }
    // Normalize before narrowing the coefficient: the product's intermediate
    // exceeds i64, while the exact normalized result fits.
    assert_eq!(
        values(
            &db,
            "SELECT CAST('1000000000.1' AS NUMERIC) * CAST('1000000000' AS NUMERIC)"
        ),
        vec![Value::Decimal {
            scaled: 1_000_000_000_100_000_000,
            scale: 0
        }]
    );
}

#[test]
fn decimal_arithmetic_in_grouped_queries_and_updates() {
    let db = db();
    db.execute("CREATE TABLE amounts (id INT PRIMARY KEY, amount NUMERIC)")
        .unwrap();
    db.execute("INSERT INTO amounts VALUES (1, '1.25'), (2, '2.50')")
        .unwrap();
    db.execute("UPDATE amounts SET amount = amount + CAST('0.10' AS NUMERIC)")
        .unwrap();
    assert_eq!(
        values(
            &db,
            "SELECT -MIN(amount), MAX(amount) - MIN(amount) FROM amounts"
        ),
        vec![
            Value::Decimal {
                scaled: -135,
                scale: 2
            },
            Value::Decimal {
                scaled: 125,
                scale: 2
            }
        ]
    );
}

#[test]
fn issue_62_cast_stays_fixed_and_timestamp_epoch_has_timezone() {
    let db = db();
    assert_eq!(values(&db, "SELECT CAST(CAST(10.2 AS DECIMAL(10,2)) AS TEXT), TO_TIMESTAMP(1711929600123456 / 1000000.0)"), vec![Value::Text("10.2".to_string()), Value::TimestampTzMicros(1_711_929_600_123_456)]);
    assert_eq!(values(&db, "SELECT TO_TIMESTAMP(CAST('1711929600.123456' AS NUMERIC)), TO_TIMESTAMP(CAST('-0.0000005' AS NUMERIC))"), vec![Value::TimestampTzMicros(1_711_929_600_123_456), Value::TimestampTzMicros(-1)]);
    for parameter in [
        Value::Float64(f64::NAN),
        Value::Float64(f64::INFINITY),
        Value::Float64(1e100),
        Value::Int64(i64::MAX),
    ] {
        assert!(db
            .execute_with_params("SELECT TO_TIMESTAMP($1)", &[parameter])
            .unwrap_err()
            .to_string()
            .contains("out of range"));
    }
}

#[test]
fn issue_65_pg_typeof_reports_native_sql_types() {
    let db = db();
    assert_eq!(
        values(
            &db,
            "SELECT
        PG_TYPEOF(TO_TIMESTAMP(1711929600123456 / 1000000.0)),
        pg_catalog.pg_typeof(CAST('0.1' AS NUMERIC)),
        PG_TYPEOF(CAST(NULL AS NUMERIC)),
        PG_TYPEOF(CAST(NULL AS TIMESTAMP)),
        PG_TYPEOF(CAST(NULL AS TIMESTAMPTZ)),
        PG_TYPEOF(NULL), PG_TYPEOF(1), PG_TYPEOF('text'), PG_TYPEOF(TRUE)"
        ),
        [
            "timestamp with time zone",
            "numeric",
            "numeric",
            "timestamp without time zone",
            "timestamp with time zone",
            "unknown",
            "bigint",
            "text",
            "boolean"
        ]
        .map(|value| Value::Text(value.to_string()))
    );
    for sql in ["SELECT PG_TYPEOF()", "SELECT PG_TYPEOF(1, 2)"] {
        assert!(db
            .execute(sql)
            .unwrap_err()
            .to_string()
            .contains("expects 1 argument"));
    }
}

#[test]
fn pg_typeof_preserves_nullable_column_and_parameter_types() {
    let db = db();
    db.execute("CREATE TABLE typed (amount NUMERIC, stamp TIMESTAMPTZ)")
        .unwrap();
    db.execute("INSERT INTO typed VALUES (NULL, NULL)").unwrap();
    for sql in [
        "SELECT PG_TYPEOF(MIN(t.amount)), PG_TYPEOF(SUM(t.amount)) FROM typed AS t HAVING PG_TYPEOF(MIN(t.amount)) = 'numeric'",
        "SELECT PG_TYPEOF(MIN(amount)), PG_TYPEOF(SUM(amount)) FROM typed WHERE 1 = 0",
    ] {
        assert_eq!(values(&db, sql), vec![Value::Text("numeric".into()), Value::Text("double precision".into())]);
    }
    assert_eq!(
        values(
            &db,
            "SELECT PG_TYPEOF(MIN(amount)), PG_TYPEOF(SUM(amount)) FROM typed"
        ),
        vec![
            Value::Text("numeric".into()),
            Value::Text("double precision".into())
        ]
    );
    assert_eq!(
        values(
            &db,
            "SELECT PG_TYPEOF(NULL || 'text'), PG_TYPEOF(NOT CAST(NULL AS BOOL))"
        ),
        vec![Value::Text("text".into()), Value::Text("boolean".into())]
    );
    assert_eq!(
        values(
            &db,
            "SELECT PG_TYPEOF(t.amount), PG_TYPEOF(t.stamp) FROM typed AS t"
        ),
        vec![
            Value::Text("numeric".into()),
            Value::Text("timestamp with time zone".into())
        ]
    );
    assert_eq!(
        db.execute_with_params(
            "SELECT PG_TYPEOF($1), PG_TYPEOF(CAST($2 AS NUMERIC))",
            &[Value::Float64(1.25), Value::Null]
        )
        .unwrap()
        .rows()[0]
            .values(),
        &[
            Value::Text("double precision".into()),
            Value::Text("numeric".into())
        ]
    );
}
