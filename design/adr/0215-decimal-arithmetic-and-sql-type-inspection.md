# ADR 0215: Decimal Arithmetic and SQL Type Inspection

**Date:** 2026-10-13
**Status:** Accepted

## Context

Issues #67, #62, and #65 expose missing decimal arithmetic, raw timestamp CLI
output, and missing SQL type inspection. ADRs 0072 and 0091 already require
checked fixed-point arithmetic. TIMESTAMPTZ already has a distinct runtime,
storage, and C ABI representation under ADR 0160.

## Decision

- Decimal arithmetic uses checked integer intermediates and the existing
  signed 64-bit coefficient plus runtime scale. Addition/subtraction align
  scales; multiplication adds scales; results normalize trailing zeros.
- Division rounds ties away from zero at the larger operand scale, with at
  least six fractional digits. Division/remainder by zero propagate NULL,
  consistent with existing numeric operators. Unrepresentable results and
  intermediate overflow return SQL errors. Integer/decimal operands remain
  exact; an explicit FLOAT64 operand promotes the operation to FLOAT64.
- Unary negation supports DECIMAL with overflow checks.
- Existing fractional SQL literals retain FLOAT64 typing. Exact arithmetic uses
  explicit DECIMAL/NUMERIC operands; default literal typing is a separate
  compatibility decision.
- TO_TIMESTAMP returns the existing TIMESTAMPTZ value, including numeric epoch
  and text/format overloads. Epoch conversion preserves microseconds and rejects
  non-finite or out-of-range input. CLI timestamps render readable dates;
  TIMESTAMPTZ output explicitly identifies UTC.
- PG_TYPEOF returns TEXT containing the SQL type name. It preserves declared
  types for casts and catalog columns, including NULL values; untyped NULL is
  `unknown`. This is a documented PostgreSQL compatibility subset, without
  introducing PostgreSQL OIDs or a REGTYPE storage type.
- Coverage and sanitizer instrumentation retain indexed-seek correctness and
  plan assertions but do not enforce ordinary wall-clock performance budgets.
  Uninstrumented builds retain the performance gates.

No persisted layout, format version, C ABI tag, or locking boundary changes.

## Validation

SQL regressions cover scale alignment, rounding, zero divisors, overflow,
mixed operands, unary negation, typed NULLs, parameter/column types, timestamp
precision, and CLI/REPL output. Binding smoke tests inspect the existing
decimal and TIMESTAMPTZ tags. Indexed-seek tests run both normally and under
coverage instrumentation.
