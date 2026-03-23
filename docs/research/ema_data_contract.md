# EMA Research OHLC Data Contract

## Purpose

This layer provides foundational, reusable data utilities for future EMA research by:
- Loading source OHLC CSV files from explicit paths.
- Applying a schema-driven source-to-canonical column mapping.
- Enforcing strict coercion and validation rules.
- Resampling canonical bars with consistent semantics.

This layer intentionally excludes strategy logic, backtesting, ranking, and optimization.

## Schema Contract

Schema file format is JSON with source column names and optional parsing settings:

Required keys:
- `timestamp`: source timestamp column
- `open`: source open column
- `high`: source high column
- `low`: source low column
- `close`: source close column

Optional keys:
- `volume`: source volume column
- `delimiter`: CSV delimiter (default `,`)
- `datetime_format`: explicit datetime parsing format passed to `pd.to_datetime`
- `timezone`: timezone to localize/convert parsed timestamps

The schema contract contains only source-column mapping and parsing options.

## Normalized Output Columns

After normalization, canonical DataFrame columns are:
- `ts`
- `open`
- `high`
- `low`
- `close`
- optional `volume`

Numeric columns are converted with `pd.to_numeric(errors="coerce")`.
Timestamp parsing uses `pd.to_datetime(..., errors="coerce")`.
Rows with nulls in required canonical fields (`ts`, `open`, `high`, `low`, `close`) are dropped.

## Resampling Semantics

Supported target timeframes:
- `5m`
- `30m`
- `1h`
- `4h`
- `1d`

Resampling is performed with:
- `label="right"`
- `closed="right"`

Aggregation rules:
- `open`: first
- `high`: max
- `low`: min
- `close`: last
- `volume`: sum (if present)

## Failure Semantics

The layer fails clearly when:
- Schema is invalid or missing required keys.
- Schema includes unsupported keys.
- CSV lacks mapped required source columns.
- Required canonical fields become fully invalid after coercion (empty dataset).
- Resampling is requested for an unsupported timeframe.
- Resampling produces zero rows.
