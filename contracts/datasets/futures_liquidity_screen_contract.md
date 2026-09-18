# futures_liquidity_screen_contract

status: implemented_contract
project: MOEX Bot
artifact_class: external_pattern
format: parquet
schema_version: futures_liquidity_screen.v1

purpose: Liquidity screen for selecting MOEX futures families/instruments for initial data lake acquisition.
producer: futures_liquidity_screen_builder
consumer:
- futures_slice1_universe_selector
- futures_data_lake_pm_review

path_pattern: ${MOEX_DATA_ROOT}/futures/screens/liquidity/snapshot_date={snapshot_date}/futures_liquidity_screen.parquet
primary_key:
- liquidity_screen_id
- snapshot_date
- board
- secid

required_fields:
- liquidity_screen_id
- snapshot_date
- board
- secid
- family_code
- asset_class
- screen_from
- screen_till
- liquidity_status
- schema_version

nullable_fields:
- median_daily_volume
- median_daily_value
- median_daily_trades
- active_days
- missing_days
- threshold_profile_id
- review_notes

status_fields:
- liquidity_status
- validation_status
- review_status

validation_rules:
- threshold_profile_id must match futures_liquidity_screen_thresholds_config when thresholds are applied.
- liquidity_status must be pass, fail, not_checked, or review_required.
- selected Slice 1 instruments must pass or be manually reviewed.

blocking_conditions:
- duplicate primary key.
- selected instrument has fail or not_checked liquidity_status without explicit review.

historical_tradestats_input:
- Read /iss/datashop/algopack/fo/tradestats/{SECID}.json through authenticated APIM with explicit screen_from/screen_till. No general-market or unauthenticated ISS fallback supplies historical metrics.
- Validate a named tradestats or data block, unambiguous SECID/date columns, and every row's requested instrument and date bounds before aggregation. Malformed/error/mixed/foreign data are rejected, not filtered into apparent success.
- Exhaust pagination before returning fetch_status=completed. With a cursor, INDEX, TOTAL, PAGESIZE and row counts must agree; without one, continue by returned row count until a valid empty page. A short page alone is not EOF.
- Repeated/overlapping row identities, non-advancing or malformed cursors, schema/cursor-total changes, an exhausted page guard and later-page failure discard the partial result. No partial volume/value/trade sums are admitted.
- source_endpoint_url is the actual route; trade_stats_rows, daily_rows and first/last_available_date describe only accepted observations. An exhausted request is not proof of every trading session or PIT history.
- Sparse legitimate observations retain existing numerical/threshold rules. This change does not fabricate dates, change formulas/universe/storage, or rewrite earlier reports.
