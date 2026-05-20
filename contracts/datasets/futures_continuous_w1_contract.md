# futures_continuous_w1_contract

status: design_contract
project: MOEX Bot
artifact_class: external_pattern
format: parquet
schema_version: futures_continuous_w1.v1

purpose: Unadjusted weekly continuous futures OHLCV bars derived only from accepted futures_continuous_d1.v1.

producer: src/moex_data/futures/continuous_w1_builder.py
consumer:
- futures_continuous_w1_quality_report
- futures_continuous_bars_access
- later_weekly_feature_builders
- later_research_and_backtest_consumers

path_pattern: ${MOEX_DATA_ROOT}/futures/continuous_w1/roll_policy={roll_policy_id}/adjustment_policy={adjustment_policy_id}/family={family_code}/week_start={week_start}/part.parquet
partitioning:
- roll_policy_id
- adjustment_policy_id
- family_code
- week_start
primary_key:
- continuous_symbol
- week_start

week_boundary_semantics:
- ISO-8601 week.
- week_start is Monday derived from D1 trade_date.
- week_end is Sunday derived from week_start plus six calendar days.
- A W1 row may contain fewer than five D1 rows because holidays and non-trading days are preserved as absence, not filled.

required_fields:
- week_start
- week_end
- iso_year
- iso_week
- continuous_symbol
- family_code
- source_trade_dates
- source_contracts
- open
- high
- low
- close
- volume
- roll_policy_id
- adjustment_policy_id
- adjustment_factor
- has_roll_boundary
- roll_map_id
- source_d1_row_count
- schema_version
- ingest_ts

nullable_fields:
- none

lineage_policy:
- source dataset must be futures_continuous_d1.v1 only.
- source_trade_dates must contain ordered distinct D1 trade_date values contributing to the W1 row.
- source_contracts must contain ordered distinct D1 source_contracts values contributing to the W1 row.
- roll_map_id must preserve ordered distinct D1 roll_map_id values contributing to the W1 row.
- source_d1_row_count must equal the number of contributing D1 rows.

validation_rules:
- schema_version must equal futures_continuous_w1.v1.
- source dataset must be futures_continuous_d1 with schema_version futures_continuous_d1.v1.
- roll_policy_id must equal expiration_minus_1_trading_session_v1.
- adjustment_policy_id must equal unadjusted_v1.
- adjustment_factor must equal 1.0.
- one W1 row must exist for each continuous_symbol/ISO-week group present in the explicit D1 source range.
- open must equal the first contributing D1 open by trade_date.
- high must equal the max contributing D1 high.
- low must equal the min contributing D1 low.
- close must equal the last contributing D1 close by trade_date.
- volume must equal the sum of contributing D1 volume.
- source_trade_dates must be non-empty.
- source_contracts must be non-empty.

blocking_conditions:
- missing D1 source for the explicit family/date range.
- invalid D1 source schema.
- wrong roll or adjustment policy.
- duplicate W1 primary key.
- invalid W1 OHLC aggregation.
- missing lineage fields.
- raw 5m source usage.
- FUTOI join usage.
- implicit latest-file autodetect.
