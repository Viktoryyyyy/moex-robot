# futures_continuous_d1_contract

status: design_contract
project: MOEX Bot
artifact_class: external_pattern
format: parquet
schema_version: futures_continuous_d1.v1

purpose: Unadjusted daily continuous futures OHLCV bars derived from futures_continuous_5m.v1.

producer: src/moex_data/futures/continuous_d1_builder.py
consumer:
- futures_continuous_quality_report
- later_futures_data_access_layer
- later_daily_feature_builders
- later_research_and_backtest_consumers

path_pattern: ${MOEX_DATA_ROOT}/futures/continuous_d1/roll_policy={roll_policy_id}/adjustment_policy={adjustment_policy_id}/family={family_code}/trade_date={trade_date}/part.parquet
partitioning:
- roll_policy_id
- adjustment_policy_id
- family_code
- trade_date
primary_key:
- continuous_symbol
- trade_date

required_fields:
- trade_date
- session_date
- continuous_symbol
- family_code
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
- schema_version
- ingest_ts

nullable_fields:
- none

status_fields:
- has_roll_boundary
- roll_policy_id
- adjustment_policy_id

lineage_policy:
- source_contracts must contain the ordered distinct source_contract values contributing to the D1 row.
- roll_map_id must reference the roll map id or ids used by the contributing 5m rows.
- if a trading session contains a roll boundary, has_roll_boundary must be true and source_contracts must contain both contributing contracts unless the roll boundary is session-aligned with no intraday overlap.

validation_rules:
- schema_version must equal futures_continuous_d1.v1.
- source dataset must be futures_continuous_5m with schema_version futures_continuous_5m.v1.
- roll_policy_id must equal expiration_minus_1_trading_session_v1.
- adjustment_policy_id must equal unadjusted_v1.
- adjustment_factor must equal 1.0.
- one D1 row must exist for each continuous_symbol/trade_date present in futures_continuous_5m.
- open must equal the first contributing 5m open by end timestamp.
- high must equal the max contributing 5m high.
- low must equal the min contributing 5m low.
- close must equal the last contributing 5m close by end timestamp.
- volume must equal the sum of contributing 5m volume.
- source_contracts must be non-empty.
- excluded instruments SiH7 and SiM7 must not appear in source_contracts.
- USDRUBF D1 rows must preserve source_contracts=[USDRUBF], continuous_symbol=USDRUBF, adjustment_factor=1.0, and has_roll_boundary=false.
- explicit partial-chain gaps must not be filled with synthetic D1 rows.

blocking_conditions:
- missing continuous 5m source rows for expected session.
- duplicate D1 primary key.
- invalid OHLC aggregation.
- missing source_contracts lineage.
- excluded instrument included.
- USDRUBF identity behavior violated.
- adjustment_factor not equal to 1.0.
