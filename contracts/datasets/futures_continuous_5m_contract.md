# futures_continuous_5m_contract

status: design_contract
project: MOEX Bot
artifact_class: external_pattern
format: parquet
schema_version: futures_continuous_5m.v1

purpose: Unadjusted 5-minute continuous futures bars built from futures_raw_5m according to futures_continuous_roll_map.v1.

producer: src/moex_data/futures/continuous_series_builder.py
consumer:
- futures_continuous_d1_builder
- futures_continuous_quality_report
- later_futures_data_access_layer
- later_research_and_backtest_consumers

path_pattern: ${MOEX_DATA_ROOT}/futures/continuous_5m/roll_policy={roll_policy_id}/adjustment_policy={adjustment_policy_id}/family={family_code}/trade_date={trade_date}/part.parquet
partitioning:
- roll_policy_id
- adjustment_policy_id
- family_code
- trade_date
primary_key:
- continuous_symbol
- trade_date
- end

required_fields:
- trade_date
- end
- session_date
- continuous_symbol
- family_code
- source_secid
- source_contract
- open
- high
- low
- close
- volume
- roll_policy_id
- adjustment_policy_id
- adjustment_factor
- is_roll_boundary
- roll_map_id
- schema_version
- ingest_ts

nullable_fields:
- none

status_fields:
- is_roll_boundary
- roll_policy_id
- adjustment_policy_id

lineage_policy:
- source_secid must equal the raw futures_raw_5m secid used for the row.
- source_contract must equal the human contract code represented by source_secid for the row.
- roll_map_id must reference exactly one futures_continuous_roll_map row whose valid window contains session_date.
- raw source partition must remain independently readable and must not be modified by the continuous builder.

validation_rules:
- schema_version must equal futures_continuous_5m.v1.
- source dataset must be futures_raw_5m with schema_version futures_raw_5m.v1.
- roll_policy_id must equal expiration_minus_1_trading_session_v1.
- adjustment_policy_id must equal unadjusted_v1.
- adjustment_factor must equal 1.0 for every row.
- OHLC fields must be non-null.
- high must be greater than or equal to low.
- open and close must be within low/high inclusive.
- primary key must be unique.
- duplicate timestamps per continuous_symbol/trade_date/end are forbidden.
- each row must map to exactly one source_contract and source_secid.
- each output row must have lineage completeness: source_secid, source_contract, roll_map_id, roll_policy_id, adjustment_policy_id, adjustment_factor.
- is_roll_boundary must be true only on rows whose session_date is the first valid_from_session for a new source contract after a roll.
- USDRUBF rows must preserve source_secid=USDRUBF, source_contract=USDRUBF, continuous_symbol=USDRUBF, adjustment_factor=1.0, and is_roll_boundary=false.
- excluded instruments SiH7 and SiM7 must not appear as source_secid or source_contract.
- explicit partial-chain gaps must result in no silently bridged rows.

blocking_conditions:
- missing roll map.
- missing raw source partition.
- ambiguous active source contract.
- duplicate timestamps.
- missing source lineage.
- adjustment_factor not equal to 1.0.
- excluded instrument included.
- USDRUBF identity behavior violated.
