# futures_continuous_roll_map_contract

status: design_contract
project: MOEX Bot
artifact_class: external_pattern
format: parquet
schema_version: futures_continuous_roll_map.v1

purpose: Versioned roll-window map that selects the active source contract for each continuous futures family under continuous v1.

producer: src/moex_data/futures/continuous_roll_map_builder.py
consumer:
- src/moex_data/futures/continuous_series_builder.py
- futures_continuous_quality_report
- futures_data_lake_pm_review

path_pattern: ${MOEX_DATA_ROOT}/futures/continuous/roll_map/snapshot_date={snapshot_date}/roll_policy={roll_policy_id}/futures_continuous_roll_map.parquet
partitioning:
- snapshot_date
- roll_policy_id
primary_key:
- roll_map_id
- snapshot_date
- family_code
- source_secid
- valid_from_session

required_fields:
- roll_map_id
- snapshot_date
- family_code
- continuous_symbol
- board
- source_secid
- source_contract_code
- next_secid
- next_contract_code
- is_perpetual
- roll_required
- roll_anchor_date
- roll_date
- valid_from_session
- valid_through_session
- calendar_status
- calendar_source
- roll_policy_id
- adjustment_policy_id
- adjustment_factor
- decision_source
- roll_status
- schema_version
- review_notes

nullable_fields:
- next_secid
- next_contract_code
- roll_anchor_date
- roll_date
- valid_through_session
- review_notes

status_fields:
- calendar_status
- decision_source
- roll_status
- is_perpetual
- roll_required

allowed_values:
  roll_policy_id:
  - expiration_minus_1_trading_session_v1
  adjustment_policy_id:
  - unadjusted_v1
  calendar_status:
  - canonical_apim_futures_xml
  decision_source:
  - registry_expiration_date
  - registry_last_trade_date_fallback
  - manual_reviewed_override
  - unresolved
  roll_status:
  - active_window
  - perpetual_identity
  - explicit_partial_chain_gap
  - blocked_unresolved_anchor
  - blocked_missing_next_contract
  - blocked_calendar

validation_rules:
- schema_version must equal futures_continuous_roll_map.v1.
- adjustment_factor must equal 1.0 for every row.
- calendar_status must equal canonical_apim_futures_xml for all buildable rows.
- roll_policy_id must equal expiration_minus_1_trading_session_v1.
- adjustment_policy_id must equal unadjusted_v1.
- decision_source is mandatory and must be one of the allowed enum values.
- ordinary expiring contracts require roll_anchor_date and roll_date.
- roll_anchor_date must come from validated expiration_date, validated last_trade_date fallback, or manual reviewed override.
- roll_date must be the trading session immediately preceding roll_anchor_date in the canonical MOEX futures session sequence.
- valid_from_session and valid_through_session define non-overlapping active source windows by family_code and continuous_symbol.
- overlapping roll-map windows are forbidden.
- missing next contract must be explicit via roll_status=blocked_missing_next_contract or explicit_partial_chain_gap.
- excluded contracts SiH7 and SiM7 must not be used as source_secid or next_secid in Slice 1 continuous outputs.
- USDRUBF must have is_perpetual=true, roll_required=false, roll_status=perpetual_identity, source_secid=USDRUBF, source_contract_code=USDRUBF, continuous_symbol=USDRUBF, next_secid null, next_contract_code null, roll_anchor_date null, and roll_date null.

blocking_conditions:
- missing normalized registry.
- missing expiration anchor for ordinary expiring source contract.
- unresolved decision_source for buildable ordinary roll row.
- invalid calendar_status.
- overlapping roll-map windows.
- ambiguous active source contract for a family/session.
- missing next contract not represented as explicit_partial_chain_gap or blocked_missing_next_contract.
- USDRUBF identity validation fails.
- adjustment_factor is not 1.0.
- unexpected included instruments appear.
- excluded instruments SiH7 or SiM7 appear.
