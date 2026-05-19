# futures_continuous_v1_l3_6_quality_report_contract

status: implemented_contract
project: MOEX Bot
artifact_class: external_pattern
format: json
schema_version: futures_continuous_v1_l3_6_quality_report.v1

purpose: Scoped quality report for PM L3-6 continuous futures v1 outputs produced by the L3-6 manifest flow.
producer: src/moex_data/futures/continuous_v1_l3_6_quality_report.py
consumer:
- PM L3-6 validation
- futures_continuous_v1_l3_6_manifest

path_pattern: ${MOEX_DATA_ROOT}/futures/quality/continuous_v1_l3_6/run_date={run_date}/quality_report.json
partitioning:
- run_date

required_fields:
- schema_version
- run_id
- run_date
- snapshot_date
- started_at
- finished_at
- scope
- row_counts
- checks
- quality_report_status
- blockers

required_checks:
- continuous_5m_row_count
- continuous_d1_row_count
- continuous_5m_primary_key_unique
- continuous_d1_primary_key_unique
- continuous_5m_duplicate_timestamps
- continuous_d1_duplicate_timestamps
- continuous_5m_ohlc_validity
- continuous_d1_ohlc_validity
- roll_policy_id
- adjustment_policy_id
- adjustment_factor
- continuous_5m_lineage_completeness
- continuous_d1_lineage_completeness
- usdrubf_identity_behavior
- no_silent_gap_bridging
- no_synthetic_replacement_contracts
- no_forbidden_or_noneligible_instruments
- no_w1
- no_materialized_15m_30m_1h_4h

validation_rules:
- schema_version must equal futures_continuous_v1_l3_6_quality_report.v1.
- scope must be derived from the L3-6 final eligibility artifact referenced by the same run manifest.
- continuous output reads must be scoped to selected continuous_v1_eligible families/secids from the L3-6 manifest flow, not by generic data-lake root glob over unrelated historical outputs.
- roll_policy_id must equal expiration_minus_1_trading_session_v1.
- adjustment_policy_id must equal unadjusted_v1.
- adjustment_factor must equal 1.0.
- continuous 5m and D1 row counts must be greater than zero.
- primary keys must be unique.
- duplicate timestamps are forbidden.
- OHLC fields must be non-null and valid.
- source lineage must be complete.
- USDRUBF must remain identity mapped when present.
- forbidden or non-eligible instruments must not appear in scoped outputs.
- W1 and materialized 15m/30m/1h/4h outputs must not be emitted by the L3-6 flow.
- quality_report_status must be pass only when all required checks are pass, explicit_gap, or not_applicable.

blocking_conditions:
- missing selected eligibility scope.
- missing scoped continuous 5m rows.
- missing scoped continuous D1 rows.
- any required check missing.
- any required check_status=fail.
- invalid roll or adjustment policy.
- lineage incompleteness.
- forbidden or non-eligible instrument in scoped outputs.
- USDRUBF identity violation.
