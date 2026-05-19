# futures_continuous_v1_l3_6_manifest_contract

status: implemented_contract
project: MOEX Bot
artifact_class: external_pattern
format: json
schema_version: futures_continuous_v1_l3_6_manifest.v1

purpose: Run manifest for PM L3-6 continuous futures v1 over the RFUD included universe.
producer: src/moex_data/futures/continuous_v1_l3_6_runner_v2.py
consumer:
- PM L3-6 validation
- later futures data access validation

path_pattern: ${MOEX_DATA_ROOT}/futures/runs/continuous_v1_l3_6/run_date={run_date}/manifest.json
partitioning:
- run_date

required_fields:
- schema_version
- run_id
- run_date
- snapshot_date
- started_at
- finished_at
- selection_mode
- roll_policy_id
- adjustment_policy_id
- adjustment_factor
- input_eligibility_snapshots
- output_eligibility
- eligible_summary
- expiration_map
- roll_map
- access_api_smoke
- output_artifacts
- preservation_checks
- builder_result_verdict

validation_rules:
- schema_version must equal futures_continuous_v1_l3_6_manifest.v1.
- roll_policy_id must equal expiration_minus_1_trading_session_v1.
- adjustment_policy_id must equal unadjusted_v1.
- adjustment_factor must equal 1.0.
- eligible_summary.rows must be greater than zero.
- access_api_smoke.status must be pass.
- preservation_checks must confirm raw 5m, FUTOI, and raw D1 partitions were not modified.

blocking_conditions:
- zero eligible instruments after merged gates.
- missing expiration map.
- missing roll map.
- missing continuous 5m output.
- missing continuous D1 output.
- access API smoke failure.
