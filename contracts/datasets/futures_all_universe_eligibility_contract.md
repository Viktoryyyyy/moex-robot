# futures_all_universe_eligibility_contract

status: implemented_contract
project: MOEX Bot
contract_id: futures_all_universe_eligibility.v1
contract_version: v1
artifact_class: eligibility_contract
format: markdown
schema_version: futures_all_universe_eligibility.v1

purpose: Freeze controlled all-futures universe eligibility and PM L3-2 first executable selection. Registry discovery creates candidates only; final included status requires mandatory downstream checks.

implementation_status:
- PM L3-2 contract/config delta accepted inside first executable cycle
- bounded first executable raw 5m slice allowed
- no all-futures all-history rollout
- no daily unattended refresh implementation
- no continuous build execution
- no W1 implementation
- no strategy, research, runtime trading, Telegram, or notification changes

related_contracts:
- contracts/datasets/futures_all_universe_snapshot_contract.md
- contracts/datasets/futures_raw_5m_loader_manifest_contract.md
- contracts/datasets/futures_futoi_5m_raw_loader_manifest_contract.md
- contracts/datasets/futures_derived_d1_ohlcv_manifest_contract.md
- contracts/datasets/futures_daily_data_refresh_manifest_contract.md
- contracts/datasets/futures_continuous_5m_contract.md
- contracts/datasets/futures_continuous_roll_map_contract.md
- contracts/datasets/futures_continuous_d1_contract.md
- contracts/datasets/moex_futures_session_calendar_contract.md

universe_definition:
- v1 universe means current available MOEX futures discovered from a canonical registry snapshot.
- Registry discovery is candidate-only and is never sufficient for included status.
- First supported expansion board is RFUD.
- Unsupported boards are deferred with unsupported_board_pending_review unless a structural exclusion reason applies.
- Historical expired universe and full historical backfill are outside PM L3-2 first executable slice.

classification_statuses:
- included
- deferred
- excluded

classification_policy:
- Every discovered instrument must be classified as exactly one of included, deferred, or excluded.
- included requires all mandatory checks required by the active stage to pass.
- missing, unresolved, partial, failed, ambiguous, or uncontracted mandatory checks result in deferred.
- structural invalidity results in excluded.
- deferred is non-final and must preserve reason detail.
- excluded is reserved for structural invalidity, explicit PM exclusion, corrupted source payload, non-future rows, duplicate identity, unsupported engine/market, or out-of-current-universe rows.

classification_record_required_fields:
- eligibility_snapshot_id
- registry_snapshot_id
- eligibility_snapshot_date
- registry_snapshot_date
- secid
- short_code
- family_code
- board
- engine
- market
- instrument_type
- classification_status
- classification_reason
- deferral_reason
- exclusion_reason
- registry_source
- identity_check_status
- board_check_status
- family_mapping_status
- raw_5m_check_status
- futoi_check_status
- liquidity_check_status
- history_depth_check_status
- expiration_policy_status
- perpetual_policy_status
- calendar_quality_status
- continuous_eligibility_status
- registry_only_eligible
- raw_5m_eligible
- futoi_eligible
- raw_d1_eligible
- continuous_v1_eligible
- access_api_eligible
- w1_eligible
- w1_status
- future_no_trade_not_yet_loadable
- expired_no_current_load_scope
- backfill_selection_status
- backfill_selection_reason
- source_scope
- notes
- schema_version

eligibility_flag_policy:
- registry_only_eligible=true means visible in registry/reporting only, not loadable.
- raw_5m_eligible=true drives raw 5m backfill selection.
- futoi_eligible=true drives FUTOI backfill selection.
- raw_d1_eligible=true drives raw D1 derivation selection.
- continuous_v1_eligible=true marks candidate only; it must not start continuous build.
- access_api_eligible=true only if at least one supported access timeframe is available.
- w1_eligible must remain false while W1 is a known program gap.
- w1_status must report known_gap, not silent failure.
- future_no_trade_not_yet_loadable=true for valid future instruments with no current historical bars yet.
- expired_no_current_load_scope=true for expired instruments outside current Slice 3 v1 load scope.
- backfill_selection_status and backfill_selection_reason must explain whether the instrument was selected, deferred, skipped, or excluded for the active stage.

raw_5m_policy:
- Raw 5m availability is mandatory for included status when dataset_stage=raw_5m.
- Probe must verify availability, schema validity, timestamp coverage, and calendar compatibility.
- Missing raw 5m data is deferred with raw_5m_unavailable.
- Failed probe execution is deferred with raw_5m_probe_failed.
- Invalid schema is deferred with raw_5m_schema_invalid.
- Calendar mismatch is deferred with raw_5m_calendar_mismatch.
- raw_5m_eligible=true is the only eligibility flag that may drive PM L3-2 first executable raw 5m backfill.
- Raw 5m checks must not start FUTOI, D1, continuous, daily refresh, or W1 construction.

futoi_policy:
- FUTOI availability is mandatory for later canonical included status where dataset_stage=futoi.
- Missing FUTOI is deferred with futoi_unavailable.
- Unresolved FUTOI identity or source lookup is deferred with futoi_unresolved.
- FUTOI backfill is out of scope for PM L3-2 first executable raw 5m slice.

raw_d1_policy:
- raw_d1_eligible=true drives later raw D1 derivation selection.
- D1 derivation is out of scope for PM L3-2 first executable raw 5m slice.

continuous_eligibility_policy:
- Existing Continuous v1 must be preserved.
- Required values remain roll_policy_id=expiration_minus_1_trading_session_v1, adjustment_policy_id=unadjusted_v1, adjustment_factor=1.0.
- continuous_v1_eligible=true is candidate-only in PM L3-2 and must not trigger continuous build.
- No silent gap bridging, inferred replacement contracts, or change to accepted roll/adjustment policy is allowed.

w1_policy:
- W1 remains a known program gap.
- W1 must be reported explicitly as known_gap.
- PM L3-2 must not implement W1 build, derivation, or access.

access_api_policy:
- Current access API supported timeframes are 5m, 15m, 30m, 1h, 4h, and D1.
- access_api_eligible=true only if at least one supported access timeframe is available.
- W1 is not included in access_api_eligible.

chunking_policy:
- Chunking uses family_code × date_range × dataset_stage.
- Date-range chunk size is configured in configs/datasets/futures_all_universe_eligibility_config.json.
- Secid-level failure isolation must exist inside each family chunk.
- Failed secid may create retry child chunks at secid × date_range × dataset_stage.

retry_resume_manifest_policy:
- Every chunk manifest must include chunk_id, input_eligibility_snapshot_id, dataset_stage, family_code, secid_list, date_from, date_till, attempt_number, previous_attempt_id, status, started_at, finished_at, failed_secid, deferred_secid, skipped_secid, output_partitions, quality_summary, error_code, error_message, retry_allowed, and next_retry_scope.
- Allowed statuses are planned, running, succeeded, partial_failed, failed, deferred, skipped_preserved, and superseded.

idempotency_policy:
- Re-running a successful chunk must not duplicate rows.
- Re-running a failed chunk must overwrite only affected pending or failed partitions.
- Valid existing partitions must not be deleted unless explicitly selected by chunk manifest.
- Existing Slice 1 / CR/GD/GL / MM outputs must be preserved.
- Deferred and excluded instruments must remain visible in reports.
- No global cleanup during backfill.
- No implicit latest-file autodetect.

failure_isolation_policy:
- Instrument-level probe failures must be isolated.
- Failure for one secid must not corrupt family-level accepted partitions.
- Failure for one family must not block unrelated families.
- Global failure is allowed only when registry snapshot or eligibility snapshot is structurally invalid.
- Missing raw/FUTOI/history must classify or defer, not silently include.
- Backfill must not silently skip failed instruments.

quality_report_required_fields:
- run_id
- chunk_id
- eligibility_snapshot_id
- registry_snapshot_id
- dataset_stage
- family_code
- secid
- date_from
- date_till
- rows_written
- rows_expected_if_known
- min_ts
- max_ts
- duplicate_ts_count
- gap_count
- null_ohlc_count
- invalid_ohlc_count
- futoi_missing_count
- calendar_status
- session_calendar_status
- source_payload_status
- partition_status
- quality_status
- failure_reason
- deferred_reason
- notes

aggregate_report_required_fields:
- candidate_universe_count
- included_count
- deferred_count
- excluded_count
- raw_5m_eligible_count
- futoi_eligible_count
- raw_d1_eligible_count
- continuous_v1_eligible_count
- access_api_eligible_count
- w1_gap_count
- failed_secid_count
- partial_failed_chunk_count
- preserved_partition_count

whitelist_preservation_policy:
- Existing accepted Slice 1 outputs are compatibility baseline, not all-universe selection logic.
- Existing accepted instruments are preserved unless a later eligibility snapshot gives a contract-valid exclusion or defer reason.
- Existing excluded/deferred behavior for SiH7 and SiM7 must not be silently promoted.
- USDRUBF perpetual identity behavior must be preserved.

deferral_reason_enum:
- unsupported_board_pending_review
- not_selected_for_first_executable_slice
- raw_5m_unavailable
- raw_5m_probe_failed
- raw_5m_schema_invalid
- raw_5m_calendar_mismatch
- futoi_unavailable
- futoi_unresolved
- futoi_source_scope_uncontracted
- ambiguous_family_futoi_mapping
- family_mapping_ambiguous
- liquidity_threshold_pending_pm_decision
- liquidity_below_threshold
- history_depth_threshold_pending_pm_decision
- history_depth_below_threshold
- expiration_anchor_missing
- expiration_anchor_conflicting
- expiration_anchor_sentinel_unreviewed
- perpetual_candidate_pending_review
- calendar_quality_unresolved
- partial_chain_missing_contract
- continuous_roll_map_not_buildable
- quality_probe_partial_failure
- future_no_trade_not_yet_loadable
- expired_no_current_load_scope
- w1_known_gap
- continuous_build_deferred

exclusion_reason_enum:
- technical_registry_row
- non_future_registry_row
- duplicate_registry_identity
- missing_required_identity_fields
- unsupported_engine_market
- explicit_pm_exclusion
- corrupt_source_payload
- not_in_current_available_universe

pm_l3_2_first_executable_slice:
- one eligibility snapshot date
- RFUD only
- included candidates only
- raw_5m_eligible=true only
- dataset_stage=raw_5m only
- one family only, preferably Si if included by generated eligibility snapshot
- max 2 secid
- 3 recent trading dates with available history
- strict chunk manifest
- strict quality report
- no FUTOI backfill
- no D1 derivation
- no continuous build
- no W1
- no daily unattended refresh

acceptance_criteria:
- registry discovery is candidate-only
- every discovered instrument is classified as exactly one of included, deferred, or excluded
- eligibility snapshot drives backfill selection
- chunking uses family_code × date_range × dataset_stage
- secid-level failure isolation exists
- chunk manifest contains required fields
- quality report contains required fields
- first executable slice is bounded as PM L3-2 approved
- re-run does not duplicate rows
- existing valid partitions are preserved
- deferred and excluded instruments remain visible
- no continuous/W1/daily-refresh expansion occurs
- no strategy, research, runtime trading code is touched

stop_conditions:
- PM L3 evidence does not match current origin/main
- all-universe snapshot artifact cannot be defined
- eligibility snapshot cannot drive backfill selection
- current contracts contradict stage-based backfill
- chunking cannot preserve existing partitions
- failure isolation cannot be implemented safely
- first slice cannot be bounded
- tests/CI fail and cannot be corrected inside approved scope
- implementation requires architecture decisions not already approved
