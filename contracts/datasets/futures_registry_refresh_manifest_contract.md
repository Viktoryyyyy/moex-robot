# futures_registry_refresh_manifest_contract

status: implemented_contract
project: MOEX Bot
scope: Slice 1.1 registry refresh automation
artifact_class: external_pattern
format: json
schema_version: futures_registry_refresh_manifest.v1

purpose: Manifest for the canonical registry refresh child stage used by unattended futures daily refresh. This stage refreshes the current ISS futures registry snapshot, normalized registry, ALGOPACK/FUTOI/OBStats/HI2 availability reports, and liquidity/history screens for the same snapshot_date before raw 5m, FUTOI raw, and derived D1 components run.
producer: src/moex_data/futures/registry_refresh_runner.py
consumer:
- src/moex_data/futures/daily_refresh_runner.py
- futures_data_lake_pm_review
- futures_daily_refresh_quality_consumer

path_pattern: ${MOEX_DATA_ROOT}/futures/runs/registry_refresh/run_date={run_date}/manifest.json
primary_key:
- run_id

required_fields:
- schema_version
- run_id
- run_date
- snapshot_date
- refresh_from
- refresh_till
- started_ts
- completed_ts
- total_duration_sec
- runner_whitelist_applied
- excluded_instruments_confirmed
- availability_max_workers
- component_execution_order
- child_component_status
- child_component_status[].duration_sec
- child_duration_summary
- availability_probe_timing_summary
- child_output_references
- output_artifacts
- output_summaries
- artifact_validation_status
- registry_refresh_result_verdict
- blockers

nullable_fields:
- refresh_from
- refresh_till

status_fields:
- registry_refresh_result_verdict
- artifact_validation_status
- child_component_status.status
- child_component_status.validation_status
- output_summaries.*.validation_status

observability_fields:
- total_duration_sec: total wall-clock runtime for registry_refresh_runner.py in seconds.
- child_component_status[].duration_sec: wall-clock runtime for each registry child component in seconds.
- child_duration_summary: map of registry child component_id to duration_sec.
- availability_probe_timing_summary: per-endpoint availability timing summary emitted by registry_evidence_artifacts_producer.py. Each endpoint entry must include selected_instrument_count, probe_count, max_workers, duration_sec, row_count, and row_count_matches_selected_instruments.
- availability_max_workers: bounded availability probe concurrency setting. Default is conservative at 4; value 1 is the required sequential fallback.

output_artifacts:
- registry_snapshot: ${MOEX_DATA_ROOT}/futures/registry/snapshot_date={snapshot_date}/futures_registry_snapshot.parquet
- normalized_registry: ${MOEX_DATA_ROOT}/futures/registry/snapshot_date={snapshot_date}/futures_normalized_instrument_registry.parquet
- algopack_fo_tradestats: ${MOEX_DATA_ROOT}/futures/availability/snapshot_date={snapshot_date}/futures_algopack_tradestats_availability_report.parquet
- moex_futoi: ${MOEX_DATA_ROOT}/futures/availability/snapshot_date={snapshot_date}/futures_futoi_availability_report.parquet
- algopack_fo_obstats: ${MOEX_DATA_ROOT}/futures/availability/snapshot_date={snapshot_date}/futures_obstats_availability_report.parquet
- algopack_fo_hi2: ${MOEX_DATA_ROOT}/futures/availability/snapshot_date={snapshot_date}/futures_hi2_availability_report.parquet
- liquidity_screen: ${MOEX_DATA_ROOT}/futures/screens/liquidity/snapshot_date={snapshot_date}/futures_liquidity_screen.parquet
- history_depth_screen: ${MOEX_DATA_ROOT}/futures/screens/history_depth/snapshot_date={snapshot_date}/futures_history_depth_screen.parquet
- manifest: ${MOEX_DATA_ROOT}/futures/runs/registry_refresh/run_date={run_date}/manifest.json

validation_rules:
- schema_version must equal futures_registry_refresh_manifest.v1.
- snapshot_date must be the same snapshot_date used by downstream raw_5m_loader.py and futoi_raw_loader.py.
- component_execution_order must equal registry_evidence_artifacts_producer, liquidity_history_metrics_probe_apim_calendar.
- total_duration_sec must be present and non-negative.
- every child_component_status row must include duration_sec.
- child_duration_summary must contain each executed registry child component.
- availability_probe_timing_summary must include endpoint-level selected_instrument_count, probe_count, max_workers, duration_sec, row_count, and row_count_matches_selected_instruments.
- availability_max_workers=1 must preserve the sequential fallback path.
- Bounded concurrency must not change endpoint candidates, selected instrument population, row ordering determinism, row-level availability statuses, or fail-closed behavior.
- registry_refresh_result_verdict must be pass only when both child components exit zero and all required output artifacts exist and validate.
- artifact_validation_status must be pass only when output_summaries.*.validation_status are pass.
- registry_snapshot and normalized_registry must have more than zero rows.
- algopack_fo_tradestats, moex_futoi, algopack_fo_obstats, and algopack_fo_hi2 availability reports must have availability_status=available for every selected Slice 1 universe row.
- liquidity_screen must contain every accepted Slice 1 whitelist instrument with liquidity_status=pass.
- history_depth_screen must contain every accepted Slice 1 whitelist instrument with history_depth_status=pass, except SiU7 may be pass or review_required because SiU7 is explicitly short-history allowed.
- runner_whitelist_applied must equal SiM6, SiU6, SiU7, SiZ6, USDRUBF for accepted Slice 1 closeout.
- excluded_instruments_confirmed must include SiH7 and SiM7.

blocking_conditions:
- any required registry, availability, liquidity, or history-depth contract is missing from repo.
- registry_evidence_artifacts_producer.py exits non-zero.
- liquidity_history_metrics_probe_apim_calendar.py exits non-zero.
- any required output artifact is missing or stale relative to the child process execution.
- any accepted whitelist instrument is absent from liquidity_screen or history_depth_screen.
- any accepted whitelist instrument other than SiU7 fails history_depth_status.
- SiU7 has history_depth_status other than pass or review_required.
- artifact_validation_status is not pass.
- concurrency changes output row count, selected instrument count, endpoint candidates, status distribution, or fail-closed behavior.

operational_notes:
- This contract removes the prior unattended-refresh dependency on fixed --snapshot-date 2026-04-29.
- The scheduler must call daily_refresh_runner.py only; it must not call registry_refresh_runner.py directly.
- registry_refresh_runner.py is a thin data-acquisition wrapper around existing registry/availability and liquidity/history producers.
- No continuous series, all-futures expansion, strategy, research, or runtime trading behavior is introduced by this contract.
