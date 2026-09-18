# futures_registry_refresh_manifest_contract

status: implemented_contract
project: MOEX Bot
scope: current-registry evidence validation with separate Slice 1 compatibility
artifact_class: external_pattern
format: json
schema_version: futures_registry_refresh_manifest.v1

purpose: Manifest for the canonical registry refresh child stage used by unattended futures daily refresh. This stage refreshes the current ISS futures registry snapshot, normalized registry, ALGOPACK/FUTOI/OBStats/HI2 availability reports, and liquidity/history screens for the same snapshot_date before raw 5m, FUTOI raw, and derived D1 components run.
producer: src/moex_data/futures/registry_refresh_runner.py
consumer:
- src/moex_data/futures/universal_daily_refresh_runner.py
- src/moex_data/futures/daily_refresh_runner.py
- futures_data_lake_pm_review
- futures_daily_refresh_quality_consumer

path_pattern: ${MOEX_DATA_ROOT}/futures/runs/registry_refresh/run_date={run_date}/manifest.json
primary_key:
- run_id

required_fields:
- schema_version
- validation_mode
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
- component_execution_order for new executions must equal registry_evidence_artifacts_producer, liquidity_history_metrics_probe.
- total_duration_sec must be present and non-negative.
- every child_component_status row must include duration_sec.
- child_duration_summary must contain each executed registry child component.
- availability_probe_timing_summary must include endpoint-level selected_instrument_count, probe_count, max_workers, duration_sec, row_count, and row_count_matches_selected_instruments.
- availability_max_workers=1 must preserve the sequential fallback path.
- Bounded concurrency must not change endpoint candidates, selected instrument population, row ordering determinism, row-level availability statuses, or fail-closed behavior.
- registry_refresh_result_verdict must be pass only when both child components exit zero and all required output artifacts exist and validate.
- artifact_validation_status must be pass only when output_summaries.*.validation_status are pass.
- registry_snapshot and normalized_registry must have more than zero rows.
- In slice1_compat only, algopack_fo_tradestats, moex_futoi, algopack_fo_obstats, and algopack_fo_hi2 availability reports must have availability_status=available for every selected Slice 1 universe row.
- In slice1_compat only, liquidity_screen must contain every accepted Slice 1 whitelist instrument with liquidity_status=pass.
- In slice1_compat only, history_depth_screen must contain every accepted Slice 1 whitelist instrument with history_depth_status=pass, except SiU7 may be pass or review_required because SiU7 is explicitly short-history allowed.
- In slice1_compat only, runner_whitelist_applied must equal SiM6, SiU6, SiU7, SiZ6, USDRUBF for accepted Slice 1 closeout.
- In slice1_compat only, excluded_instruments_confirmed must include SiH7 and SiM7.

blocking_conditions:
- any required registry, availability, liquidity, or history-depth contract is missing from repo.
- registry_evidence_artifacts_producer.py exits non-zero.
- moex_data.futures.liquidity_history_metrics_probe exits non-zero.
- any required output artifact is missing or stale relative to the child process execution.
- In slice1_compat only, any accepted whitelist instrument is absent from liquidity_screen or history_depth_screen.
- In slice1_compat only, any accepted whitelist instrument other than SiU7 fails history_depth_status.
- In slice1_compat only, SiU7 has history_depth_status other than pass or review_required.
- artifact_validation_status is not pass.
- concurrency changes output row count, selected instrument count, endpoint candidates, status distribution, or fail-closed behavior.

operational_notes:
- This contract removes the prior unattended-refresh dependency on fixed --snapshot-date 2026-04-29.
- The canonical scheduler calls universal_daily_refresh_runner.py, which explicitly selects current_registry for the registry child. daily_refresh_runner.py remains a Slice 1 compatibility caller.
- registry_refresh_runner.py is a thin data-acquisition wrapper around existing registry/availability and liquidity/history producers.
- No continuous series, all-futures expansion, strategy, research, or runtime trading behavior is introduced by this contract.

entrypoint_and_historical_compatibility:
- The screen child is launched as python -m moex_data.futures.liquidity_history_metrics_probe, using the runner's interpreter and repository working directory.
- The runner prepends its absolute src directory to the child PYTHONPATH, preserves other inherited environment values, and does not modify the parent environment.
- The existing producer uses observed AlgoPack TradeStats dates. This repair does not restore the removed Calendar API wrapper or change date-source, selection, numerical, history-completeness, or quality policies.
- Historical manifests may contain the legacy component ID liquidity_history_metrics_probe_apim_calendar and its original command. Preserve those records, statuses, timestamps, and evidence without rewriting or reclassifying them; the legacy ID is not a current executable alias.
- The daily manifest schema and existing output paths are unchanged. A historical failed run does not become successful when a later run uses the corrected entrypoint.

manifest_attempt_retention:
- history_path_pattern: ${MOEX_DATA_ROOT}/futures/runs/registry_refresh/run_date={run_date}/manifest_history/{sha256}.json
- Before replacing the daily manifest, retain its exact original bytes, including malformed or empty legacy evidence, and the exact newly serialized manifest bytes. SHA256 is computed over bytes, not run_id or parsed/reformatted JSON; identical bytes reuse the same verified archive.
- Archives are published from flushed and fsynced temporary files by create-only hard link. An existing archive must be a regular file containing identical bytes; mismatch, unreadable evidence or an archive failure stops publication without replacing the daily manifest.
- The daily manifest remains a complete manifest at its existing path, not a new pointer schema. It is atomically replaced only after archive files and their directories have been synced. Existing daily file permissions are preserved; new files start private to their writer.
- Manifest publication uses a nonblocking POSIX flock on the persistent sibling .manifest.lock inode. A competing publication fails explicitly; kernel release on process exit permits later retries. Do not unlink the lock file. This is not a replacement for the scheduler's existing whole-refresh serialization.
- This publication procedure targets the existing Linux runtime and requires regular files, hard-link support and directory fsync. File symlinks and a symlinked history directory are refused. No guarantee is made against a non-cooperating process modifying the storage.
- Temporary-file failures cannot publish a partial named archive. Archive/write/replace errors propagate as execution failure, never a success report. A directory-fsync failure after replacement can leave the new daily file visible, but both versions have already been archived; do not infer success from visibility alone.
- Retention covers manifest bytes only, not copies of every referenced child artifact, and does not recover attempts overwritten before this repair. No archive cleanup, history backfill or source refresh policy is introduced.

validation_modes:
- New manifests require validation_mode=current_registry or slice1_compat. Historical manifests without this field retain their original legacy interpretation; never relabel or rewrite them as canonical acceptance.
- registry_refresh_runner.py defaults to slice1_compat for existing callers. The universal runner must pass --validation-mode current_registry, including --stage/--stop-after paths. Debug --family/--secid filters must not narrow registry evidence validation.
- current_registry rejects explicit --whitelist or --excluded arguments, including empty values, before child execution. Canonical manifests record runner_whitelist_applied=[] and excluded_instruments_confirmed=[]; these empty compatibility fields do not make new exclusion/admission decisions.
- slice1_compat retains existing whitelist, exclusion, availability and screen-validation functions, defaults and review-ready handling. No replacement fixed contract list is introduced.

current_registry_evidence_validation:
- This mode validates candidate evidence only. A pass means complete, coherent outputs from both current producers; it does not mean every instrument is eligible, every source is available, every screen passed, or full historical/PIT coverage is proven.
- Snapshot and normalized registry must be nonempty, have unique case-insensitive board/SECID identities, and exactly matching identity sets for the requested snapshot_date. Required identity/provenance fields must be nonempty text; normalized snapshot_id/source_snapshot_id, engine and market must agree with the raw registry.
- Reuse registry_evidence_artifacts_producer.select_all_rfud_instruments for expected candidate scope. Do not use Slice 1 constants, count-only comparison, fabricated expired-contract rows or a new universe selector.
- Family mapping and each of the four availability reports must cover exactly that candidate set, with no missing/extra/duplicate identity. Same-snapshot IDs, family identity where resolved, endpoint IDs and schema versions must agree with their producers.
- Family mapping retains the current producer's derived_rule/pass and unresolved/failed outcomes. A resolved family must agree with normalized registry; unresolved evidence does not become an accepted mapping.
- Availability rows require probe_status=completed and the contracted availability_status vocabulary. available, unavailable, partial, error and not_checked are retained in status_counts without relabeling; availability is not required for all candidates. Incomplete probes and unknown statuses fail artifact validation.
- Probe and screen windows must match the existing producer defaults or explicit registry-runner bounds. This validation does not change the 14-day availability or 365-day screen defaults, the universal runner's existing bound forwarding, or the acquisition requests.
- Reuse liquidity_history_metrics_probe.selected_instruments_from_artifacts for screen coverage: exactly the TradeStats-available current candidates, not all registry rows and not a Slice 1 list. No available TradeStats instruments remains an explicit blocking condition because the existing producer cannot produce an accepted empty screen.
- Both screen artifacts must have the expected schema, identity, window and explicit pass/fail/review_required outcome. pass/review_required requires completed fetch, metrics_computed and ready_for_pm_review. fail requires failed validation and blocked review; it is retained as a negative outcome, not relabeled as success. Unknown or incoherent states fail structural validation.
- output_summaries.*.validation_scope=current_registry_evidence_only distinguishes artifact validation from the unchanged row quality outcomes. Preserve status_counts and expected/candidate counts. Do not set included/deferred/excluded or loadability flags here; existing downstream eligibility remains authoritative.
- Validate the seven evidence artifacts before starting the expensive metrics child. If evidence is invalid or screen selection cannot resolve, write a failed manifest with the specific validation blocker and do not launch that child. After both children finish, revalidate all nine artifacts.
- Existing child nonzero-return, missing/stale-output, manifest archival and atomic publication failures remain blocking. A structurally valid report cannot cancel a child failure.
- No eligibility code/configuration, numerical threshold, formula, source loader, source-scope repair, current universe, historical artifact, service, timer, concurrency or timeout policy is changed by this mode separation.
