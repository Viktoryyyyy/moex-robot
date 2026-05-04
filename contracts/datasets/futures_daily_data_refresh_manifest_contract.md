# futures_daily_data_refresh_manifest_contract

status: implemented_contract
project: MOEX Bot
artifact_class: external_pattern
format: json
schema_version: futures_daily_data_refresh_manifest.v1

purpose: Top-level Slice 1.1 daily data refresh run manifest that orchestrates registry refresh, raw 5m futures loader, FUTOI raw loader, and derived D1 OHLCV builder as separate child components while preserving their independent storage zones and manifests.
producer: src/moex_data/futures/daily_refresh_runner.py
consumer:
- futures_data_lake_pm_review
- futures_daily_refresh_quality_consumer
- later_scheduler_or_cron_wrapper
- later_futures_continuous_builder_integration

path_pattern: ${MOEX_DATA_ROOT}/futures/runs/daily_refresh/run_date={run_date}/manifest.json
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
- runner_whitelist_applied
- excluded_instruments_confirmed
- component_execution_order
- child_component_status
- child_manifest_references
- per_instrument_status
- short_history_flag_check
- excluded_instruments_check
- artifact_validation_status
- daily_refresh_result_verdict
- blockers
- output_artifacts

nullable_fields:
- refresh_from
- refresh_till

status_fields:
- daily_refresh_result_verdict
- artifact_validation_status
- child_component_status.status
- short_history_flag_check.status
- excluded_instruments_check.status

validation_rules:
- schema_version must equal futures_daily_data_refresh_manifest.v1.
- runner_whitelist_applied must equal SiM6, SiU6, SiU7, SiZ6, USDRUBF for accepted Slice 1 closeout.
- excluded_instruments_confirmed must include SiH7 and SiM7.
- component_execution_order must equal registry_refresh_runner, raw_5m_loader, futoi_raw_loader, derived_d1_ohlcv_builder for the implemented Slice 1.1 runner.
- daily_refresh_result_verdict must be pass only when every child component status is pass and artifact_validation_status is pass.
- child_manifest_references must include manifest references for registry_refresh_runner, raw_5m_loader, futoi_raw_loader, and derived_d1_ohlcv_builder.
- registry_refresh_runner child manifest must conform to futures_registry_refresh_manifest.v1.
- raw_5m_loader child manifest must conform to futures_raw_5m_loader_manifest.v1.
- futoi_raw_loader child manifest must conform to futures_futoi_5m_raw_loader_manifest.v1.
- derived_d1_ohlcv_builder child manifest must conform to futures_derived_d1_ohlcv_manifest.v1.
- registry_refresh_runner must execute before raw_5m_loader.
- raw_5m_loader and futoi_raw_loader must receive the same snapshot_date that registry_refresh_runner refreshed.
- all child output_artifacts and partition_paths_created must exist at validation time.
- per_instrument_status must contain only accepted whitelist instruments.
- excluded_instruments_check.status must equal pass.
- short_history_flag_check.status must equal pass and must confirm SiU7 short_history_flag=true across downstream data components.
- no child manifest or partition path may include secid=SiH7 or secid=SiM7 in downstream loader/builder outputs.
- if any child component fails, the runner must fail closed and must not execute later components.

continuous_builder_integration_contract:
- continuous futures v1 integration is declared as the next fail-closed extension point, not as part of the implemented Slice 1.1 manifest schema.
- current futures_daily_data_refresh_manifest.v1 remains valid until the continuous builder is implemented.
- when continuous builder is enabled, it must run only after registry_refresh_runner, raw_5m_loader, futoi_raw_loader, and derived_d1_ohlcv_builder have passed.
- enabled continuous builder runs must produce futures_continuous_builder_manifest.v1 before the top-level daily refresh can return pass.
- enabled continuous builder runs must preserve separate raw 5m, FUTOI raw, raw derived D1, continuous roll map, continuous 5m, and continuous D1 storage zones.
- enabled continuous builder runs must fail closed if futures_continuous_roll_map.v1, futures_continuous_5m.v1, futures_continuous_d1.v1, or futures_continuous_quality_report.v1 validation fails.
- enabled continuous builder runs must not silently bridge partial Si-chain gaps caused by excluded SiH7 or SiM7.
- enabled continuous builder runs must preserve USDRUBF identity behavior.
- a future schema version must explicitly add continuous_series_builder to component_execution_order and child_manifest_references when implementation is added.

blocking_conditions:
- any required child manifest contract is missing from repo.
- registry_refresh_runner fails or its manifest verdict is not pass.
- raw_5m_loader fails or its manifest verdict is not pass.
- futoi_raw_loader fails or its manifest verdict is not pass.
- derived_d1_ohlcv_builder fails or its manifest verdict is not pass.
- any child manifest is stale relative to the child process execution.
- any accepted whitelist instrument is missing from child instrument_summaries.
- any excluded instrument appears in downstream child summaries or partition paths.
- SiU7 short_history_flag is not true.
- child output artifacts or created partitions are missing.
- when continuous builder is enabled, its child manifest, roll map, continuous 5m, continuous D1, and quality report must all validate before top-level pass.
