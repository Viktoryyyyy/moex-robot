# futures_rfud_candidates_evidence_contract

status: implemented_contract
project: MOEX Bot
artifact_class: external_pattern
format: parquet
schema_version: futures_rfud_candidates_evidence_scope.v1

purpose: Evidence-only universe scope for broad current RFUD candidates used for controlled rollout ranking inputs without changing Slice 1 behavior.

producer:
- algopack_availability_probe
- futures_liquidity_screen_builder
- futures_history_depth_screen_builder

consumer:
- futures_data_lake_pm_review
- controlled_rfud_rollout_ranking_review

universe_scope: rfud_candidates
selection_rule:
- select all current instruments from the normalized registry where board is rfud.
- do not use manual candidate guessing.
- do not start raw 5m load, FUTOI load, backfill, continuous-series build, daily refresh expansion, or strategy/runtime changes.

path_patterns:
- ${MOEX_DATA_ROOT}/futures/registry/universe_scope=rfud_candidates/snapshot_date={snapshot_date}/futures_registry_snapshot.parquet
- ${MOEX_DATA_ROOT}/futures/registry/universe_scope=rfud_candidates/snapshot_date={snapshot_date}/futures_normalized_instrument_registry.parquet
- ${MOEX_DATA_ROOT}/futures/availability/universe_scope=rfud_candidates/snapshot_date={snapshot_date}/futures_algopack_tradestats_availability_report.parquet
- ${MOEX_DATA_ROOT}/futures/availability/universe_scope=rfud_candidates/snapshot_date={snapshot_date}/futures_futoi_availability_report.parquet
- ${MOEX_DATA_ROOT}/futures/availability/universe_scope=rfud_candidates/snapshot_date={snapshot_date}/futures_obstats_availability_report.parquet
- ${MOEX_DATA_ROOT}/futures/availability/universe_scope=rfud_candidates/snapshot_date={snapshot_date}/futures_hi2_availability_report.parquet
- ${MOEX_DATA_ROOT}/futures/screens/liquidity/universe_scope=rfud_candidates/snapshot_date={snapshot_date}/futures_liquidity_screen.parquet
- ${MOEX_DATA_ROOT}/futures/screens/history_depth/universe_scope=rfud_candidates/snapshot_date={snapshot_date}/futures_history_depth_screen.parquet

validation_rules:
- default CLI universe_scope must be slice1.
- slice1 must preserve legacy output paths.
- rfud_candidates must write evidence artifacts only under universe_scope=rfud_candidates paths.
- broad RFUD candidate count must be greater than 7 when the normalized registry has more than 7 RFUD instruments.
- generated artifacts must be readable parquet files.

blocking_conditions:
- Slice 1 default behavior changes.
- rfud_candidates writes to legacy Slice 1 evidence paths.
- implementation requires historical backfill, raw 5m/FUTOI loader expansion, daily refresh expansion, continuous series expansion, or trading runtime changes.
