# futures_daily_refresh_scheduler_contract

status: implemented_contract
project: MOEX Bot
artifact_class: external_contract
format: markdown
schema_version: futures_daily_refresh_scheduler_contract.v1

purpose: Server-applied scheduler contract for the daily unattended MOEX futures data refresh runner.

source_of_truth:
- GitHub stores this scheduler contract and the runner code.
- Server stores systemd unit/timer state, data lake artifacts, run manifests, and logs.
- Server filesystem is applied state only and must not redefine stage order or artifact contracts.

entrypoint:
- module: moex_data.futures.daily_refresh_runner
- canonical_command: cd /home/trader/moex_bot/moex-robot && source /home/trader/moex_bot/venv/bin/activate && PYTHONPATH=src python -m moex_data.futures.daily_refresh_runner --data-root /home/trader/moex_bot/data

runtime_environment:
- venv: /home/trader/moex_bot/venv
- repo_root: /home/trader/moex_bot/moex-robot
- data_root_cli_arg: /home/trader/moex_bot/data
- pythonpath: src
- dotenv_policy: .env may be loaded only inside Python process through python-dotenv, never by shell-sourcing as production dependency.

required_stage_order:
1. registry_refresh_runner
2. raw_5m_loader
3. futoi_raw_loader
4. derived_d1_ohlcv_builder
5. expiration_map_builder
6. continuous_roll_map_builder
7. continuous_5m_builder
8. continuous_d1_builder
9. continuous_w1_builder
10. continuous_builder_manifest
11. continuous_quality_report

stage_policies:
- registry refresh must complete before any downstream raw/FUTOI/D1/continuous stage.
- raw 5m and FUTOI raw remain separate storage zones.
- raw D1 derivation must use accepted raw 5m partitions.
- continuous 5m must use accepted raw/FUTOI-independent roll mapping and accepted raw 5m inputs.
- continuous D1 must derive from accepted continuous 5m.
- continuous W1 must derive only from accepted continuous D1.
- quality reports must be generated before the top-level daily refresh verdict is pass.
- one daily manifest must be written at ${MOEX_DATA_ROOT}/futures/runs/daily_refresh/run_date={run_date}/manifest.json.

fail_closed_policy:
- If any child component fails, the runner must stop and must not execute later components.
- daily_refresh_result_verdict=pass is allowed only when all executed child components and quality gates pass.
- Missing, stale, malformed, or invalid child artifacts are blocking.
- Previous valid artifacts must remain available if the current run fails.
- No global cleanup is allowed during unattended daily refresh.

scheduler_policy:
- systemd timer is the accepted applied-state scheduler mechanism.
- The timer must run after regular MOEX evening data publication windows.
- The timer must call the canonical module entrypoint, not an ad hoc script that changes stage order.
- The scheduler must not source .env in shell as a production dependency.
- The scheduler must not run strategy, research, or runtime trading logic.

observability_policy:
- The daily manifest is the primary run status artifact.
- The latest manifest path must be inspectable by date partition.
- A failed run must preserve blocker details in the daily manifest blockers field.
- Child manifests and quality reports must be referenced from the daily manifest.

forbidden_scope:
- no strategy changes
- no research result generation
- no runtime trading logic changes
- no FUTOI pre-join into OHLCV
- no materialized 15m, 30m, 1h, or 4h outputs
- no change to roll_policy_id=expiration_minus_1_trading_session_v1
- no change to adjustment_policy_id=unadjusted_v1
- no server-first code edits

acceptance_criteria:
- Repository contains this contract.
- daily_refresh_runner.py requires this contract before execution.
- daily_refresh_runner.py component_execution_order matches required_stage_order.
- GitHub Actions tests pass before merge to origin/main.
- Server apply happens only after origin/main contains the accepted commit.
- Real server run writes one daily manifest and exits 0 only on pass.
