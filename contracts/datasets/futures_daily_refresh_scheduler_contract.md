# futures_daily_refresh_scheduler_contract

status: implemented_contract
project: MOEX Bot
artifact_class: external_contract
format: markdown
schema_version: futures_daily_refresh_scheduler_contract.v1

purpose: Server-applied scheduler contract for the canonical universal MOEX futures data refresh runner.

source_of_truth:
- GitHub stores this scheduler contract and the runner code.
- Server stores systemd unit/timer state, data lake artifacts, run manifests, and logs.
- Server filesystem is applied state only and must not redefine stage order, universe semantics, or artifact contracts.

canonical_entrypoint:
- module: moex_data.futures.universal_daily_refresh_runner
- canonical_command: cd /home/trader/moex_bot/moex-robot && source /home/trader/moex_bot/venv/bin/activate && PYTHONPATH=src python -m moex_data.futures.universal_daily_refresh_runner --data-root /home/trader/moex_bot/data

legacy_compatibility_entrypoint:
- module: moex_data.futures.daily_refresh_runner
- status: compatibility_only
- note: Legacy runner may remain available for Slice 1 compatibility/debug, but it must not define canonical all-universe daily refresh semantics.

runtime_environment:
- venv: /home/trader/moex_bot/venv
- repo_root: /home/trader/moex_bot/moex-robot
- data_root_cli_arg: /home/trader/moex_bot/data
- pythonpath: src
- canonical_dotenv: /home/trader/moex_bot/.env
- repository_local_dotenv: forbidden_duplicate_must_be_absent_on_applied_state
- dotenv_policy: canonical top-level runtime loads the parent env explicitly; parameterless child discovery is permitted only when the repository-local duplicate is absent, so discovery reaches the canonical parent env; shell-sourcing .env is never a production dependency.

required_canonical_stage_order:
1. registry_refresh
2. all_universe_eligibility_snapshot
3. raw_5m_refresh
4. futoi_raw_refresh
5. raw_d1_derivation
6. continuous_eligibility_refinement
7. expiration_map
8. roll_map
9. continuous_5m
10. continuous_d1
11. continuous_w1
12. quality_reports
13. unified_manifest

stage_policies:
- registry refresh must complete before downstream raw/FUTOI/D1/continuous stages.
- all-universe eligibility snapshot must define canonical included/deferred/excluded status.
- raw 5m and FUTOI raw remain separate storage zones.
- raw D1 derivation must use accepted raw 5m partitions.
- continuous 5m must use accepted raw/FUTOI-independent roll mapping and accepted raw 5m inputs.
- continuous D1 must derive from accepted continuous 5m.
- continuous W1 must derive only from accepted continuous D1.
- quality reports must be generated before the top-level daily refresh verdict is pass.
- one universal daily manifest must be written at ${MOEX_DATA_ROOT}/futures/runs/universal_daily_refresh/run_date={run_date}/manifest.json.

fail_closed_policy:
- If any child component fails, the runner must stop and must not execute later components.
- universal_daily_refresh_result_verdict=pass is allowed only when all executed child components and quality gates pass.
- Missing, stale, malformed, or invalid child artifacts are blocking.
- Missing canonical all-universe producer components are blocking.
- Previous valid artifacts must remain available if the current run fails.
- No global cleanup is allowed during unattended daily refresh.

scheduler_policy:
- systemd timer is the accepted applied-state scheduler mechanism.
- The timer must run after regular MOEX evening data publication windows.
- The timer must call the canonical module entrypoint, not an ad hoc script that changes stage order.
- The scheduler must not source .env in shell as a production dependency.
- The scheduler must not run strategy, research, or runtime trading logic.
- The scheduler must not point canonical daily refresh to the legacy Slice 1 compatibility runner.

observability_policy:
- The universal daily manifest is the primary run status artifact.
- The latest manifest path must be inspectable by date partition.
- A failed run must preserve blocker details in the universal daily manifest blockers field.
- Child manifests and quality reports must be referenced from the universal daily manifest.

forbidden_scope:
- no strategy changes
- no research result generation
- no runtime trading logic changes
- no FUTOI pre-join into OHLCV
- no materialized 15m, 30m, 1h, or 4h outputs
- no change to roll_policy_id=expiration_minus_1_trading_session_v1
- no change to adjustment_policy_id=unadjusted_v1
- no server-first code edits
- no Slice 1 whitelist semantics as canonical daily refresh scope

acceptance_criteria:
- Repository contains this contract.
- Repository contains futures_universal_daily_refresh_manifest_contract.md.
- universal_daily_refresh_runner.py requires the universal manifest contract before execution.
- universal_daily_refresh_runner.py canonical_stage_order matches required_canonical_stage_order.
- canonical parent dotenv path is explicit in the runtime contract.
- applied server state has no repository-local duplicate dotenv after migration.
- GitHub Actions tests pass before merge to origin/main.
- Server apply happens only after origin/main contains the accepted commit.
- Real server run writes one universal daily manifest.
- If canonical all-universe producer stages are missing, real server run fails closed with explicit blocker rather than silently falling back to Slice 1.
