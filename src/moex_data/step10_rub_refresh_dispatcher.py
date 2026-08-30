from __future__ import annotations

import argparse
import json
from collections.abc import Mapping, Sequence
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Final
from zoneinfo import ZoneInfo

from moex_data import step10_rub_refresh_scheduler as step10


SCHEMA_VERSION: Final[str] = "step10_rub_daily_refresh_dispatch.v1"
BLOCKED_MODE: Final[str] = "FUTOI_GOVERNED_BLOCKED_STAGE7_ONLY"
FULL_MODE: Final[str] = "FUTOI_PROMOTION_ALLOWED_FULL_STAGE10"


def _blocked_stage7_refresh(
    *,
    through_date: str,
    run_id: str,
    repo_root: str | Path,
    env_file: str | None,
    timeout: float,
    now_utc: datetime,
    governance: Mapping[str, object],
) -> dict[str, object]:
    root = step10._data_root()
    repo = step10._repo_root(repo_root)
    del repo
    market_today = now_utc.astimezone(ZoneInfo(step10.MARKET_TZ)).date()
    if date.fromisoformat(through_date) >= market_today:
        step10._fail("through_date must be a completed Moscow calendar date before today")

    run_root = step10._reserve_run_root(root, run_id)
    pointer_snapshot: dict[Path, bytes] | None = None
    rollback_expected: dict[Path, bytes] = {}
    started = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    try:
        pointer_snapshot = step10._snapshot_pointers(root)
        rollback_expected.update(pointer_snapshot)

        def capture_promoted_pointers(stage: int) -> None:
            rollback_expected.update(step10._capture_pointer_state(root, {stage}))

        stage7_base_date, stage7_base = step10._load_stage7_base(root, now_utc)
        if date.fromisoformat(stage7_base_date) > date.fromisoformat(through_date):
            step10._fail("through_date is older than current Stage 7 accepted history")

        calendar_start = min(
            date.fromisoformat(stage7_base_date) + timedelta(days=1),
            date.fromisoformat(through_date) - timedelta(days=14),
        ).isoformat()
        trading_dates_all = step10._calendar_dates(
            start_date=calendar_start,
            end_date=through_date,
            timeout=timeout,
        )
        if not trading_dates_all:
            step10._fail("MOEX futures calendar produced no completed trading date")
        latest_trade_date = max(trading_dates_all)
        stage7_new_dates = [value for value in trading_dates_all if value > stage7_base_date]
        weekly_boundary_completed = date.fromisoformat(through_date).weekday() == 6

        stage7_outputs = step10._stage7_refresh(
            root=root,
            run_root=run_root,
            run_id=run_id,
            base_frames=stage7_base,
            trading_dates=stage7_new_dates,
            rebuild_weekly=weekly_boundary_completed,
            weekly_boundary_end=through_date,
            timeout=timeout,
        )

        stage3_date, stage4_date = step10._latest_source_dates(root, now_utc)
        if stage3_date != stage4_date:
            step10._fail("Stage 3/4 current accepted trade dates are not aligned")
        if stage3_date > latest_trade_date:
            step10._fail("Stage 3/4 current accepted date is ahead of scheduler latest completed trading date")
        if stage3_date < latest_trade_date:
            source_refresh = step10._run_stage3_stage4(
                latest_trade_date=latest_trade_date,
                reference_date=market_today.isoformat(),
                run_id=run_id,
                env_file=str(env_file or ""),
                timeout=timeout,
                after_promotion=capture_promoted_pointers,
            )
        else:
            source_refresh = {"status": "no_op", "trade_date": latest_trade_date}

        pointer_records = [step10._pointer_from_output(root, output, run_id) for output in stage7_outputs]
        needs_stage7_promotion = bool(stage7_new_dates) or weekly_boundary_completed
        if needs_stage7_promotion:
            if len(stage7_outputs) != 8 or len(pointer_records) != 8:
                step10._fail("Stage 10 governed Stage 7 pointer set incomplete")
            step10._transactional_pointer_replace(pointer_records)
            rollback_expected.update(step10._capture_written_pointer_state(pointer_records))
            stage7_pointer_promotion = {"status": "promoted", "pointer_count": 8}
        else:
            if stage7_outputs or pointer_records:
                step10._fail("Stage 10 governed Stage 7 no-op produced unexpected outputs")
            stage7_pointer_promotion = {"status": "no_op", "pointer_count": 0}

        smoke_as_of = datetime.now(timezone.utc)
        smoke = step10._stage9_smoke(smoke_as_of)
        finished = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
        result: dict[str, object] = {
            "schema_version": SCHEMA_VERSION,
            "project": "MOEX_Bot",
            "stage": 10,
            "status": "succeeded",
            "dispatcher_mode": BLOCKED_MODE,
            "acceptance_contract_id": step10.CONTRACT_ID,
            "run_id": run_id,
            "through_date": through_date,
            "latest_completed_trading_date": latest_trade_date,
            "stage7_base_trade_date": stage7_base_date,
            "new_trading_dates": stage7_new_dates,
            "new_trading_date_count": len(stage7_new_dates),
            "source_refresh": source_refresh,
            "futoi_governance": dict(governance),
            "stage5": {
                "status": "governed_blocked_not_run",
                "output_count": 0,
                "canonical_pointer_promotion": False,
                "reason": "FUTOI factual live authority is not accepted",
            },
            "stage7": {
                "status": "refreshed" if needs_stage7_promotion else "no_op",
                "output_count": len(stage7_outputs),
                "canonical_pointer_promotion": stage7_pointer_promotion,
            },
            "stage9_smoke": smoke,
            "deterministic_refresh_order": [
                "futoi_governance",
                "calendar",
                "stage7_raw_and_derived",
                "stage3",
                "stage4",
                "stage7_pointer_promotion",
                "stage9_smoke",
            ],
            "futoi_block_does_not_block_stage7": True,
            "pointer_rollback_on_failure": True,
            "implicit_latest_used": False,
            "network_sources_explicitly_bounded_by_date": True,
            "historical_pit_research_ready_claimed": False,
            "started_at_utc": started,
            "finished_at_utc": finished,
        }
        manifest_path = run_root / "run_manifest.json"
        step10._atomic_json(manifest_path, result)
        result["run_manifest_ref"] = step10._rooted_ref(root, manifest_path)
        return result
    except Exception as exc:
        rollback_error: str | None = None
        if pointer_snapshot is not None:
            try:
                step10._restore_pointer_snapshot(pointer_snapshot, rollback_expected)
            except Exception as rollback_exc:  # pragma: no cover - catastrophic filesystem boundary
                rollback_error = str(rollback_exc)
        failure: dict[str, object] = {
            "schema_version": SCHEMA_VERSION,
            "project": "MOEX_Bot",
            "stage": 10,
            "status": "failed",
            "dispatcher_mode": BLOCKED_MODE,
            "run_id": run_id,
            "through_date": through_date,
            "error": str(exc),
            "current_pointer_rollback_attempted": pointer_snapshot is not None,
            "current_pointer_rollback_status": "failed" if rollback_error else ("restored" if pointer_snapshot is not None else "not_needed"),
            "rollback_error": rollback_error,
            "implicit_latest_used": False,
            "started_at_utc": started,
            "finished_at_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        }
        try:
            step10._atomic_json(run_root / "run_manifest.json", failure)
        except Exception:
            pass
        if rollback_error:
            raise step10.Step10RefreshError(str(exc) + "; pointer rollback failed: " + rollback_error) from exc
        raise step10.Step10RefreshError(str(exc)) from exc


def run_refresh(
    *,
    through_date: str,
    run_id: str,
    repo_root: str | Path = ".",
    env_file: str | None = step10.CANONICAL_ENV_PATH,
    timeout: float = 60.0,
    now_utc: datetime | None = None,
) -> dict[str, object]:
    checked_through = step10._iso_date(through_date, "through_date")
    checked_run = step10._safe_token(run_id, "run_id")
    step10.load_env_file(env_file)
    repo = step10._repo_root(repo_root)
    now = now_utc or datetime.now(timezone.utc)
    if now.tzinfo is None or now.utcoffset() is None:
        step10._fail("now_utc must be timezone-aware")
    now = now.astimezone(timezone.utc)
    governance = step10._futoi_stage5_promotion_governance(repo)

    if bool(governance.get("promotion_allowed")):
        result = step10.run_refresh(
            through_date=checked_through,
            run_id=checked_run,
            repo_root=repo,
            env_file=env_file,
            timeout=timeout,
            now_utc=now,
        )
        result["dispatcher_mode"] = FULL_MODE
        result["dispatcher_futoi_governance_checked"] = True
        return result

    return _blocked_stage7_refresh(
        through_date=checked_through,
        run_id=checked_run,
        repo_root=repo,
        env_file=env_file,
        timeout=timeout,
        now_utc=now,
        governance=governance,
    )


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Dispatch Stage 10 RUB refresh under the canonical FUTOI governance contract.")
    parser.add_argument("--through-date", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--repo-root", default=".")
    parser.add_argument("--env-file", default=step10.CANONICAL_ENV_PATH)
    parser.add_argument("--timeout", type=float, default=60.0)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        result = run_refresh(
            through_date=args.through_date,
            run_id=args.run_id,
            repo_root=args.repo_root,
            env_file=args.env_file,
            timeout=args.timeout,
        )
    except Exception as exc:
        print(json.dumps({"project": "MOEX_Bot", "stage": 10, "status": "failed", "error": str(exc), "implicit_latest_used": False}, ensure_ascii=False, sort_keys=True))
        return 1
    print(json.dumps(result, ensure_ascii=False, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
