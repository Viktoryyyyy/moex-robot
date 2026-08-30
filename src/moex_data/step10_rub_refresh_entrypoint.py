from __future__ import annotations

import argparse
import json
from collections.abc import Sequence
from datetime import datetime, timezone
from pathlib import Path

from moex_data import step10_rub_refresh_dispatcher as dispatcher
from moex_data import step10_rub_refresh_scheduler as step10
from moex_data.futures import futoi_live_factual_refresh as futoi_factual


PROJECT = "MOEX_Bot"
SCHEMA_VERSION = "step10_rub_refresh_entrypoint.v1"


def _run_futoi_factual_non_blocking(*, through_date: str, run_id: str, timeout: float) -> dict[str, object]:
    try:
        return futoi_factual.run_refresh(
            through_date=through_date,
            run_id=run_id + "_futoi_factual",
            timeout=timeout,
        )
    except Exception as exc:
        return {
            "schema_version": futoi_factual.SCHEMA_VERSION,
            "project": PROJECT,
            "status": "FAILED_NON_BLOCKING",
            "error_class": exc.__class__.__name__,
            "error": str(exc),
            "factual_authority": False,
            "directional_authority": False,
            "action_authority": False,
            "stage5_pointer_promotion_performed": False,
        }


def _insert_futoi_refresh_order(order: list[object], dispatcher_mode: object) -> None:
    if "futoi_raw_factual_refresh" in order:
        return
    if dispatcher_mode == dispatcher.FULL_MODE:
        if "stage5_raw_and_derived" not in order:
            raise step10.Step10RefreshError("full-mode Stage 10 order missing stage5_raw_and_derived")
        order.insert(order.index("stage5_raw_and_derived"), "futoi_raw_factual_refresh")
        return
    if dispatcher_mode == dispatcher.BLOCKED_MODE:
        if "calendar" not in order:
            raise step10.Step10RefreshError("blocked-mode Stage 10 order missing calendar")
        order.insert(order.index("calendar"), "futoi_raw_factual_refresh")
        return
    raise step10.Step10RefreshError("unknown Stage 10 dispatcher_mode during FUTOI manifest augmentation")


def _augment_manifest(result: dict[str, object], futoi_result: dict[str, object]) -> None:
    root = step10._data_root()
    run_id = str(result.get("run_id") or "").strip()
    if not run_id:
        raise step10.Step10RefreshError("Stage 10 result missing run_id")
    manifest_path = root / "runs" / "step10_rub_daily_refresh" / ("run_id=" + run_id) / "run_manifest.json"
    if manifest_path.is_symlink() or not manifest_path.is_file():
        raise step10.Step10RefreshError("Stage 10 run manifest missing before entrypoint augmentation")
    result["entrypoint_schema_version"] = SCHEMA_VERSION
    result["futoi_factual_refresh"] = futoi_result
    result["futoi_factual_refresh_blocks_stage7"] = False
    order = result.get("deterministic_refresh_order")
    if isinstance(order, list):
        _insert_futoi_refresh_order(order, result.get("dispatcher_mode"))
    else:
        raise step10.Step10RefreshError("Stage 10 result missing deterministic_refresh_order")
    step10._atomic_json(manifest_path, result)


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
    now = now_utc or datetime.now(timezone.utc)
    if now.tzinfo is None or now.utcoffset() is None:
        step10._fail("now_utc must be timezone-aware")
    now = now.astimezone(timezone.utc)

    futoi_result = _run_futoi_factual_non_blocking(
        through_date=checked_through,
        run_id=checked_run,
        timeout=timeout,
    )
    result = dispatcher.run_refresh(
        through_date=checked_through,
        run_id=checked_run,
        repo_root=repo_root,
        env_file=env_file,
        timeout=timeout,
        now_utc=now,
    )
    _augment_manifest(result, futoi_result)
    return result


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run canonical Stage 10 refresh with independent factual-only Si FUTOI raw refresh."
    )
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
        print(
            json.dumps(
                {
                    "project": PROJECT,
                    "schema_version": SCHEMA_VERSION,
                    "status": "failed",
                    "error": str(exc),
                    "implicit_latest_used": False,
                },
                ensure_ascii=False,
                sort_keys=True,
            )
        )
        return 1
    print(json.dumps(result, ensure_ascii=False, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
