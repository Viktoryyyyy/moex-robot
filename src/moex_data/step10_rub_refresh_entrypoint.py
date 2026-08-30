from __future__ import annotations

import argparse
import json
from collections.abc import Sequence
from datetime import datetime, timezone
from pathlib import Path

from moex_data import step10_rub_refresh_dispatcher as dispatcher
from moex_data import step10_rub_refresh_scheduler as step10
from moex_data.futures import futoi_live_factual_refresh_source_native as futoi_factual


PROJECT = "MOEX_Bot"
SCHEMA_VERSION = "step10_rub_refresh_entrypoint.v1"


class Step10EntrypointError(step10.Step10RefreshError):
    def __init__(self, message: str, *, futoi_result: dict[str, object]) -> None:
        super().__init__(message)
        self.futoi_result = futoi_result


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
    result_context: dict[str, object] = {
        "entrypoint_schema_version": SCHEMA_VERSION,
        "futoi_factual_refresh": futoi_result,
        "futoi_factual_refresh_blocks_stage7": False,
    }
    try:
        return dispatcher.run_refresh(
            through_date=checked_through,
            run_id=checked_run,
            repo_root=repo_root,
            env_file=env_file,
            timeout=timeout,
            now_utc=now,
            result_context=result_context,
        )
    except Exception as exc:
        raise Step10EntrypointError(str(exc), futoi_result=futoi_result) from exc


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
    except Step10EntrypointError as exc:
        print(
            json.dumps(
                {
                    "project": PROJECT,
                    "schema_version": SCHEMA_VERSION,
                    "status": "failed",
                    "error": str(exc),
                    "futoi_factual_refresh": exc.futoi_result,
                    "futoi_factual_refresh_blocks_stage7": False,
                    "dispatcher_transaction_context_bound": True,
                    "implicit_latest_used": False,
                },
                ensure_ascii=False,
                sort_keys=True,
                default=str,
            )
        )
        return 1
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
