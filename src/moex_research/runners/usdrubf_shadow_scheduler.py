from __future__ import annotations

import argparse
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
import fcntl
import json
import os
from pathlib import Path
import time
from typing import Callable, Iterator, Mapping, TextIO

from src.moex_research.intelligence.usdrubf_shadow_runtime import ShadowJsonStore
from src.moex_research.runners.usdrubf_live_shadow_smoke import run_once as run_live_shadow_once


PROJECT = "MOEX_Bot"
MODE = "controlled_shadow_scheduler"
STATUS_FILENAME = "shadow_scheduler_status.json"
LOCK_FILENAME = ".shadow_scheduler.lock"
_MIN_INTERVAL_SECONDS = 60
_MAX_INTERVAL_SECONDS = 3600
_MAX_FINITE_CYCLES = 10_000


class ShadowSchedulerError(RuntimeError):
    """Raised when the controlled scheduler boundary is violated."""


class SchedulerAlreadyRunning(ShadowSchedulerError):
    """Raised when the state root is already locked by another scheduler process."""


@dataclass(frozen=True)
class SchedulerConfig:
    state_root: Path
    interval_seconds: int
    max_cycles: int
    max_prior_lookback_days: int
    news_timeout_seconds: float
    news_max_events: int


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Controlled recurring USDRUBF RUB Intelligence shadow scheduler"
    )
    parser.add_argument("--state-root", required=True)
    parser.add_argument("--interval-seconds", type=int, default=300)
    parser.add_argument(
        "--max-cycles",
        type=int,
        default=0,
        help="0 means run until interrupted; positive values provide a bounded proof run",
    )
    parser.add_argument("--max-prior-lookback-days", type=int, default=7)
    parser.add_argument("--news-timeout-seconds", type=float, default=10.0)
    parser.add_argument("--news-max-events", type=int, default=20)
    return parser


def _validate_config(args: argparse.Namespace) -> SchedulerConfig:
    state_root = Path(args.state_root).expanduser()
    if not state_root.is_absolute():
        raise ShadowSchedulerError("state_root must be an explicit absolute path")
    if not _MIN_INTERVAL_SECONDS <= args.interval_seconds <= _MAX_INTERVAL_SECONDS:
        raise ShadowSchedulerError(
            f"interval_seconds must be within {_MIN_INTERVAL_SECONDS}..{_MAX_INTERVAL_SECONDS}"
        )
    if args.max_cycles < 0 or args.max_cycles > _MAX_FINITE_CYCLES:
        raise ShadowSchedulerError(
            f"max_cycles must be 0 or within 1..{_MAX_FINITE_CYCLES}"
        )
    if not 1 <= args.max_prior_lookback_days <= 31:
        raise ShadowSchedulerError("max_prior_lookback_days must be within 1..31")
    if not 0 < args.news_timeout_seconds <= 60:
        raise ShadowSchedulerError("news_timeout_seconds must be within (0, 60]")
    if not 1 <= args.news_max_events <= 100:
        raise ShadowSchedulerError("news_max_events must be within 1..100")
    return SchedulerConfig(
        state_root=state_root,
        interval_seconds=args.interval_seconds,
        max_cycles=args.max_cycles,
        max_prior_lookback_days=args.max_prior_lookback_days,
        news_timeout_seconds=args.news_timeout_seconds,
        news_max_events=args.news_max_events,
    )


def _aware_now(now_fn: Callable[[], datetime]) -> datetime:
    value = now_fn()
    if value.tzinfo is None or value.utcoffset() is None:
        raise ShadowSchedulerError("scheduler clock must be timezone-aware")
    return value


def _parse_aware(value: object, field: str) -> datetime:
    if not isinstance(value, str) or not value.strip():
        raise ShadowSchedulerError(f"{field} must be a non-empty ISO datetime")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise ShadowSchedulerError(f"{field} must be an ISO datetime") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ShadowSchedulerError(f"{field} must be timezone-aware")
    return parsed


def _prepare_state_root(state_root: Path) -> None:
    state_root.mkdir(parents=True, exist_ok=True)
    if state_root.is_symlink() or not state_root.is_dir():
        raise ShadowSchedulerError("state_root must be a regular directory, not a symlink")


def _write_atomic_json(path: Path, payload: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_suffix(path.suffix + ".tmp")
    serialized = json.dumps(
        dict(payload),
        ensure_ascii=False,
        indent=2,
        sort_keys=True,
        allow_nan=False,
    )
    temp_path.write_text(serialized, encoding="utf-8")
    temp_path.replace(path)


@contextmanager
def _single_instance_lock(
    state_root: Path,
    *,
    now_fn: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
) -> Iterator[Path]:
    """Hold a process-scoped non-blocking advisory lock for this state root.

    The lock file may remain on disk, but the kernel lock is released automatically
    when the descriptor closes, so a crashed process cannot leave a false stale lock.
    """

    _prepare_state_root(state_root)
    lock_path = state_root / LOCK_FILENAME
    handle: TextIO = lock_path.open("a+", encoding="utf-8")
    try:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise SchedulerAlreadyRunning(
                f"another shadow scheduler already holds {lock_path}"
            ) from exc

        acquired_at = _aware_now(now_fn)
        handle.seek(0)
        handle.truncate()
        json.dump(
            {
                "pid": os.getpid(),
                "acquired_at": acquired_at.isoformat(),
                "project": PROJECT,
                "mode": MODE,
            },
            handle,
            ensure_ascii=False,
            sort_keys=True,
        )
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
        yield lock_path
    finally:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        finally:
            handle.close()


def _live_cycle_args(config: SchedulerConfig) -> argparse.Namespace:
    """Pin S6.1 to the accepted non-action live boundary.

    No Flowise transport, FUTOI authority, alert delivery or broker/order execution
    is enabled by this scheduler.
    """

    return argparse.Namespace(
        state_root=str(config.state_root),
        max_prior_lookback_days=config.max_prior_lookback_days,
        enable_futoi=False,
        safe_wait_agent=True,
        cbr_macro=True,
        live_news=True,
        news_timeout_seconds=config.news_timeout_seconds,
        news_max_events=config.news_max_events,
        flowise_endpoint=None,
        flowise_request_field=None,
        flowise_response_field=None,
        flowise_timeout_seconds=20.0,
    )


def _prior_state_summary(state_root: Path) -> tuple[bool, str | None]:
    state = ShadowJsonStore(state_root).load_market_state()
    if state is None:
        return False, None
    return True, state.as_of_timestamp


def _validate_cycle_result(
    result: Mapping[str, object],
    *,
    state_root: Path,
    prior_as_of_timestamp: str | None,
) -> None:
    required = {
        "status",
        "as_of_timestamp",
        "decision_agent_mode",
        "futoi_quality",
        "news_mode",
        "macro_mode",
        "significant_change",
        "action_candidate",
        "market_state_path",
        "change_detection_path",
    }
    missing = required - set(result)
    if missing:
        raise ShadowSchedulerError(
            "live cycle result missing fields: " + ",".join(sorted(missing))
        )
    if result["status"] != "COMPLETED":
        raise ShadowSchedulerError("live cycle did not complete")
    if result["decision_agent_mode"] != "SAFE_WAIT":
        raise ShadowSchedulerError("S6.1 scheduler accepts SAFE_WAIT decision mode only")
    if result["futoi_quality"] != "BLOCKED":
        raise ShadowSchedulerError("S6.1 scheduler requires governed FUTOI exclusion")
    if result["news_mode"] != "LIVE_RSS_DETERMINISTIC_NEUTRAL":
        raise ShadowSchedulerError("S6.1 scheduler requires accepted live News mode")
    if result["macro_mode"] != "LIVE_CBR":
        raise ShadowSchedulerError("S6.1 scheduler requires accepted live CBR Macro mode")
    if not isinstance(result["significant_change"], bool):
        raise ShadowSchedulerError("significant_change must be boolean")
    if not isinstance(result["action_candidate"], bool):
        raise ShadowSchedulerError("action_candidate must be boolean")

    current_as_of = _parse_aware(result["as_of_timestamp"], "as_of_timestamp")
    if prior_as_of_timestamp is not None:
        prior_as_of = _parse_aware(prior_as_of_timestamp, "prior_as_of_timestamp")
        if current_as_of <= prior_as_of:
            raise ShadowSchedulerError(
                "scheduled cycle as_of_timestamp must advance beyond persisted state"
            )

    root_resolved = state_root.resolve()
    for field in ("market_state_path", "change_detection_path"):
        raw = result[field]
        if not isinstance(raw, str) or not raw.strip():
            raise ShadowSchedulerError(f"{field} must be a non-empty path")
        path = Path(raw)
        if not path.is_absolute():
            path = (Path.cwd() / path).resolve()
        else:
            path = path.resolve()
        if path.parent != root_resolved:
            raise ShadowSchedulerError(f"{field} escaped the explicit state_root")


def _base_status(config: SchedulerConfig, now: datetime) -> dict[str, object]:
    return {
        "project": PROJECT,
        "mode": MODE,
        "scheduler_status": "STARTING",
        "interval_seconds": config.interval_seconds,
        "max_cycles": config.max_cycles,
        "cycle_count": 0,
        "successful_cycles": 0,
        "failed_cycles": 0,
        "prior_state_present": False,
        "prior_as_of_timestamp": None,
        "last_cycle_started_at": None,
        "last_cycle_finished_at": None,
        "last_cycle_status": None,
        "last_cycle_as_of_timestamp": None,
        "last_significant_change": None,
        "last_action_candidate": None,
        "last_market_state_path": None,
        "last_change_detection_path": None,
        "last_error_class": None,
        "last_error": None,
        "updated_at": now.isoformat(),
    }


def _print_cycle_status(payload: Mapping[str, object], *, status_path: Path) -> None:
    print(f"PROJECT={PROJECT}")
    print(f"MODE={MODE}")
    print(f"SCHEDULER_STATUS={payload['scheduler_status']}")
    print(f"CYCLE_COUNT={payload['cycle_count']}")
    print(f"SUCCESSFUL_CYCLES={payload['successful_cycles']}")
    print(f"FAILED_CYCLES={payload['failed_cycles']}")
    print(f"PRIOR_STATE_PRESENT={payload['prior_state_present']}")
    print(f"PRIOR_AS_OF_TIMESTAMP={payload['prior_as_of_timestamp'] or 'NONE'}")
    print(f"LAST_CYCLE_STATUS={payload['last_cycle_status'] or 'NONE'}")
    print(f"LAST_CYCLE_AS_OF_TIMESTAMP={payload['last_cycle_as_of_timestamp'] or 'NONE'}")
    print(f"LAST_SIGNIFICANT_CHANGE={payload['last_significant_change']}")
    print(f"LAST_ACTION_CANDIDATE={payload['last_action_candidate']}")
    print(f"SCHEDULER_STATUS_PATH={status_path}")
    if payload.get("last_error_class"):
        print(f"ERROR_CLASS={payload['last_error_class']}")
        print(f"ERROR={payload['last_error']}")


def run_scheduler(
    config: SchedulerConfig,
    *,
    cycle_runner: Callable[[argparse.Namespace], Mapping[str, object]] = run_live_shadow_once,
    prior_state_loader: Callable[[Path], tuple[bool, str | None]] = _prior_state_summary,
    sleep_fn: Callable[[float], None] = time.sleep,
    now_fn: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
) -> Mapping[str, object]:
    """Run recurring shadow cycles while reusing one persistent ShadowJsonStore root."""

    _prepare_state_root(config.state_root)
    status_path = config.state_root / STATUS_FILENAME
    status = _base_status(config, _aware_now(now_fn))
    _write_atomic_json(status_path, status)

    try:
        while config.max_cycles == 0 or int(status["cycle_count"]) < config.max_cycles:
            cycle_number = int(status["cycle_count"]) + 1
            cycle_started_at = _aware_now(now_fn)
            status["cycle_count"] = cycle_number
            status["last_cycle_started_at"] = cycle_started_at.isoformat()
            status["scheduler_status"] = "RUNNING"
            status["last_error_class"] = None
            status["last_error"] = None

            try:
                prior_present, prior_as_of = prior_state_loader(config.state_root)
                status["prior_state_present"] = prior_present
                status["prior_as_of_timestamp"] = prior_as_of

                result = dict(cycle_runner(_live_cycle_args(config)))
                _validate_cycle_result(
                    result,
                    state_root=config.state_root,
                    prior_as_of_timestamp=prior_as_of,
                )
            except Exception as exc:
                finished_at = _aware_now(now_fn)
                status["failed_cycles"] = int(status["failed_cycles"]) + 1
                status["scheduler_status"] = "BLOCKED"
                status["last_cycle_finished_at"] = finished_at.isoformat()
                status["last_cycle_status"] = "BLOCKED"
                status["last_error_class"] = exc.__class__.__name__
                status["last_error"] = str(exc)
                status["updated_at"] = finished_at.isoformat()
                _write_atomic_json(status_path, status)
                _print_cycle_status(status, status_path=status_path)
                return dict(status)

            finished_at = _aware_now(now_fn)
            status["successful_cycles"] = int(status["successful_cycles"]) + 1
            status["last_cycle_finished_at"] = finished_at.isoformat()
            status["last_cycle_status"] = "COMPLETED"
            status["last_cycle_as_of_timestamp"] = result["as_of_timestamp"]
            status["last_significant_change"] = result["significant_change"]
            status["last_action_candidate"] = result["action_candidate"]
            status["last_market_state_path"] = result["market_state_path"]
            status["last_change_detection_path"] = result["change_detection_path"]
            status["updated_at"] = finished_at.isoformat()

            finite_complete = config.max_cycles > 0 and cycle_number >= config.max_cycles
            status["scheduler_status"] = "COMPLETED" if finite_complete else "RUNNING"
            _write_atomic_json(status_path, status)
            _print_cycle_status(status, status_path=status_path)

            if finite_complete:
                return dict(status)
            sleep_fn(float(config.interval_seconds))
    except KeyboardInterrupt:
        stopped_at = _aware_now(now_fn)
        status["scheduler_status"] = "STOPPED"
        status["updated_at"] = stopped_at.isoformat()
        _write_atomic_json(status_path, status)
        _print_cycle_status(status, status_path=status_path)
        return dict(status)

    return dict(status)


def _print_blocked(exc: Exception) -> None:
    print(f"PROJECT={PROJECT}")
    print(f"MODE={MODE}")
    print("SCHEDULER_STATUS=BLOCKED")
    print(f"ERROR_CLASS={exc.__class__.__name__}")
    print(f"ERROR={exc}")


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        config = _validate_config(args)
        with _single_instance_lock(config.state_root):
            result = run_scheduler(config)
    except Exception as exc:
        _print_blocked(exc)
        return 2

    status = result.get("scheduler_status")
    if status == "COMPLETED":
        return 0
    if status == "STOPPED":
        return 130
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
