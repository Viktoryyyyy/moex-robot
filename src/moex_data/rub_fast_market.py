"""Independent persisted market-data refresh; readers never fetch quotes."""
from __future__ import annotations

import argparse
from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import json

SCHEMA = "rub_fast_market.v1"
MAX_AGE_SECONDS = 60
MAX_BYTES = 2_000_000


def _time(value):
    value = datetime.fromisoformat(value) if isinstance(value, str) else value
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("aware timestamp required")
    return value.astimezone(timezone.utc)


def _digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, allow_nan=False,
                                    separators=(",", ":")).encode()).hexdigest()


def state_path(root):
    from src.moex_research.runners import usdrubf_s7_3_chat_analysis_snapshot as base
    parent = root / base.STATE_RELATIVE_DIR
    if parent.is_symlink() or not parent.resolve().is_relative_to(root.resolve()):
        raise ValueError("snapshot path escaped data root")
    folder = parent / "fast_market"
    if folder.is_symlink() or not folder.resolve().is_relative_to(parent.resolve()):
        raise ValueError("fast market path escaped state directory")
    return folder


def refresh(root, *, loader=None, clock=lambda: datetime.now(timezone.utc)):
    from src.moex_research.runners import usdrubf_s7_3_chat_analysis_snapshot as base
    from moex_data.synchronized_live_market_oi_context_partial import fetch_live_snapshot
    folder = state_path(root)
    folder.mkdir(parents=True, exist_ok=True)
    with base._single_refresh_lock(folder):
        started = _time(clock())
        try:
            market = (fetch_live_snapshot if loader is None else loader)()
            _digest(market)
            status, error = "COLLECTED", None
        except Exception as exc:
            market, status, error = None, "FAILED", type(exc).__name__
        completed = _time(clock())
        if completed < started:
            market, status, error = None, "FAILED", "ClockRegression"
        value = dict(schema_version=SCHEMA, started_at=started.isoformat(),
                     completed_at=completed.isoformat(), status=status,
                     error_class=error, market=market, market_sha256=_digest(market))
        base._atomic_write(folder / "current.json", value)
        return value


def apply(snapshot, *, root, now):
    """Overlay only opted-in fast facts and their derived basis, then age-check."""
    folder = state_path(root)
    marker = folder / "enabled"
    if not marker.exists() and not marker.is_symlink():
        return snapshot
    from src.moex_research.runners import usdrubf_s7_3_chat_analysis_snapshot_live_market_oi as live
    from moex_data.synchronized_live_market_oi_context import LOGICAL_ORDER, SCHEMA_VERSION
    result = deepcopy(snapshot)
    now = _time(now)
    completed = None
    try:
        if marker.is_symlink() or not marker.is_file():
            raise ValueError("invalid enabled marker")
        if marker.stat().st_size > 256 or marker.read_text(encoding="utf-8").strip() != SCHEMA:
            raise ValueError("invalid enabled marker schema")
        path = folder / "current.json"
        if path.is_symlink() or not path.is_file() or path.stat().st_size > MAX_BYTES:
            raise ValueError("missing or invalid fast market file")
        value = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(value, dict) or value.get("schema_version") != SCHEMA or value.get("status") != "COLLECTED":
            raise ValueError("fast market collection unavailable")
        started, completed = _time(value["started_at"]), _time(value["completed_at"])
        if not started <= completed <= now or (now - started).total_seconds() > MAX_AGE_SECONDS:
            raise ValueError("fast market generation expired or future")
        market = value["market"]
        if _digest(market) != value["market_sha256"]:
            raise ValueError("fast market digest mismatch")
        if not isinstance(market, dict) or not isinstance(market.get("instruments"), dict) or set(market["instruments"]) != set(LOGICAL_ORDER):
            raise ValueError("fast market instrument scope mismatch")
        if market.get("schema_version") != SCHEMA_VERSION or not isinstance(market.get("bindings"), dict):
            raise ValueError("fast market schema or bindings missing")
        for key, item in market["instruments"].items():
            if not isinstance(item, dict):
                raise ValueError("invalid market row")
            if item.get("logical_id") != key or not item.get("secid") or market["bindings"].get(key) != item["secid"]:
                raise ValueError("market row identity mismatch")
            _time(item["timestamp"])
            if _time(item["received_at_utc"]) > completed:
                raise ValueError("source receipt exceeds completion")
        if not isinstance(market.get("quality"), dict) or not isinstance(market.get("synchronization"), dict):
            raise ValueError("missing market quality")
        error = None
    except (ValueError, TypeError, KeyError, OverflowError, OSError) as exc:
        market = {"status": "UNAVAILABLE", "quality": {}, "synchronization": {},
                  "error_class": type(exc).__name__, "error": str(exc)}
        error = str(exc)
    attempted = now.isoformat() if completed is None else completed.isoformat()
    live.attach_live_market_oi_context(result, market, attempted_at_utc=attempted)
    live.attach_live_basis_carry_context(result, market, attempted_at_utc=attempted)
    result["fast_market_read"] = dict(schema_version=SCHEMA, read_at=now.isoformat(),
        completed_at=completed.isoformat() if completed else None,
        error=error, network_fetch_performed=False)
    return result


def main():
    from dotenv import load_dotenv
    from src.moex_research.runners import usdrubf_s7_3_chat_analysis_snapshot as base
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--enable", action="store_true")
    args = parser.parse_args()
    load_dotenv(base.PROJECT_ENV_PATH, override=False)
    root = base._data_root()
    value = refresh(root)
    if args.enable:
        if value["status"] != "COLLECTED":
            raise RuntimeError("initial fast collection failed")
        marker = state_path(root) / "enabled"
        if marker.is_symlink():
            raise ValueError("enabled marker must not be a symlink")
        marker.write_text(SCHEMA, encoding="utf-8")
    print(json.dumps({k: v for k, v in value.items() if k != "market"}))
    return 0 if value["status"] == "COLLECTED" else 1


if __name__ == "__main__":
    raise SystemExit(main())
