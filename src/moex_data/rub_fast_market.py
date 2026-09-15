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
        previous = None
        previous_completed = None
        prior_path = folder / "current.json"
        if prior_path.is_file() and not prior_path.is_symlink() and prior_path.stat().st_size <= MAX_BYTES:
            try:
                prior = json.loads(prior_path.read_text(encoding='utf-8'))
                previous = prior.get('accepted_dated_market') if isinstance(prior, dict) else None
                if isinstance(prior,dict) and prior.get('schema_version')==SCHEMA:
                    previous_completed=_time(prior['completed_at'])
            except (OSError, ValueError,TypeError,KeyError):
                pass
        started = _time(clock())
        if previous_completed is not None and started<previous_completed:
            raise ValueError('fast_capture_clock_reversed_before_previous_completion')
        dated_evidence = {}
        try:
            market = fetch_live_snapshot(dated_evidence_sink=dated_evidence) if loader is None else loader()
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
        from moex_data.rub_dated_context import capture
        from src.moex_research.runners import usdrubf_s7_3_chat_analysis_snapshot_live_market_oi as live
        witness = {'components': {}, 'authority': {}, 'analysis_views': {}, 'analysis_workflow': {}}
        if status == 'COLLECTED':
            live.attach_live_market_oi_context(witness, market, attempted_at_utc=completed.isoformat())
            live.attach_live_basis_carry_context(witness, market, attempted_at_utc=completed.isoformat())
        value['accepted_dated_market'] = capture(previous, components=witness['components'], now=completed, kind='market')
        from moex_data.rub_dated_market_source import capture as capture_source
        value['accepted_dated_market'] = capture_source(value['accepted_dated_market'], dated_evidence, now=completed)
        from moex_data import rub_contract_price_market_oi_observed as paired
        if isinstance(market,dict):
            value[paired.CURRENT_KEY]=paired.capture_current(market,started=started,completed=completed,
                now_fn=clock if market.get('original_forts_http_evidence') is not None else None)
            value['completed_at']=value[paired.CURRENT_KEY]['capture']['captured_at_utc']
            market.pop('original_forts_http_evidence',None)
            value['market_sha256']=_digest(market)
        def fits(candidate):
            return len(json.dumps(candidate,sort_keys=True,ensure_ascii=False,allow_nan=False,
                                  separators=(',',':')).encode('utf-8'))+1<=MAX_BYTES
        if not fits(value):
            # Optional paired proof must not evict independently admitted facts.
            value.pop(paired.CURRENT_KEY,None)
            value['optional_current_proof_error']='FastMarketByteLimit'
        if not fits(value):
            value.update(status='FAILED',error_class='FastMarketByteLimit',
                         market=None,market_sha256=_digest(None))
        if not fits(value):
            # Re-admit only the previous independent witnesses at this read clock;
            # capture with no new components preserves their original timestamps.
            value['accepted_dated_market']=capture(previous,components={},
                now=_time(value['completed_at']),kind='market')
        if not fits(value):
            value['accepted_dated_market']=None
        if not fits(value):
            raise ValueError('FastMarketByteLimit')
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
    dated = None
    paired_current = None
    optional_current_proof_error = None
    try:
        if marker.is_symlink() or not marker.is_file():
            raise ValueError("invalid enabled marker")
        if marker.stat().st_size > 256 or marker.read_text(encoding="utf-8").strip() != SCHEMA:
            raise ValueError("invalid enabled marker schema")
        path = folder / "current.json"
        if path.is_symlink() or not path.is_file() or path.stat().st_size > MAX_BYTES:
            raise ValueError("missing or invalid fast market file")
        value = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(value, dict) and value.get('schema_version') == SCHEMA:
            dated = value.get('accepted_dated_market')
            if value.get('optional_current_proof_error')=='FastMarketByteLimit':
                optional_current_proof_error='FastMarketByteLimit'
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
        from moex_data.rub_contract_price_market_oi_observed import CURRENT_KEY
        paired_current=value.get(CURRENT_KEY)
    except (ValueError, TypeError, KeyError, OverflowError, OSError) as exc:
        market = {"status": "UNAVAILABLE", "quality": {}, "synchronization": {},
                  "error_class": type(exc).__name__, "error": str(exc)}
        error = str(exc)
    attempted = now.isoformat() if completed is None else completed.isoformat()
    live.attach_live_market_oi_context(result, market, attempted_at_utc=attempted)
    live.attach_live_basis_carry_context(result, market, attempted_at_utc=attempted)
    result["fast_market_read"] = dict(schema_version=SCHEMA, read_at=now.isoformat(),
        completed_at=completed.isoformat() if completed else None,
        error=error, network_fetch_performed=False,
        optional_current_proof_error=optional_current_proof_error)
    result['accepted_dated_market'] = deepcopy(dated)
    from moex_data.rub_contract_price_market_oi_observed import CURRENT_KEY
    result[CURRENT_KEY]=deepcopy(paired_current)
    return result


def collection_summary(value):
    """Bounded CLI diagnostics; raw dated evidence stays only in canonical state."""
    from moex_data.rub_contract_price_market_oi_observed import CURRENT_KEY
    result = {key: item for key, item in value.items() if key not in ('market', 'accepted_dated_market',CURRENT_KEY)}
    paired=value.get(CURRENT_KEY) or {}
    current=paired.get('capture') or {}
    result['contract_price_market_oi_current']={'status':'AVAILABLE' if current.get('facts') else 'UNAVAILABLE',
        'original_buffer_count':len(current.get('original_byte_buffers',{}))}
    store = value.get('accepted_dated_market') or {}
    result['accepted_dated_market'] = {'frame_count': len(store.get('frames', {})),
        'selected_observation_ids': sorted(store.get('selections', {})),
        'last_source_refusal_count': len(store.get('last_source_admission_refusals', {}))}
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
    print(json.dumps(collection_summary(value)))
    return 0 if value["status"] == "COLLECTED" else 1


if __name__ == "__main__":
    raise SystemExit(main())
