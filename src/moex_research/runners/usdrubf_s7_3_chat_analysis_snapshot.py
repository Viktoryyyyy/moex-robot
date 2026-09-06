from __future__ import annotations

import argparse
from contextlib import contextmanager
from dataclasses import asdict, dataclass, is_dataclass
from datetime import date, datetime, timezone
import fcntl
import json
import math
import os
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Callable, Iterator, Mapping, Sequence

from dotenv import load_dotenv

from moex_data import step9_rub_analysis_bundle as step9
from moex_research.external_data import moex_cnyrub_algopack_history as cny_spot
from moex_research.external_data import moex_cnyrubf_algopack_history as cny_futures
from moex_research.external_data.moex_cnyrub_algopack_timestamp_policy import (
    install_timestamp_policy,
)
from src.moex_research.intelligence.usdrubf_live_shadow_bridge import (
    MOSCOW,
    SECID_KEY,
    build_live_decision_input,
    closed_bars,
    find_prior_session,
    load_futoi_context,
)
from src.moex_research.runners import usdrubf_live_shadow_smoke as live


PROJECT = "MOEX_Bot"
MODE = "s7_3_chat_analysis_snapshot"
SCHEMA_VERSION = "rub_chat_analysis_snapshot.v1"
REFRESH_INTERVAL_SECONDS = 600
STALE_AFTER_SECONDS = 1200
STATE_RELATIVE_DIR = Path("state/rub_intelligence/chat_analysis_snapshot")
CURRENT_FILENAME = "current.json"
LOCK_FILENAME = ".refresh.lock"
PROJECT_ENV_PATH = Path(__file__).resolve().parents[4] / ".env"


class ChatAnalysisSnapshotError(RuntimeError):
    pass


@dataclass(frozen=True)
class ProducedComponent:
    data: Mapping[str, object]
    data_as_of: datetime | str | None


ComponentProducer = Callable[[datetime], ProducedComponent]


def _aware(value: datetime | str, field: str) -> datetime:
    if isinstance(value, str):
        try:
            value = datetime.fromisoformat(value)
        except ValueError as exc:
            raise ChatAnalysisSnapshotError(f"{field} must be ISO datetime") from exc
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ChatAnalysisSnapshotError(f"{field} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _iso(value: datetime) -> str:
    return _aware(value, "timestamp").isoformat()


def _jsonable(value: object) -> object:
    if is_dataclass(value):
        return _jsonable(asdict(value))
    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            raise ChatAnalysisSnapshotError("naive datetime cannot enter snapshot")
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_jsonable(item) for item in value]
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ChatAnalysisSnapshotError("non-finite float cannot enter snapshot")
        return value
    return value


def _data_root() -> Path:
    raw = os.environ.get("MOEX_DATA_ROOT", "")
    if not raw or raw != raw.strip():
        raise ChatAnalysisSnapshotError("MOEX_DATA_ROOT is required without surrounding whitespace")
    root = Path(raw)
    if not root.is_absolute():
        raise ChatAnalysisSnapshotError("MOEX_DATA_ROOT must be absolute")
    if root.is_symlink() or not root.is_dir():
        raise ChatAnalysisSnapshotError("MOEX_DATA_ROOT must be an existing non-symlink directory")
    return root.resolve(strict=True)


def snapshot_state_dir(root: Path) -> Path:
    candidate = root / STATE_RELATIVE_DIR
    candidate.mkdir(parents=True, exist_ok=True)
    if candidate.is_symlink() or not candidate.is_dir():
        raise ChatAnalysisSnapshotError("snapshot state directory must be a regular directory")
    resolved = candidate.resolve(strict=True)
    try:
        resolved.relative_to(root)
    except ValueError as exc:
        raise ChatAnalysisSnapshotError("snapshot state directory escaped MOEX_DATA_ROOT") from exc
    return resolved


def current_snapshot_path(root: Path) -> Path:
    return snapshot_state_dir(root) / CURRENT_FILENAME


def _load_previous(path: Path) -> Mapping[str, object] | None:
    if not path.exists():
        return None
    if path.is_symlink() or not path.is_file():
        raise ChatAnalysisSnapshotError("current snapshot must be a regular non-symlink file")
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise ChatAnalysisSnapshotError("current snapshot is not valid JSON") from exc
    if not isinstance(value, Mapping):
        raise ChatAnalysisSnapshotError("current snapshot must contain an object")
    identity = value.get("identity")
    if not isinstance(identity, Mapping):
        raise ChatAnalysisSnapshotError("current snapshot missing identity")
    if value.get("schema_version") != SCHEMA_VERSION or identity.get("project") != PROJECT:
        raise ChatAnalysisSnapshotError("current snapshot identity/schema mismatch")
    components = value.get("components")
    if not isinstance(components, Mapping):
        raise ChatAnalysisSnapshotError("current snapshot missing components")
    return value


def _atomic_write(path: Path, payload: Mapping[str, object]) -> None:
    if path.exists() and path.is_symlink():
        raise ChatAnalysisSnapshotError("snapshot output must not be a symlink")
    path.parent.mkdir(parents=True, exist_ok=True)
    serialized = json.dumps(
        _jsonable(payload),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ) + "\n"
    temp_name: str | None = None
    try:
        with NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=path.parent,
            prefix=".current.",
            suffix=".tmp",
            delete=False,
        ) as handle:
            temp_name = handle.name
            handle.write(serialized)
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(temp_name, 0o640)
        os.replace(temp_name, path)
        temp_name = None
    finally:
        if temp_name is not None:
            try:
                Path(temp_name).unlink(missing_ok=True)
            except OSError:
                pass


@contextmanager
def _single_refresh_lock(state_dir: Path) -> Iterator[None]:
    lock_path = state_dir / LOCK_FILENAME
    handle = lock_path.open("a+", encoding="utf-8")
    try:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise ChatAnalysisSnapshotError("another chat snapshot refresh is already running") from exc
        yield
    finally:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        finally:
            handle.close()


def _stage9_component(scope: str) -> ComponentProducer:
    def produce(now: datetime) -> ProducedComponent:
        bundle = step9.build_analysis_bundle(scope=scope, as_of=_iso(now))
        freshness = bundle["server_core"]["freshness_alignment"]
        newest = freshness.get("newest_selected_causal_ts_utc")
        return ProducedComponent(data=bundle, data_as_of=newest)

    return produce


def _live_market_component(now: datetime) -> ProducedComponent:
    now_moscow = now.astimezone(MOSCOW)
    current_trade_date = now_moscow.date()
    current_raw = tuple(live._load_bars(SECID_KEY, current_trade_date))
    if not current_raw:
        raise ChatAnalysisSnapshotError("current Moscow date has no USDRUBF 5m bars")
    current_closed = closed_bars(current_raw, as_of_timestamp=now_moscow)
    if not current_closed:
        raise ChatAnalysisSnapshotError("current Moscow date has no closed USDRUBF 5m bars")
    market_data_as_of = current_closed[-1]["end"]
    prior_trade_date, prior_bars = find_prior_session(
        current_trade_date,
        loader=live._load_bars,
        max_lookback_days=7,
    )
    futoi = load_futoi_context(
        prior_trade_date=prior_trade_date,
        current_trade_date=current_trade_date,
        fallback_available_at=market_data_as_of,
        enabled=False,
    )
    inputs = build_live_decision_input(
        current_session_bars=current_closed,
        prior_session_bars=prior_bars,
        wall_clock_as_of=now_moscow,
        futoi_context=futoi,
        news_events=(),
        macro_state=None,
    )
    data = {
        "instrument": inputs.instrument,
        "trade_date": current_trade_date.isoformat(),
        "prior_trade_date": prior_trade_date.isoformat(),
        "market_data_as_of": market_data_as_of,
        "price": inputs.price,
        "trend": inputs.trend,
        "market_regime": inputs.market_regime,
        "active_levels": inputs.active_levels,
        "level_interactions": inputs.level_interactions,
        "ema_3_19": {
            **asdict(inputs.ema_3_19_ai),
            "standalone_directional_authority": False,
            "s7_2_verdict": "REJECT_AS_STANDALONE_DIRECTIONAL_SIGNAL",
        },
        "futoi": {
            **asdict(inputs.futoi),
            "action_authority": False,
        },
        "current_closed_5m_bar_count": len(current_closed),
        "prior_session_5m_bar_count": len(prior_bars),
    }
    return ProducedComponent(data=data, data_as_of=market_data_as_of)


def _macro_component(now: datetime) -> ProducedComponent:
    del now
    state, macro_as_of = live._load_current_cbr_macro_state()
    data = {
        "mode": "LIVE_CBR",
        "state": state,
        "action_authority": False,
    }
    return ProducedComponent(data=data, data_as_of=macro_as_of)


def _news_component(now: datetime) -> ProducedComponent:
    del now
    events, news_as_of, summary = live._load_current_live_news(
        timeout_seconds=10.0,
        max_events=20,
    )
    data = {
        "mode": "LIVE_RSS_DETERMINISTIC_NEUTRAL",
        "summary": dict(summary),
        "events": events,
        "directional_action_authority": False,
    }
    return ProducedComponent(data=data, data_as_of=news_as_of)


def _cny_spot_component(now: datetime) -> ProducedComponent:
    trade_date = now.astimezone(MOSCOW).date()
    identity = cny_spot.load_security_identity()
    candles = cny_spot.load_daily_history(identity, from_date=trade_date, till_date=trade_date)
    if not candles:
        raise ChatAnalysisSnapshotError("CNYRUB_TOM returned no current-date AlgoPack rows")
    candle = candles[-1]
    data = {
        "mode": "LIVE_ALGOPACK_PARTIAL_DAY_CONTEXT",
        "observation": candle,
        "action_authority": False,
        "partial_day": True,
    }
    return ProducedComponent(data=data, data_as_of=candle.source_available_at)


def _cny_futures_component(now: datetime) -> ProducedComponent:
    trade_date = now.astimezone(MOSCOW).date()
    identity = cny_futures.load_security_identity()
    candles = cny_futures.load_daily_history(identity, from_date=trade_date, till_date=trade_date)
    if not candles:
        raise ChatAnalysisSnapshotError("CNYRUBF returned no current-date AlgoPack rows")
    candle = candles[-1]
    data = {
        "mode": "LIVE_ALGOPACK_PARTIAL_DAY_CONTEXT",
        "observation": candle,
        "action_authority": False,
        "partial_day": True,
    }
    return ProducedComponent(data=data, data_as_of=candle.source_available_at)


def default_producers() -> Mapping[str, ComponentProducer]:
    return {
        "stage9_daily": _stage9_component("daily"),
        "stage9_weekly": _stage9_component("weekly"),
        "live_market_structure": _live_market_component,
        "cbr_macro": _macro_component,
        "official_news": _news_component,
        "cnyrub_spot_live": _cny_spot_component,
        "cnyrubf_live": _cny_futures_component,
    }


def _previous_component(previous: Mapping[str, object] | None, name: str) -> Mapping[str, object] | None:
    if previous is None:
        return None
    components = previous.get("components")
    if not isinstance(components, Mapping):
        return None
    value = components.get(name)
    if not isinstance(value, Mapping) or value.get("data") is None:
        return None
    return value


def _component_payload(
    name: str,
    producer: ComponentProducer,
    *,
    now: datetime,
    previous: Mapping[str, object] | None,
) -> dict[str, object]:
    attempted_at = _iso(now)
    try:
        produced = producer(now)
        raw_as_of = produced.data_as_of
        data_as_of = None if raw_as_of is None else _iso(_aware(raw_as_of, f"{name}.data_as_of"))
        return {
            "status": "READY",
            "refresh_attempted_at": attempted_at,
            "last_success_at": attempted_at,
            "data_as_of": data_as_of,
            "refresh_error_class": None,
            "refresh_error": None,
            "data": _jsonable(produced.data),
        }
    except Exception as exc:
        prior = _previous_component(previous, name)
        if prior is not None:
            return {
                "status": "RETAINED_PREVIOUS",
                "refresh_attempted_at": attempted_at,
                "last_success_at": prior.get("last_success_at"),
                "data_as_of": prior.get("data_as_of"),
                "refresh_error_class": exc.__class__.__name__,
                "refresh_error": str(exc),
                "data": prior.get("data"),
            }
        return {
            "status": "UNAVAILABLE",
            "refresh_attempted_at": attempted_at,
            "last_success_at": None,
            "data_as_of": None,
            "refresh_error_class": exc.__class__.__name__,
            "refresh_error": str(exc),
            "data": None,
        }


def _stage9_blocks(component: Mapping[str, object], prefixes: Sequence[str]) -> list[object]:
    data = component.get("data")
    if not isinstance(data, Mapping):
        return []
    core = data.get("server_core")
    if not isinstance(core, Mapping):
        return []
    blocks = core.get("blocks")
    if not isinstance(blocks, Sequence) or isinstance(blocks, (str, bytes)):
        return []
    result: list[object] = []
    for block in blocks:
        if not isinstance(block, Mapping):
            continue
        block_id = str(block.get("block_id", ""))
        if any(block_id.startswith(prefix) for prefix in prefixes):
            result.append(block)
    return result


def build_snapshot(
    *,
    now: datetime,
    previous: Mapping[str, object] | None = None,
    producers: Mapping[str, ComponentProducer] | None = None,
) -> dict[str, object]:
    now_utc = _aware(now, "now")
    selected_producers = dict(default_producers() if producers is None else producers)
    required = {
        "stage9_daily",
        "stage9_weekly",
        "live_market_structure",
        "cbr_macro",
        "official_news",
        "cnyrub_spot_live",
        "cnyrubf_live",
    }
    if set(selected_producers) != required:
        raise ChatAnalysisSnapshotError("producer set mismatch")

    components = {
        name: _component_payload(
            name,
            selected_producers[name],
            now=now_utc,
            previous=previous,
        )
        for name in sorted(required)
    }
    components["oil"] = {
        "status": "GOVERNED_BLOCKED",
        "refresh_attempted_at": _iso(now_utc),
        "last_success_at": None,
        "data_as_of": None,
        "refresh_error_class": None,
        "refresh_error": None,
        "data": {
            "reason": "no oil source is currently LIVE_ACCEPTED for this RUB Intelligence snapshot",
            "moex_brent_futures_daily": "GOVERNED_BLOCKED",
            "cme_wti_pre_moex": "GOVERNED_BLOCKED",
            "missing_oil_must_not_be_interpreted_as_neutral": True,
            "action_authority": False,
        },
    }

    daily = components["stage9_daily"]
    market = components["live_market_structure"]
    carry_blocks = _stage9_blocks(daily, ("stage4.basis.",))
    cny_blocks = _stage9_blocks(
        daily,
        (
            "stage3.spot.cny_tom",
            "stage3.quote.cr_",
            "stage3.oi.cr_",
            "stage5.futoi_eod.cr_",
            "stage5.positioning.cr_",
        ),
    )

    statuses = {name: str(value["status"]) for name, value in components.items()}
    unavailable = sorted(name for name, status in statuses.items() if status == "UNAVAILABLE")
    retained = sorted(name for name, status in statuses.items() if status == "RETAINED_PREVIOUS")
    overall_status = "READY" if not unavailable and not retained else "PARTIAL"

    market_data = market.get("data") if isinstance(market, Mapping) else None
    levels = []
    interactions = []
    if isinstance(market_data, Mapping):
        levels = list(market_data.get("active_levels", []))
        interactions = list(market_data.get("level_interactions", []))

    return {
        "schema_version": SCHEMA_VERSION,
        "identity": {
            "project": PROJECT,
            "mode": MODE,
            "generated_at_utc": _iso(now_utc),
            "snapshot_kind": "server_persisted_data_only_context_for_separate_analysis_chats",
        },
        "refresh_policy": {
            "expected_refresh_interval_seconds": REFRESH_INTERVAL_SECONDS,
            "snapshot_stale_after_seconds": STALE_AFTER_SECONDS,
            "component_failure_policy": "retain_previous_component_if_available_else_unavailable",
            "atomic_publish": True,
        },
        "readiness": {
            "status": overall_status,
            "component_statuses": statuses,
            "unavailable_components": unavailable,
            "retained_previous_components": retained,
        },
        "components": components,
        "analysis_views": {
            "levels": levels,
            "level_interactions": interactions,
            "carry": carry_blocks,
            "cny_accepted_context": cny_blocks,
            "cny_live_component_refs": ["cnyrub_spot_live", "cnyrubf_live"],
            "rates_component_ref": "cbr_macro",
            "news_component_ref": "official_news",
            "oil_component_ref": "oil",
        },
        "analysis_workflow": {
            "weekly_regime": {
                "consumer": "SEPARATE_ANALYSIS_CHAT",
                "component_refs": ["stage9_weekly", "cbr_macro", "official_news", "cnyrub_spot_live", "cnyrubf_live", "oil"],
            },
            "daily_structure": {
                "consumer": "SEPARATE_ANALYSIS_CHAT",
                "component_refs": ["stage9_daily", "live_market_structure"],
            },
            "levels": {
                "consumer": "SEPARATE_ANALYSIS_CHAT",
                "view_ref": "analysis_views.levels",
            },
            "carry_rates": {
                "consumer": "SEPARATE_ANALYSIS_CHAT",
                "view_refs": ["analysis_views.carry", "components.cbr_macro"],
            },
            "cny_oil": {
                "consumer": "SEPARATE_ANALYSIS_CHAT",
                "component_refs": ["cnyrub_spot_live", "cnyrubf_live", "oil"],
            },
            "news_macro": {
                "consumer": "SEPARATE_ANALYSIS_CHAT",
                "component_refs": ["official_news", "cbr_macro"],
            },
            "scenario": {"consumer": "SEPARATE_ANALYSIS_CHAT", "server_generated": False},
            "buy_sell_out": {"consumer": "SEPARATE_ANALYSIS_CHAT", "server_generated": False},
            "invalidation": {"consumer": "SEPARATE_ANALYSIS_CHAT", "server_generated": False},
        },
        "authority": {
            "data_only": True,
            "server_generates_market_analysis": False,
            "server_generates_scenario": False,
            "server_generates_buy_sell_out": False,
            "server_generates_invalidation": False,
            "ema_standalone_directional_authority": False,
            "ema_s7_2_verdict": "REJECT_AS_STANDALONE_DIRECTIONAL_SIGNAL",
            "news_directional_action_authority": False,
            "broker_execution": False,
            "telegram_delivery": False,
        },
    }


def refresh_snapshot(
    *,
    now_fn: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
    producers: Mapping[str, ComponentProducer] | None = None,
) -> tuple[dict[str, object], Path]:
    load_dotenv(PROJECT_ENV_PATH, override=False)
    install_timestamp_policy()
    root = _data_root()
    state_dir = snapshot_state_dir(root)
    path = state_dir / CURRENT_FILENAME
    with _single_refresh_lock(state_dir):
        previous = _load_previous(path)
        now = _aware(now_fn(), "clock")
        snapshot = build_snapshot(now=now, previous=previous, producers=producers)
        _atomic_write(path, snapshot)
    return snapshot, path


def read_current_snapshot(
    *,
    now_fn: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
) -> tuple[dict[str, object], Path]:
    root = _data_root()
    path = current_snapshot_path(root)
    snapshot = _load_previous(path)
    if snapshot is None:
        raise ChatAnalysisSnapshotError("current snapshot does not exist")
    identity = snapshot["identity"]
    generated = _aware(identity["generated_at_utc"], "generated_at_utc")
    now = _aware(now_fn(), "clock")
    if generated > now:
        raise ChatAnalysisSnapshotError("current snapshot generated_at_utc is in the future")
    age = int((now - generated).total_seconds())
    result = dict(snapshot)
    result["read_freshness"] = {
        "read_at_utc": _iso(now),
        "snapshot_age_seconds": age,
        "status": "FRESH" if age <= STALE_AFTER_SECONDS else "STALE",
    }
    return result, path


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Publish/read S7.3 server snapshot for analysis chats")
    action = parser.add_mutually_exclusive_group(required=True)
    action.add_argument("--refresh", action="store_true")
    action.add_argument("--read-current", action="store_true")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        if args.refresh:
            snapshot, path = refresh_snapshot()
            readiness = snapshot["readiness"]
            print(f"PROJECT={PROJECT}")
            print(f"MODE={MODE}")
            print("STATUS=COMPLETED")
            print(f"SNAPSHOT_STATUS={readiness['status']}")
            print(f"SNAPSHOT_PATH={path}")
            print(f"GENERATED_AT_UTC={snapshot['identity']['generated_at_utc']}")
            print("COMPONENT_STATUSES=" + json.dumps(readiness["component_statuses"], sort_keys=True))
            return 0
        snapshot, _ = read_current_snapshot()
        print(json.dumps(snapshot, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False))
        return 0
    except Exception as exc:
        print(f"PROJECT={PROJECT}")
        print(f"MODE={MODE}")
        print("STATUS=BLOCKED")
        print(f"ERROR_CLASS={exc.__class__.__name__}")
        print(f"ERROR={exc}")
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
