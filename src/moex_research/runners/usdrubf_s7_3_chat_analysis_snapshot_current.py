from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, timezone
from typing import Mapping, Sequence

from moex_data.futures import front_next_binding
from src.moex_research.runners import usdrubf_live_shadow_smoke as live
from src.moex_research.runners import usdrubf_s7_3_chat_analysis_snapshot as base
from src.moex_research.runners import usdrubf_s7_3_chat_analysis_snapshot_futoi as futoi


PROJECT = base.PROJECT
MODE = base.MODE
SOURCE_ID = "moex_algopack_fo_tradestats_5m"
SOURCE_CONTRACT_REF = "contracts/sources/futures/moex_algopack_fo_tradestats_5m.v1.yaml"


class CurrentChatSnapshotError(base.ChatAnalysisSnapshotError):
    pass


def _finite_number(value: object, field: str) -> float:
    if isinstance(value, bool):
        raise CurrentChatSnapshotError(f"{field} must be finite numeric")
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise CurrentChatSnapshotError(f"{field} must be finite numeric") from exc
    if not math.isfinite(number):
        raise CurrentChatSnapshotError(f"{field} must be finite numeric")
    return number


def _cnyrubf_live_component(now: datetime) -> base.ProducedComponent:
    now_utc = base._aware(now, "cnyrubf_live.now")
    now_moscow = now_utc.astimezone(base.MOSCOW)
    trade_date = now_moscow.date()
    trade_date_text = trade_date.isoformat()

    bindings = front_next_binding.discover_front_next(
        root="CR",
        as_of_date=trade_date_text,
    )
    front = next((item for item in bindings if item.get("role") == "front"), None)
    if not isinstance(front, Mapping):
        raise CurrentChatSnapshotError("current CR front binding is missing")
    secid = str(front.get("secid") or "").strip()
    if not secid:
        raise CurrentChatSnapshotError("current CR front binding has no secid")
    if str(front.get("as_of_date") or "") != trade_date_text:
        raise CurrentChatSnapshotError("current CR front binding date mismatch")

    raw_bars = tuple(live._load_bars(secid, trade_date))
    if not raw_bars:
        raise CurrentChatSnapshotError("current CR front has no 5m TradeStats bars")
    closed = tuple(base.closed_bars(raw_bars, as_of_timestamp=now_moscow))
    if not closed:
        raise CurrentChatSnapshotError("current CR front has no closed 5m TradeStats bars")

    normalized: list[dict[str, object]] = []
    previous_end: datetime | None = None
    for index, bar in enumerate(closed):
        if not isinstance(bar, Mapping):
            raise CurrentChatSnapshotError("current CR 5m bar must be an object")
        end = bar.get("end")
        if not isinstance(end, datetime) or end.tzinfo is None or end.utcoffset() is None:
            raise CurrentChatSnapshotError("current CR 5m bar end must be timezone-aware")
        end_moscow = end.astimezone(base.MOSCOW)
        if end_moscow.date() != trade_date:
            raise CurrentChatSnapshotError("current CR 5m bar trade date mismatch")
        if previous_end is not None and end <= previous_end:
            raise CurrentChatSnapshotError("current CR 5m bars are not strictly increasing")
        previous_end = end
        normalized.append(
            {
                "end": end,
                "open": _finite_number(bar.get("open"), f"bar[{index}].open"),
                "high": _finite_number(bar.get("high"), f"bar[{index}].high"),
                "low": _finite_number(bar.get("low"), f"bar[{index}].low"),
                "close": _finite_number(bar.get("close"), f"bar[{index}].close"),
                "volume": _finite_number(bar.get("volume"), f"bar[{index}].volume"),
            }
        )

    observation = {
        "source_id": SOURCE_ID,
        "source_contract_ref": SOURCE_CONTRACT_REF,
        "instrument_id": "cnyrubf_front_contract",
        "root": "CR",
        "secid": secid,
        "trade_date": trade_date_text,
        "binding": dict(front),
        "open": normalized[0]["open"],
        "high": max(float(item["high"]) for item in normalized),
        "low": min(float(item["low"]) for item in normalized),
        "close": normalized[-1]["close"],
        "volume": sum(float(item["volume"]) for item in normalized),
        "last_closed_5m_bar_end": normalized[-1]["end"],
        "closed_5m_bar_count": len(normalized),
    }
    data = {
        "mode": "LIVE_ALGOPACK_FO_TRADESTATS_5M_PARTIAL_DAY_CONTEXT",
        "observation": observation,
        "action_authority": False,
        "partial_day": True,
        "implicit_latest_used": False,
    }
    return base.ProducedComponent(data=data, data_as_of=normalized[-1]["end"])


def current_producers() -> Mapping[str, base.ComponentProducer]:
    producers = dict(base.default_producers())
    producers["cnyrubf_live"] = _cnyrubf_live_component
    return producers


def refresh_snapshot(
    *,
    now_fn=lambda: datetime.now(timezone.utc),
) -> tuple[dict[str, object], object]:
    return futoi.refresh_snapshot(now_fn=now_fn, producers=current_producers())


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Publish/read S7.3 current chat snapshot with governed FUTOI and current CNYRUBF intraday context"
    )
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
        snapshot, _path = futoi.read_current_snapshot()
        print(json.dumps(snapshot, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False))
        return 0
    except Exception as exc:
        print(f"PROJECT={PROJECT}")
        print(f"MODE={MODE}")
        print("STATUS=FAILED")
        print(f"ERROR_CLASS={exc.__class__.__name__}")
        print(f"ERROR={exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
