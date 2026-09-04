from __future__ import annotations

from datetime import datetime, timezone
import inspect
import json
import math
from pathlib import Path

import pytest

from src.moex_research.runners import usdrubf_s7_3_chat_analysis_snapshot as base
from src.moex_research.runners import usdrubf_s7_3_chat_analysis_snapshot_live_market_oi as canonical
from src.moex_research.runners import usdrubf_user_position_context as position


NOW = datetime(2026, 9, 4, 9, 30, tzinfo=timezone.utc)
FORBIDDEN_FIELDS = {
    "contracts",
    "position_size",
    "capital",
    "max_position",
    "stops",
    "limits",
    "take_profits",
    "leverage",
    "pnl",
    "risk_percentage",
    "drawdown",
    "recommendations",
}


def _set(root: Path, direction: object, average: object = None):
    return position.set_user_position_context(
        direction=direction,
        average_entry_price=average,
        now_fn=lambda: NOW,
        root=root,
    )


def test_long_short_and_flat_valid_semantics(tmp_path: Path) -> None:
    for direction, average in (("LONG", 86.15), ("SHORT", 85.95), ("FLAT", None)):
        payload, path = _set(tmp_path, direction, average)
        stored = json.loads(path.read_text(encoding="utf-8"))
        assert stored == payload
        assert set(stored) == position.PERSISTED_KEYS
        assert stored["instrument"] == "USDRUBF"
        assert stored["direction"] == direction
        assert stored["average_entry_price"] == average
        assert stored["user_input_updated_at"] == NOW.isoformat()
        assert stored["source_semantics"] == "explicit_user_input"
        assert not (set(stored) & FORBIDDEN_FIELDS)

        context = position.build_canonical_user_position_context(tmp_path)
        assert set(context) == position.CANONICAL_KEYS
        assert context["instrument"] == "USDRUBF"
        assert context["direction"] == direction
        assert context["average_entry_price"] == average
        assert context["user_input_updated_at"] == NOW.isoformat()
        assert context["status"] == "AVAILABLE"
        assert context["availability"] == "EXPLICIT_USER_INPUT_AVAILABLE"
        assert context["explicit_user_input"] is True
        assert not (set(context) & FORBIDDEN_FIELDS)


@pytest.mark.parametrize("direction", ["LONG", "SHORT"])
def test_long_short_without_average_rejected(tmp_path: Path, direction: str) -> None:
    with pytest.raises(position.UserPositionContextError, match="requires average_entry_price"):
        _set(tmp_path, direction)


@pytest.mark.parametrize("value", [0, -1, math.nan, math.inf, -math.inf, True])
def test_invalid_average_rejected(tmp_path: Path, value: object) -> None:
    with pytest.raises(position.UserPositionContextError, match="finite positive number"):
        _set(tmp_path, "LONG", value)


def test_flat_with_average_rejected(tmp_path: Path) -> None:
    with pytest.raises(position.UserPositionContextError, match="requires average_entry_price=null"):
        _set(tmp_path, "FLAT", 86.0)


@pytest.mark.parametrize("direction", ["long", "BUY", "SELL", "OUT", "", None])
def test_invalid_direction_rejected(tmp_path: Path, direction: object) -> None:
    with pytest.raises(position.UserPositionContextError, match="LONG, SHORT, FLAT"):
        _set(tmp_path, direction, 86.0)


def test_absence_is_unavailable_and_not_flat(tmp_path: Path) -> None:
    context = position.build_canonical_user_position_context(tmp_path)
    assert context == {
        "instrument": "USDRUBF",
        "direction": None,
        "average_entry_price": None,
        "user_input_updated_at": None,
        "status": "UNAVAILABLE",
        "availability": "NO_EXPLICIT_USER_INPUT",
        "explicit_user_input": False,
    }
    assert context["direction"] != "FLAT"


def test_invalid_persisted_state_fails_closed(tmp_path: Path) -> None:
    path = position.user_position_context_path(tmp_path)
    path.write_text(
        json.dumps(
            {
                "instrument": "USDRUBF",
                "direction": "LONG",
                "average_entry_price": 0,
                "user_input_updated_at": NOW.isoformat(),
                "source_semantics": "explicit_user_input",
            }
        ),
        encoding="utf-8",
    )
    context = position.build_canonical_user_position_context(tmp_path)
    assert context["status"] == "UNAVAILABLE"
    assert context["availability"] == "INVALID_EXPLICIT_USER_INPUT"
    assert context["explicit_user_input"] is False
    assert context["direction"] is None


def test_extra_position_fields_fail_closed(tmp_path: Path) -> None:
    path = position.user_position_context_path(tmp_path)
    path.write_text(
        json.dumps(
            {
                "instrument": "USDRUBF",
                "direction": "SHORT",
                "average_entry_price": 85.0,
                "user_input_updated_at": NOW.isoformat(),
                "source_semantics": "explicit_user_input",
                "contracts": 20,
            }
        ),
        encoding="utf-8",
    )
    context = position.build_canonical_user_position_context(tmp_path)
    assert context["availability"] == "INVALID_EXPLICIT_USER_INPUT"
    assert context["explicit_user_input"] is False


def test_persistence_is_atomic(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    calls: list[tuple[str, str]] = []
    original_replace = base.os.replace

    def recording_replace(source: str, destination: str | Path) -> None:
        calls.append((str(source), str(destination)))
        original_replace(source, destination)

    monkeypatch.setattr(base.os, "replace", recording_replace)
    _, path = _set(tmp_path, "LONG", 86.25)
    assert calls == [(calls[0][0], str(path))]
    assert not list(path.parent.glob(".current.*.tmp"))


def test_no_market_inference_and_no_network_path(tmp_path: Path) -> None:
    snapshot = {
        "components": {
            "live_market_structure": {
                "data": {"instrument": "USDRUBF", "price": 99.0, "trend": "BULLISH_USD"}
            }
        }
    }
    position.attach_user_position_context(snapshot, root=tmp_path)
    assert snapshot["user_position_context"]["availability"] == "NO_EXPLICIT_USER_INPUT"
    assert snapshot["user_position_context"]["direction"] is None

    source = inspect.getsource(position)
    for forbidden in ("fetch_live_snapshot", "_load_bars(", "requests.", "httpx.", "urlopen(", "broker"):
        assert forbidden not in source


def test_attachment_does_not_change_governance_authority(tmp_path: Path) -> None:
    _set(tmp_path, "SHORT", 85.75)
    authority = {
        "stage5_full_mode_ready": False,
        "stage5_pointer_promotion_performed": False,
        "directional_authority": False,
        "action_authority": False,
        "standalone_buy_sell_authority": False,
    }
    snapshot = {"authority": dict(authority)}
    position.attach_user_position_context(snapshot, root=tmp_path)
    assert snapshot["authority"] == authority
    serialized = json.dumps(snapshot["user_position_context"], sort_keys=True)
    assert "BUY" not in serialized
    assert "SELL" not in serialized
    assert "OUT" not in serialized


def test_canonical_refresh_attaches_before_atomic_publish() -> None:
    source = inspect.getsource(canonical.refresh_snapshot)
    assert source.index("user_position.attach_user_position_context") < source.index("base._atomic_write")


def test_manual_cli_shape() -> None:
    long_args = position.parse_args(["--direction", "LONG", "--average-entry-price", "86.15"])
    short_args = position.parse_args(["--direction", "SHORT", "--average-entry-price", "85.95"])
    flat_args = position.parse_args(["--direction", "FLAT"])
    assert (long_args.direction, long_args.average_entry_price) == ("LONG", 86.15)
    assert (short_args.direction, short_args.average_entry_price) == ("SHORT", 85.95)
    assert (flat_args.direction, flat_args.average_entry_price) == ("FLAT", None)
