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
FORBIDDEN_POSITION_FIELDS = {
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
    "buy_sell_out",
}


def _set(
    root: Path,
    direction: str,
    average_entry_price: object = None,
) -> tuple[dict[str, object], Path]:
    return position.set_user_position_context(
        direction=direction,
        average_entry_price=average_entry_price,
        now_fn=lambda: NOW,
        root=root,
    )


def test_long_valid_average_persists_exact_schema_and_timestamp(tmp_path: Path) -> None:
    payload, path = _set(tmp_path, "LONG", 86.15)

    assert payload == {
        "instrument": "USDRUBF",
        "direction": "LONG",
        "average_entry_price": 86.15,
        "user_input_updated_at": NOW.isoformat(),
        "source_semantics": "explicit_user_input",
    }
    stored = json.loads(path.read_text(encoding="utf-8"))
    assert stored == payload
    assert set(stored) == position.PERSISTED_KEYS
    assert not (set(stored) & FORBIDDEN_POSITION_FIELDS)

    context = position.build_canonical_user_position_context(tmp_path)
    assert context == {
        "instrument": "USDRUBF",
        "direction": "LONG",
        "average_entry_price": 86.15,
        "user_input_updated_at": NOW.isoformat(),
        "status": "AVAILABLE",
        "availability": "EXPLICIT_USER_INPUT_AVAILABLE",
        "explicit_user_input": True,
    }
    assert set(context) == position.CANONICAL_KEYS
    assert not (set(context) & FORBIDDEN_POSITION_FIELDS)


def test_short_valid_average_is_preserved(tmp_path: Path) -> None:
    _set(tmp_path, "SHORT", 85.95)
    context = position.build_canonical_user_position_context(tmp_path)

    assert context["direction"] == "SHORT"
    assert context["average_entry_price"] == 85.95
    assert context["explicit_user_input"] is True


def test_flat_requires_and_persists_null_average(tmp_path: Path) -> None:
    payload, _ = _set(tmp_path, "FLAT")
    context = position.build_canonical_user_position_context(tmp_path)

    assert payload["direction"] == "FLAT"
    assert payload["average_entry_price"] is None
    assert context["direction"] == "FLAT"
    assert context["average_entry_price"] is None
    assert context["status"] == "AVAILABLE"
    assert context["explicit_user_input"] is True


@pytest.mark.parametrize("direction", ["LONG", "SHORT"])
def test_long_short_without_average_rejected(tmp_path: Path, direction: str) -> None:
    with pytest.raises(position.UserPositionContextError, match="requires average_entry_price"):
        _set(tmp_path, direction)


@pytest.mark.parametrize("value", [0, -1, float("nan"), float("inf"), float("-inf")])
def test_nonpositive_or_nonfinite_average_rejected(tmp_path: Path, value: float) -> None:
    with pytest.raises(position.UserPositionContextError, match="finite positive number"):
        _set(tmp_path, "LONG", value)


def test_bool_average_rejected(tmp_path: Path) -> None:
    with pytest.raises(position.UserPositionContextError, match="finite positive number"):
        _set(tmp_path, "SHORT", True)


def test_flat_with_average_rejected(tmp_path: Path) -> None:
    with pytest.raises(position.UserPositionContextError, match="requires average_entry_price=null"):
        _set(tmp_path, "FLAT", 86.0)


@pytest.mark.parametrize("direction", ["long", "BUY", "SELL", "OUT", "", None])
def test_invalid_direction_rejected(tmp_path: Path, direction: object) -> None:
    with pytest.raises(position.UserPositionContextError, match="LONG, SHORT, FLAT"):
        position.set_user_position_context(
            direction=direction,
            average_entry_price=86.0,
            now_fn=lambda: NOW,
            root=tmp_path,
        )


def test_absence_is_explicitly_unavailable_and_not_flat(tmp_path: Path) -> None:
    context = position.build_canonical_user_position_context(tmp_path)

    assert context["status"] == "UNAVAILABLE"
    assert context["availability"] == "NO_EXPLICIT_USER_INPUT"
    assert context["explicit_user_input"] is False
    assert context["direction"] is None
    assert context["direction"] != "FLAT"
    assert context["average_entry_price"] is None


def test_invalid_persisted_state_fails_closed_instead_of_becoming_flat(tmp_path: Path) -> None:
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
    assert context["direction"] != "FLAT"


def test_extra_persisted_fields_are_rejected_fail_closed(tmp_path: Path) -> None:
    path = position.user_position_context_path(tmp_path)
    path.write_text(
        json.dumps(
            {
                "instrument": "USDRUBF",
                "direction": "LONG",
                "average_entry_price": 86.0,
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


def test_atomic_persistence_uses_replace_and_leaves_no_temp_file(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    calls: list[tuple[str, str]] = []
    original_replace = base.os.replace

    def recording_replace(source: str, destination: str | Path) -> None:
        calls.append((str(source), str(destination)))
        original_replace(source, destination)

    monkeypatch.setattr(base.os, "replace", recording_replace)
    _, path = _set(tmp_path, "LONG", 86.25)

    assert len(calls) == 1
    assert calls[0][1] == str(path)
    assert path.is_file()
    assert not list(path.parent.glob(".current.*.tmp"))


def test_position_context_does_not_infer_from_market_or_history(tmp_path: Path) -> None:
    snapshot = {
        "components": {
            "live_market_structure": {
                "data": {
                    "instrument": "USDRUBF",
                    "price": 99.0,
                    "trend": "BULLISH_USD",
                    "market_regime": "UP",
                }
            }
        }
    }

    position.attach_user_position_context(snapshot, root=tmp_path)
    context = snapshot["user_position_context"]
    assert context["status"] == "UNAVAILABLE"
    assert context["availability"] == "NO_EXPLICIT_USER_INPUT"
    assert context["direction"] is None
    assert context["average_entry_price"] is None


def test_attachment_leaves_all_trading_authority_flags_unchanged(tmp_path: Path) -> None:
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


def test_manual_state_module_contains_no_market_or_network_fetch_path() -> None:
    source = inspect.getsource(position)

    for forbidden in (
        "fetch_live_snapshot",
        "_load_bars(",
        "requests.",
        "httpx.",
        "urlopen(",
        "broker",
    ):
        assert forbidden not in source


def test_canonical_refresh_attaches_position_before_atomic_publish() -> None:
    source = inspect.getsource(canonical.refresh_snapshot)
    attach = source.index("user_position.attach_user_position_context")
    publish = source.index("base._atomic_write")

    assert attach < publish


def test_canonical_read_route_exposes_position_without_changing_other_sections(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("MOEX_DATA_ROOT", str(tmp_path))
    _set(tmp_path, "LONG", 86.4)
    original_snapshot = {
        "schema_version": "rub_chat_analysis_snapshot.v1",
        "authority": {"server_generates_buy_sell_out": False},
        "components": {},
        "analysis_views": {},
        "analysis_workflow": {},
    }

    monkeypatch.setattr(canonical, "attach_live_market_oi_context", lambda *args, **kwargs: None)
    monkeypatch.setattr(canonical, "attach_live_basis_carry_context", lambda *args, **kwargs: None)

    result = canonical.load_live_analysis_snapshot(
        now_fn=lambda: NOW,
        reader=lambda **kwargs: (dict(original_snapshot), tmp_path / "current.json"),
        live_loader=lambda: {},
    )

    assert result["schema_version"] == "rub_chat_analysis_snapshot.v1"
    assert result["authority"] == original_snapshot["authority"]
    assert result["user_position_context"]["direction"] == "LONG"
    assert result["user_position_context"]["average_entry_price"] == 86.4
    assert result["user_position_context"]["explicit_user_input"] is True


def test_nan_and_infinity_never_serialize() -> None:
    for value in (math.nan, math.inf, -math.inf):
        with pytest.raises(position.UserPositionContextError):
            position.validate_user_position_input(
                direction="SHORT",
                average_entry_price=value,
                user_input_updated_at=NOW,
            )
