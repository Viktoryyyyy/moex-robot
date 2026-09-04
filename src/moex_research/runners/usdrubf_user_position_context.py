from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import math
from pathlib import Path
from typing import Callable, Mapping, Sequence

from src.moex_research.runners import usdrubf_s7_3_chat_analysis_snapshot as base


PROJECT = base.PROJECT
INSTRUMENT = "USDRUBF"
USER_POSITION_FILENAME = "user_position_context.json"
SOURCE_SEMANTICS = "explicit_user_input"
VALID_DIRECTIONS = frozenset({"LONG", "SHORT", "FLAT"})
PERSISTED_KEYS = frozenset(
    {
        "instrument",
        "direction",
        "average_entry_price",
        "user_input_updated_at",
        "source_semantics",
    }
)
CANONICAL_KEYS = frozenset(
    {
        "instrument",
        "direction",
        "average_entry_price",
        "user_input_updated_at",
        "status",
        "availability",
        "explicit_user_input",
    }
)


class UserPositionContextError(base.ChatAnalysisSnapshotError):
    pass


def user_position_context_path(root: Path) -> Path:
    return base.snapshot_state_dir(root) / USER_POSITION_FILENAME


def _average_entry_price(value: object, *, direction: str) -> float | None:
    if direction == "FLAT":
        if value is not None:
            raise UserPositionContextError("FLAT requires average_entry_price=null")
        return None
    if value is None:
        raise UserPositionContextError(f"{direction} requires average_entry_price")
    if isinstance(value, bool):
        raise UserPositionContextError("average_entry_price must be a finite positive number")
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise UserPositionContextError("average_entry_price must be a finite positive number") from exc
    if not math.isfinite(number) or number <= 0:
        raise UserPositionContextError("average_entry_price must be a finite positive number")
    return number


def validate_user_position_input(
    *,
    direction: object,
    average_entry_price: object,
    user_input_updated_at: datetime | str,
) -> dict[str, object]:
    if not isinstance(direction, str) or direction not in VALID_DIRECTIONS:
        raise UserPositionContextError("direction must be one of LONG, SHORT, FLAT")
    updated_at = base._aware(user_input_updated_at, "user_input_updated_at")
    return {
        "instrument": INSTRUMENT,
        "direction": direction,
        "average_entry_price": _average_entry_price(
            average_entry_price,
            direction=direction,
        ),
        "user_input_updated_at": base._iso(updated_at),
        "source_semantics": SOURCE_SEMANTICS,
    }


def _validate_persisted_payload(value: object) -> dict[str, object]:
    if not isinstance(value, Mapping):
        raise UserPositionContextError("user position state must contain a JSON object")
    if set(value) != PERSISTED_KEYS:
        raise UserPositionContextError("user position state fields do not match the canonical schema")
    if value.get("instrument") != INSTRUMENT:
        raise UserPositionContextError("user position instrument must be USDRUBF")
    if value.get("source_semantics") != SOURCE_SEMANTICS:
        raise UserPositionContextError("user position source_semantics must be explicit_user_input")
    return validate_user_position_input(
        direction=value.get("direction"),
        average_entry_price=value.get("average_entry_price"),
        user_input_updated_at=value.get("user_input_updated_at"),
    )


def load_user_position_context(root: Path) -> dict[str, object] | None:
    path = user_position_context_path(root)
    if not path.exists():
        return None
    if path.is_symlink() or not path.is_file():
        raise UserPositionContextError("user position state must be a regular non-symlink file")
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise UserPositionContextError("user position state is not valid JSON") from exc
    return _validate_persisted_payload(value)


def set_user_position_context(
    *,
    direction: object,
    average_entry_price: object = None,
    now_fn: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
    root: Path | None = None,
) -> tuple[dict[str, object], Path]:
    state_root = base._data_root() if root is None else root
    payload = validate_user_position_input(
        direction=direction,
        average_entry_price=average_entry_price,
        user_input_updated_at=now_fn(),
    )
    path = user_position_context_path(state_root)
    base._atomic_write(path, payload)
    return payload, path


def _unavailable_context(availability: str) -> dict[str, object]:
    return {
        "instrument": INSTRUMENT,
        "direction": None,
        "average_entry_price": None,
        "user_input_updated_at": None,
        "status": "UNAVAILABLE",
        "availability": availability,
        "explicit_user_input": False,
    }


def build_canonical_user_position_context(root: Path) -> dict[str, object]:
    try:
        payload = load_user_position_context(root)
    except UserPositionContextError:
        return _unavailable_context("INVALID_EXPLICIT_USER_INPUT")
    if payload is None:
        return _unavailable_context("NO_EXPLICIT_USER_INPUT")
    context = {
        "instrument": payload["instrument"],
        "direction": payload["direction"],
        "average_entry_price": payload["average_entry_price"],
        "user_input_updated_at": payload["user_input_updated_at"],
        "status": "AVAILABLE",
        "availability": "EXPLICIT_USER_INPUT_AVAILABLE",
        "explicit_user_input": True,
    }
    if set(context) != CANONICAL_KEYS:
        raise UserPositionContextError("canonical user position context schema mismatch")
    return context


def attach_user_position_context(snapshot: dict[str, object], *, root: Path) -> None:
    snapshot["user_position_context"] = build_canonical_user_position_context(root)


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Persist explicit USDRUBF user position context")
    parser.add_argument("--direction", required=True, choices=sorted(VALID_DIRECTIONS))
    parser.add_argument("--average-entry-price", type=float)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        base.load_dotenv(base.PROJECT_ENV_PATH, override=False)
        payload, path = set_user_position_context(
            direction=args.direction,
            average_entry_price=args.average_entry_price,
        )
        print(f"PROJECT={PROJECT}")
        print("STATUS=COMPLETED")
        print(f"INSTRUMENT={payload['instrument']}")
        print(f"DIRECTION={payload['direction']}")
        print(f"AVERAGE_ENTRY_PRICE={payload['average_entry_price']}")
        print(f"USER_INPUT_UPDATED_AT={payload['user_input_updated_at']}")
        print(f"STATE_PATH={path}")
        return 0
    except Exception as exc:
        print(f"PROJECT={PROJECT}")
        print("STATUS=BLOCKED")
        print(f"ERROR_CLASS={exc.__class__.__name__}")
        print(f"ERROR={exc}")
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
