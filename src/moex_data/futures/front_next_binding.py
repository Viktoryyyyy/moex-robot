from __future__ import annotations

import argparse
import json
import os
import re
from collections.abc import Mapping, Sequence
from datetime import date
from typing import Final

import pandas as pd
import requests

SOURCE_ID: Final[str] = "moex_iss_forts_securities_reference"
ENDPOINT_PATH: Final[str] = "/iss/engines/futures/markets/forts/securities.json"
DEFAULT_BASE_URL: Final[str] = "https://iss.moex.com"
BOARD_ID: Final[str] = "RFUD"
ROOTS: Final[dict[str, str]] = {"Si": "Si", "CR": "CR"}
ROLE_INSTRUMENT_IDS: Final[dict[tuple[str, str], str]] = {
    ("Si", "front"): "si_front_contract",
    ("Si", "next"): "si_next_contract",
    ("CR", "front"): "cr_front_contract",
    ("CR", "next"): "cr_next_contract",
}


class FrontNextBindingError(ValueError):
    pass


def _require_date(value: str) -> str:
    try:
        return date.fromisoformat(str(value).strip()).isoformat()
    except ValueError as exc:
        raise FrontNextBindingError("as_of_date must be explicit YYYY-MM-DD") from exc


def _require_root(value: str) -> str:
    text = str(value).strip()
    for root in ROOTS:
        if text.casefold() == root.casefold():
            return root
    raise FrontNextBindingError("root must be one of: Si, CR")


def _block_to_frame(payload: Mapping[str, object], block_name: str) -> pd.DataFrame:
    block = payload.get(block_name)
    if not isinstance(block, Mapping):
        raise FrontNextBindingError("MOEX ISS response missing securities block")
    columns = block.get("columns")
    rows = block.get("data")
    if not isinstance(columns, list) or not isinstance(rows, list):
        raise FrontNextBindingError("MOEX ISS securities block has invalid shape")
    return pd.DataFrame(rows, columns=columns)


def fetch_reference_frame(*, timeout: float = 30.0, base_url: str | None = None) -> tuple[pd.DataFrame, str]:
    base = str(base_url or os.environ.get("MOEX_ISS_URL", DEFAULT_BASE_URL)).strip().rstrip("/")
    if not base:
        raise FrontNextBindingError("MOEX ISS base URL is required")
    url = base + ENDPOINT_PATH
    response = requests.get(
        url,
        params={
            "iss.meta": "off",
            "securities.columns": "SECID,BOARDID,LASTTRADEDATE",
        },
        timeout=timeout,
        headers={"User-Agent": "moex_bot_step3_front_next/1.0"},
    )
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, Mapping):
        raise FrontNextBindingError("MOEX ISS response root is not an object")
    frame = _block_to_frame(payload, "securities")
    if frame.empty:
        raise FrontNextBindingError("MOEX ISS securities response is empty")
    return frame, str(getattr(response, "url", url))


def bind_front_next(frame: pd.DataFrame, *, root: str, as_of_date: str) -> list[dict[str, str]]:
    checked_root = _require_root(root)
    checked_as_of = _require_date(as_of_date)
    column_map = {str(column).upper(): column for column in frame.columns}
    required = ("SECID", "BOARDID", "LASTTRADEDATE")
    missing = [name for name in required if name not in column_map]
    if missing:
        raise FrontNextBindingError("MOEX ISS reference schema missing: " + ",".join(missing))

    secid_col = column_map["SECID"]
    board_col = column_map["BOARDID"]
    expiry_col = column_map["LASTTRADEDATE"]
    pattern = re.compile(r"^" + re.escape(ROOTS[checked_root]) + r"[HMUZ][0-9]$", re.IGNORECASE)

    work = frame.copy()
    work = work.loc[work[board_col].astype(str).str.upper().eq(BOARD_ID)].copy()
    work = work.loc[work[secid_col].astype(str).map(lambda value: bool(pattern.fullmatch(value.strip())))].copy()
    work["_last_trade_date"] = pd.to_datetime(work[expiry_col], errors="coerce").dt.date
    if work["_last_trade_date"].isna().any():
        raise FrontNextBindingError("eligible FORTS contract has invalid LASTTRADEDATE")
    as_of = date.fromisoformat(checked_as_of)
    work = work.loc[work["_last_trade_date"] >= as_of].copy()
    work["_secid_sort"] = work[secid_col].astype(str)
    work = work.sort_values(["_last_trade_date", "_secid_sort"], kind="stable").reset_index(drop=True)
    if len(work.index) < 2:
        raise FrontNextBindingError("fewer than two eligible contracts for explicit root/as_of_date")

    selected = work.iloc[:2]
    result: list[dict[str, str]] = []
    for role, (_, row) in zip(("front", "next"), selected.iterrows()):
        secid = str(row[secid_col]).strip()
        result.append(
            {
                "root": checked_root,
                "as_of_date": checked_as_of,
                "role": role,
                "instrument_id": ROLE_INSTRUMENT_IDS[(checked_root, role)],
                "secid": secid,
                "last_trade_date": row["_last_trade_date"].isoformat(),
                "source_id": SOURCE_ID,
            }
        )
    if result[0]["secid"].casefold() == result[1]["secid"].casefold():
        raise FrontNextBindingError("front and next SECIDs must be distinct")
    return result


def discover_front_next(*, root: str, as_of_date: str, timeout: float = 30.0, base_url: str | None = None) -> list[dict[str, str]]:
    frame, _ = fetch_reference_frame(timeout=timeout, base_url=base_url)
    return bind_front_next(frame, root=root, as_of_date=as_of_date)


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Deterministically bind explicit Si/CR front and next contracts.")
    parser.add_argument("--as-of", required=True, dest="as_of_date")
    parser.add_argument("--root", action="append", required=True, choices=("Si", "CR"))
    parser.add_argument("--timeout", type=float, default=30.0)
    parser.add_argument("--iss-base-url", default=None)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        frame, source_url = fetch_reference_frame(timeout=args.timeout, base_url=args.iss_base_url)
        bindings: list[dict[str, str]] = []
        for root in args.root:
            bindings.extend(bind_front_next(frame, root=root, as_of_date=args.as_of_date))
        payload = {
            "status": "succeeded",
            "source_id": SOURCE_ID,
            "source_url": source_url,
            "as_of_date": _require_date(args.as_of_date),
            "bindings": bindings,
            "latest_autodetect_used": False,
        }
    except Exception as exc:
        payload = {"status": "failed", "source_id": SOURCE_ID, "error": str(exc), "latest_autodetect_used": False}
        print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
        return 1
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
