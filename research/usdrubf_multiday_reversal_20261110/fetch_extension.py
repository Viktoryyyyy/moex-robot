"""Fetch a bounded USDRUBF AlgoPack/ISS extension for the frozen study inputs.

The script is research-only. It writes new files below an explicit output
directory and never mutates registered data, manifests, monitors or pilots.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
from typing import Any

import pandas as pd
import requests

from moex_research.intelligence.usdrubf_reversal_candle_monitor import (
    APIM_BASE_URL,
    TRADESTATS_PATH,
    _load_env_file,
)
from moex_data.futures.liquidity_history_metrics_probe import fetch_paged_frame


ISS_HISTORY_URL = (
    "https://iss.moex.com/iss/history/engines/futures/markets/forts/"
    "securities/USDRUBF.json"
)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _fetch_history(from_date: str, till_date: str, timeout: float) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    start = 0
    while True:
        response = requests.get(
            ISS_HISTORY_URL,
            params={
                "from": from_date,
                "till": till_date,
                "start": start,
                "iss.meta": "off",
            },
            timeout=timeout,
        )
        response.raise_for_status()
        payload = response.json()
        block = payload.get("history") or payload.get("data") or {}
        columns = block.get("columns") or []
        data = block.get("data") or []
        if not columns or not data:
            break
        rows.extend(dict(zip(columns, values)) for values in data)
        cursor = payload.get("history.cursor") or payload.get("data.cursor") or {}
        cursor_columns = cursor.get("columns") or []
        cursor_data = cursor.get("data") or []
        if not cursor_columns or not cursor_data:
            break
        item = dict(zip(cursor_columns, cursor_data[0]))
        page_size = int(item.get("PAGESIZE", item.get("pagesize", len(data))))
        total = int(item.get("TOTAL", item.get("total", len(rows))))
        index = int(item.get("INDEX", item.get("index", start)))
        next_start = index + page_size
        if next_start >= total:
            break
        if next_start <= start:
            raise RuntimeError("non-increasing ISS history cursor")
        start = next_start
    return rows


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--from", dest="from_date", required=True)
    parser.add_argument("--till", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--env-file", default="/home/trader/moex_bot/.env")
    parser.add_argument("--timeout", type=float, default=45.0)
    args = parser.parse_args()

    _load_env_file(args.env_file)
    if not os.environ.get("MOEX_API_KEY", "").strip():
        raise RuntimeError("MOEX_API_KEY is required")
    output = Path(args.output_dir).expanduser().resolve()
    output.mkdir(parents=True, exist_ok=True)
    tradestats_path = output / "tradestats_extension.csv"
    history_path = output / "history_extension.json"
    manifest_path = output / "extension_manifest.json"
    for path in (tradestats_path, history_path, manifest_path):
        if path.exists():
            raise FileExistsError(str(path))

    tradestats = fetch_paged_frame(
        APIM_BASE_URL,
        TRADESTATS_PATH,
        {"from": args.from_date, "till": args.till},
        "tradestats",
        float(args.timeout),
        True,
    )
    if tradestats.empty:
        raise RuntimeError("AlgoPack returned no TradeStats rows")
    tradestats.to_csv(tradestats_path, index=False)
    history_rows = _fetch_history(args.from_date, args.till, float(args.timeout))
    history_payload = {
        "source": ISS_HISTORY_URL,
        "from": args.from_date,
        "till": args.till,
        "total": len(history_rows),
        "rows": history_rows,
    }
    history_path.write_text(
        json.dumps(history_payload, ensure_ascii=False, indent=2, allow_nan=False),
        encoding="utf-8",
    )
    manifest = {
        "schema_version": "usdrubf_multiday_reversal_extension.v1",
        "from": args.from_date,
        "till": args.till,
        "tradestats_rows": int(len(tradestats)),
        "tradestats_columns": [str(column) for column in tradestats.columns],
        "history_rows": len(history_rows),
        "files": {
            tradestats_path.name: _sha256(tradestats_path),
            history_path.name: _sha256(history_path),
        },
    }
    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2, allow_nan=False),
        encoding="utf-8",
    )
    print(json.dumps(manifest, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
