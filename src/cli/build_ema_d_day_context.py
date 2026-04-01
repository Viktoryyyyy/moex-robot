#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import tempfile
from typing import Dict

from src.applied.context_filter.d_day_context import build_context_payload


REQUIRED_KEYS = [
    "target_day",
    "source_trade_date",
    "features",
    "score",
    "band",
    "decision",
    "blocked",
    "status",
    "reason",
    "generated_at",
]


def die(msg: str, code: int = 2) -> None:
    print(msg, file=sys.stderr)
    raise SystemExit(code)


def ensure_dir_for_file(path: str) -> None:
    d = os.path.dirname(os.path.abspath(path))
    if d:
        os.makedirs(d, exist_ok=True)


def atomic_write_text(path: str, text: str) -> None:
    ensure_dir_for_file(path)
    d = os.path.dirname(os.path.abspath(path))
    fd, tmp_path = tempfile.mkstemp(prefix=".tmp_", dir=d)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as f:
            f.write(text)
        os.replace(tmp_path, path)
    finally:
        try:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
        except Exception:
            pass


def atomic_write_csv(path: str, row: Dict[str, object]) -> None:
    ensure_dir_for_file(path)
    d = os.path.dirname(os.path.abspath(path))
    fd, tmp_path = tempfile.mkstemp(prefix=".tmp_", dir=d)
    header = [
        "target_day",
        "source_trade_date",
        "d1_vol_z",
        "d1_body_ratio",
        "score",
        "band",
        "decision",
        "blocked",
        "status",
        "reason",
        "generated_at",
    ]
    features = row.get("features") or {}
    csv_row = {
        "target_day": row.get("target_day"),
        "source_trade_date": row.get("source_trade_date"),
        "d1_vol_z": features.get("d1_vol_z"),
        "d1_body_ratio": features.get("d1_body_ratio"),
        "score": row.get("score"),
        "band": row.get("band"),
        "decision": row.get("decision"),
        "blocked": row.get("blocked"),
        "status": row.get("status"),
        "reason": row.get("reason"),
        "generated_at": row.get("generated_at"),
    }
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=header)
            w.writeheader()
            w.writerow(csv_row)
        os.replace(tmp_path, path)
    finally:
        try:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
        except Exception:
            pass


def validate_payload(payload: Dict[str, object]) -> None:
    for k in REQUIRED_KEYS:
        if k not in payload:
            die("payload missing required key: " + k)
    features = payload.get("features")
    if not isinstance(features, dict):
        die("payload features must be dict")
    for k in ["d1_vol_z", "d1_body_ratio"]:
        if k not in features:
            die("payload features missing key: " + k)
    if payload.get("decision") not in ("allowed", "blocked"):
        die("payload decision invalid: " + str(payload.get("decision")))
    if payload.get("status") not in ("ok", "error"):
        die("payload status invalid: " + str(payload.get("status")))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--master-path", required=True)
    ap.add_argument("--target-day", required=True)
    ap.add_argument("--out-json", required=True)
    ap.add_argument("--out-csv", default="")
    args = ap.parse_args()

    payload = build_context_payload(master_path=args.master_path, target_day=args.target_day)
    validate_payload(payload)

    atomic_write_text(args.out_json, json.dumps(payload, ensure_ascii=False, indent=2) + "\n")
    if args.out_csv:
        atomic_write_csv(args.out_csv, payload)

    print("target_day=" + str(payload.get("target_day")) + " source_trade_date=" + str(payload.get("source_trade_date")) + " band=" + str(payload.get("band")) + " decision=" + str(payload.get("decision")) + " status=" + str(payload.get("status")))

    if payload.get("status") != "ok":
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
