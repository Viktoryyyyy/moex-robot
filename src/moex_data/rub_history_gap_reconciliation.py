"""Classify bounded historical gaps and expose explicitly sourced OHLCV candidates."""
from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path
from zoneinfo import ZoneInfo

MOSCOW = ZoneInfo("Europe/Moscow")
FIELDS = ("open", "high", "low", "close", "volume")


def timestamp(value, *, aware=False):
    parsed = datetime.fromisoformat(str(value))
    if parsed.tzinfo is None:
        if aware:
            raise ValueError("timezone-aware timestamp required")
        parsed = parsed.replace(tzinfo=MOSCOW)
    return parsed.astimezone(MOSCOW)


def number(value):
    if isinstance(value, bool):
        raise ValueError("boolean is not a candle number")
    result = Decimal(str(value))
    if not result.is_finite() or result <= 0:
        raise ValueError("positive finite candle number required")
    return result


def aggregate(rows):
    if not rows:
        raise ValueError("adjacent minute interval is empty")
    return {"open":rows[0]["open"],"close":rows[-1]["close"],
            "high":max(row["high"] for row in rows),"low":min(row["low"] for row in rows),
            "volume":sum(row["volume"] for row in rows)}


def reconcile(entry: dict) -> dict:
    left, right = timestamp(entry["left_end"],aware=True), timestamp(entry["right_end"],aware=True)
    start = left-timedelta(minutes=5)
    if any(t.minute % 5 or t.second or t.microsecond for t in (left,right)):
        raise ValueError("5m boundary alignment required")
    if not timedelta(minutes=10) <= right-left <= timedelta(minutes=55):
        raise ValueError("bounded internal gap of 1 to 10 missing intervals required")
    expected_url = "https://apim.moex.com/iss/engines/futures/markets/forts/boards/RFUD/securities/"+entry["secid"]+"/candles.json"
    if entry["url"] != expected_url:
        raise ValueError("minute source identity mismatch")
    query=entry["query"]
    if query.get("interval") != 1 or timestamp(query["from"]) != start or timestamp(query["till"]) != right-timedelta(seconds=1):
        raise ValueError("minute query does not cover the exact bounded window")
    checked = timestamp(entry["checked_at_utc"],aware=True)
    if checked < right:
        raise ValueError("incomplete source window")
    block = entry["minute_payload"]["candles"]
    columns = block["columns"]
    if len(columns) != len(set(columns)) or not set(FIELDS+('begin','end')).issubset(columns):
        raise ValueError("invalid minute schema")
    rows, seen = [], set()
    for values in block["data"]:
        if len(values) != len(columns):
            raise ValueError("invalid minute row width")
        row=dict(zip(columns,values));begin=timestamp(row["begin"]);end=timestamp(row["end"])
        if begin.second or begin.microsecond or end != begin+timedelta(seconds=59):
            raise ValueError("complete minute required")
        if begin in seen or not start <= begin < right:
            raise ValueError("duplicate or out-of-window minute")
        seen.add(begin)
        row.update({field:number(row[field]) for field in FIELDS})
        if not row["low"] <= min(row["open"],row["close"]) <= max(row["open"],row["close"]) <= row["high"]:
            raise ValueError("invalid minute OHLC")
        rows.append((begin,row))
    rows.sort(key=lambda item:item[0])
    neighbors={timestamp(row["end"],aware=True):row for row in entry["neighbors"]}
    if len(entry["neighbors"]) != 2 or set(neighbors) != {left,right}:
        raise ValueError("exactly two adjacent raw bars required")
    for end in (left,right):
        observed=aggregate([row for begin,row in rows if end-timedelta(minutes=5) <= begin < end])
        if any(observed[field] != number(neighbors[end][field]) for field in FIELDS):
            raise ValueError("adjacent raw OHLCV mismatch")
    intervals=[]
    end=left+timedelta(minutes=5)
    while end < right:
        selected=[(begin,row) for begin,row in rows if end-timedelta(minutes=5) <= begin < end]
        item={"interval_end":end.isoformat()}
        if not selected:
            item["status"]="CORROBORATED_EMPTY"
        elif len(selected) != 5:
            # Sparse minutes may be legitimate, but cannot prove a complete repair here.
            item.update(status="UNRESOLVED_PARTIAL_MINUTES",minute_count=len(selected))
        else:
            values=aggregate([row for _,row in selected])
            item.update(status="OHLCV_RECOVERY_CANDIDATE",ohlcv={key:str(value) for key,value in values.items()},
                source_id="moex_iss_forts_rfud_1m",source_sec_id=entry["secid"],
                availability_ts=entry["checked_at_utc"],minute_count=5,
                unknown_fields=["open_interest","num_trades","value"],
                requires_explicit_secondary_source_acceptance=True)
        intervals.append(item);end+=timedelta(minutes=5)
    return {"secid":entry["secid"],"left_end":left.isoformat(),"right_end":right.isoformat(),
        "status":"CLASSIFIED" if all(item["status"] != "UNRESOLVED_PARTIAL_MINUTES" for item in intervals) else "UNRESOLVED",
        "intervals":intervals,"adjacent_ohlcv_exact_match":True,
        "raw_data_changed":False,"model_acceptance_granted":False,
        "evidence_sha256":hashlib.sha256(json.dumps(entry,sort_keys=True,separators=(',',':')).encode()).hexdigest()}


def report(entries):
    results=[]
    for entry in entries:
        try: results.append(reconcile(entry))
        except Exception as error:
            results.append({"secid":entry.get("secid"),"left_end":entry.get("left_end"),"status":"UNRESOLVED","reason":str(error)})
    intervals=[item for result in results for item in result.get("intervals",[])]
    return {"schema_version":"rub_history_gap_reconciliation.v1","gaps":results,
        "gap_count":len(results),"missing_5m_intervals":len(intervals),
        "empty_interval_count":sum(item["status"] == "CORROBORATED_EMPTY" for item in intervals),
        "recovery_candidate_count":sum(item["status"] == "OHLCV_RECOVERY_CANDIDATE" for item in intervals),
        "unresolved_gap_count":sum(result["status"] == "UNRESOLVED" for result in results),
        "raw_data_changed":False,"model_acceptance_granted":False}


if __name__ == "__main__":
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--evidence",type=Path,required=True)
    args=parser.parse_args()
    result=report(json.loads(args.evidence.read_text(encoding="utf-8")))
    print(json.dumps(result,ensure_ascii=False,indent=2))
    raise SystemExit(1 if result["unresolved_gap_count"] else 0)
