"""Instrument-neutral observed-slot arithmetic and independent view verification."""
from dataclasses import dataclass
from decimal import Decimal, localcontext
from hashlib import sha256
import json
from math import fsum, sqrt, isclose

SIDE_FIELDS = ("long", "short", "net", "long_participants", "short_participants")
LAGS = (1, 5, 20)
WINDOWS = (252, 504)
COLUMNS = ("total_open_interest",) + tuple(side + "." + field for side in ("fiz", "yur") for field in SIDE_FIELDS)
CLOCKS = ("snapshot_ts", "availability_ts_utc", "source_publication_time", "ingest_ts_utc")
FIELDS = ("total_open_interest",) + tuple(side + "." + field for side in ("fiz", "yur") for field in (
    "long", "short", "net", "gross", "long_participants", "short_participants",
    "long_share_of_oi", "short_share_of_oi", "net_share_of_oi", "gross_share_of_two_sided_oi"))
STAT_FIELDS = tuple(field for field in FIELDS if not field.endswith("participants"))

@dataclass(frozen=True)
class StatisticsProfile:
    instrument_id: str
    schema: str
    store_key: str
    scope: str
    semantics_version: str


def _require(condition, message):
    if not condition:
        raise AssertionError(message)


def _digest(value):
    return sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode()).hexdigest()


def _values(fact):
    oi = fact["total_open_interest"]
    result = {"total_open_interest": oi}
    for side in ("fiz", "yur"):
        value = fact[side]
        result.update({side + "." + key: value[key] for key in SIDE_FIELDS})
        result[side + ".gross"] = value["long"] + value["short"]
        for key in ("long", "short", "net"):
            result[side + "." + key + "_share_of_oi"] = value[key] / oi
        result[side + ".gross_share_of_two_sided_oi"] = (value["long"] + value["short"]) / (2 * oi)
    return result


def _encode_row(fact, proof_id):
    numbers = [fact["total_open_interest"]] + [fact[s][k] for s in ("fiz", "yur") for k in SIDE_FIELDS]
    return {"trade_date": fact["trade_date"], "status": "AVAILABLE", "values": numbers,
            "clocks": {key: fact.get(key) for key in CLOCKS}, "proof_id": proof_id, "reason": None}


def _decode_row(row):
    values = dict(zip(COLUMNS, row["values"]))
    return {"trade_date": row["trade_date"], **row["clocks"], "total_open_interest": values["total_open_interest"],
            **{side: {key: values[side + "." + key] for key in SIDE_FIELDS} for side in ("fiz", "yur")}}


def _summary(slots, facts, anchor, excluded):
    values = {day: _values(facts[day]) for day in slots if day in facts}
    anchor_day = anchor["trade_date"]
    if not slots or slots[-1] != anchor_day or anchor_day not in values:
        raise ValueError("statistics_summary_anchor")
    anchor_values = values[anchor_day]
    windows = {}
    for window in WINDOWS:
        selected = slots[-window:]
        days = [day for day in selected if day in values]
        missing = [{"trade_date": day, "reason": excluded[day]} for day in selected if day not in values]
        variables = {}
        for field in STAT_FIELDS:
            sample = [values[day][field] for day in days]
            result = {"status": "UNAVAILABLE", "reason": "minimum_two_admitted_observations_required",
                      "population_mean": None, "population_std_ddof_0": None, "percentile": None, "zscore": None, "zscore_reason": None}
            if len(sample) >= 2:
                mean = fsum(sample) / len(sample)
                std = sqrt(fsum((v-mean)**2 for v in sample) / len(sample)) if len(set(sample)) > 1 else 0.0
                result.update(status="AVAILABLE", reason=None, population_mean=mean, population_std_ddof_0=std,
                    percentile=sum(v <= anchor_values[field] for v in sample)/len(sample),
                    zscore=(anchor_values[field]-mean)/std if std else None,
                    zscore_reason="zero_population_variance" if not std else None)
            variables[field] = result
        windows[str(window)] = {"expected_window_slots": window, "observed_slot_count": len(selected),
            "sample_count": len(days), "coverage_status": "COMPLETE" if len(days) == window else "PARTIAL",
            "sample_dates": days, "excluded_dates": missing, "missing_observed_history_slots": max(0, window-len(selected)),
            "slot_start_date": selected[0], "slot_end_date": selected[-1], "variables": variables}
    changes = {}
    for lag in LAGS:
        target = slots[-1-lag] if len(slots) > lag else None
        changes[str(lag)] = {"target_trade_date": target, "anchor_trade_date": anchor_day,
            "status": "AVAILABLE" if target in values else "UNAVAILABLE", "values": None,
            "reason": None if target in values else (excluded.get(target) or "insufficient_exact_observed_slots")}
        if target in values:
            changes[str(lag)]["values"] = {field: anchor_values[field]-values[target][field] for field in FIELDS}
    return {"status": "AVAILABLE", "anchor_trade_date": anchor_day, "anchor_values": anchor_values,
            "changes": changes, "windows": windows}


def verify_view(result, slots, source, rows, view_metadata, *, label="Si"):
    _require(set(result) == {"status", "anchor_trade_date", "anchor_values", "changes", "windows", *view_metadata}
        and result["status"] == "AVAILABLE" and result["anchor_trade_date"] == slots[-1], label+" statistics admitted view inventory")
    _require(_digest({key: result[key] for key in view_metadata}) == _digest(view_metadata),
                   label+" statistics independent consumer semantics and anchor clocks")
    _require(set(result["changes"]) == {str(lag) for lag in LAGS}
        and set(result["windows"]) == {str(window) for window in WINDOWS}, label+" statistics window and lag inventory")
    def numbers(fact):
        oi = Decimal(fact["total_open_interest"])
        result = {"total_open_interest": oi}
        for side in ("fiz", "yur"):
            raw = fact[side]
            for field in SIDE_FIELDS: result[side + "." + field] = Decimal(raw[field])
            result[side + ".gross"] = Decimal(raw["long"]) + Decimal(raw["short"])
            for field in ("long", "short", "net"):
                result[side + "." + field + "_share_of_oi"] = Decimal(raw[field]) / oi
            result[side + ".gross_share_of_two_sided_oi"] = (Decimal(raw["long"]) + Decimal(raw["short"])) / (2*oi)
        return result
    decimal_values = {day: numbers(source[day]) for day in slots if day in source}
    last = decimal_values[slots[-1]]
    def equal(actual, calculated):
        _require(type(actual) in (int, float) and isclose(actual, float(calculated), rel_tol=1e-11, abs_tol=1e-11), label+" statistics independent arithmetic")
    _require(set(result["anchor_values"]) == set(FIELDS), label+" statistics derived field coverage")
    for key in FIELDS: equal(result["anchor_values"][key], last[key])
    for lag in LAGS:
        target = slots[-1-lag] if len(slots) > lag else None
        item = result["changes"][str(lag)]
        _require(set(item) == {"target_trade_date", "anchor_trade_date", "status", "values", "reason"}
            and item["target_trade_date"] == target and item["anchor_trade_date"] == slots[-1], label+" statistics exact lag date")
        if target in decimal_values:
            _require(item["status"] == "AVAILABLE" and set(item["values"]) == set(FIELDS), label+" statistics lag coverage")
            _require(item["reason"] is None, label+" statistics lag reason")
            for key in FIELDS:
                if "share" not in key: _require(type(item["values"][key]) is int, label+" statistics integer delta")
                equal(item["values"][key], last[key]-decimal_values[target][key])
        else:
            excluded = {row["trade_date"]: row["reason"] for row in rows if row["status"] == "UNAVAILABLE"}
            _require(item["status"] == "UNAVAILABLE" and item["values"] is None
                and item["reason"] == (excluded.get(target) or "insufficient_exact_observed_slots"), label+" statistics missing lag")
    for window in WINDOWS:
        item = result["windows"][str(window)]
        selected = slots[-window:]; days = [day for day in selected if day in source]
        excluded = {row["trade_date"]: row["reason"] for row in rows if row["status"] == "UNAVAILABLE"}
        expected_excluded = [{"trade_date": day, "reason": excluded[day]} for day in selected if day not in source]
        window_metadata = {"expected_window_slots": window, "observed_slot_count": len(selected), "sample_count": len(days),
            "coverage_status": "COMPLETE" if len(days) == window else "PARTIAL", "sample_dates": days,
            "excluded_dates": expected_excluded, "missing_observed_history_slots": max(0, window-len(selected)),
            "slot_start_date": selected[0], "slot_end_date": selected[-1]}
        _require(_digest({key: val for key, val in item.items() if key != "variables"}) == _digest(window_metadata),
                       label+" statistics independent window metadata")
        _require(set(item) == {"expected_window_slots", "observed_slot_count", "sample_count", "coverage_status", "sample_dates",
            "excluded_dates", "missing_observed_history_slots", "slot_start_date", "slot_end_date", "variables"}
            and item["excluded_dates"] == expected_excluded and item["missing_observed_history_slots"] == max(0, window-len(selected))
            and item["slot_start_date"] == selected[0] and item["slot_end_date"] == selected[-1], label+" statistics exact excluded slot inventory")
        _require(item["sample_dates"] == days and item["sample_count"] == len(days)
            and item["expected_window_slots"] == window and item["observed_slot_count"] == len(selected)
            and item["coverage_status"] == ("COMPLETE" if len(days) == window else "PARTIAL"), label+" statistics exact subset coverage")
        _require(set(item["variables"]) == set(STAT_FIELDS), label+" statistics variable coverage")
        for key in STAT_FIELDS:
            metric = item["variables"][key]
            _require(set(metric) == {"status", "reason", "population_mean", "population_std_ddof_0", "percentile", "zscore", "zscore_reason"}, label+" statistics metric shape")
            if len(days) < 2:
                _require(metric["status"] == "UNAVAILABLE" and metric["reason"] == "minimum_two_admitted_observations_required"
                    and all(metric[k] is None for k in ("population_mean", "population_std_ddof_0", "percentile", "zscore", "zscore_reason")), label+" statistics minimum sample")
                continue
            with localcontext() as ctx:
                ctx.prec = 40
                values = [decimal_values[day][key] for day in days]
                mean = sum(values) / len(values)
                variance = sum((v-mean)**2 for v in values) / len(values)
                std = variance.sqrt()
                _require(metric["status"] == "AVAILABLE", label+" statistics metric omitted")
                _require(metric["reason"] is None, label+" statistics metric reason")
                equal(metric["population_mean"], mean); equal(metric["population_std_ddof_0"], std)
                equal(metric["percentile"], Decimal(sum(v <= last[key] for v in values))/len(values))
                if len(set(values)) == 1:
                    _require(metric["zscore"] is None and metric["zscore_reason"] == "zero_population_variance", label+" statistics constant sample")
                else:
                    _require(metric["zscore_reason"] is None, label+" statistics nonconstant zscore reason")
                    equal(metric["zscore"], (last[key]-mean)/std)

