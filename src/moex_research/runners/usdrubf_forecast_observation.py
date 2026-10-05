"""Bounded one-pass observation. No paper fills without an execution contract."""
from __future__ import annotations

from datetime import timedelta
from io import BytesIO
from pathlib import Path

from ..intelligence.usdrubf_forecast_journal import (
    ForecastJournalError, decode, digest, encode, fields, read_bytes, timestamp,
)
from ..intelligence.usdrubf_forecast_evaluation import FACTS_VERSION, FORECAST_V3, price, evaluate, validate_forecast


def _existing(journal, kind, identifier):
    path = journal._path(kind, identifier)
    if not path.exists():
        return None
    ref = {"kind": kind, "id": identifier, "sha256": digest(read_bytes(path))}
    journal.read(ref)
    return journal.acknowledge(ref)


def accepted_facts(journal, spec, reader, now):
    """Reuse Stage 7's admitted content-generation reader and partition validator.

    Only relevant partitions are materialized from verified exact byte objects.
    The observed partition dates do not establish a complete observation grid.
    """
    import pandas as pd
    import pyarrow.parquet as pq
    from moex_data.futures import freeze_step7_accepted_raw_5m as layer
    from moex_data.step9_rub_analysis_bundle import PointerSpec, _to_utc_series
    current = reader.get("mode") == "accepted_current"
    fields(reader, {"mode", "data_root"} if current else {"mode", "data_root", "accepted_start_date", "accepted_end_date"})
    root = Path(reader["data_root"]).resolve(strict=True)
    repo = Path(__file__).resolve().parents[3]
    if current:
        scope = layer.accepted_quote_history(root, "usdrubf_futures_family", repo_root=repo, current=True, as_of=now)
    else:
        scope = layer.accepted_quote_history(root, "usdrubf_futures_family",
            reader["accepted_start_date"], reader["accepted_end_date"], repo_root=repo)
    start, end = timestamp(spec["horizon_start"]), timestamp(spec["horizon_end"])
    if any(timestamp(b) - timestamp(a) != timedelta(minutes=5) for a, b in spec["observation_grid"]):
        raise ForecastJournalError("approved factual adapter requires explicit 5m observation grid")
    expectation = layer.quote_validation_expectation("usdrubf_futures_family",
        scope.accepted_dates[0] if current else reader["accepted_start_date"],
        scope.accepted_dates[-1] if current else reader["accepted_end_date"])
    selected = [r for r in scope.records if (start - timedelta(days=1)).date().isoformat() <= r["trade_date"] <= (end + timedelta(days=1)).date().isoformat()]
    if len(selected) > 40:
        raise ForecastJournalError("factual partition bound exceeded")
    frozen = []
    bars = []
    total = 0
    for record in selected:
        path = Path(record["snapshot_path"])
        path.resolve(strict=True).relative_to(root)
        raw = read_bytes(path)
        total += len(raw)
        if total > 64 * 1024 * 1024 or digest(raw) != record["sha256"]:
            raise ForecastJournalError("factual byte budget or content-attestation mismatch")
        footer = pq.ParquetFile(BytesIO(raw)).metadata
        if footer.num_rows > 100_000 or footer.num_columns > 100:
            raise ForecastJournalError("factual parquet bounds exceeded")
        frame = pd.read_parquet(BytesIO(raw))
        rows, _ = layer.stage2._validate_quote_partition(repo, frame, expectation,
            record["trade_date"], "forecast_observation_validation")
        if rows != record["row_count"]:
            raise ForecastJournalError("attested row count mismatch")
        key = journal._object(raw)
        frozen.append({"source_ref": str(path), "object_sha256": key, "trade_date": record["trade_date"]})
        temporal = PointerSpec("forecast.usdrubf", 3, "futures_raw_5m", "usdrubf_futures_family", "ts")
        frame = frame.copy()
        frame["ts"] = _to_utc_series(frame, temporal)
        for row in frame.to_dict("records"):
            close_at = pd.Timestamp(row["ts"])
            if close_at.tzinfo is None:
                raise ForecastJournalError("aware source bar timestamp required")
            close_at = close_at.to_pydatetime()
            open_at = close_at - timedelta(minutes=5)
            if open_at < start or close_at > end or close_at > now:
                continue
            bars.append({"open_at": open_at.isoformat(), "close_at": close_at.isoformat(),
                **{k: str(price(str(row[k]))) for k in ("open", "high", "low", "close")}})
    bars.sort(key=lambda b: timestamp(b["close_at"]))
    # Freeze the exact admission anchors, not only their identifiers.
    anchors = []
    for ref, expected in (scope.admission_anchors if current else
                         ((scope.pointer_ref, scope.marker_sha256), (scope.manifest_ref, scope.manifest_sha256))):
        path = layer._expand_root_ref(root, ref, "forecast admission anchor")
        raw = read_bytes(path)
        if digest(raw) != expected:
            raise ForecastJournalError("admission anchor changed during observation")
        anchors.append({"source_ref": ref, "object_sha256": journal._object(raw)})
    facts = {"schema_version": FACTS_VERSION, "instrument": "USDRUBF", "contract": "USDRUBF", "bars": bars}
    as_of = max((timestamp(b["close_at"]) for b in bars), default=start)
    source = {"source_ref": ("accepted_quote_history:" if current else "stage2_content_attested:") + scope.acceptance_run_id,
        "schema_version": FACTS_VERSION, "code_revision": "recorded_in_evaluation_runtime",
        "data_as_of": as_of.isoformat(), "available_at": now.isoformat(), "received_at": now.isoformat(),
        "quality_limitations": []}
    return encode(facts), source, {"partitions": frozen, "admission_anchors": anchors,
        "coverage_scope": "predeclared_grid_only_not_exchange_calendar", "historical_publication_time": "UNKNOWN"}


def run(journal, request):
    required = {"schema_version", "forecasts", "limit", "reader"}
    if isinstance(request, dict) and "revision" in request:
        required.add("revision")
    fields(request, required)
    if request["schema_version"] != "usdrubf.forecast_observation_request.v1":
        raise ForecastJournalError("unsupported observation request")
    if not isinstance(request["reader"], dict):
        raise ForecastJournalError("reader must be an object")
    limit = request["limit"]
    refs = request["forecasts"]
    if type(limit) is not int or not 1 <= limit <= 100 or not isinstance(refs, list) or len(refs) > limit:
        raise ForecastJournalError("explicit bounded forecast list required")
    if len({encode(ref) for ref in refs}) != len(refs):
        raise ForecastJournalError("duplicate forecast reference")
    records = [journal.read(ref) for ref in refs]
    if any(record["kind"] != "forecast" for record in records):
        raise ForecastJournalError("forecast references required")
    now = journal._now()
    revision = request.get("revision")
    previous_state = None
    if revision is not None:
        fields(revision, {"supersedes", "reason"})
        from ..intelligence.usdrubf_forecast_journal import text
        text(revision["reason"])
        if len(refs) != 1:
            raise ForecastJournalError("revision requires one exact forecast")
        previous_record = journal.read(revision["supersedes"])
        previous_state = previous_record["payload"]
        if previous_record["kind"] != "observation" or previous_state["forecast"] != refs[0] or timestamp(previous_record["recorded_at"]) > now:
            raise ForecastJournalError("observation revision identity/clock mismatch")
    output = []
    for ref, record in zip(refs, records):
        spec = validate_forecast(record["payload"])
        if spec["schema_version"] == FORECAST_V3:
            output.append({"forecast": ref, "status": "DEFERRED",
                "reason": spec["evaluation_policy"]["reason"], "requires_new_forecast_revision": True})
            continue
        identifier = "observe-" + ref["sha256"]
        if revision is not None:
            identifier = "revision-" + digest(encode({"forecast": ref, "revision": revision}))
        completed = _existing(journal, "observation", identifier)
        if completed:
            state = journal.read(completed)["payload"]
            journal.reproduce(state["evaluation"])
            for item in state["provenance"].get("partitions", []) + state["provenance"].get("admission_anchors", []):
                journal.object_bytes(item["object_sha256"])
            output.append({"forecast": ref, "observation": completed, "status": state["status"]})
            continue
        if now < timestamp(spec["horizon_end"]):
            output.append({"forecast": ref, "status": "PENDING", "reason": "HORIZON_NOT_CLOSED"})
            continue
        # A crash after factual capture resumes from those bytes, never refetches.
        factual = _existing(journal, "input", identifier)
        if factual:
            item = journal.read(factual)["payload"]
            raw, source, provenance = journal.object_bytes(item["object_sha256"]), item["source"], item["provenance"]
        else:
            reader = request["reader"]
            if reader.get("mode") == "synthetic":
                if spec.get("context", {}).get("registration_class") != "SYNTHETIC":
                    raise ForecastJournalError("synthetic reader cannot score a real forecast")
                fields(reader, {"mode", "facts", "source"})
                raw, source, provenance = encode(reader["facts"]), reader["source"], {"synthetic": True}
            elif reader.get("mode") in {"accepted_stage2", "accepted_current"}:
                try:
                    raw, source, provenance = accepted_facts(journal, spec, reader, now)
                except (ValueError, OSError) as exc:
                    output.append({"forecast": ref, "status": "NOT_EVALUABLE",
                        "reason": "APPROVED_FACTUAL_SOURCE_UNAVAILABLE: " + str(exc), "retryable": True})
                    continue
            else:
                raise ForecastJournalError("approved reader required; paper execution unavailable")
            from ..intelligence.usdrubf_forecast_journal import validate_source
            validate_source(source, now)
            evaluate(spec, decode(raw), evaluated_at=now, source_as_of=timestamp(source["data_as_of"]), limitations=source["quality_limitations"])
            factual = journal._put("input", identifier, {"source": source,
                "object_sha256": journal._object(raw), "provenance": provenance}, journal._finished(now))
        for item in provenance.get("partitions", []) + provenance.get("admission_anchors", []):
            journal.object_bytes(item["object_sha256"])
        evaluation = journal.evaluate(identifier, ref, raw, source,
            supersedes=previous_state["evaluation"] if previous_state else None,
            revision_reason=revision["reason"] if revision else None)
        result = journal.read(evaluation)["payload"]["report"]
        status = "COMPLETED" if result["coverage"]["status"] == "COMPLETE_FOR_DECLARED_GRID" else "NOT_EVALUABLE"
        state = {"forecast": ref, "factual_input": factual, "evaluation": evaluation,
            "status": status, "watermark": max((bar["close_at"] for bar in decode(raw)["bars"]), key=timestamp, default=None), "provenance": provenance,
            "mode": "FORECAST_OBSERVATION", "paper_pnl": None}
        if revision:
            state["revision"] = revision
        observation = journal._put("observation", identifier, state, journal._finished(now))
        output.append({"forecast": ref, "observation": observation, "status": status})
    return {"schema_version": "usdrubf.forecast_observation_run.v1", "items": output}


def report(journal, refs):
    if not isinstance(refs, list) or len(refs) > 100:
        raise ForecastJournalError("bounded observation refs required")
    groups = {}
    seen = set()
    for ref in refs:
        state = journal.read(ref)
        if state["kind"] != "observation":
            raise ForecastJournalError("observation reference required")
        state = state["payload"]
        evaluation = journal.read(state["evaluation"])["payload"]
        result = journal.reproduce(state["evaluation"])
        forecast = journal.read(state["forecast"])
        spec = forecast["payload"]
        root = forecast
        chain = set()
        while root["payload"]["supersedes"] is not None:
            if root["id"] in chain or len(chain) >= 1000:
                raise ForecastJournalError("forecast revision cycle/bound exceeded")
            chain.add(root["id"])
            root = journal.read(root["payload"]["supersedes"])
        # Reject ambiguous selection rather than choose the best revision/outcome.
        if root["id"] in seen:
            raise ForecastJournalError("one selected version per original forecast required")
        seen.add(root["id"])
        key = (spec["method_version"], spec.get("context", {}).get("horizon_label", "EXACT_GRID"), evaluation["registration_class"])
        group = groups.setdefault(key, {"eligible_count": 0, "complete_count": 0,
            "direction_evaluable_count": 0, "direction_correct_count": 0,
            "scenarios": {}, "unverifiable": [], "hypothesis_errors": []})
        group["eligible_count"] += 1
        group["complete_count"] += result["coverage"]["status"] == "COMPLETE_FOR_DECLARED_GRID"
        direction = result["direction"]
        group["direction_evaluable_count"] += direction["correct"] is not None
        group["direction_correct_count"] += direction["correct"] is True
        if direction["correct"] is False:
            group["hypothesis_errors"].append({"forecast": state["forecast"], "criterion": "direction"})
        if result["coverage"]["status"] != "COMPLETE_FOR_DECLARED_GRID":
            group["unverifiable"].append({"forecast": state["forecast"], "coverage": result["coverage"]})
        for scenario in result["scenarios"]:
            outcome = scenario["status"]
            group["scenarios"][outcome] = group["scenarios"].get(outcome, 0) + 1
            if outcome == "NOT_EVALUABLE":
                group["unverifiable"].append({"forecast": state["forecast"], "scenario": scenario})
    return {"schema_version": "usdrubf.forecast_error_report.v1", "groups": [
        {"method_version": key[0], "horizon": key[1], "registration_class": key[2], **value}
        for key, value in sorted(groups.items())], "paper_pnl": None,
        "denominator": "one_explicitly_selected_version_per_original_forecast", "automatic_training": False}
