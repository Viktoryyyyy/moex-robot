import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

from moex_data.futures import all_universe_raw_5m_backfill_slice as source
from moex_data.futures import continuous_d1_builder as cd1
from moex_data.futures import continuous_l3_6_common as common
from moex_data.futures import continuous_quality_report as quality
from moex_data.futures import continuous_roll_map_builder as roll
from moex_data.futures import continuous_series_builder as c5
from moex_data.futures import date_source_provenance as provenance
from moex_data.futures import daily_refresh_runner as compatibility_daily
from moex_data.futures import derived_d1_ohlcv_builder as d1
from moex_data.futures import raw_5m_loader as loader
from moex_data.futures import universal_daily_refresh_runner as daily


INGEST = "2026-05-21T08:00:00Z"
DAYS = ["2026-05-18", "2026-05-19"]


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def make_admitted(root, label=provenance.OBSERVED, days=None, layout="chunk", secid="USDRUBF", family="USDRUBF", gap=False):
    days = days or DAYS
    payload = pd.DataFrame([
        {"tradedate": day, "tradetime": time, "secid": secid,
         "pr_open": opening, "pr_high": high, "pr_low": low, "pr_close": close, "vol": volume}
        for day in days for time, opening, high, low, close, volume in
        [("10:00:00", 80, 83, 79, 82, 10), ("10:15:00" if gap else "10:05:00", 82, 84, 81, 83, 20)]
    ])
    endpoint = "https://apim.moex.com" + provenance.observed.observed_date_source_endpoint(secid)
    raw, meta = loader.normalize_tradestats(payload, secid, family, "RFUD", endpoint, INGEST, False, label)
    assert not meta["error"]
    if layout == "loader":
        raw = raw.drop(columns=["source_secid"])  # Original XML schema, not a new producer claim.
    paths = loader.write_partitions(raw, root, family, secid)
    run_id = "retained-" + layout + "-" + min(days)
    if layout == "chunk":
        chunk = "fixture-" + secid + "-" + min(days)
        erow = {"family_code": family, "secid": secid, "eligibility_snapshot_id": "eligibility-1"}
        q = source.qrow(run_id, chunk, erow, "registry-1", min(days), max(days), raw, "completed", "", paths)
        q["calendar_status"] = q["session_calendar_status"] = label
        qp = root / "futures/all_universe/quality/raw_5m_backfill" / ("chunk_id=" + chunk) / "quality_report.parquet"
        mp = root / "futures/all_universe/runs/raw_5m_backfill" / ("chunk_id=" + chunk) / "manifest.json"
        manifest = {"schema_version": source.SCHEMA_MANIFEST, "chunk_id": chunk,
            "input_eligibility_snapshot_id": "eligibility-1", "dataset_stage": "raw_5m", "family_code": family,
            "secid_list": [secid], "date_from": min(days), "date_till": max(days), "status": "succeeded",
            "started_at": run_id, "finished_at": "2026-05-21T08:01:00Z", "failed_secid": [],
            "skipped_secid": [], "deferred_secid": [], "output_partitions": paths}
    else:
        counts = loader.quality_counts(raw, set(days))
        q = {**counts, "schema_version": loader.SCHEMA_QUALITY, "run_id": run_id, "board": "RFUD",
             "family_code": family, "secid": secid, "requested_from": min(days), "requested_till": max(days),
             "source_endpoint_url": endpoint, "fetch_status": "completed", "normalization_error": None,
             provenance.STATUS: label, "quality_status": "pass"}
        qp = root / "futures/quality/raw_5m_loader/run_date=2026-05-21/futures_raw_5m_quality_report.parquet"
        mp = root / "futures/runs/raw_5m_loader/run_date=2026-05-21/manifest.json"
        manifest = {"schema_version": loader.SCHEMA_MANIFEST, "run_id": run_id, "ingest_ts": INGEST,
            "partition_paths_created": paths, "instrument_summaries": {secid: {"quality_status": "pass",
                "requested_from": min(days), "requested_till": max(days), "rows": len(raw), "partition_count": len(paths)}},
            "calendar_validation_summary": {provenance.STATUS: label, "calendar_from": min(days),
                "calendar_till": max(days), "expected_trading_days": len(days)}}
    qp.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame([q]).to_parquet(qp, index=False)
    mp.parent.mkdir(parents=True, exist_ok=True)
    mp.write_text(json.dumps(manifest), encoding="utf-8")
    return [Path(p) for p in paths], qp, mp


def admit(root, paths):
    return d1.validate_raw(d1.read_raw(paths), root)


def continuous(raw):
    row = roll.build_perpetual_row(pd.Series({"secid": "USDRUBF", "family_code": "USDRUBF", "board": "RFUD"}), "2026-05-21", "roll-run")
    mapping = pd.DataFrame([row])
    assert c5.validate_roll_map(mapping, c5.ROLL_POLICY_ID, c5.ADJUSTMENT_POLICY_ID, ["USDRUBF"], []) == []
    out = c5.select_continuous_rows(c5.normalize_raw(raw), c5.normalize_roll_map(mapping), INGEST, c5.ROLL_POLICY_ID, c5.ADJUSTMENT_POLICY_ID)
    assert not c5.validate_continuous(out, [], c5.ROLL_POLICY_ID, c5.ADJUSTMENT_POLICY_ID)
    result = cd1.aggregate_d1(cd1.normalize_continuous_5m(out), INGEST)
    assert not cd1.validate_d1(result, out, [], cd1.ROLL_POLICY_ID, cd1.ADJUSTMENT_POLICY_ID)
    return out, result, mapping


@pytest.mark.parametrize("label,layout", [(provenance.XML, "loader"), (provenance.XML, "chunk"), (provenance.OBSERVED, "chunk")])
def test_actual_raw_d1_continuous_seam_preserves_admission(tmp_path, label, layout):
    paths, qp, mp = make_admitted(tmp_path, label, layout=layout)
    before = {str(p): sha(p) for p in [*paths, qp, mp]}
    raw = admit(tmp_path, paths)
    daily_bars = d1.aggregate_d1(d1.normalize_raw(raw), INGEST)
    five, downstream, mapping = continuous(raw)
    assert set(daily_bars[provenance.STATUS]) == {label}
    assert set(downstream[provenance.STATUS]) == {label}
    assert set(five["roll_date_source_status"]) == {provenance.OBSERVED}
    assert daily_bars.iloc[0][["open", "high", "low", "close", "volume"]].tolist() == [80, 84, 79, 83, 30]
    assert not provenance.continuous_seam_blockers(five, downstream)
    for value in downstream[provenance.EVIDENCE]:
        item = json.loads(value)[0]
        assert item["scope"] == provenance.EVIDENCE_SCOPE
        assert item["quality_sha256"] == sha(qp)
        assert item["manifest_sha256"] == sha(mp)
        assert item["raw_sha256"] == sha(tmp_path / item["partition"])
    assert before == {str(p): sha(p) for p in [*paths, qp, mp]}


def test_mixed_history_preserves_per_date_quality_and_manifest_summaries(tmp_path):
    old, _, _ = make_admitted(tmp_path, provenance.XML, [DAYS[0]], "loader")
    new, _, _ = make_admitted(tmp_path, provenance.OBSERVED, [DAYS[1]])
    raw = admit(tmp_path, old + new)
    bars = d1.aggregate_d1(d1.normalize_raw(raw), INGEST)
    assert bars[provenance.STATUS].tolist() == [provenance.XML, provenance.OBSERVED]
    summaries = d1.per_instrument(raw, bars, [])
    q = d1.build_quality_rows("run", "2026-05-21", summaries).iloc[0]
    assert q[provenance.STATUS] == "mixed_date_sources"
    assert json.loads(q["date_source_by_trade_date_json"]) == dict(zip(DAYS, [provenance.XML, provenance.OBSERVED]))
    assert provenance.summary(bars)["statuses"] == sorted(provenance.SUPPORTED)
    _, downstream, _ = continuous(raw)
    assert downstream[provenance.STATUS].tolist() == bars[provenance.STATUS].tolist()


@pytest.mark.parametrize("field,value", [(provenance.STATUS, None), (provenance.STATUS, "untrusted"),
    ("secid", "SiM6"), ("family_code", "Si"), ("board", None), ("schema_version", None),
    ("source", "other"), ("source_secid", None), ("source_secid", "SiM6"),
    ("source_endpoint_url", "https://apim.moex.com/iss/datashop/algopack/fo/tradestats/SiM6.json"),
    ("ts", None), ("end", "2026-05-18 11:00:00"), ("session_date", "2026-05-20"),
    ("open", float("inf")), ("volume", None)])
def test_invalid_raw_never_admitted(tmp_path, field, value):
    paths, _, _ = make_admitted(tmp_path)
    frame = pd.read_parquet(paths[0])
    if field == "open":
        frame["open"] = frame["open"].astype(float)
    frame.loc[0, field] = value
    frame.to_parquet(paths[0], index=False)
    with pytest.raises(RuntimeError, match="date_source_provenance"):
        admit(tmp_path, paths)


def test_conflicting_provenance_inside_one_day_rejected(tmp_path):
    paths, _, _ = make_admitted(tmp_path)
    raw = d1.read_raw(paths)
    raw.loc[0, provenance.STATUS] = provenance.XML
    with pytest.raises(RuntimeError, match="contradictory calendar"):
        d1.validate_raw(raw, tmp_path)
    with pytest.raises(RuntimeError, match="contradictory calendar"):
        d1.aggregate_d1(d1.normalize_raw(raw), INGEST)


@pytest.mark.parametrize("mutation", ["missing_manifest", "wrong_run", "wrong_family", "failed_instrument", "wrong_date",
    "missing_membership", "quality_count", "quality_min_ts", "quality_label", "quality_error", "quality_failed",
    "raw_newer", "cohort_missing", "cohort_ingest", "legacy_ingest"])
def test_mismatched_or_stale_evidence_fails_closed(tmp_path, mutation):
    layout = "loader" if mutation == "legacy_ingest" else "chunk"
    paths, qp, mp = make_admitted(tmp_path, provenance.XML if layout == "loader" else provenance.OBSERVED, layout=layout)
    m = json.loads(mp.read_text())
    q = pd.read_parquet(qp)
    if mutation == "missing_manifest":
        mp.unlink()
    elif mutation == "wrong_run": m["started_at"] = "other"
    elif mutation == "wrong_family": m["family_code"] = "Si"
    elif mutation == "failed_instrument": m["failed_secid"] = ["USDRUBF"]
    elif mutation == "wrong_date": m["date_from"] = "2026-05-17"
    elif mutation == "missing_membership": m["output_partitions"] = []
    elif mutation == "quality_count": q.loc[0, "rows_written"] = 1
    elif mutation == "quality_min_ts": q.loc[0, "min_ts"] = "2026-05-18 11:00:00"
    elif mutation == "quality_label": q.loc[0, "calendar_status"] = provenance.XML
    elif mutation == "quality_error": q.loc[0, "duplicate_ts_count"] = 1
    elif mutation == "quality_failed": q.loc[0, "quality_status"] = "fail"
    elif mutation == "raw_newer": m["finished_at"] = "2026-05-21T07:59:00Z"
    elif mutation == "legacy_ingest": m["ingest_ts"] = "2026-05-21T07:59:00Z"
    elif mutation == "cohort_missing": paths[1].unlink()
    elif mutation == "cohort_ingest":
        raw = pd.read_parquet(paths[1])
        raw["ingest_ts"] = "2026-05-21T07:59:00Z"
        raw.to_parquet(paths[1], index=False)
    if mutation != "missing_manifest": mp.write_text(json.dumps(m))
    q.to_parquet(qp, index=False)
    with pytest.raises(RuntimeError, match="unverifiable"):
        admit(tmp_path, paths[:1])


def test_gap_unknown_is_preserved_and_duplicates_and_unclosed_dates_rejected(tmp_path):
    paths, _, _ = make_admitted(tmp_path, gap=True)
    raw = admit(tmp_path, paths)
    assert json.loads(raw.iloc[0][provenance.EVIDENCE])[0]["gap_count"] is None
    assert "not_a_complete_exchange_calendar" in provenance.summary(raw)["coverage_claim"]
    with pytest.raises(RuntimeError, match="unclosed"):
        provenance.AdmissionIndex(tmp_path, closed_before=DAYS[1]).admit(d1.read_raw(paths))
    duplicate = pd.concat([raw, raw.iloc[:1]], ignore_index=True)
    with pytest.raises(RuntimeError, match="duplicate raw"):
        provenance.validate_structure(duplicate)


def test_partial_chunk_can_admit_successful_instrument_but_not_cumulative_other_membership(tmp_path):
    paths, qp, mp = make_admitted(tmp_path)
    m = json.loads(mp.read_text())
    m.update(status="partial_failed", failed_secid=["OTHER"], secid_list=["OTHER", "USDRUBF"], input_eligibility_snapshot_id="eligibility-other")
    mp.write_text(json.dumps(m))
    q = pd.read_parquet(qp)
    other = q.iloc[0].to_dict()
    other.update(secid="OTHER", eligibility_snapshot_id="eligibility-other", quality_status="fail", rows_written=0)
    q = pd.concat([pd.DataFrame([other]), q], ignore_index=True)
    q.to_parquet(qp, index=False)
    assert len(admit(tmp_path, paths)) == 4
    foreign = str(tmp_path / "futures/raw_5m/trade_date=2026-05-18/family=USDRUBF/secid=OTHER/part.parquet")
    q["output_partitions_json"] = json.dumps([foreign])
    q.to_parquet(qp, index=False)
    m["output_partitions"] = [foreign, *m["output_partitions"]]
    mp.write_text(json.dumps(m))
    with pytest.raises(RuntimeError, match="unverifiable"):
        admit(tmp_path, paths)


def test_retry_and_rejected_input_preserve_existing_artifacts(tmp_path):
    paths, qp, mp = make_admitted(tmp_path)
    raw = admit(tmp_path, paths)
    bars = d1.aggregate_d1(d1.normalize_raw(raw), INGEST)
    outputs = [Path(p) for p in d1.write_partitions(bars, tmp_path)]
    before = {str(p): sha(p) for p in [*paths, qp, mp, *outputs]}
    retry = d1.aggregate_d1(d1.normalize_raw(admit(tmp_path, paths)), "2026-05-22T08:00:00Z")
    d1.write_partitions(retry, tmp_path)
    assert before == {str(p): sha(p) for p in [*paths, qp, mp, *outputs]}
    q = pd.read_parquet(qp)
    q["quality_status"] = "fail"
    q.to_parquet(qp, index=False)
    with pytest.raises(RuntimeError): admit(tmp_path, paths)
    assert {str(p): sha(p) for p in outputs} == {str(p): before[str(p)] for p in outputs}


@pytest.mark.parametrize("label", sorted(provenance.SUPPORTED))
def test_roll_source_pairs_are_validated_without_changing_policy(label):
    row = roll.build_perpetual_row(pd.Series({"secid": "USDRUBF", "family_code": "USDRUBF", "board": "RFUD"}), "2026-05-21", "roll")
    row.update(calendar_status=label, calendar_source=provenance.ROLL_SOURCES[label])
    frame = pd.DataFrame([row])
    assert not c5.validate_roll_map(frame, c5.ROLL_POLICY_ID, c5.ADJUSTMENT_POLICY_ID, ["USDRUBF"], [])
    assert common.roll_buildable_map(frame)["USDRUBF"][0]
    frame["calendar_source"] = "unrelated"
    assert c5.validate_roll_map(frame, c5.ROLL_POLICY_ID, c5.ADJUSTMENT_POLICY_ID, ["USDRUBF"], [])
    assert not common.roll_buildable_map(frame)["USDRUBF"][0]


def test_legacy_continuous_outputs_remain_readable_without_invented_origin(tmp_path):
    paths, _, _ = make_admitted(tmp_path)
    five, _, _ = continuous(admit(tmp_path, paths))
    legacy = five.drop(columns=[provenance.STATUS, provenance.EVIDENCE, "roll_date_source_status", "roll_date_source"])
    old = cd1.aggregate_d1(cd1.normalize_continuous_5m(legacy), INGEST)
    assert provenance.STATUS not in old
    assert provenance.summary(old)["status"] == "legacy_not_recorded"
    partial = legacy.assign(roll_date_source_status=provenance.OBSERVED, roll_date_source=provenance.ROLL_SOURCES[provenance.OBSERVED])
    assert provenance.derived_blockers(partial)


@pytest.mark.parametrize("use_cache", [False, True])
def test_admission_index_rejects_replaced_quality_instead_of_hashing_new_failure_as_old_pass(tmp_path, use_cache):
    paths, qp, _ = make_admitted(tmp_path)
    index = provenance.AdmissionIndex(tmp_path)
    raw = d1.read_raw(paths)
    if use_cache:
        assert len(index.admit(raw)) == 4
    q = pd.read_parquet(qp)
    q["quality_status"] = "fail"
    q.to_parquet(qp, index=False)
    with pytest.raises(RuntimeError, match="unverifiable"):
        index.admit(raw)


def test_read_mixed_legacy_and_new_continuous_partitions_without_fabricating_or_dropping_provenance(tmp_path):
    paths, _, _ = make_admitted(tmp_path)
    five, _, _ = continuous(admit(tmp_path, paths))
    old_path, new_path = tmp_path / "old.parquet", tmp_path / "new.parquet"
    old = five.loc[five["trade_date"] == DAYS[0]].drop(columns=list(provenance.DERIVED_FIELDS))
    old.to_parquet(old_path, index=False)
    five.loc[five["trade_date"] == DAYS[1]].to_parquet(new_path, index=False)
    combined = cd1.read_partitions([old_path, new_path])
    assert not cd1.validate_continuous_5m(combined, [], cd1.ROLL_POLICY_ID, cd1.ADJUSTMENT_POLICY_ID)
    bars = cd1.aggregate_d1(cd1.normalize_continuous_5m(combined), INGEST)
    assert not provenance.continuous_seam_blockers(combined, bars)
    assert provenance.summary(bars)["statuses"] == [provenance.OBSERVED, "legacy_not_recorded"]
    output_paths = cd1.write_partitions(Path.cwd(), tmp_path, bars, cd1.ROLL_POLICY_ID, cd1.ADJUSTMENT_POLICY_ID)
    old_result = pd.read_parquet(next(p for p in output_paths if "trade_date=" + DAYS[0] in p))
    new_result = pd.read_parquet(next(p for p in output_paths if "trade_date=" + DAYS[1] in p))
    assert not any(field in old_result for field in provenance.DERIVED_FIELDS)
    assert set(new_result[provenance.STATUS]) == {provenance.OBSERVED}


def test_continuous_d1_provenance_tampering_is_reported(tmp_path):
    paths, _, _ = make_admitted(tmp_path)
    five, bars, _ = continuous(admit(tmp_path, paths))
    bars.loc[0, provenance.EVIDENCE] = "[]"
    assert provenance.continuous_seam_blockers(five, bars)


@pytest.mark.parametrize("field,value", [("raw_sha256", "invalid"), ("run_id", None),
    ("partition", "futures/raw_5m/trade_date=2026-05-17/family=USDRUBF/secid=USDRUBF/part.parquet")])
def test_downstream_rejects_malformed_or_mismatched_evidence(tmp_path, field, value):
    paths, _, _ = make_admitted(tmp_path)
    five, bars, _ = continuous(admit(tmp_path, paths))
    items = json.loads(bars.iloc[0][provenance.EVIDENCE])
    items[0][field] = value
    bars.loc[0, provenance.EVIDENCE] = json.dumps(items)
    assert provenance.continuous_seam_blockers(five, bars)


def install_eligibility(root):
    path = d1.eligibility_path(root, "2026-05-21", "")
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame([{"secid": "USDRUBF", "family_code": "USDRUBF", "board": "RFUD", "classification_status": "included",
        "raw_5m_eligible": True, "raw_d1_eligible": False, "schema_version": d1.SCHEMA_ELIGIBILITY, "notes": "fixture"}]).to_parquet(path, index=False)


def test_real_d1_cli_entrypoint_quality_manifest_and_failure_preservation(tmp_path, monkeypatch):
    paths, qp, _ = make_admitted(tmp_path)
    install_eligibility(tmp_path)
    monkeypatch.setattr(d1, "load_dotenv", None)
    monkeypatch.setattr(d1.sys, "argv", ["builder", "--run-date", "2026-05-21", "--snapshot-date", "2026-05-21", "--data-root", str(tmp_path)])
    assert d1.main() == 0
    outputs = d1.output_paths(tmp_path, "2026-05-21")
    manifest = json.loads(Path(outputs["manifest"]).read_text())
    report = pd.read_parquet(outputs["quality_report"])
    assert manifest["calendar_validation_summary"][provenance.STATUS] == provenance.OBSERVED
    assert report.iloc[0][provenance.STATUS] == provenance.OBSERVED
    assert manifest["instrument_summaries"]["USDRUBF"]["date_source_by_trade_date"] == dict.fromkeys(DAYS, provenance.OBSERVED)
    protected = [Path(outputs["manifest"]), Path(outputs["quality_report"]), d1.refined_eligibility_path(tmp_path, "2026-05-21", "")]
    before = {str(p): sha(p) for p in protected}
    broken = pd.read_parquet(paths[0])
    broken[provenance.STATUS] = "invalid"
    broken.to_parquet(paths[0], index=False)
    with pytest.raises(RuntimeError): d1.main()
    assert before == {str(p): sha(p) for p in protected}


def test_daily_runner_propagates_real_d1_failure_and_stops_before_downstream(tmp_path, monkeypatch):
    paths, _, _ = make_admitted(tmp_path)
    install_eligibility(tmp_path)
    raw = pd.read_parquet(paths[0])
    raw[provenance.STATUS] = "unverifiable"
    raw.to_parquet(paths[0], index=False)
    monkeypatch.setattr(daily, "load_dotenv", None)
    monkeypatch.setattr(daily, "CANONICAL_STAGE_IDS", ["raw_d1_derivation", "continuous_d1"])
    monkeypatch.setattr(daily.sys, "argv", ["daily", "--run-date", "2026-05-21", "--snapshot-date", "2026-05-21", "--data-root", str(tmp_path)])
    assert daily.main() == 1
    manifests = list((tmp_path / "futures").rglob("manifest.json"))
    result = next(json.loads(p.read_text()) for p in manifests if "universal_daily_refresh" in str(p))
    assert result["universal_daily_refresh_result_verdict"] == "fail"
    stages = result["child_component_status"]
    assert stages[0]["stage_id"] == "raw_d1_derivation"
    assert len(stages) == 1
    assert "date_source_provenance" in stages[0]["stderr_tail"]


def test_continuous_quality_and_manifest_keep_raw_and_roll_sources_separate(tmp_path):
    paths, _, _ = make_admitted(tmp_path, provenance.XML, layout="loader")
    five, bars, mapping = continuous(admit(tmp_path, paths))
    placeholder = tmp_path / "placeholder.parquet"
    rows = quality.build_quality_rows(Path.cwd(), tmp_path, "run", "2026-05-21", "2026-05-21",
        ["USDRUBF"], [], placeholder, placeholder, placeholder, [placeholder], [placeholder],
        True, True, True, five, bars, mapping, pd.DataFrame())
    report = pd.DataFrame(rows)
    observed = report.loc[report["family_code"] == "USDRUBF"]
    assert set(observed["calendar_status"]) == {provenance.OBSERVED}
    assert {json.loads(x)["status"] for x in observed["date_source_summary_json"]} == {provenance.XML}
    check = observed.loc[observed["check_id"] == "date_source_provenance"].iloc[0]
    assert check["check_status"] == "pass"
    result = quality.build_manifest("run", "2026-05-21", "2026-05-21", INGEST, INGEST,
        ["USDRUBF"], [], placeholder, placeholder, placeholder, [placeholder], [placeholder],
        placeholder, placeholder, tmp_path, tmp_path, five, bars, mapping, report, "a" * 40)
    assert result["calendar_status"] == provenance.OBSERVED
    assert result["raw_date_source_summary"]["status"] == provenance.XML
    assert result["date_source_evidence"][0]["status"] == provenance.XML


def compatibility_manifest_fixture(root, labels):
    mapping = pd.DataFrame([{"schema_version": "futures_continuous_roll_map.v1",
        "calendar_status": label, "calendar_source": provenance.ROLL_SOURCES[label]} for label in labels])
    paths = {"continuous_builder_manifest": str(root / "manifest.json"),
        "continuous_quality_report": str(root / "quality.parquet"), "continuous_roll_map": str(root / "roll.parquet")}
    mapping.to_parquet(paths["continuous_roll_map"], index=False)
    pd.DataFrame([{"schema_version": "futures_continuous_quality_report.v1", "check_status": "pass"}]).to_parquet(
        paths["continuous_quality_report"], index=False)
    summary = provenance.summary(mapping, "calendar_status")
    manifest = {"schema_version": "futures_continuous_builder_manifest.v1", "builder_result_verdict": "pass",
        "builder_whitelist_applied": ["USDRUBF"], "excluded_instruments_confirmed": [],
        "roll_policy_id": compatibility_daily.ROLL_POLICY_ID, "adjustment_policy_id": compatibility_daily.ADJUSTMENT_POLICY_ID,
        "calendar_status": summary["status"], "roll_date_source_summary": summary,
        "quality_status_counts": {"pass": 1}, "usdrubf_identity_check": {"status": "pass"}, "source_lineage_check": {"status": "pass"}}
    Path(paths["continuous_builder_manifest"]).write_text(json.dumps(manifest))
    return paths, manifest, mapping


@pytest.mark.parametrize("labels", [[provenance.XML], [provenance.OBSERVED], [provenance.XML, provenance.OBSERVED]])
def test_compatibility_runner_accepts_actual_roll_provenance(tmp_path, labels):
    paths, manifest, _ = compatibility_manifest_fixture(tmp_path, labels)
    actual, _ = compatibility_daily.validate_continuous_manifest(paths, ["USDRUBF"], [])
    assert actual["calendar_status"] == manifest["calendar_status"]


def test_compatibility_runner_keeps_legacy_xml_manifest_readable(tmp_path):
    paths, manifest, _ = compatibility_manifest_fixture(tmp_path, [provenance.XML])
    manifest.pop("roll_date_source_summary")
    Path(paths["continuous_builder_manifest"]).write_text(json.dumps(manifest))
    assert compatibility_daily.validate_continuous_manifest(paths, ["USDRUBF"], [])[0] == manifest


@pytest.mark.parametrize("mutation", ["wrong_source", "null_source", "missing_source", "unsupported_status",
    "relabeled_manifest", "wrong_summary", "missing_summary", "empty_roll_map"])
def test_compatibility_runner_rejects_unverifiable_roll_provenance(tmp_path, mutation):
    paths, manifest, mapping = compatibility_manifest_fixture(tmp_path, [provenance.OBSERVED])
    if mutation == "wrong_source": mapping["calendar_source"] = provenance.ROLL_SOURCES[provenance.XML]
    elif mutation == "null_source": mapping["calendar_source"] = None
    elif mutation == "missing_source": mapping = mapping.drop(columns=["calendar_source"])
    elif mutation == "unsupported_status": mapping["calendar_status"] = "untrusted"
    elif mutation == "relabeled_manifest": manifest["calendar_status"] = provenance.XML
    elif mutation == "wrong_summary": manifest["roll_date_source_summary"]["rows_by_status"][provenance.OBSERVED] = 2
    elif mutation == "missing_summary": manifest.pop("roll_date_source_summary")
    elif mutation == "empty_roll_map": mapping = mapping.iloc[:0]
    mapping.to_parquet(paths["continuous_roll_map"], index=False)
    Path(paths["continuous_builder_manifest"]).write_text(json.dumps(manifest))
    with pytest.raises(RuntimeError):
        compatibility_daily.validate_continuous_manifest(paths, ["USDRUBF"], [])
