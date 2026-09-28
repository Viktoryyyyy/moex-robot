"""Synthetic source inputs; real refresh, admission, Parquet, JSON and reader."""
import base64
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from hashlib import sha256
import json

import pandas as pd
import pytest

from moex_data import rub_analysis_bundle_v2 as bundle
from moex_data import step9_rub_analysis_bundle as legacy
from moex_data import step7_rub_native_d1_w1_materializer as stage7
from src.moex_research.runners import usdrubf_s7_3_chat_analysis_snapshot_live_market_oi as live
from test_step9_rub_analysis_bundle import _materialize_pointer
from test_live_market_publication_freshness import shifted_market
from test_futoi_live_date_identity_repair_v1 import _binding_for, _root_frame, source

NOW = datetime(2026, 9, 24, 7, 36, tzinfo=timezone.utc)


def differences(left, right, path=""):
    if isinstance(left, dict) and isinstance(right, dict):
        return [diff for key in left.keys() | right.keys()
                for diff in differences(left.get(key), right.get(key), path+"/"+str(key))]
    if isinstance(left, list) and isinstance(right, list) and len(left) == len(right):
        return [diff for i, (a, b) in enumerate(zip(left, right)) for diff in differences(a, b, path+"/"+str(i))]
    return [] if left == right else [(path, left, right)]


def accepted_periods(root, monkeypatch):
    monkeypatch.setenv("MOEX_DATA_ROOT", str(root))
    for instrument, secid in (("usdrubf_futures_family", "USDRUBF"), ("cnyrubf_futures_family", "CNYRUBF")):
        rows = []
        for i, day in enumerate(pd.date_range("2026-08-03", "2026-09-23", freq="B")):
            rows.append(dict(instrument_id=instrument, secid=secid, timeframe="1D",
                trade_date=str(day.date()), period_start_date=str(day.date()), period_end_date=str(day.date()),
                availability_ts_utc=(day.to_pydatetime().replace(tzinfo=timezone.utc)+timedelta(days=1, hours=3)).isoformat(),
                open=80.+i, high=82.+i, low=79.+i, close=81.+i, volume=100, value=10000,
                num_trades=10, source_row_count=10, source_period_count=1, source_lineage_sha256="a"*64))
        d1 = pd.DataFrame(rows)
        frames = {"1D": d1, "1W": stage7.build_w1(d1, history_start="2026-08-03", history_end="2026-09-23")}
        for spec in legacy._stage7_specs("weekly"):
            if spec.instrument_id != instrument:
                continue
            frame = frames[spec.timeframe].copy()
            if spec.dataset_id == "rub_technical_features_htf":
                frame = stage7.build_technical_features(frame, source_ohlcv_run_id="producer_run")
            frame["build_ts_utc"] = (NOW-timedelta(minutes=1)).isoformat()
            path = _materialize_pointer(root, spec)
            pointer = json.loads(path.read_bytes())
            partition = root / pointer["partition_ref"].removeprefix("${MOEX_DATA_ROOT}/")
            frame.to_parquet(partition, index=False)
            pointer["partition_sha256"] = sha256(partition.read_bytes()).hexdigest()
            path.write_text(json.dumps(pointer))


def producers():
    result = live.base.default_producers()
    for name in list(result):
        if not name.startswith("stage9_"):
            def missing(now):
                raise OSError("synthetic fixture has no external source")
            result[name] = missing
    return result


def source_io(root, monkeypatch, *, failed=None):
    from moex_data.futures import materialize_futoi_instrument as materializer
    accepted_periods(root, monkeypatch)
    monkeypatch.setattr(materializer, "_registry_binding", lambda _path, instrument: _binding_for(instrument))
    monkeypatch.setattr(materializer, "_utc_now_root", lambda: NOW.isoformat())
    monkeypatch.setattr(source.observed_dates, "reference_secid", lambda inst: "USDRUBF")
    monkeypatch.setattr(source.observed_dates, "_exact_date_has_secid", lambda day, **kw: str(day) in {"2026-09-23", "2026-09-24"})
    def fetch(ticker, day, timeout, url):
        instrument = ticker+"_futures_family"
        raw = _root_frame(day=day, instrument=instrument)
        raw["sess_id"] = 2**53+1
        raw["seqnum"] = 2**53+3
        if (ticker, day) == failed:
            latest = raw.copy()
            latest["seqnum"] = 2**53+4
            latest.loc[0, ["pos", "pos_long", "pos_short"]] = [726369, 927387, -201018]
            latest.loc[1, ["pos", "pos_long", "pos_short"]] = [-726363, 4297389, -5023752]
            raw = pd.concat([raw, latest], ignore_index=True)
        return raw, "https://apim.moex.com/iss/analyticalproducts/futoi/securities/"+ticker+".json"
    monkeypatch.setattr(materializer, "_fetch_exact", fetch)
    monkeypatch.setattr(live.current_context.current, "current_producers", producers)
    monkeypatch.setattr(live.base, "load_dotenv", lambda *a, **k: None)
    monkeypatch.setattr(live.base, "install_timestamp_policy", lambda: None)
    monkeypatch.setattr("moex_data.rub_dated_hour_source.acquire", lambda **kw: {"latest_attempts": {}})


def current(view):
    return view["components"]["stage9_daily"]["data"]["sections"]["current_market"]["items"]


@pytest.mark.parametrize("failed", [None, ("si", "2026-09-24"), ("si", "2026-09-23"),
                                    ("cr", "2026-09-24"), ("cr", "2026-09-23")])
def test_heavy_refresh_saved_json_and_reader_keep_sections_and_independent_roots(tmp_path, monkeypatch, failed):
    source_io(tmp_path, monkeypatch, failed=failed)
    before = {str(p): p.read_bytes() for p in tmp_path.rglob("current.json")}
    saved, path = live.refresh_snapshot(now_fn=lambda: NOW, live_loader=lambda: shifted_market(NOW))
    assert json.loads(path.read_bytes()) == saved
    frozen_bytes = path.read_bytes()
    read, _ = live.base.read_current_snapshot(now_fn=lambda: NOW)
    assert path.read_bytes() == frozen_bytes
    for view in (saved, read):
        for scope, count in (("daily", 4), ("weekly", 8)):
            component = view["components"]["stage9_"+scope]
            data = component["data"]
            assert component["status"] == "PARTIAL"
            assert data["schema_version"] == bundle.SCHEMA
            assert data["server_core"]["freshness_alignment"]["status"] == "POLICY_DEFINED"
            assert data["readiness"]["analysis_bundle_complete"] is False
            assert set(data["sections"]) == set(bundle.POLICY["sections"])
            periods = data["sections"]["completed_periods"]
            assert len(periods["items"]) == count
            assert periods["status"] == "AVAILABLE", periods
            for item in periods["items"].values():
                assert item["freshness"]["source_end_date"] in ("2026-09-23", "2026-09-20")
                assert item["freshness"]["session_completion_proven"] is False
                assert item["freshness"]["current_use_allowed"] is False
            assert data["position_risk"]["status"] == "not_supplied"
            assert data["external_context"]["missing_does_not_mean_neutral"] is True
            assert all(not flag for key, flag in data["quality_gates"].items() if key in bundle.FLAGS)
        items = current(view)
        assert items["si_front"]["status"] == "AVAILABLE"
        assert items["basis_carry"]["values"]
        for ticker, name in (("si", "futoi_live"), ("cr", "futoi_live_cr")):
            expected = failed != (ticker, "2026-09-24")
            assert (items[name]["status"] == "AVAILABLE") is expected, items[name]
            if expected:
                fact = items[name]["values"]["factual"]
                assert fact["sess_id"] == 2**53+1
                assert items[name]["source_identity"]["source_ticker"] == ticker
                assert "secid" not in fact
                assert items[name]["evidence_verification"]["status"] == "PASS"
            else:
                assert items[name]["evidence"]["failed_attempt_evidence"]
    assert read["factual_release"]["analysis_bundles"]["daily"]["schema_version"] == bundle.SCHEMA
    if failed is None:
        from moex_data import rub_factual_release
        exported = rub_factual_release.build(saved, now=NOW, code_revision="a"*40)
        assert not differences(exported["analysis_bundles"]["weekly"]["sections"], read["factual_release"]["analysis_bundles"]["weekly"]["sections"])
        built = legacy.build_analysis_bundle(scope="daily", as_of=NOW.isoformat(), schema_version=bundle.SCHEMA)
        assert built == read["components"]["stage9_daily"]["data"]
        package = rub_factual_release.compact(saved, now=NOW, code_revision="a"*40)
        assert "base64" not in json.dumps(package)
        for scope in ("daily", "weekly"):
            compact = package["analysis_bundles"][scope]
            full = exported["analysis_bundles"][scope]
            assert compact["selection_contract"] == full["selection_contract"]
            assert compact["readiness"] == full["readiness"]
            assert compact["sections"]["current_market"] == full["sections"]["current_market"]
            assert compact["sections"]["historical_comparisons"] == full["sections"]["historical_comparisons"]
            for block in compact["server_core"]["blocks"]:
                assert block["source_envelope"]["pointer_sha256"]
                assert block["frozen_evidence_ref"]["bytes_in_compact_package"] is False
        exported_path = rub_factual_release.export_current(output=tmp_path/"export", now_fn=lambda: NOW,
            reader=live.base.read_current_snapshot, code_revision="a"*40)
        assert json.loads(exported_path.read_bytes())["analysis_bundles"] == package["analysis_bundles"]
        from moex_data.rub_factual_release_acceptance import projection_completeness
        for seconds in (1, 61, 1201):
            later = NOW+timedelta(seconds=seconds)
            later_release = rub_factual_release.build(saved, now=later, code_revision="a"*40)
            projection_completeness(saved, later_release, now=later)
            later_items = later_release["analysis_bundles"]["daily"]["sections"]["current_market"]["items"]
            if seconds >= 61:
                assert later_items["si_front"]["status"] == "UNAVAILABLE"
            if seconds >= 1201:
                assert later_items["futoi_live"]["status"] == "UNAVAILABLE"
        for mutation in ("missing_bundle", "missing_section", "changed_value", "changed_identity", "extra_item", "changed_evidence"):
            damaged = deepcopy(exported)
            daily = damaged["analysis_bundles"]["daily"]
            items = daily["sections"]["current_market"]["items"]
            if mutation == "missing_bundle": damaged["analysis_bundles"].pop("daily")
            elif mutation == "missing_section": daily["sections"].pop("current_market")
            elif mutation == "changed_value": items["si_front"]["values"]["last"] += 1
            elif mutation == "changed_identity": daily["identity"]["scope"] = "weekly"
            elif mutation == "extra_item": items["invented"] = deepcopy(items["si_front"])
            else: daily["server_core"]["blocks"][0]["provenance"]["partition_sha256"] = "0"*64
            with pytest.raises(AssertionError, match="Stage9 projection"):
                projection_completeness(read, damaged, now=NOW)
        for defect in ("raw_schema_version", "source_identity_scope", "source_ticker", "instrument_id", "source_id"):
            changed = deepcopy(saved)
            changed["components"]["futoi_live"]["data"]["current_intraday"].pop(defect)
            live.base._atomic_write(path, changed)
            refused, _ = live.base.read_current_snapshot(now_fn=lambda: NOW)
            assert current(refused)["futoi_live"]["status"] == "UNAVAILABLE"
            assert current(refused)["futoi_live_cr"]["status"] == "AVAILABLE"
            factors = {fact["factor"] for fact in refused["factual_release"]["facts"]}
            assert "futoi_live" not in factors and "futoi_live_cr" in factors
            assert refused["components"]["futoi_live"]["data"]["factual_authority"] is False
            if defect == "raw_schema_version":
                compact_refusal = rub_factual_release.compact(changed, now=NOW, code_revision="a"*40)
                assert compact_refusal["analysis_bundles"]["daily"]["sections"]["current_market"]["items"]["futoi_live_cr"]["status"] == "AVAILABLE"
        live.base._atomic_write(path, saved)
    assert all(__import__('pathlib').Path(p).read_bytes() == raw for p, raw in before.items())
    expired, _ = live.base.read_current_snapshot(now_fn=lambda: NOW+timedelta(minutes=21))
    for name in ("si_front", "futoi_live", "futoi_live_cr"):
        assert current(expired)[name]["status"] == "UNAVAILABLE"
    assert expired["components"]["stage9_weekly"]["data"]["sections"]["completed_periods"]["status"] == "AVAILABLE"


def test_frozen_periods_replay_original_buffers_without_paths_and_refuse_tampering(tmp_path, monkeypatch):
    accepted_periods(tmp_path, monkeypatch)
    data = bundle.seed(scope="weekly", now=NOW)
    assert all(b["status"] == "ready" for b in bundle.period_blocks(data, now=NOW))
    monkeypatch.setenv("MOEX_DATA_ROOT", str(tmp_path/"empty"))
    (tmp_path/"empty").mkdir()
    assert all(b["status"] == "ready" for b in bundle.period_blocks(data, now=NOW))
    for field in ("manifest_ref", "quality_report_ref", "partition_ref"):
        changed = deepcopy(data)
        changed["server_core"]["blocks"][0]["source_envelope"]["buffers_base64"][field] = base64.b64encode(b"corrupt").decode()
        replay = bundle.period_blocks(changed, now=NOW)
        assert replay[0]["status"] == "UNAVAILABLE"
        assert "sha256 mismatch" in replay[0]["reason"]
        assert any(b["status"] == "ready" for b in replay[1:])
    for mutate in (lambda b: b.update(instrument_id="wrong"),
                   lambda b: b["selected_observation"].update(close=999),
                   lambda b: b["source_envelope"].pop("schema_version")):
        changed = deepcopy(data)
        mutate(changed["server_core"]["blocks"][0])
        assert bundle.period_blocks(changed, now=NOW)[0]["status"] == "UNAVAILABLE"


def test_shared_capture_and_cross_scope_mismatch_refusal(tmp_path, monkeypatch):
    accepted_periods(tmp_path, monkeypatch)
    selected = producers()
    daily = selected["stage9_daily"](NOW).data
    spec = legacy._stage7_specs("daily")[0]
    legacy._pointer_path(tmp_path, spec).write_text("corrupt advanced pointer")
    weekly = selected["stage9_weekly"](NOW).data
    assert daily["server_core"]["blocks"] == [b for b in weekly["server_core"]["blocks"] if b["timeframe"] == "1D"]
    # A second persisted generation with valid evidence is still not the same selection.
    changed = deepcopy(weekly)
    block = changed["server_core"]["blocks"][0]
    block["source_envelope"]["selection_as_of_utc"] = (NOW+timedelta(seconds=1)).isoformat()
    snapshot = {"components": {"stage9_daily": {"data": daily}, "stage9_weekly": {"data": changed}}}
    result = bundle._periods(snapshot, now=NOW+timedelta(seconds=1))
    assert result["daily"][0]["reason"] == "daily_weekly_D1_evidence_generation_mismatch"
    assert result["weekly"][0]["status"] == "UNAVAILABLE"


def test_publication_finish_has_no_evidence_reads_and_refuses_expired_oil(tmp_path, monkeypatch):
    from unit.test_moex_brent_factual import collect, documents, component
    accepted_periods(tmp_path, monkeypatch)
    snapshot = live.base.build_snapshot(now=NOW, producers=producers())
    oil = collect(documents(expiry="2026-11-02", published="2026-09-23"), now=NOW-timedelta(seconds=1199))[0]
    snapshot["components"]["oil"] = component(oil)
    prepared = bundle.prepare(snapshot, now=NOW)
    assert any(f["factor"] == "oil" for f in prepared["release"]["facts"])
    later = NOW+timedelta(seconds=2)
    live.base.finalize_snapshot_timing(snapshot, started=NOW, completed=later)
    def denied(*args, **kwargs):
        pytest.fail("publication finish performed evidence I/O")
    monkeypatch.setattr(legacy, "_read_pointer_block", denied)
    monkeypatch.setattr(pd, "read_parquet", denied)
    monkeypatch.setattr(bundle, "_contract", denied)
    bundle.finish(snapshot, prepared, now=later)
    external = snapshot["components"]["stage9_daily"]["data"]["external_context"]
    assert external["admitted_dated_facts"] == []
    assert external["refused_dated_facts"][0]["factor"] == "oil"


def test_nested_historical_gaps_and_earliest_existing_expiry_remain_visible():
    value = {"status": "AVAILABLE", "accepted_at_utc": NOW.isoformat(),
        "dated": {"source_anchor_clocks": {"snapshot_ts": (NOW-timedelta(days=4)).isoformat()},
                  "windows": {"20": {"status": "UNAVAILABLE", "sample_count": 1, "reason": "insufficient_coverage"}}}}
    current = bundle._dated_item("statistics", value, now=NOW)
    assert current["status"] == "PARTIAL"
    assert current["coverage_refusals"]
    expired = bundle._dated_item("statistics", value, now=NOW+timedelta(seconds=1))
    assert expired["status"] == "UNAVAILABLE"
    assert expired["refused_evidence"]["accepted_at_utc"] == NOW.isoformat()


def test_selected_v2_builder_never_falls_back_to_legacy(tmp_path, monkeypatch):
    monkeypatch.setenv("MOEX_DATA_ROOT", str(tmp_path))
    with pytest.raises(Exception, match="current snapshot does not exist"):
        legacy.build_analysis_bundle(scope="daily", as_of=NOW.isoformat(), schema_version=bundle.SCHEMA)
