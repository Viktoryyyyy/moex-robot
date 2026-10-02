"""Real Stage7/physical-validator fixtures; only fixed Stage2 admission is stubbed."""
from datetime import datetime, timezone
from hashlib import sha256
import json
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

from moex_data.futures import accepted_quote_history_current as current
from moex_data.futures import freeze_step7_accepted_raw_5m as layer
from moex_data import step7_rub_native_d1_w1_materializer as m
from moex_data import step10_rub_refresh_scheduler as scheduler

INSTRUMENT = "usdrubf_futures_family"
NOW = datetime(2030, 1, 1, tzinfo=timezone.utc)
REPO = Path(__file__).resolve().parents[2]


def write(path, obj):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj), encoding="utf-8")
    return path


def digest(path):
    return sha256(path.read_bytes()).hexdigest()


def raw_frame(day, mutation=None):
    rows = [{"instrument_id": INSTRUMENT, "source_id": layer.SOURCE_ID,
        "secid": "USDRUBF", "board": "RFUD", "market": "forts", "engine": "futures",
        "source": layer.stage2.quote_core.SOURCE_CANDIDATE_APIM_TRADESTATS,
        "trade_date": day, "session_date": day, "ts": pd.Timestamp(day + f" 12:{minute:02d}:00"),
        "ingest_ts": day + "T10:00:00Z", "open": 100.0, "high": 104.0,
        "low": 99.0, "close": 103.0, "volume": 10.0, "value": 1000.0, "num_trades": 2}
        for minute in (5, 10)]
    if mutation == "foreign":
        rows[0]["secid"] = "SiU6"
    if mutation == "duplicate":
        rows.append(rows[0].copy())
    if mutation == "negative":
        rows[0]["volume"] = -1
    return pd.DataFrame(rows)


def freeze(root, run, day, mutation=None):
    path = run / "inputs" / "stage7_raw" / ("instrument_id=" + INSTRUMENT) / ("trade_date=" + day) / "part.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    raw_frame(day, mutation).to_parquet(path, index=False)
    record = {"trade_date": day, "instrument_id": INSTRUMENT, "sha256": digest(path),
        "frozen_ref": layer._rooted_ref(root, path), "canonical_ref": layer._rooted_ref(root, path),
        "independent_inode_exact_byte_copy": True}
    manifest = scheduler._write_step7_frozen_manifest(root=root, run_root=run, instrument_id=INSTRUMENT, records=[record])
    return path, manifest


@pytest.fixture
def history(tmp_path, monkeypatch):
    root = tmp_path / "data"; root.mkdir()
    monkeypatch.setenv("MOEX_DATA_ROOT", str(root))
    base_day = "2026-09-25"
    raw, frozen = freeze(root, root / "base", base_day)
    frame = m.build_d1(data_root=root, frozen_manifest_path=frozen, instrument_id=INSTRUMENT,
                       history_start=base_day, history_end=base_day)
    anchor = write(root / "base-anchor.json", {})
    ref = layer._rooted_ref(root, anchor)
    base = layer.AcceptedQuoteHistory(INSTRUMENT, layer.SOURCE_ID, "USDRUBF", (base_day,), (),
        "legacy", ref, ref, ref, "", digest(anchor), digest(anchor), "",
        ({"trade_date": base_day, "snapshot_path": str(raw), "sha256": digest(raw), "row_count": 2},), 2)
    original = layer.accepted_quote_history
    def admitted(*args, **kwargs):
        return original(*args, **kwargs) if kwargs.get("current") else base
    monkeypatch.setattr(layer, "accepted_quote_history", admitted)
    monkeypatch.setattr(layer.content_attestation, "_repo_expectation", lambda *args:
        SimpleNamespace(history=SimpleNamespace(date_start=base_day, date_end=base_day)))
    return SimpleNamespace(root=root, base=base, frame=frame, runs=[])


def advance(h, day, mutation=None):
    run_id = "refresh_" + day.replace("-", "")
    run = h.root / "runs" / "step10_rub_daily_refresh" / ("run_id=" + run_id)
    raw, frozen = freeze(h.root, run, day, mutation)
    base_path = run / "inputs" / "base.parquet"
    h.frame.to_parquet(base_path, index=False)
    lineage = scheduler._write_stage7_rolling_lineage(root=h.root, run_root=run, instrument_id=INSTRUMENT,
        base_snapshot=base_path, delta_manifest=frozen, history_start=h.frame.trade_date.min(),
        base_history_end=h.frame.trade_date.max(), delta_start=day, delta_end=day)
    delta = m.build_d1(data_root=h.root, frozen_manifest_path=frozen, instrument_id=INSTRUMENT, history_start=day, history_end=day)
    h.frame = pd.concat([h.frame, delta], ignore_index=True)
    output = m._write_output(run_root=run, dataset_id="rub_native_ohlcv_htf", instrument_id=INSTRUMENT,
        timeframe="1D", producer_run_id=run_id + "_" + INSTRUMENT + "_d1", frame=h.frame,
        source_ref=layer._rooted_ref(h.root, lineage), history_start=h.frame.trade_date.min(), history_end=day)
    parent = write(run / "run_manifest.json", {"stage": 10, "status": "succeeded", "run_id": run_id,
        "acceptance_contract_id": "step10_rub_daily_refresh_acceptance.v1", "through_date": day,
        "new_trading_dates": [day], "latest_completed_trading_date": day,
        "started_at_utc": "2026-09-01T00:00:00Z", "finished_at_utc": "2029-01-01T00:00:00Z",
        "stage7": {"canonical_pointer_promotion": {"status": "promoted", "pointer_count": 8}}})
    pointer_path, pointer = scheduler._pointer_from_output(h.root, output, run_id)
    write(pointer_path, pointer)
    item = SimpleNamespace(run=run, raw=raw, frozen=frozen, lineage=lineage, parent=parent,
                           pointer=pointer_path, output=output)
    h.runs.append(item)
    return item


def resolve(h, **kwargs):
    return layer.accepted_quote_history(h.root, INSTRUMENT, repo_root=REPO, current=True, as_of=kwargs.get("now", NOW))


def mutate(path, fn):
    obj = json.loads(path.read_text()); fn(obj); write(path, obj)


def test_normal_update_sunday_and_repeat_without_reader_configuration_change(history):
    first = advance(history, "2026-09-27")  # Source-observed Sunday; Saturday is not invented.
    a = resolve(history)
    assert a.accepted_dates == ("2026-09-25", "2026-09-27")
    assert a == resolve(history)
    baseline_bytes = Path(history.base.records[0]["snapshot_path"]).read_bytes()
    old_objects = {ref: layer._expand_root_ref(history.root, ref, "test").read_bytes()
                   for ref, _ in a.admission_anchors}
    advance(history, "2026-09-28")
    b = resolve(history)
    assert b.accepted_dates[-1] == "2026-09-28" and b.row_count == 6
    assert a.acceptance_run_id != b.acceptance_run_id
    assert Path(history.base.records[0]["snapshot_path"]).read_bytes() == baseline_bytes
    # Exact captured anchors remain independently replayable after current moves.
    assert all(sha256(old_objects[ref]).hexdigest() == digest_ for ref, digest_ in a.admission_anchors)
    assert layer.accepted_quote_history(history.root, INSTRUMENT, "2026-09-25", "2026-09-25") == history.base


@pytest.mark.parametrize("failure", ["failed", "unpromoted", "hash", "missing", "observed_gap", "lineage",
                                      "quality", "prefix", "future_finish", "future_date", "pointer_hash"])
def test_reject_invalid_admission(history, failure):
    item = advance(history, "2026-09-27")
    if failure == "failed": mutate(item.parent, lambda x: x.update(status="failed"))
    elif failure == "unpromoted": mutate(item.parent, lambda x: x["stage7"]["canonical_pointer_promotion"].update(status="no_op"))
    elif failure == "hash": item.raw.write_bytes(item.raw.read_bytes() + b"changed")
    elif failure == "missing": item.frozen.unlink()
    elif failure == "observed_gap": mutate(item.parent, lambda x: x.update(new_trading_dates=["2026-09-26", "2026-09-27"]))
    elif failure == "lineage": mutate(item.lineage, lambda x: x.update(delta_manifest_sha256="0"*64))
    elif failure == "quality": mutate(Path(item.output["quality_report_path"]), lambda x: x.update(quality_status="fail"))
    elif failure == "prefix": mutate(item.lineage, lambda x: x.update(base_snapshot_sha256="0"*64))
    elif failure == "future_finish": mutate(item.parent, lambda x: x.update(finished_at_utc="2031-01-01T00:00:00Z"))
    elif failure == "future_date": mutate(item.parent, lambda x: x.update(through_date="2030-01-01"))
    elif failure == "pointer_hash": mutate(item.pointer, lambda x: x.update(partition_sha256="0"*64))
    with pytest.raises((ValueError, OSError)):
        resolve(history)


@pytest.mark.parametrize("failure", ["foreign", "duplicate", "negative"])
def test_physical_validation_even_with_internally_consistent_hashes(history, failure):
    if failure == "foreign":
        with pytest.raises(ValueError): advance(history, "2026-09-27", failure)
    else:
        advance(history, "2026-09-27", failure)
        with pytest.raises(ValueError): resolve(history)


def test_rotation_during_resolution_fails_closed(history, monkeypatch):
    item = advance(history, "2026-09-27")
    original = current.Evidence.anchors
    def rotated(self, exclude):
        mutate(item.pointer, lambda x: x.update(run_id="rotated"))
        return original(self, exclude)
    monkeypatch.setattr(current.Evidence, "anchors", rotated)
    with pytest.raises(ValueError, match="changed during resolution"):
        resolve(history)


def test_real_observation_reader_without_forecast_registration(history, tmp_path):
    from moex_research.intelligence.usdrubf_forecast_journal import ForecastJournal, decode
    from moex_research.runners.usdrubf_forecast_observation import accepted_facts
    advance(history, "2026-09-27")
    journal = ForecastJournal(tmp_path / "journal", clock=lambda: NOW)
    spec = {"horizon_start": "2026-09-27T09:00:00Z", "horizon_end": "2026-09-27T09:10:00Z",
        "observation_grid": [["2026-09-27T09:00:00Z", "2026-09-27T09:05:00Z"],
                             ["2026-09-27T09:05:00Z", "2026-09-27T09:10:00Z"]]}
    reader = {"mode": "accepted_current", "data_root": str(history.root)}
    raw, source, provenance = accepted_facts(journal, spec, reader, NOW)
    assert len(decode(raw)["bars"]) == 2
    assert source["data_as_of"] == "2026-09-27T09:10:00+00:00"
    assert len(provenance["admission_anchors"]) > 2
    captured = [journal.object_bytes(x["object_sha256"]) for x in provenance["admission_anchors"]]
    advance(history, "2026-09-28")
    assert captured == [journal.object_bytes(x["object_sha256"]) for x in provenance["admission_anchors"]]
    assert not list((journal.root / "records").rglob("*.json"))


def test_current_mode_rejects_explicit_legacy_range(history):
    with pytest.raises(ValueError, match="cannot override"):
        layer.accepted_quote_history(history.root, INSTRUMENT, "2026-09-25", "2026-09-25", current=True)


def test_evidence_resource_bounds(history, monkeypatch):
    advance(history, "2026-09-27")
    monkeypatch.setattr(current, "MAX_FILE", 10)
    with pytest.raises(ValueError, match="bound exceeded"):
        resolve(history)
