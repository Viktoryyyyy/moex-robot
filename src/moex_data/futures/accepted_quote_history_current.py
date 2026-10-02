"""Read the admitted Stage2 prefix and Stage10's immutable Stage7 raw deltas.

The existing Stage7 D1 pointer is the admission boundary. This module neither
collects data nor publishes a second pointer. Raw lineage hashes, not matching
prices alone, bind every appended date to that boundary.
"""
from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone
from hashlib import sha256
from io import BytesIO
import json
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
import pyarrow.parquet as pq

from . import freeze_step7_accepted_raw_5m as layer
from .. import step7_rub_native_d1_w1_materializer as materializer
from .. import step9_rub_analysis_bundle as step9

MAX_RUNS = 4096
MAX_FILE = 8 * 1024 * 1024
MAX_BYTES = 128 * 1024 * 1024
FIELDS = ("open", "high", "low", "close", "volume", "value", "num_trades",
          "source_row_count", "source_period_count", "source_lineage_sha256")


def need(condition, message):
    if not condition:
        raise layer.Step7RawFreezeError(message)


class Evidence:
    def __init__(self, root):
        self.root = root.resolve(strict=True)
        self.raw = {}
        self.total = 0

    def path(self, value):
        text = str(value)
        path = self.root / text[len(layer.ROOT_PREFIX):] if text.startswith(layer.ROOT_PREFIX) else Path(text)
        need(path.is_absolute() and ".." not in path.parts, "invalid history evidence path")
        need(not path.is_symlink() and path.resolve(strict=True).is_relative_to(self.root), "history evidence escaped root")
        return path.resolve(strict=True)

    def read(self, value, expected=None):
        path = self.path(value)
        if path not in self.raw:
            size = path.stat().st_size
            need(path.is_file() and size <= MAX_FILE and self.total + size <= MAX_BYTES, "history evidence byte bound exceeded")
            with path.open("rb") as stream:
                raw = stream.read(MAX_FILE + 1)
            need(len(raw) == size and len(raw) <= MAX_FILE, "history evidence changed while reading")
            self.total += len(raw)
            self.raw[path] = raw
        raw = self.raw[path]
        need(expected is None or sha256(raw).hexdigest() == expected, "history evidence SHA mismatch")
        return raw

    def json(self, value, expected=None):
        result = json.loads(self.read(value, expected))
        need(isinstance(result, dict), "history evidence object required")
        return result

    def frame(self, value, expected=None, max_rows=20000):
        raw = self.read(value, expected)
        footer = pq.ParquetFile(BytesIO(raw)).metadata
        need(footer.num_rows <= max_rows and footer.num_columns <= 100, "history parquet bound exceeded")
        return pd.read_parquet(BytesIO(raw))

    def anchors(self, exclude):
        for path, raw in self.raw.items():
            need(path.stat().st_size == len(raw), "history evidence changed during resolution")
            with path.open("rb") as stream:
                need(stream.read(len(raw) + 1) == raw, "history evidence changed during resolution")
        return tuple((layer.ROOT_PREFIX + path.relative_to(self.root).as_posix(), sha256(raw).hexdigest())
                     for path, raw in sorted(self.raw.items()) if path not in exclude)


def clock(value):
    result = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    need(result.tzinfo is not None and result.utcoffset() is not None, "aware history clock required")
    return result.astimezone(timezone.utc)


def _parent(ev, path, now):
    parent = ev.json(path)
    need(parent.get("status") == "succeeded" and parent.get("stage") == 10,
         "history parent must be successful Stage10")
    need(parent.get("acceptance_contract_id") == "step10_rub_daily_refresh_acceptance.v1", "history parent contract mismatch")
    need(path.parent.name == "run_id=" + str(parent.get("run_id")), "history parent run mismatch")
    promoted = parent.get("stage7", {}).get("canonical_pointer_promotion", {})
    need(promoted.get("status") == "promoted" and promoted.get("pointer_count") == 8,
         "history parent did not promote Stage7")
    need(clock(parent["started_at_utc"]) <= clock(parent["finished_at_utc"]) <= now, "history parent clock mismatch")
    through = layer._iso_date(parent["through_date"], "parent through_date")
    need(through < now.astimezone(ZoneInfo("Europe/Moscow")).date().isoformat(), "history parent includes unclosed date")
    return parent


def _check_frame(frame, instrument, now):
    need(not frame.empty and not frame["trade_date"].duplicated().any(), "admitted D1 empty or duplicate dates")
    need(set(frame.instrument_id) == {instrument} and set(frame.secid) == {layer.EXPECTED_SECID[instrument]}
         and set(frame.timeframe) == {"1D"}, "admitted D1 identity mismatch")
    dates = frame.trade_date.astype(str).tolist()
    need(dates == sorted(dates) and all(layer._iso_date(d, "D1 date") == d for d in dates), "admitted D1 dates invalid")
    need(all(d < now.astimezone(ZoneInfo("Europe/Moscow")).date().isoformat() for d in dates), "unclosed admitted D1 date")
    need(all(clock(t) <= now for t in frame.availability_ts_utc), "admitted D1 is not yet available")


def resolve(root: Path, instrument: str, *, repo_root: Path, as_of=None):
    now = clock(as_of or datetime.now(timezone.utc))
    layer._require_stage2_root(root)
    expectation = layer.content_attestation._repo_expectation(repo_root, layer.SOURCE_DATASET_ID, instrument).history
    base = layer.accepted_quote_history(root, instrument, expectation.date_start, expectation.date_end, repo_root=repo_root)
    ev = Evidence(root)
    for ref, digest in ((base.pointer_ref, base.marker_sha256), (base.manifest_ref, base.manifest_sha256)):
        ev.read(ref, digest)
    ev.read(base.acceptance_report_ref)
    spec = next(s for s in step9.pointer_specs("weekly") if s.stage == 7 and s.dataset_id == "rub_native_ohlcv_htf"
                and s.instrument_id == instrument and s.timeframe == "1D")
    pointer_path = step9._pointer_path(root, spec)
    pointer = ev.json(pointer_path)
    buffers = {field: ev.read(pointer[field], pointer[field.removesuffix("_ref") + "_sha256"])
               for field in ("manifest_ref", "quality_report_ref", "partition_ref")}
    # Existing canonical admission/identity/quality/causality checks, same bytes.
    ev.frame(pointer["partition_ref"], pointer["partition_sha256"])
    step9._read_pointer_block(root, spec, now, pointer_bytes=ev.read(pointer_path), evidence_buffers=buffers)
    need(pointer.get("promotion_basis") == "stage10_validated_rolling_refresh", "current history requires Stage10 admission")
    accepted = ev.frame(pointer["partition_ref"])
    _check_frame(accepted, instrument, now)
    indexed = accepted.set_index("trade_date")
    base_dates = set(base.accepted_dates)
    need(set(d for d in indexed.index if d <= expectation.date_end) == base_dates, "admitted D1 baseline date mismatch")
    for record in base.records:
        row = indexed.loc[record["trade_date"]]
        need(row["source_lineage_sha256"] == record["sha256"] and row["source_row_count"] == record["row_count"],
             "admitted D1 baseline content mismatch")
    wanted = set(indexed.index) - base_dates
    need(all(d > expectation.date_end for d in wanted), "history delta overlaps baseline")
    runs_root = root / "runs" / "step10_rub_daily_refresh"
    current_run = runs_root / ("run_id=" + layer._safe_token(pointer["acceptance_run_id"], "acceptance run"))
    current_parent = _parent(ev, current_run / "run_manifest.json", now)
    need(ev.path(pointer["partition_ref"]).is_relative_to(current_run), "current D1 parent binding mismatch")
    need(max(indexed.index) == current_parent["latest_completed_trading_date"], "current D1 source date mismatch")
    paths = []
    for directory in runs_root.iterdir():
        if directory.name.startswith("run_id=") and directory.is_dir():
            paths.append(directory)
            need(len(paths) <= MAX_RUNS, "history parent search bound exceeded")
    found = {}
    raw_paths = set()
    for run in sorted(paths):
        parent_path = run / "run_manifest.json"
        frozen_path = run / "inputs" / "stage7_frozen" / ("instrument_id=" + instrument) / "manifest.json"
        if not parent_path.is_file() or not frozen_path.is_file():
            continue
        # Only successful source-date candidates are considered; rejected runs
        # cannot supply facts. The exact admitted raw SHA remains authoritative.
        parent = ev.json(parent_path)
        dates = parent.get("new_trading_dates", [])
        if parent.get("status") != "succeeded" or not isinstance(dates, list) or not wanted.intersection(dates):
            if parent_path.resolve() != (current_run / "run_manifest.json").resolve():
                ev.total -= len(ev.raw.pop(parent_path.resolve()))
            continue
        parent = _parent(ev, parent_path, now)
        need(dates == sorted(set(dates)) and max(dates) <= parent["through_date"], "parent observed date scope mismatch")
        suffix = Path("dataset_id=rub_native_ohlcv_htf/timeframe=1D") / ("instrument_id=" + instrument)
        manifest = ev.json(run / "state" / "refresh" / suffix / "manifest.json")
        quality = ev.json(run / "state" / "quality" / suffix / "quality_report.json")
        for support in (manifest, quality):
            need(all(support.get(k) == v for k, v in {"dataset_id": "rub_native_ohlcv_htf", "instrument_id": instrument,
                 "timeframe": "1D", "quality_status": "pass", "duplicate_period_count": 0,
                 "run_id": parent["run_id"] + "_" + instrument + "_d1"}.items()), "delta D1 support identity/quality mismatch")
        lineage_path = ev.path(manifest["source_ref"])
        need(lineage_path.is_relative_to(run), "delta lineage escaped parent")
        lineage = ev.json(lineage_path)
        need(lineage.get("schema_version") == "step10_stage7_rolling_lineage.v1" and lineage.get("instrument_id") == instrument,
             "delta lineage identity mismatch")
        need(ev.path(lineage["delta_manifest_ref"]) == frozen_path.resolve(), "delta manifest not bound by lineage")
        frozen = ev.json(frozen_path, lineage["delta_manifest_sha256"])
        base_frame = ev.frame(lineage["base_snapshot_ref"], lineage["base_snapshot_sha256"])
        output_path = run / "market" / "derived" / suffix / "part.parquet"
        need(ev.path(manifest["partition_path"]) == output_path.resolve(), "delta output path mismatch")
        output = ev.frame(output_path)
        _check_frame(output, instrument, now)
        need(len(output) == manifest["row_count"] == quality["row_count"], "delta D1 row count mismatch")
        need(clock(manifest["build_ts_utc"]) <= clock(parent["finished_at_utc"]), "delta build after parent finish")
        expected_dates = [d for d in indexed.index if d <= max(dates)]
        need(output.trade_date.tolist() == expected_dates, "delta output not an admitted history prefix")
        for field in FIELDS:
            need(output[field].reset_index(drop=True).equals(accepted.loc[accepted.trade_date.isin(expected_dates), field].reset_index(drop=True)),
                 "delta output differs from admitted history prefix: " + field)
        need(base_frame.trade_date.tolist() == [d for d in expected_dates if d < min(dates)], "delta base dates mismatch")
        for field in FIELDS:
            need(base_frame[field].reset_index(drop=True).equals(output.loc[output.trade_date < min(dates), field].reset_index(drop=True)),
                 "delta base content mismatch: " + field)
        need(lineage["base_history_end"] < min(dates) and lineage["delta_start"] == min(dates)
             and lineage["delta_end"] == max(dates), "delta lineage range mismatch")
        need([r["trade_date"] for r in frozen.get("partitions", [])] == dates, "delta missing observed source date")
        for record in frozen["partitions"]:
            path = ev.path(record["frozen_ref"])
            need(path.is_relative_to(run), "raw delta escaped parent")
            frame = ev.frame(path, record["sha256"], max_rows=100000)
            day = record["trade_date"]
            check = layer.quote_validation_expectation(instrument, min(dates), max(dates))
            rows, _ = layer.stage2._validate_quote_partition(repo_root, frame, check, day, "accepted_history_reader")
            timestamps = step9._to_utc_series(frame, step9.PointerSpec("history.raw", 3, layer.SOURCE_DATASET_ID, instrument, "ts"))
            need(not timestamps.duplicated().any() and bool((timestamps <= pd.Timestamp(now)).all()), "duplicate or future raw candle")
            raw_paths.add(path)
            if day in wanted:
                need(record["sha256"] == indexed.loc[day, "source_lineage_sha256"], "conflicting admitted raw version")
                if day in found:
                    need(found[day]["sha256"] == record["sha256"], "conflicting duplicate raw date")
                else:
                    found[day] = {"trade_date": day, "snapshot_path": str(path), "sha256": record["sha256"], "row_count": rows}
        rebuilt = materializer.build_d1(data_root=root, frozen_manifest_path=frozen_path,
            instrument_id=instrument, history_start=min(dates), history_end=max(dates), evidence_buffers=ev.raw).set_index("trade_date")
        for day in wanted.intersection(dates):
            for field in FIELDS:
                a, b = rebuilt.loc[day, field], indexed.loc[day, field]
                need((pd.isna(a) and pd.isna(b)) or a == b, "delta differs from admitted D1: " + day + " " + field)
    need(set(found) == wanted, "admitted raw tail coverage incomplete")
    records = base.records + tuple(found[d] for d in sorted(found))
    dates = tuple(r["trade_date"] for r in records)
    anchors = ev.anchors(raw_paths)
    identity = sha256(json.dumps(anchors, separators=(",", ":")).encode()).hexdigest()
    return replace(base, accepted_dates=dates, records=records, row_count=sum(r["row_count"] for r in records),
        acceptance_run_id="stage7_current_" + identity, partition_dates_sha256=layer._date_set_sha(list(dates)),
        partition_content_set_sha256=sha256("".join(r["trade_date"] + "\t" + r["sha256"] + "\n" for r in records).encode()).hexdigest(),
        admission_anchors=anchors)
