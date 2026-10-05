"""Validate retained raw admission evidence without inventing historical attestation.

Both producers predate content-addressed raw admission receipts. Their retained
quality/manifest metadata can be checked for consistency, not used to prove that
today's bytes are the original exchange response. New derived evidence hashes the
bytes actually read and states that limited scope explicitly.
"""
import hashlib
import json
import os
import re
import tempfile
from pathlib import Path
from urllib.parse import urlsplit

import numpy as np
import pandas as pd

from moex_data.futures import refresh_forts_raw_5m_incremental as observed
from moex_data.futures.slice1_common import today_msk

XML = "canonical_apim_futures_xml"
OBSERVED = "authoritative_observed_algopack_tradestats"
SUPPORTED = {XML, OBSERVED}
STATUS = "calendar_denominator_status"
EVIDENCE = "date_source_evidence_json"
EVIDENCE_SCOPE = "retained_admission_metadata_verified_not_historical_content_attestation"
DERIVED_FIELDS = (STATUS, EVIDENCE, "roll_date_source_status", "roll_date_source")
LEGACY_MARKER = "_legacy_date_source_unrecorded"
ROLL_SOURCES = {
    XML: "MOEX_APIM_XML:/iss/calendars",
    OBSERVED: observed.OBSERVED_DATE_SOURCE_ID + ":" + observed.OBSERVED_DATE_SOURCE_ENDPOINT,
}


def fail(message):
    raise RuntimeError("date_source_provenance: " + message)


def canonical_json(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def read_partition(path):
    before = digest(path)
    frame = pd.read_parquet(path)
    if digest(path) != before:
        fail("raw changed while reading")
    frame["_source_partition_path"] = str(path)
    frame["_source_partition_sha256"] = before
    return frame


def read_quality(path):
    before = digest(path)
    frame = pd.read_parquet(path)
    if digest(path) != before:
        fail("quality changed while reading")
    frame["_quality_report_path"] = str(path)
    frame["_quality_report_sha256"] = before
    return frame


def read_derived_partition(path):
    frame = pd.read_parquet(path)
    present = [field in frame for field in DERIVED_FIELDS]
    if any(present) and not all(present):
        fail("incomplete new continuous provenance fields")
    # Preserve physical absence through concat with newer partitions. Null fields
    # in a new-format file never qualify as an old-format artifact.
    frame[LEGACY_MARKER] = not any(present)
    frame["_source_partition_path"] = str(path)
    return frame


def write_derived_partition(path, frame):
    """Do not replace identical output merely because the retry has a new clock."""
    path = Path(path)
    if LEGACY_MARKER in frame:
        legacy = frame[LEGACY_MARKER].eq(True)
        if legacy.any() and not legacy.all():
            fail("cannot publish mixed recorded/unrecorded provenance in one partition")
        frame = frame.drop(columns=[LEGACY_MARKER, *(DERIVED_FIELDS if legacy.all() else ())], errors="ignore")
    if path.exists():
        previous = pd.read_parquet(path)
        try:
            pd.testing.assert_frame_equal(previous.drop(columns=["ingest_ts"], errors="ignore").reset_index(drop=True),
                frame.drop(columns=["ingest_ts"], errors="ignore").reset_index(drop=True), check_dtype=False, check_exact=True)
            return
        except AssertionError:
            pass
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=path.parent, suffix=".parquet", delete=False) as temp:
        temporary = Path(temp.name)
    try:
        frame.to_parquet(temporary, index=False)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def single(frame, column):
    if column not in frame or frame.empty or frame[column].isna().any():
        fail("missing/null " + column)
    values = frame[column].astype(str).unique().tolist()
    if len(values) != 1 or not values[0].strip():
        fail("contradictory " + column)
    return values[0]


def partition_identity(path):
    parts = Path(str(path)).as_posix().split("/")
    if len(parts) < 5 or parts[-5] != "raw_5m" or parts[-1] != "part.parquet":
        fail("invalid raw partition path: " + str(path))
    values = []
    for part, prefix in zip(parts[-4:-1], ("trade_date=", "family=", "secid=")):
        if not part.startswith(prefix) or not part[len(prefix):]:
            fail("invalid raw partition identity: " + str(path))
        values.append(part[len(prefix):])
    return tuple(values)


def relative_partition(key):
    day, family, secid = key
    return Path("futures/raw_5m") / ("trade_date=" + day) / ("family=" + family) / ("secid=" + secid) / "part.parquet"


def utc(value):
    try:
        result = pd.Timestamp(value)
        if pd.isna(result) or result.tzinfo is None:
            fail("admission timestamp must include timezone")
        return result.tz_convert("UTC")
    except (ValueError, TypeError) as exc:
        fail("invalid admission timestamp: " + str(exc))


def validate_structure(frame, closed_before=None):
    cutoff = closed_before or today_msk()
    required = ["trade_date", "ts", "end", "session_date", "board", "secid", "family_code",
                "open", "high", "low", "close", "volume", "schema_version", STATUS,
                "source", "source_endpoint_url", "ingest_ts", "_source_partition_path", "_source_partition_sha256"]
    missing = [x for x in required if x not in frame or frame[x].isna().any()]
    if frame.empty or missing:
        fail("empty raw or missing/null fields: " + ",".join(missing))
    if not frame["schema_version"].eq("futures_raw_5m.v1").all() or not frame["board"].eq("RFUD").all():
        fail("raw schema/board mismatch")
    if not frame[STATUS].isin(SUPPORTED).all():
        fail("unsupported raw date source")
    if frame.duplicated(["secid", "trade_date", "ts"]).any():
        fail("duplicate raw timestamp")
    numeric = frame[["open", "high", "low", "close", "volume"]].apply(pd.to_numeric, errors="coerce")
    if not np.isfinite(numeric.to_numpy(dtype=float)).all():
        fail("non-finite raw OHLCV")
    if ((numeric["high"] < numeric["low"]) | (numeric["open"] > numeric["high"])
            | (numeric["open"] < numeric["low"]) | (numeric["close"] > numeric["high"])
            | (numeric["close"] < numeric["low"]) | (numeric["volume"] < 0)).any():
        fail("invalid raw OHLCV")
    for (_, _), group in frame.groupby(["secid", "trade_date"], dropna=False):
        single(group, STATUS)
        single(group, "family_code")
    for path, part in frame.groupby("_source_partition_path", sort=True):
        key = partition_identity(path)
        if tuple(single(part, x) for x in ("trade_date", "family_code", "secid")) != key:
            fail("partition identity mismatch: " + str(path))
        day, _, secid = key
        try:
            if pd.Timestamp(day).date().isoformat() != day or day >= cutoff:
                fail("unclosed or invalid trade date: " + day)
        except (ValueError, TypeError):
            fail("invalid trade date: " + day)
        ts = pd.to_datetime(part["ts"], errors="coerce")
        ends = pd.to_datetime(part["end"], errors="coerce")
        if ts.isna().any() or ends.isna().any() or not ts.eq(ends).all() or not ts.dt.strftime("%Y-%m-%d").eq(day).all():
            fail("raw timestamp/date mismatch")
        if not part["session_date"].astype(str).eq(day).all():
            fail("raw session/date mismatch")
        if single(part, "source") != "MOEX_ALGOPACK_FO_TRADESTATS":
            fail("raw source identity mismatch")
        endpoint = urlsplit(single(part, "source_endpoint_url"))
        if (endpoint.scheme != "https" or endpoint.netloc not in {"apim.moex.com", "iss.moex.com"}
                or endpoint.path != observed.observed_date_source_endpoint(secid) or endpoint.query or endpoint.fragment):
            fail("raw source endpoint/instrument mismatch")
        label = single(part, STATUS)
        # Historical XML rows predate the explicit source_secid column. Their
        # exact instrument endpoint remains mandatory; observed rows require both.
        if label == OBSERVED or ("source_secid" in part and part["source_secid"].notna().any()):
            if single(part, "source_secid").upper() != secid.upper():
                fail("source_secid mismatch")
        ingest = utc(single(part, "ingest_ts"))
        if ingest > pd.Timestamp.now(tz="UTC"):
            fail("future raw ingest timestamp")


def quality_paths(root):
    root = Path(root) / "futures"
    return sorted((root / "all_universe/quality/raw_5m_backfill").glob("chunk_id=*/quality_report.parquet")) + sorted(
        (root / "quality/raw_5m_loader").glob("run_date=*/futures_raw_5m_quality_report.parquet"))


class AdmissionIndex:
    def __init__(self, data_root, quality=None, closed_before=None):
        self.root = Path(data_root)
        self.cutoff = closed_before or today_msk()
        self.rows = {}
        self.cache = {}
        if quality is None:
            frames = []
            paths = quality_paths(self.root)
        else:
            # Legacy witnesses validate already-selected historical partitions;
            # they never expand the existing eligibility/selection universe.
            frames = [quality]
            paths = sorted((self.root / "futures/quality/raw_5m_loader").glob("run_date=*/futures_raw_5m_quality_report.parquet"))
        if paths:
            for path in paths:
                frame = read_quality(path)
                frames.append(frame)
        quality = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
        for row in quality.to_dict("records"):
            if row.get("quality_status") == "pass":
                self.rows.setdefault((str(row.get("family_code")), str(row.get("secid"))), []).append(row)

    def cohort(self, q):
        cache_key = (q.get("_quality_report_path"), q.get("secid"), q.get("run_id"))
        if cache_key in self.cache:
            value = self.cache[cache_key]
            if isinstance(value, Exception):
                raise value
            if (digest(self.root / value["quality_path"]) != value["quality_sha256"]
                    or digest(self.root / value["manifest_path"]) != value["manifest_sha256"]):
                fail("cached admission witness changed")
            return value
        try:
            value = self._cohort(q)
        except (RuntimeError, ValueError, TypeError, KeyError, OSError) as exc:
            self.cache[cache_key] = exc
            raise
        self.cache[cache_key] = value
        return value

    def _cohort(self, q):
        qp = Path(q["_quality_report_path"])
        qhash = digest(qp)
        if qhash != q.get("_quality_report_sha256"):
            fail("quality changed since admission index read")
        family, secid = str(q["family_code"]), str(q["secid"])
        legacy = q.get("schema_version") == "futures_raw_5m_quality_report.v1"
        if legacy:
            mp = self.root / "futures/runs/raw_5m_loader" / qp.parent.name / "manifest.json"
            label = q.get(STATUS)
            start, end = str(q.get("requested_from")), str(q.get("requested_till"))
            if q.get("fetch_status") != "completed" or q.get("board") != "RFUD":
                fail("legacy admission source/board rejected")
            if pd.notna(q.get("normalization_error")) and str(q.get("normalization_error")).strip():
                fail("legacy normalization failed")
        else:
            if q.get("schema_version") != "futures_all_universe_raw_5m_quality_report.v1":
                fail("unsupported admission schema")
            mp = self.root / "futures/all_universe/runs/raw_5m_backfill" / ("chunk_id=" + str(q.get("chunk_id"))) / "manifest.json"
            label = q.get("calendar_status")
            start, end = str(q.get("date_from")), str(q.get("date_till"))
            if (q.get("dataset_stage") != "raw_5m" or q.get("partition_status") != "written"
                    or q.get("source_payload_status") != "completed" or q.get("session_calendar_status") != label):
                fail("chunk admission source/status rejected")
        if label not in SUPPORTED:
            fail("unsupported admission provenance")
        for field in ("fetch_error", "failure_reason", "deferred_reason"):
            if pd.notna(q.get(field)) and str(q[field]).strip():
                fail("contradictory successful admission: " + field)
        for field in ("duplicate_ts_count", "null_ohlc_count", "invalid_ohlc_count"):
            if pd.isna(q.get(field)) or float(q[field]) != 0:
                fail("raw quality error: " + field)
        mbytes = mp.read_bytes()
        m = json.loads(mbytes)
        if legacy:
            if m.get("schema_version") != "futures_raw_5m_loader_manifest.v1" or m.get("run_id") != q.get("run_id"):
                fail("legacy manifest/run mismatch")
            summary = m.get("instrument_summaries", {}).get(secid, {})
            cal = m.get("calendar_validation_summary", {})
            if (summary.get("quality_status") != "pass" or summary.get("requested_from") != start
                    or summary.get("requested_till") != end or cal.get(STATUS) != label
                    or str(cal.get("calendar_from")) > start or str(cal.get("calendar_till")) < end
                    or not isinstance(cal.get("expected_trading_days"), int) or cal["expected_trading_days"] <= 0):
                fail("legacy calendar/instrument admission mismatch")
            if pd.isna(q.get("off_calendar_date_count")) or float(q["off_calendar_date_count"]) != 0:
                fail("legacy off-calendar dates")
            members = m.get("partition_paths_created", [])
            expected_rows = q.get("rows")
        else:
            full_quality = pd.read_parquet(qp)
            if (full_quality.empty or "secid" not in full_quality or "eligibility_snapshot_id" not in full_quality
                    or full_quality["secid"].duplicated().any()
                    or full_quality["secid"].astype(str).tolist() != m.get("secid_list")
                    or full_quality["eligibility_snapshot_id"].isna().any()
                    or m.get("input_eligibility_snapshot_id") != full_quality["eligibility_snapshot_id"].iloc[0]):
                fail("chunk quality roster/first eligibility mismatch")
            # The producer records the first selected instrument's eligibility
            # ID on the chunk, while each quality row has its own instrument ID.
            for field, expected in (("schema_version", "futures_all_universe_raw_5m_quality_report.v1"),
                                    ("run_id", q.get("run_id")), ("chunk_id", q.get("chunk_id")),
                                    ("family_code", family), ("registry_snapshot_id", q.get("registry_snapshot_id"))):
                if single(full_quality, field) != expected:
                    fail("chunk quality identity mismatch: " + field)
            if (m.get("schema_version") != "futures_all_universe_raw_5m_chunk_manifest.v1"
                    or m.get("chunk_id") != q.get("chunk_id") or m.get("started_at") != q.get("run_id")
                    or m.get("family_code") != family or secid not in m.get("secid_list", [])
                    or m.get("date_from") != start or m.get("date_till") != end
                    or m.get("dataset_stage") != "raw_5m" or m.get("status") not in {"succeeded", "partial_failed"}
                    or any(secid in m.get(field, []) for field in ("failed_secid", "skipped_secid", "deferred_secid"))):
                fail("chunk manifest/instrument admission mismatch")
            members = json.loads(q.get("output_partitions_json", "null"))
            if not isinstance(members, list):
                fail("missing quality partition membership")
            manifest_keys = {partition_identity(x) for x in m.get("output_partitions", [])}
            if any(partition_identity(x) not in manifest_keys for x in members):
                fail("quality/manifest partition membership mismatch")
            expected_rows = q.get("rows_written")
        # Older qrow accumulated paths from previous instruments in the chunk.
        keys = sorted({partition_identity(x) for x in members if partition_identity(x)[1:] == (family, secid)})
        if not keys or any(not start <= key[0] <= end for key in keys):
            fail("admission date/partition mismatch")
        frames, hashes = [], {}
        for key in keys:
            path = self.root / relative_partition(key)
            part = read_partition(path)
            before = single(part, "_source_partition_sha256")
            frames.append(part)
            hashes[key] = before
        raw = pd.concat(frames, ignore_index=True)
        validate_structure(raw, self.cutoff)
        if single(raw, STATUS) != label:
            fail("raw/admission provenance mismatch")
        ingest = utc(single(raw, "ingest_ts"))
        if legacy:
            if ingest != utc(m.get("ingest_ts")) or single(raw, "source_endpoint_url") != q.get("source_endpoint_url"):
                fail("legacy raw/manifest ingest or endpoint mismatch")
            if summary.get("rows") != len(raw) or summary.get("partition_count") != len(keys):
                fail("legacy instrument count mismatch")
        elif ingest > utc(m.get("finished_at")):
            fail("raw newer than admission manifest")
        ts = pd.to_datetime(raw["ts"])
        if (pd.isna(expected_rows) or float(expected_rows) != len(raw)
                or ts.min() != pd.Timestamp(q.get("min_ts")) or ts.max() != pd.Timestamp(q.get("max_ts"))):
            fail("raw/admission full instrument count or timestamp mismatch")
        if digest(qp) != qhash or mp.read_bytes() != mbytes:
            fail("admission changed during evidence read")
        return {"keys": keys, "hashes": hashes, "label": label, "ingest": ingest,
                "quality_sha256": qhash, "manifest_sha256": hashlib.sha256(mbytes).hexdigest(),
                "quality_path": str(qp.relative_to(self.root)), "manifest_path": str(mp.relative_to(self.root)),
                "run_id": q.get("run_id"), "gap_count": None if pd.isna(q.get("gap_count")) else q.get("gap_count"),
                "missing_expected_trading_days": None if pd.isna(q.get("missing_expected_trading_days")) else int(q["missing_expected_trading_days"])}

    def admit(self, frame):
        validate_structure(frame, self.cutoff)
        out = frame.copy()
        out[EVIDENCE] = ""
        for path, part in out.groupby("_source_partition_path", sort=True):
            key = partition_identity(path)
            label, ingest = single(part, STATUS), utc(single(part, "ingest_ts"))
            raw_hash = digest(path)
            if single(part, "_source_partition_sha256") != raw_hash:
                fail("raw changed since initial read")
            evidence = []
            errors = []
            for q in self.rows.get(key[1:], []):
                qlabel = q.get(STATUS) if q.get("schema_version") == "futures_raw_5m_quality_report.v1" else q.get("calendar_status")
                if qlabel != label:
                    continue
                start = str(q.get("requested_from") if q.get("schema_version") == "futures_raw_5m_quality_report.v1" else q.get("date_from"))
                end = str(q.get("requested_till") if q.get("schema_version") == "futures_raw_5m_quality_report.v1" else q.get("date_till"))
                if not start <= key[0] <= end:
                    continue
                try:
                    cohort = self.cohort(q)
                    if key not in cohort["keys"] or cohort["hashes"][key] != raw_hash or cohort["ingest"] != ingest:
                        fail("partition does not match admitted cohort")
                    evidence.append({"partition": relative_partition(key).as_posix(), "raw_sha256": raw_hash,
                        "status": label, "raw_ingest_ts": str(ingest), "run_id": cohort["run_id"],
                        "quality_path": cohort["quality_path"], "quality_sha256": cohort["quality_sha256"],
                        "manifest_path": cohort["manifest_path"], "manifest_sha256": cohort["manifest_sha256"],
                        "scope": EVIDENCE_SCOPE, "gap_count": cohort["gap_count"],
                        "missing_expected_trading_days": cohort["missing_expected_trading_days"]})
                except (RuntimeError, ValueError, TypeError, KeyError, OSError) as exc:
                    errors.append(str(exc))
            if not evidence:
                fail("unverifiable partition " + relative_partition(key).as_posix() + ": " + "; ".join(sorted(set(errors))[:3]))
            out.loc[part.index, EVIDENCE] = canonical_json(sorted(evidence, key=canonical_json))
        out.attrs["date_source_cutoff_msk_date"] = self.cutoff
        return out


def group_fields(frame):
    label = single(frame, STATUS)
    if label not in SUPPORTED:
        fail("unsupported derived group provenance")
    if EVIDENCE not in frame or frame[EVIDENCE].isna().any():
        fail("missing derived group evidence")
    records = {}
    day, family = single(frame, "trade_date"), single(frame, "family_code")
    source_column = "secid" if "secid" in frame else "source_secid" if "source_secid" in frame else None
    if source_column:
        secids = set(frame[source_column].dropna().astype(str))
    else:
        secids = {str(x) for values in frame["source_contracts"] for x in values}
    witnessed_secids = set()
    for encoded in frame[EVIDENCE].unique():
        items = json.loads(encoded)
        if not isinstance(items, list) or not items:
            fail("empty derived group evidence")
        for item in items:
            if item.get("status") != label or item.get("scope") != EVIDENCE_SCOPE:
                fail("derived evidence/status mismatch")
            key = partition_identity(item.get("partition", ""))
            if key[:2] != (day, family) or key[2] not in secids:
                fail("derived evidence/partition identity mismatch")
            if not all(re.fullmatch(r"[0-9a-f]{64}", str(item.get(x, ""))) for x in
                       ("raw_sha256", "quality_sha256", "manifest_sha256")):
                fail("invalid derived evidence digest")
            if not all(isinstance(item.get(x), str) and item[x].strip() for x in ("run_id", "quality_path", "manifest_path")):
                fail("missing derived admission identity")
            utc(item.get("raw_ingest_ts"))
            witnessed_secids.add(key[2])
            records[canonical_json(item)] = item
    if witnessed_secids != secids:
        fail("incomplete derived instrument evidence")
    return {STATUS: label, EVIDENCE: canonical_json([records[k] for k in sorted(records)])}


def summary(frame, column=STATUS):
    if column not in frame or frame.empty:
        return {"status": "legacy_not_recorded", "statuses": [], "coverage_claim": "not_established"}
    labels = frame[column].copy()
    if column == STATUS and LEGACY_MARKER in frame:
        labels = labels.where(~frame[LEGACY_MARKER].eq(True), "legacy_not_recorded")
    values = sorted(labels.dropna().astype(str).unique().tolist())
    status = values[0] if len(values) == 1 else "mixed_date_sources"
    return {"status": status, "statuses": values,
            "rows_by_status": {str(k): int(v) for k, v in labels.value_counts(dropna=False).items()},
            "coverage_claim": "observed_dates_are_not_a_complete_exchange_calendar",
            "evidence_scope": EVIDENCE_SCOPE}


def roll_source_valid(row):
    label = row.get("calendar_status")
    return label in ROLL_SOURCES and row.get("calendar_source") == ROLL_SOURCES[label]


def derived_fields(frame):
    """Old continuous v1 artifacts without all four new fields remain readable."""
    if LEGACY_MARKER in frame and frame[LEGACY_MARKER].eq(True).any():
        if (not frame[LEGACY_MARKER].eq(True).all()
                or any(field in frame and frame[field].notna().any() for field in DERIVED_FIELDS)):
            fail("contradictory recorded/unrecorded provenance in derived group")
        return {LEGACY_MARKER: True}
    if not any(field in frame for field in DERIVED_FIELDS):
        return {LEGACY_MARKER: True}
    if not all(field in frame for field in DERIVED_FIELDS):
        fail("incomplete new continuous provenance fields")
    fields = group_fields(frame)
    for key in ("roll_date_source_status", "roll_date_source"):
        fields[key] = single(frame, key)
    if ROLL_SOURCES.get(fields["roll_date_source_status"]) != fields["roll_date_source"]:
        fail("derived roll source mismatch")
    fields[LEGACY_MARKER] = False
    return fields


def derived_blockers(frame):
    if not any(field in frame for field in DERIVED_FIELDS):
        return []
    try:
        for _, part in frame.groupby(["continuous_symbol", "trade_date"], dropna=False):
            derived_fields(part)
    except (RuntimeError, ValueError, TypeError, KeyError) as exc:
        return [str(exc)]
    return []


def evidence_inventory(frame):
    if EVIDENCE not in frame:
        return []
    records = {}
    for encoded in frame[EVIDENCE].dropna().unique():
        for item in json.loads(encoded):
            records[canonical_json(item)] = item
    return [records[key] for key in sorted(records)]


def continuous_seam_blockers(c5, d1):
    blockers = derived_blockers(c5) + derived_blockers(d1)
    if blockers or (STATUS not in c5 and STATUS not in d1):
        return blockers
    if STATUS not in c5 or STATUS not in d1:
        return ["continuous date-source lineage missing at D1 boundary"]
    by_key = {key: derived_fields(part) for key, part in c5.groupby(["continuous_symbol", "trade_date"])}
    d1_keys = set(d1.groupby(["continuous_symbol", "trade_date"]).groups)
    if set(by_key) != d1_keys:
        blockers.append("continuous 5m/D1 date-source key sets differ: missing D1="
            + str(sorted(map(str, set(by_key) - d1_keys))) + "; missing 5m="
            + str(sorted(map(str, d1_keys - set(by_key)))))
    for key, part in d1.groupby(["continuous_symbol", "trade_date"]):
        if by_key.get(key) != derived_fields(part):
            blockers.append("continuous D1 date-source evidence differs from 5m: " + str(key))
    return blockers
