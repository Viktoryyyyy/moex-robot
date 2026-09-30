"""Explicit-input, append-only research journal. No collector or trading authority."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
from hashlib import sha256
import json
import os
from pathlib import Path
import re
import stat
import subprocess
import sys
from importlib.metadata import distributions
from tempfile import NamedTemporaryFile
from typing import Callable

VERSION = "usdrubf.forecast_journal.v1"
MAX_BYTES = 64 * 1024 * 1024


class ForecastJournalError(ValueError):
    """Invalid, conflicting or unverifiable research evidence."""


def timestamp(value: object) -> datetime:
    if not isinstance(value, str):
        raise ForecastJournalError("timestamp must be an aware ISO string")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ForecastJournalError("invalid timestamp") from exc
    if parsed.utcoffset() is None:
        raise ForecastJournalError("naive timestamp")
    return parsed.astimezone(timezone.utc)


def text(value: object) -> str:
    if not isinstance(value, str) or not value.strip() or value != value.strip():
        raise ForecastJournalError("nonempty trimmed text required")
    return value


def fields(value: object, required: set[str]) -> dict:
    if not isinstance(value, dict) or set(value) != required:
        raise ForecastJournalError("object field set mismatch: " + str(sorted(required)))
    return value


def digest(data: bytes) -> str:
    return sha256(data).hexdigest()


def encode(value: object) -> bytes:
    try:
        return json.dumps(value, sort_keys=True, ensure_ascii=False,
                          separators=(",", ":"), allow_nan=False).encode("utf-8")
    except (TypeError, ValueError, RecursionError) as exc:
        raise ForecastJournalError("invalid JSON value") from exc


def decode(data: bytes) -> object:
    if not isinstance(data, bytes) or len(data) > MAX_BYTES:
        raise ForecastJournalError("bytes required; maximum artifact size is 64 MiB")

    def pairs(items: list) -> dict:
        result = {}
        for key, value in items:
            if key in result:
                raise ForecastJournalError("duplicate JSON key: " + key)
            result[key] = value
        return result

    def constant(value: str) -> None:
        raise ForecastJournalError("nonfinite JSON constant: " + value)

    try:
        result = json.loads(data.decode("utf-8"), object_pairs_hook=pairs,
                            parse_constant=constant)
        encode(result)  # Also rejects overflowed JSON numbers such as 1e999.
        return result
    except (UnicodeError, ValueError, RecursionError) as exc:
        raise ForecastJournalError("invalid strict JSON") from exc


def _token(value: object) -> str:
    if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}", value):
        raise ForecastJournalError("unsafe record identifier")
    return value


def _hash(value: object) -> str:
    if not isinstance(value, str) or not re.fullmatch(r"[0-9a-f]{64}", value):
        raise ForecastJournalError("SHA-256 required")
    return value


def validate_source(meta: object, now: datetime) -> dict:
    meta = fields(meta, {"source_ref", "schema_version", "code_revision",
                         "data_as_of", "available_at", "received_at", "quality_limitations"})
    for key in ("source_ref", "schema_version", "code_revision"):
        text(meta[key])
    a, b, c = (timestamp(meta[k]) for k in ("data_as_of", "available_at", "received_at"))
    if not a <= b <= c <= now:
        raise ForecastJournalError("source times must satisfy data <= available <= received <= clock")
    if not isinstance(meta["quality_limitations"], list):
        raise ForecastJournalError("quality_limitations must be an explicit list")
    for limitation in meta["quality_limitations"]:
        text(limitation)
    return meta


def read_bytes(path: Path) -> bytes:
    """Read a regular file without following its final symlink (POSIX boundary)."""
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        if not stat.S_ISREG(os.fstat(fd).st_mode):
            raise ForecastJournalError("regular file required")
        with os.fdopen(fd, "rb", closefd=False) as handle:
            data = handle.read(MAX_BYTES + 1)
        if len(data) > MAX_BYTES:
            raise ForecastJournalError("artifact exceeds 64 MiB")
        return data
    finally:
        os.close(fd)


def runtime_identity() -> dict:
    """Pin every repository Python helper, contracts and installed distributions.

    A commit alone cannot prove a dirty checkout; the file inventory is authoritative.
    """
    repo = Path(__file__).resolve().parents[3]
    files = {}
    for folder in ("src", "contracts", "configs"):
        for path in sorted((repo / folder).rglob("*")):
            if path.is_file() and path.suffix in {".py", ".inc", ".json", ".yaml", ".yml", ".toml", ".txt"}:
                files[path.relative_to(repo).as_posix()] = digest(read_bytes(path))
    for name in ("requirements.txt", "pyproject.toml", "pytest.ini"):
        if (repo / name).is_file():
            files[name] = digest(read_bytes(repo / name))
    try:
        revision = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=repo,
                                          stderr=subprocess.DEVNULL, text=True).strip()
    except (OSError, subprocess.CalledProcessError):
        revision = "UNAVAILABLE_SOURCE_TREE_ONLY"
    return {"code_revision": revision, "files_sha256": digest(encode(files)), "files": files,
            "python": sys.version, "dependencies": sorted(
                (d.metadata.get("Name", "unknown"), d.version) for d in distributions())}


class ForecastJournal:
    """Local-clock registration, not an independent timestamp/notarization service.

    The explicitly selected root must be controlled by the owner. Returned hashes
    should be retained externally. No directory discovery or mutable pointer is used.
    """

    def __init__(self, root: Path | str, *,
                 clock: Callable[[], datetime] = lambda: datetime.now(timezone.utc)) -> None:
        if os.name != "posix":
            raise ForecastJournalError("POSIX journal required")
        self.root = Path(root).absolute()
        self.clock = clock
        for directory in (self.root, self.root / "objects", self.root / "records"):
            # Top-down, including existing ancestors: a concurrent creator or a
            # previous failed fsync may have left a visible but non-durable entry.
            for part in reversed((directory, *directory.parents)):
                if part.is_symlink():
                    raise ForecastJournalError("symlink journal directory")
                part.mkdir(exist_ok=True, mode=0o700)
                self._sync_directory(part.parent)

    @staticmethod
    def _sync_directory(directory: Path) -> None:
        fd = os.open(directory, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)

    def _now(self) -> datetime:
        now = self.clock()
        if not isinstance(now, datetime) or now.utcoffset() is None:
            raise ForecastJournalError("aware registrar clock required")
        return now.astimezone(timezone.utc)

    def _finished(self, started: datetime) -> datetime:
        finished = self._now()
        if finished < started:
            raise ForecastJournalError("registrar clock moved backwards")
        return finished

    @staticmethod
    def _publish(path: Path, data: bytes) -> None:
        """fsync then atomic no-replace link; never truncate an existing record."""
        if len(data) > MAX_BYTES:
            raise ForecastJournalError("record exceeds 64 MiB")
        temp = None
        try:
            with NamedTemporaryFile(dir=path.parent, delete=False) as handle:
                temp = Path(handle.name)
                handle.write(data)
                handle.flush()
                os.fchmod(handle.fileno(), 0o444)
                os.fsync(handle.fileno())
            try:
                os.link(temp, path)
            except FileExistsError:
                pass
            # Also sync on retry/concurrent publication before acknowledging it.
            ForecastJournal._sync_directory(path.parent)
        finally:
            if temp is not None:
                temp.unlink(missing_ok=True)

    def _object(self, data: bytes) -> str:
        key = digest(data)
        path = self.root / "objects" / key
        self._publish(path, data)
        if read_bytes(path) != data:
            raise ForecastJournalError("object hash collision or tampering")
        return key

    def object_bytes(self, key: str) -> bytes:
        data = read_bytes(self.root / "objects" / _hash(key))
        if digest(data) != key:
            raise ForecastJournalError("object hash mismatch")
        return data

    def _path(self, kind: str, identifier: str) -> Path:
        if not isinstance(kind, str) or kind not in {"input", "forecast", "evaluation", "position", "risk", "observation", "experiment", "research"}:
            raise ForecastJournalError("unsupported record kind")
        return self.root / "records" / (kind + "." + _token(identifier) + ".json")

    def _put(self, kind: str, identifier: str, payload: dict, now: datetime) -> dict:
        path = self._path(kind, identifier)
        record = {"project": "MOEX_Bot", "schema_version": VERSION, "kind": kind,
                  "id": identifier, "recorded_at": now.isoformat(), "payload": payload}
        self._publish(path, encode(record))
        data = read_bytes(path)
        existing = decode(data)
        self._validate_record(existing, kind, identifier)
        if encode(existing["payload"]) != encode(payload):
            raise ForecastJournalError("record ID already contains different content")
        return {"kind": kind, "id": identifier, "sha256": digest(data)}

    @staticmethod
    def _validate_record(record: object, kind: str, identifier: str) -> None:
        record = fields(record, {"project", "schema_version", "kind", "id", "recorded_at", "payload"})
        if (record["project"], record["schema_version"], record["kind"], record["id"]) != (
                "MOEX_Bot", VERSION, kind, identifier):
            raise ForecastJournalError("record identity mismatch")
        timestamp(record["recorded_at"])
        if not isinstance(record["payload"], dict):
            raise ForecastJournalError("record payload must be object")

    def read(self, ref: dict) -> dict:
        fields(ref, {"kind", "id", "sha256"})
        data = read_bytes(self._path(ref["kind"], ref["id"]))
        if digest(data) != _hash(ref["sha256"]):
            raise ForecastJournalError("record hash mismatch")
        record = decode(data)
        self._validate_record(record, ref["kind"], ref["id"])
        return record

    def acknowledge(self, ref: dict) -> dict:
        """Durably acknowledge an existing record after an interrupted publish."""
        record = self.read(ref)
        if timestamp(record["recorded_at"]) > self._now():
            raise ForecastJournalError("existing record is ahead of registrar clock")
        self._sync_directory(self.root / "objects")
        self._sync_directory(self.root / "records")
        return ref

    def verify_input(self, ref: dict) -> dict:
        record = self.read(ref)
        if record["kind"] != "input":
            raise ForecastJournalError("input reference required")
        payload = record["payload"]
        raw = self.object_bytes(payload["object_sha256"])
        if "logical_sha256" in payload:
            from moex_data.rub_snapshot_serialization import expand
            logical = self.object_bytes(payload["logical_sha256"])
            if len(logical) != payload["logical_length"] or encode(expand(decode(raw))) != logical:
                raise ForecastJournalError("logical input mismatch")
        return record

    def capture(self, identifier: str, data: bytes, metadata: dict) -> dict:
        """Freeze exact source bytes; metadata timestamps are caller declarations."""
        now = self._now()
        validate_source(metadata, now)
        decode(data)  # First slice accepts JSON factual packages, including external additions.
        key = self._object(data)
        return self._put("input", identifier, {"source": metadata, "object_sha256": key}, self._finished(now))

    def register(self, identifier: str, spec: dict) -> dict:
        from .usdrubf_forecast_evaluation import validate_forecast
        now = self._now()
        validate_forecast(spec)
        issued = timestamp(spec["issued_at"])
        if issued > now:
            raise ForecastJournalError("forecast issued_at is in the future")
        for ref in spec["inputs"]:
            record = self.verify_input(ref)
            if record["kind"] != "input" or timestamp(record["recorded_at"]) > now:
                raise ForecastJournalError("input record required before registration")
            source = record["payload"]
            validate_source(source["source"], now)
            cutoff = "available_at" if spec.get("context") is not None else "received_at"
            if timestamp(source["source"][cutoff]) > issued:
                raise ForecastJournalError("future input relative to forecast issued_at")
            self.object_bytes(source["object_sha256"])
        if spec.get("context") is not None:
            from ..consumers.usdrubf_forecast_cycle import validate_baseline
            validate_baseline(self, spec)
        if spec["supersedes"] is not None:
            previous = self.read(spec["supersedes"])
            if previous["kind"] != "forecast" or previous["id"] == identifier:
                raise ForecastJournalError("revision requires a distinct existing forecast")
            old = previous["payload"]
            if (old["instrument"], old["contract"]) != (spec["instrument"], spec["contract"]):
                raise ForecastJournalError("revision identity mismatch")
            if issued < timestamp(old["issued_at"]) or now < timestamp(previous["recorded_at"]):
                raise ForecastJournalError("revision predates original")
        return self._put("forecast", identifier, spec, self._finished(now))

    def evaluate(self, identifier: str, forecast_ref: dict, facts: bytes, source: dict, *,
                 supersedes: dict | None = None, revision_reason: str | None = None) -> dict:
        from . import usdrubf_forecast_evaluation as rules
        now = self._now()
        record = self.read(forecast_ref)
        if record["kind"] != "forecast" or timestamp(record["recorded_at"]) > now:
            raise ForecastJournalError("past registered forecast required")
        if supersedes is not None:
            previous = self.read(supersedes)
            if (previous["kind"] != "evaluation" or previous["id"] == identifier
                    or previous["payload"]["forecast"] != forecast_ref
                    or timestamp(previous["recorded_at"]) > now):
                raise ForecastJournalError("evaluation revision requires distinct prior assessment of same forecast")
            text(revision_reason)
        elif revision_reason is not None:
            raise ForecastJournalError("evaluation revision reason without predecessor")
        # Recheck every linked byte, even when the evaluator needs only the frozen spec.
        for ref in record["payload"]["inputs"]:
            self.verify_input(ref)
        if record["payload"].get("context") is not None:
            from ..consumers.usdrubf_forecast_cycle import validate_baseline
            validate_baseline(self, record["payload"])
        validate_source(source, now)
        raw = decode(facts)
        report = rules.evaluate(record["payload"], raw, evaluated_at=now,
                          source_as_of=timestamp(source["data_as_of"]),
                          limitations=source["quality_limitations"])
        key = self._object(facts)
        prospective = timestamp(record["recorded_at"]) <= timestamp(record["payload"]["horizon_start"])
        declared = record["payload"].get("context", {}).get("registration_class")
        category = ("SYNTHETIC" if declared == "SYNTHETIC" else
                    "PROSPECTIVE_LOCAL" if prospective and declared != "RETROSPECTIVE" else "RETROSPECTIVE")
        payload = {"forecast": forecast_ref, "facts_sha256": key, "facts_source": source,
                   "evaluator_version": rules.EVALUATOR_VERSION,
                   "evaluator_sha256": digest(read_bytes(Path(rules.__file__))),
                   "runtime": runtime_identity(),
                   "registration_class": category,
                   "report": report}
        if supersedes is not None:
            payload.update(supersedes=supersedes, revision_reason=revision_reason)
        return self._put("evaluation", identifier, payload, self._finished(now))

    def reproduce(self, evaluation_ref: dict) -> dict:
        from . import usdrubf_forecast_evaluation as rules
        saved = self.read(evaluation_ref)
        if saved["kind"] != "evaluation":
            raise ForecastJournalError("evaluation reference required")
        payload = saved["payload"]
        if "runtime" in payload and encode(payload["runtime"]) != encode(runtime_identity()):
            raise ForecastJournalError("use the exact recorded code and dependencies")
        if (payload["evaluator_version"] != rules.EVALUATOR_VERSION
                or payload["evaluator_sha256"] != digest(read_bytes(Path(rules.__file__)))):
            raise ForecastJournalError("use the exact recorded evaluator version")
        forecast = self.read(payload["forecast"])
        for ref in forecast["payload"]["inputs"]:
            self.verify_input(ref)
        if forecast["payload"].get("context") is not None:
            from ..consumers.usdrubf_forecast_cycle import validate_baseline
            validate_baseline(self, forecast["payload"])
        source = payload["facts_source"]
        result = rules.evaluate(forecast["payload"], decode(self.object_bytes(payload["facts_sha256"])),
                          evaluated_at=timestamp(saved["recorded_at"]),
                          source_as_of=timestamp(source["data_as_of"]),
                          limitations=source["quality_limitations"])
        if encode(result) != encode(payload["report"]):
            raise ForecastJournalError("evaluation reproduction mismatch")
        return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    sub = parser.add_subparsers(dest="action", required=True)
    capture = sub.add_parser("capture")
    capture.add_argument("--id", required=True)
    capture.add_argument("--source", type=Path, required=True)
    capture.add_argument("--metadata", type=Path, required=True)
    register = sub.add_parser("register")
    register.add_argument("--id", required=True)
    register.add_argument("--spec", type=Path, required=True)
    score = sub.add_parser("evaluate")
    score.add_argument("--id", required=True)
    score.add_argument("--forecast-ref", type=Path, required=True)
    score.add_argument("--facts", type=Path, required=True)
    score.add_argument("--metadata", type=Path, required=True)
    score.add_argument("--supersedes", type=Path)
    score.add_argument("--revision-reason")
    replay = sub.add_parser("reproduce")
    replay.add_argument("--evaluation-ref", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        journal = ForecastJournal(args.root)
        load = lambda path: decode(read_bytes(path))
        if args.action == "capture":
            result = journal.capture(args.id, read_bytes(args.source), load(args.metadata))
        elif args.action == "register":
            result = journal.register(args.id, load(args.spec))
        elif args.action == "evaluate":
            result = journal.evaluate(args.id, load(args.forecast_ref),
                                      read_bytes(args.facts), load(args.metadata),
                                      supersedes=load(args.supersedes) if args.supersedes else None,
                                      revision_reason=args.revision_reason)
        else:
            result = journal.reproduce(load(args.evaluation_ref))
        print(encode(result).decode("utf-8"))
        return 0
    except (ForecastJournalError, OSError) as exc:
        parser.exit(2, "forecast journal: " + str(exc) + "\n")


if __name__ == "__main__":
    # Use the canonical module so evaluator and CLI share one exception class.
    from .usdrubf_forecast_journal import main as _entrypoint
    raise SystemExit(_entrypoint())
