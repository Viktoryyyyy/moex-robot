from __future__ import annotations

import argparse
import hashlib
import io
import json
import os
import re
import stat
import subprocess
import sys
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Final, Mapping, Sequence

OPERATIONAL_INVOCATION: Final[str] = (
    "PYTHONPATH=src python -B "
    "src/moex_research/runners/"
    "usdrubf_phase8_7a_futoi_si_experimental_runtime.py"
)

if __name__ == "__main__" and (
    not sys.dont_write_bytecode or sys.pycache_prefix is not None
):
    raise RuntimeError(
        "experimental runtime requires no-bytecode startup; use "
        + OPERATIONAL_INVOCATION
    )

import pandas as pd

from moex_research.runners import (
    usdrubf_phase8_7a_futoi_si_runtime as base,
)

PROJECT: Final[str] = base.PROJECT
TASK_ID: Final[str] = (
    "ema_3_19_ai_phase_8_7a_futoi_si_historical_runtime_enablement_v1"
)
POLICY_CONTRACT_ID: Final[str] = (
    "usdrubf_phase8_7a_futoi_si_experimental_runtime_policy_v2"
)
POLICY_CONTRACT_VERSION: Final[str] = "2.2"
AUTHORITY_MODE: Final[str] = "futoi_si_historical_experimental_only"
POLICY_FLAG: Final[str] = "--experimental-authority-contract-path"
RUNTIME_AUTHORITY_FLAG: Final[str] = "--runtime-authority-evidence-path"
POLICY_REPO_PATH: Final[str] = (
    "contracts/experiments/"
    "usdrubf_phase8_7a_futoi_si_experimental_runtime_authority_v1.json"
)
AUTHORIZED_DATA_ROOT: Final[Path] = Path("/home/trader/moex_bot/data")
TRUSTED_AUTHORITY_ROOT: Final[Path] = Path(
    "/etc/moex_bot/runtime_authorities/ema_3_19_ai"
)
TRUSTED_AUTHORITY_OWNER_UID: Final[int] = 0
TRUSTED_GIT_PATH: Final[str] = "/usr/bin:/bin"
TRUSTED_GIT_CONFIG_HOME: Final[str] = "/nonexistent"
OUTPUT_PARENT_RELATIVE: Final[Path] = Path(
    "research/ema_3_19_ai/phase8_7a_futoi_si_source_validation"
)
EXPERIMENTAL_STATUS: Final[str] = (
    "moex_futoi_si_experimental_dataset_materialized"
)
FAIL_STATUS: Final[str] = "moex_futoi_si_source_not_ready"
TECHNICAL_GATES: Final[tuple[str, ...]] = (
    "G1_immutable_inputs",
    "G2_exact_route_and_transport",
    "G4_schema_and_pairing",
    "G6_exact_coverage",
    "G7_numerical_and_chronology",
    "G8_provenance_and_no_leakage",
)
AUTHORIZATION_ID_PATTERN: Final[re.Pattern[str]] = re.compile(
    r"^[A-Za-z0-9][A-Za-z0-9_.:-]{7,127}$"
)
SHA40_PATTERN: Final[re.Pattern[str]] = re.compile(r"^[0-9a-f]{40}$")
OPEN_DIRECTORY_FLAGS: Final[int] = (
    os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC
)
OPEN_READ_FLAGS: Final[int] = os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC
OPEN_WRITE_FLAGS: Final[int] = (
    os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC
)


@dataclass(frozen=True)
class ExperimentalRuntimeRequest:
    base_request: base.RuntimeRequest
    policy_contract_path: Path
    runtime_authority_evidence_path: Path


@dataclass(frozen=True)
class CapturedInput:
    name: str
    path: Path
    payload: bytes
    sha256: str
    device: int
    inode: int


def build_argument_parser() -> argparse.ArgumentParser:
    parser = base.build_argument_parser()
    parser.prog = OPERATIONAL_INVOCATION
    parser.add_argument(POLICY_FLAG, required=True)
    parser.add_argument(RUNTIME_AUTHORITY_FLAG, required=True)
    return parser


def request_from_args(args: argparse.Namespace) -> ExperimentalRuntimeRequest:
    base_request = base.request_from_args(args)
    policy_path = base._input_file(
        getattr(args, "experimental_authority_contract_path"),
        ".json",
        POLICY_FLAG,
    )
    authority_path = base._input_file(
        getattr(args, "runtime_authority_evidence_path"),
        ".json",
        RUNTIME_AUTHORITY_FLAG,
    )
    existing = {
        path.resolve()
        for name, path in base_request.__dict__.items()
        if name.endswith("_path")
    }
    if policy_path.resolve() in existing or authority_path.resolve() in existing:
        raise base.validation.FutoiSiSourceValidationError(
            "experimental policy and runtime authority must be distinct inputs",
            blocker="provenance_not_sufficient",
        )
    if policy_path.resolve() == authority_path.resolve():
        raise base.validation.FutoiSiSourceValidationError(
            "runtime authority evidence cannot equal the checked-in policy",
            blocker="provenance_not_sufficient",
        )
    return ExperimentalRuntimeRequest(
        base_request=base_request,
        policy_contract_path=policy_path,
        runtime_authority_evidence_path=authority_path,
    )


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _fail(message: str) -> base.validation.FutoiSiSourceValidationError:
    return base.validation.FutoiSiSourceValidationError(
        message,
        blocker="provenance_not_sufficient",
    )


def _sanitized_git_environment() -> dict[str, str]:
    environment = {
        key: value
        for key, value in os.environ.items()
        if not key.upper().startswith("GIT_")
    }
    environment["PATH"] = TRUSTED_GIT_PATH
    environment["HOME"] = TRUSTED_GIT_CONFIG_HOME
    environment["XDG_CONFIG_HOME"] = TRUSTED_GIT_CONFIG_HOME
    return environment


def _git_command(repo_root: Path, *args: str) -> list[str]:
    return [
        "git",
        "-C",
        str(repo_root.resolve()),
        "-c",
        "core.fsmonitor=false",
        "-c",
        "core.untrackedCache=false",
        "-c",
        "core.checkStat=default",
        "-c",
        "core.trustctime=true",
        *args,
    ]


def _run_git(
    repo_root: Path,
    *args: str,
    allow_empty: bool = False,
) -> str:
    completed = subprocess.run(
        _git_command(repo_root, *args),
        check=False,
        capture_output=True,
        text=True,
        env=_sanitized_git_environment(),
    )
    value = completed.stdout.strip()
    if completed.returncode != 0 or (not allow_empty and not value):
        raise _fail("applied git state cannot prove runtime provenance")
    return value


def _run_git_bytes(repo_root: Path, *args: str) -> bytes:
    completed = subprocess.run(
        _git_command(repo_root, *args),
        check=False,
        capture_output=True,
        env=_sanitized_git_environment(),
    )
    if completed.returncode != 0:
        raise _fail("applied git object cannot prove runtime provenance")
    return bytes(completed.stdout)


def _verify_git_context(repo_root: Path) -> None:
    observed = Path(_run_git(repo_root, "rev-parse", "--show-toplevel")).resolve()
    if observed != repo_root.resolve():
        raise _fail("Git commands are not bound to the executed repository")
    if _run_git(repo_root, "rev-parse", "--is-inside-work-tree") != "true":
        raise _fail("executed repository is not a Git worktree")


def _read_git_blob_bytes(repo_root: Path, blob_sha: str) -> bytes:
    payload = _run_git_bytes(repo_root, "cat-file", "blob", blob_sha)
    header = f"blob {len(payload)}\0".encode("ascii")
    observed = hashlib.sha1(
        header + payload,
        usedforsecurity=False,
    ).hexdigest()
    if observed != blob_sha:
        raise _fail("tracked policy Git blob identity mismatch")
    return payload


def _verify_no_repository_bytecode(repo_root: Path) -> None:
    offenders: list[str] = []
    try:
        for root, directory_names, file_names in os.walk(
            repo_root,
            topdown=True,
            followlinks=False,
        ):
            current = Path(root)
            relative = current.relative_to(repo_root)
            if ".git" in directory_names:
                directory_names.remove(".git")
            for directory_name in tuple(directory_names):
                if directory_name == "__pycache__":
                    offenders.append((relative / directory_name).as_posix())
                    directory_names.remove(directory_name)
            for file_name in file_names:
                if Path(file_name).suffix.lower() in {".pyc", ".pyo"}:
                    offenders.append((relative / file_name).as_posix())
    except (OSError, ValueError) as exc:
        raise _fail("repository bytecode inventory cannot be verified") from exc
    if offenders:
        raise _fail(
            "ignored executable Python bytecode is present: "
            + ", ".join(sorted(offenders))
        )


def _verify_loaded_runtime_modules(
    repo_root: Path,
    modules: Sequence[object] | None = None,
) -> None:
    resolved_root = repo_root.resolve()
    critical_modules = tuple(
        modules
        if modules is not None
        else (sys.modules.get(__name__), base, base.validation)
    )
    for module in critical_modules:
        module_name = str(getattr(module, "__name__", "<unknown>"))
        raw_path = getattr(module, "__file__", None)
        if not isinstance(raw_path, str) or not raw_path:
            raise _fail(f"loaded runtime module has no source path: {module_name}")
        try:
            source_path = Path(raw_path).resolve(strict=True)
            source_path.relative_to(resolved_root)
        except (OSError, ValueError) as exc:
            raise _fail(
                f"loaded runtime module is outside the executed repository: {module_name}"
            ) from exc
        if source_path.suffix.lower() != ".py" or "__pycache__" in source_path.parts:
            raise _fail(
                f"loaded runtime module does not use trusted Python source: {module_name}"
            )


def _verify_no_hidden_index_flags(repo_root: Path) -> None:
    tagged = _run_git(
        repo_root,
        "ls-files",
        "-v",
        "-z",
        allow_empty=True,
    )
    hidden: list[str] = []
    for entry in tagged.split("\0"):
        if not entry:
            continue
        tag = entry[0]
        path = entry[2:] if len(entry) > 2 and entry[1] == " " else entry[1:]
        if tag.islower() or tag == "S":
            hidden.append(path)
    if hidden:
        raise _fail(
            "tracked files use assume-unchanged or skip-worktree flags: "
            + ", ".join(sorted(hidden))
        )


def _verify_clean_worktree(repo_root: Path) -> None:
    status = _run_git(
        repo_root,
        "status",
        "--porcelain=v1",
        "--untracked-files=all",
        allow_empty=True,
    )
    if status:
        raise _fail("executed repository worktree or index differs from HEAD")


def _data_root() -> Path:
    raw = str(os.environ.get("MOEX_DATA_ROOT") or "").strip()
    if not raw:
        raise _fail("MOEX_DATA_ROOT is required for experimental runtime")
    candidate = Path(os.path.abspath(os.path.expanduser(raw)))
    if candidate != AUTHORIZED_DATA_ROOT:
        raise _fail("MOEX_DATA_ROOT differs from the canonical data root")
    return AUTHORIZED_DATA_ROOT


def _json_object_from_bytes(payload: bytes, label: str) -> dict[str, Any]:
    try:
        value = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise _fail(f"{label} is not valid UTF-8 JSON") from exc
    if not isinstance(value, dict):
        raise _fail(f"{label} must be a JSON object")
    return value


def _sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _git_blob_sha1_bytes(payload: bytes) -> str:
    header = f"blob {len(payload)}\0".encode("ascii")
    return hashlib.sha1(
        header + payload,
        usedforsecurity=False,
    ).hexdigest()


def _read_fd_all(descriptor: int) -> bytes:
    chunks: list[bytes] = []
    while True:
        chunk = os.read(descriptor, 1024 * 1024)
        if not chunk:
            return b"".join(chunks)
        chunks.append(chunk)


def _capture_file_once(path: Path, name: str) -> CapturedInput:
    try:
        descriptor = os.open(path, OPEN_READ_FLAGS)
    except OSError as exc:
        raise _fail(f"runtime input cannot be opened safely: {name}") from exc
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode):
            raise _fail(f"runtime input is not a regular file: {name}")
        payload = _read_fd_all(descriptor)
    finally:
        os.close(descriptor)
    return CapturedInput(
        name=name,
        path=path,
        payload=payload,
        sha256=_sha256_bytes(payload),
        device=metadata.st_dev,
        inode=metadata.st_ino,
    )


def _capture_runtime_inputs(
    request: base.RuntimeRequest,
) -> dict[str, CapturedInput]:
    inventory = {
        "modeling_dataset": request.modeling_dataset_path,
        "dataset_manifest": request.dataset_manifest_path,
        "feature_schema": request.feature_schema_path,
        "m0_validation_predictions": request.m0_validation_predictions_path,
        "phase83_aggregate_metrics": request.phase83_aggregate_metrics_path,
        "phase83_gate_results": request.phase83_gate_results_path,
        "experiment_contract": request.experiment_contract_path,
        "license_access_evidence": request.license_access_evidence_path,
        "pit_semantics_evidence": request.pit_semantics_evidence_path,
    }
    return {
        name: _capture_file_once(path, name)
        for name, path in inventory.items()
    }


def _json_snapshot(snapshot: CapturedInput) -> dict[str, Any]:
    return _json_object_from_bytes(snapshot.payload, snapshot.name)


def _parquet_snapshot(snapshot: CapturedInput) -> pd.DataFrame:
    try:
        return pd.read_parquet(io.BytesIO(snapshot.payload))
    except Exception as exc:
        raise _fail(f"invalid parquet input: {snapshot.name}") from exc


def _verify_contract_snapshot(snapshot: CapturedInput) -> dict[str, Any]:
    contract = _json_snapshot(snapshot)
    identity = contract.get("contract_identity")
    source = contract.get("source_identity")
    if not isinstance(identity, dict) or not isinstance(source, dict):
        raise _fail("FUTOI contract identity is malformed")
    if (
        identity.get("project") != PROJECT
        or identity.get("phase") != "8.7A"
        or identity.get("contract_version") != "1.6"
        or source.get("source_ticker") != "Si"
        or source.get("exact_path") != base.validation.FUTOI_PATH
        or source.get("target_security_id")
        != base.validation.TARGET_SECURITY_ID
    ):
        raise _fail("FUTOI experiment contract mismatch")
    if _git_blob_sha1_bytes(snapshot.payload) != base.CONTRACT_GIT_BLOB_SHA1:
        raise _fail("FUTOI experiment contract digest mismatch")
    return contract


def _verify_captured_inputs(
    snapshots: Mapping[str, CapturedInput],
) -> dict[str, str]:
    bad = [
        name
        for name, expected in base.EXPECTED_FROZEN_SHA256.items()
        if snapshots.get(name) is None or snapshots[name].sha256 != expected
    ]
    if bad:
        raise _fail("immutable input hash mismatch: " + ", ".join(sorted(bad)))
    manifest = _json_snapshot(snapshots["dataset_manifest"])
    schema = _json_snapshot(snapshots["feature_schema"])
    if not manifest or not schema:
        raise _fail("frozen manifest or feature schema is empty")
    base._validate_phase83_evidence(
        _json_snapshot(snapshots["phase83_aggregate_metrics"]),
        _json_snapshot(snapshots["phase83_gate_results"]),
    )
    _verify_contract_snapshot(snapshots["experiment_contract"])
    return {
        **{
            name: snapshots[name].sha256
            for name in base.EXPECTED_FROZEN_SHA256
        },
        "experiment_contract": _git_blob_sha1_bytes(
            snapshots["experiment_contract"].payload
        ),
        "license_access_evidence": snapshots[
            "license_access_evidence"
        ].sha256,
        "pit_semantics_evidence": snapshots["pit_semantics_evidence"].sha256,
    }


def _positive_int(value: object) -> int | None:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        return None
    return value


def _validate_trusted_metadata(
    metadata: os.stat_result,
    *,
    label: str,
    require_directory: bool,
) -> None:
    expected_type = (
        stat.S_ISDIR(metadata.st_mode)
        if require_directory
        else stat.S_ISREG(metadata.st_mode)
    )
    if not expected_type:
        raise _fail(f"{label} has an invalid filesystem type")
    if metadata.st_uid != TRUSTED_AUTHORITY_OWNER_UID:
        raise _fail(f"{label} is not owned by the trusted authority owner")
    if metadata.st_mode & 0o022:
        raise _fail(f"{label} is group or world writable")


def _validate_data_root_metadata(
    metadata: os.stat_result,
    *,
    label: str,
) -> None:
    if not stat.S_ISDIR(metadata.st_mode):
        raise _fail(f"{label} is not a directory")
    if metadata.st_mode & 0o022:
        raise _fail(f"{label} is group or world writable")


def _open_trusted_authority_root() -> int:
    if not TRUSTED_AUTHORITY_ROOT.is_absolute():
        raise _fail("trusted runtime-authority root must be absolute")
    try:
        current = os.open("/", OPEN_DIRECTORY_FLAGS)
    except OSError as exc:
        raise _fail("filesystem root cannot be opened safely") from exc
    try:
        _validate_trusted_metadata(
            os.fstat(current),
            label="filesystem root",
            require_directory=True,
        )
        for part in TRUSTED_AUTHORITY_ROOT.parts[1:]:
            try:
                next_fd = os.open(
                    part,
                    OPEN_DIRECTORY_FLAGS,
                    dir_fd=current,
                )
            except OSError as exc:
                raise _fail(
                    "trusted runtime-authority root cannot be traversed safely"
                ) from exc
            try:
                _validate_trusted_metadata(
                    os.fstat(next_fd),
                    label=f"trusted authority ancestor {part}",
                    require_directory=True,
                )
            except Exception:
                os.close(next_fd)
                raise
            os.close(current)
            current = next_fd
        return current
    except Exception:
        os.close(current)
        raise


def _open_canonical_data_root(
    expected_device: int | None = None,
    expected_inode: int | None = None,
) -> int:
    _data_root()
    if (expected_device is None) != (expected_inode is None):
        raise _fail("canonical data root identity is incomplete")
    try:
        current = os.open("/", OPEN_DIRECTORY_FLAGS)
    except OSError as exc:
        raise _fail("filesystem root cannot be opened for data-root traversal") from exc
    try:
        _validate_data_root_metadata(
            os.fstat(current),
            label="filesystem root",
        )
        for part in AUTHORIZED_DATA_ROOT.parts[1:]:
            try:
                next_fd = os.open(
                    part,
                    OPEN_DIRECTORY_FLAGS,
                    dir_fd=current,
                )
            except OSError as exc:
                raise _fail("canonical data root cannot be traversed safely") from exc
            try:
                _validate_data_root_metadata(
                    os.fstat(next_fd),
                    label=f"canonical data-root ancestor {part}",
                )
            except Exception:
                os.close(next_fd)
                raise
            os.close(current)
            current = next_fd
        metadata = os.fstat(current)
        if expected_device is not None and (
            metadata.st_dev != expected_device or metadata.st_ino != expected_inode
        ):
            raise _fail("canonical data root physical identity mismatch")
        return current
    except Exception:
        os.close(current)
        raise


def _assert_data_root_identity(device: int, inode: int) -> None:
    descriptor = _open_canonical_data_root(device, inode)
    os.close(descriptor)


def _read_trusted_authority_once(
    path: Path,
) -> tuple[dict[str, Any], bytes, Path]:
    if (
        not path.is_absolute()
        or path.parent != TRUSTED_AUTHORITY_ROOT
        or path.suffix.lower() != ".json"
        or path.name in {"", ".", ".."}
    ):
        raise _fail("runtime authority evidence is outside the trusted issuer root")
    root_fd = _open_trusted_authority_root()
    try:
        try:
            descriptor = os.open(
                path.name,
                OPEN_READ_FLAGS,
                dir_fd=root_fd,
            )
        except OSError as exc:
            raise _fail("runtime authority evidence cannot be opened safely") from exc
    finally:
        os.close(root_fd)
    try:
        metadata = os.fstat(descriptor)
        _validate_trusted_metadata(
            metadata,
            label="runtime authority evidence",
            require_directory=False,
        )
        payload = _read_fd_all(descriptor)
    finally:
        os.close(descriptor)
    parsed = _json_object_from_bytes(payload, "runtime authority evidence")
    authorization_id = str(parsed.get("authorization_id") or "").strip()
    if not AUTHORIZATION_ID_PATTERN.fullmatch(authorization_id):
        raise _fail("runtime authority authorization_id is malformed")
    if path.name != f"{authorization_id}.json":
        raise _fail("runtime authority filename does not match authorization_id")
    return parsed, payload, TRUSTED_AUTHORITY_ROOT / path.name


def _verify_policy_contract(request: ExperimentalRuntimeRequest) -> dict[str, Any]:
    path = request.policy_contract_path
    repo_root = _repo_root()
    canonical_path = (repo_root / POLICY_REPO_PATH).resolve()
    if path.resolve() != canonical_path:
        raise _fail("experimental policy must use its canonical repository path")
    _verify_git_context(repo_root)
    head = _run_git(repo_root, "rev-parse", "HEAD").lower()
    if head != request.base_request.git_commit_sha:
        raise _fail("runtime git SHA differs from applied repository HEAD")
    _verify_no_repository_bytecode(repo_root)
    _verify_loaded_runtime_modules(repo_root)
    _verify_no_hidden_index_flags(repo_root)
    _verify_clean_worktree(repo_root)
    tracked_blob = _run_git(
        repo_root,
        "rev-parse",
        f"{head}:{POLICY_REPO_PATH}",
    ).lower()
    policy_bytes = _read_git_blob_bytes(repo_root, tracked_blob)
    payload = _json_object_from_bytes(policy_bytes, "experimental runtime policy")
    identity = payload.get("contract_identity")
    parent = payload.get("parent_contract")
    boundary = payload.get("policy_boundary")
    authority = payload.get("authority_boundaries")
    gates = payload.get("gate_policy")
    runtime = payload.get("runtime_policy")
    required_fields = payload.get("required_runtime_authority_fields")
    if not all(
        isinstance(item, Mapping)
        for item in (identity, parent, boundary, authority, gates, runtime)
    ) or not isinstance(required_fields, list):
        raise _fail("experimental runtime policy structure mismatch")
    assert isinstance(identity, Mapping)
    assert isinstance(parent, Mapping)
    assert isinstance(boundary, Mapping)
    assert isinstance(authority, Mapping)
    assert isinstance(gates, Mapping)
    assert isinstance(runtime, Mapping)
    forbidden = (
        "phase8_7b_feature_computation_allowed",
        "model_fitting_allowed",
        "production_prediction_allowed",
        "model_or_strategy_promotion_allowed",
        "raw_payload_redistribution_allowed",
        "broker_action_allowed",
        "trading_action_allowed",
    )
    if (
        identity.get("project") != PROJECT
        or identity.get("task_id") != TASK_ID
        or identity.get("contract_id") != POLICY_CONTRACT_ID
        or identity.get("contract_version") != POLICY_CONTRACT_VERSION
        or identity.get("phase") != "8.7A"
        or identity.get("status") != "experimental_runtime_policy_active"
        or parent.get("git_blob_sha1") != base.CONTRACT_GIT_BLOB_SHA1
        or parent.get("source_ticker") != base.validation.SOURCE_TICKER
        or parent.get("target_instrument_id")
        != base.validation.TARGET_INSTRUMENT_ID
        or boundary.get("checked_in_policy_is_runtime_authority") is not False
        or boundary.get("separate_runtime_authority_evidence_required") is not True
        or boundary.get("required_runtime_authority_flag")
        != RUNTIME_AUTHORITY_FLAG
        or boundary.get("runtime_authority_must_not_be_stored_in_repository")
        is not True
        or boundary.get("runtime_authority_must_bind_data_root_identity")
        is not True
        or boundary.get("canonical_data_root")
        != AUTHORIZED_DATA_ROOT.as_posix()
        or boundary.get("canonical_data_root_open_mode")
        != "nofollow_dirfd_from_filesystem_root"
        or boundary.get("data_root_ancestor_group_world_writable_allowed")
        is not False
        or boundary.get("trusted_runtime_authority_root")
        != TRUSTED_AUTHORITY_ROOT.as_posix()
        or boundary.get("trusted_runtime_authority_owner_uid")
        != TRUSTED_AUTHORITY_OWNER_UID
        or boundary.get("trusted_runtime_authority_filename_rule")
        != "authorization_id.json"
        or boundary.get(
            "trusted_runtime_authority_group_world_writable_allowed"
        )
        is not False
        or boundary.get("trusted_runtime_authority_ancestors_must_be_root_owned")
        is not True
        or boundary.get("module_claims_global_single_use") is not False
        or "data_root_device" not in required_fields
        or "data_root_inode" not in required_fields
        or authority.get("mode") != AUTHORITY_MODE
        or authority.get("approved_by") != "PM_L2_PHASE_OWNER"
        or authority.get("historical_authenticated_retrieval_allowed") is not True
        or authority.get("phase8_7a_source_validation_allowed") is not True
        or any(authority.get(name) is not False for name in forbidden)
        or tuple(
            gates.get("experimental_dataset_status_requires_technical_gates")
            or ()
        )
        != TECHNICAL_GATES
        or gates.get("g3_or_g5_must_not_be_forced_to_pass") is not True
        or gates.get("experimental_dataset_status") != EXPERIMENTAL_STATUS
        or gates.get("failure_status") != FAIL_STATUS
        or runtime.get("required_policy_flag") != POLICY_FLAG
        or runtime.get("operational_invocation") != OPERATIONAL_INVOCATION
        or runtime.get("output_artifact_count") != 10
        or runtime.get(
            "data_root_identity_verified_before_retrieval_and_artifact_write"
        )
        is not True
        or runtime.get("fallback_or_substitution_allowed") is not False
        or runtime.get("raw_response_persistence_allowed") is not False
    ):
        raise _fail("experimental runtime policy mismatch")
    return {
        "contract_id": POLICY_CONTRACT_ID,
        "contract_version": POLICY_CONTRACT_VERSION,
        "git_blob_sha1": tracked_blob,
        "sha256": _sha256_bytes(policy_bytes),
    }


def _aware_timestamp(value: object) -> bool:
    text = str(value or "").strip()
    if not text:
        return False
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return False
    return parsed.tzinfo is not None and parsed.utcoffset() is not None


def _canonical_output_relative(run_id: str) -> Path:
    return OUTPUT_PARENT_RELATIVE / f"run_id={run_id}"


def _canonical_output_path(run_id: str) -> Path:
    return _data_root() / _canonical_output_relative(run_id)


def _verify_runtime_authority(
    request: ExperimentalRuntimeRequest,
) -> dict[str, Any]:
    payload, raw_bytes, trusted_path = _read_trusted_authority_once(
        request.runtime_authority_evidence_path
    )
    base_request = request.base_request
    expected_output = _canonical_output_path(base_request.run_id)
    forbidden = (
        "phase8_7b_feature_computation_allowed",
        "model_fitting_allowed",
        "production_prediction_allowed",
        "model_or_strategy_promotion_allowed",
        "raw_payload_redistribution_allowed",
        "broker_action_allowed",
        "trading_action_allowed",
    )
    authorization_id = str(payload.get("authorization_id") or "").strip()
    authority_sha = str(payload.get("git_commit_sha") or "").strip().lower()
    data_root_device = _positive_int(payload.get("data_root_device"))
    data_root_inode = _positive_int(payload.get("data_root_inode"))
    if (
        payload.get("project") != PROJECT
        or payload.get("task_id") != TASK_ID
        or not AUTHORIZATION_ID_PATTERN.fullmatch(authorization_id)
        or payload.get("approved_by") != "PM_L2_PHASE_OWNER"
        or payload.get("mode") != AUTHORITY_MODE
        or not SHA40_PATTERN.fullmatch(authority_sha)
        or authority_sha != base_request.git_commit_sha
        or payload.get("run_id") != base_request.run_id
        or payload.get("data_root") != AUTHORIZED_DATA_ROOT.as_posix()
        or data_root_device is None
        or data_root_inode is None
        or payload.get("output_dir") != expected_output.as_posix()
        or not _aware_timestamp(payload.get("issued_at"))
        or payload.get("historical_authenticated_retrieval_allowed") is not True
        or payload.get("phase8_7a_source_validation_allowed") is not True
        or any(payload.get(name) is not False for name in forbidden)
    ):
        raise _fail("runtime authority evidence does not match the exact invocation")
    if base_request.output_dir.absolute() != expected_output:
        raise _fail("runtime output directory differs from exact authority evidence")
    assert data_root_device is not None
    assert data_root_inode is not None
    _assert_data_root_identity(data_root_device, data_root_inode)
    return {
        "authorization_id": authorization_id,
        "approved_by": "PM_L2_PHASE_OWNER",
        "mode": AUTHORITY_MODE,
        "git_commit_sha": authority_sha,
        "run_id": base_request.run_id,
        "data_root": AUTHORIZED_DATA_ROOT.as_posix(),
        "data_root_device": data_root_device,
        "data_root_inode": data_root_inode,
        "output_dir": expected_output.as_posix(),
        "issued_at": str(payload.get("issued_at")),
        "evidence_path": trusted_path.as_posix(),
        "evidence_sha256": _sha256_bytes(raw_bytes),
        "trusted_owner_uid": TRUSTED_AUTHORITY_OWNER_UID,
        "global_single_use_claimed": False,
        "production_use_allowed": False,
        "feature_computation_allowed": False,
        "model_fitting_allowed": False,
        "promotion_allowed": False,
        "trading_allowed": False,
    }


def _open_child_directory(parent_fd: int, name: str, *, create: bool) -> int:
    try:
        return os.open(name, OPEN_DIRECTORY_FLAGS, dir_fd=parent_fd)
    except FileNotFoundError:
        if not create:
            raise
    except OSError as exc:
        raise _fail("canonical output ancestor cannot be opened safely") from exc
    try:
        os.mkdir(name, mode=0o750, dir_fd=parent_fd)
    except FileExistsError:
        pass
    except OSError as exc:
        raise _fail("canonical output ancestor cannot be created safely") from exc
    try:
        return os.open(name, OPEN_DIRECTORY_FLAGS, dir_fd=parent_fd)
    except OSError as exc:
        raise _fail("canonical output ancestor cannot be reopened safely") from exc


def _open_existing_chain(root_fd: int, parts: Sequence[str]) -> int:
    current = os.dup(root_fd)
    try:
        for part in parts:
            next_fd = os.open(part, OPEN_DIRECTORY_FLAGS, dir_fd=current)
            os.close(current)
            current = next_fd
        return current
    except Exception:
        os.close(current)
        raise


def _create_output_directory(
    run_id: str,
    *,
    data_root_device: int,
    data_root_inode: int,
) -> tuple[int, int, Path]:
    root = _data_root()
    relative = _canonical_output_relative(run_id)
    root_fd = _open_canonical_data_root(data_root_device, data_root_inode)
    parent_fd = os.dup(root_fd)
    try:
        for part in relative.parts[:-1]:
            next_fd = _open_child_directory(parent_fd, part, create=True)
            os.close(parent_fd)
            parent_fd = next_fd
        leaf = relative.parts[-1]
        try:
            os.mkdir(leaf, mode=0o750, dir_fd=parent_fd)
        except FileExistsError as exc:
            raise _fail("experimental output directory must not pre-exist") from exc
        except OSError as exc:
            raise _fail("experimental output directory cannot be created safely") from exc
        output_fd = os.open(leaf, OPEN_DIRECTORY_FLAGS, dir_fd=parent_fd)
        return root_fd, output_fd, root / relative
    except Exception:
        os.close(root_fd)
        raise
    finally:
        os.close(parent_fd)


def _open_output_file(output_fd: int, name: str) -> int:
    try:
        return os.open(name, OPEN_WRITE_FLAGS, 0o640, dir_fd=output_fd)
    except OSError as exc:
        raise _fail(f"runtime artifact cannot be created safely: {name}") from exc


def _write_json_fd(output_fd: int, name: str, payload: Mapping[str, object]) -> None:
    descriptor = _open_output_file(output_fd, name)
    with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
        json.dump(payload, stream, ensure_ascii=False, indent=2, sort_keys=True)
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())


def _write_csv_fd(output_fd: int, name: str, frame: pd.DataFrame) -> None:
    descriptor = _open_output_file(output_fd, name)
    with os.fdopen(descriptor, "w", encoding="utf-8", newline="") as stream:
        frame.to_csv(stream, index=False)
        stream.flush()
        os.fsync(stream.fileno())


def _write_parquet_fd(output_fd: int, name: str, frame: pd.DataFrame) -> None:
    descriptor = _open_output_file(output_fd, name)
    with os.fdopen(descriptor, "wb") as stream:
        frame.to_parquet(stream, index=False)
        stream.flush()
        os.fsync(stream.fileno())


def _verify_output_directory_identity(
    root_fd: int,
    output_fd: int,
    run_id: str,
    *,
    data_root_device: int,
    data_root_inode: int,
) -> None:
    relative = _canonical_output_relative(run_id)
    fresh_root_fd = _open_canonical_data_root(
        data_root_device,
        data_root_inode,
    )
    observed_fd: int | None = None
    try:
        retained_root = os.fstat(root_fd)
        if (retained_root.st_dev, retained_root.st_ino) != (
            data_root_device,
            data_root_inode,
        ):
            raise _fail("retained data root differs from authority evidence")
        current_root = os.fstat(fresh_root_fd)
        if (retained_root.st_dev, retained_root.st_ino) != (
            current_root.st_dev,
            current_root.st_ino,
        ):
            raise _fail("canonical data root changed during artifact creation")
        try:
            observed_fd = _open_existing_chain(fresh_root_fd, relative.parts)
        except OSError as exc:
            raise _fail("canonical output path changed during artifact creation") from exc
        expected = os.fstat(output_fd)
        observed = os.fstat(observed_fd)
        if (expected.st_dev, expected.st_ino) != (observed.st_dev, observed.st_ino):
            raise _fail("canonical output path no longer identifies artifacts")
    finally:
        if observed_fd is not None:
            os.close(observed_fd)
        os.close(fresh_root_fd)


def _write_validation_artifacts_secure(
    output_dir: Path,
    *,
    run_id: str,
    data_root_device: int,
    data_root_inode: int,
    input_identity_verification: Mapping[str, object],
    route_validation: Mapping[str, object],
    license_validation: Mapping[str, object],
    schema_profile: Mapping[str, object],
    pairs: Sequence[base.validation.FutoiDailyPair],
    matrix: pd.DataFrame,
    coverage: pd.DataFrame,
    diagnostics: pd.DataFrame,
    blockers: Mapping[str, object],
    gates: Mapping[str, object],
) -> tuple[str, ...]:
    expected_output = _canonical_output_path(run_id)
    if output_dir.absolute() != expected_output:
        raise _fail("secure artifact writer received a non-canonical output path")
    root_fd, output_fd, created_path = _create_output_directory(
        run_id,
        data_root_device=data_root_device,
        data_root_inode=data_root_inode,
    )
    try:
        if created_path != expected_output:
            raise _fail("secure artifact writer created an unexpected output path")
        json_payloads = {
            "input_identity_verification.json": input_identity_verification,
            "official_route_validation.json": route_validation,
            "futoi_si_license_access_validation.json": license_validation,
            "futoi_si_schema_profile.json": schema_profile,
            "source_blocker_register.json": blockers,
            "gate_results.json": gates,
        }
        for name, payload in json_payloads.items():
            _write_json_fd(output_fd, name, payload)
        daily = pd.DataFrame(
            [pair.as_record() for pair in pairs],
            columns=base.validation.DAILY_POSITIONING_COLUMNS,
        )
        _write_parquet_fd(
            output_fd,
            "futoi_si_daily_positioning.parquet",
            daily,
        )
        _write_parquet_fd(
            output_fd,
            "futoi_si_pit_acceptance_matrix.parquet",
            matrix,
        )
        _write_csv_fd(output_fd, "coverage_by_source.csv", coverage)
        _write_csv_fd(
            output_fd,
            "session_alignment_diagnostics.csv",
            diagnostics,
        )
        names = tuple(sorted(os.listdir(output_fd)))
        if set(names) != set(base.validation.REQUIRED_RUNTIME_ARTIFACTS):
            raise _fail("runtime artifact inventory mismatch")
        os.fsync(output_fd)
        _verify_output_directory_identity(
            root_fd,
            output_fd,
            run_id,
            data_root_device=data_root_device,
            data_root_inode=data_root_inode,
        )
        return names
    finally:
        os.close(output_fd)
        os.close(root_fd)


def _source_error_record(
    *,
    trade_date_value: str | None,
    blocker: str,
    reason: str,
) -> dict[str, object]:
    return {
        "trade_date": trade_date_value,
        "blocker": blocker,
        "reason": reason,
    }


def execute(request: ExperimentalRuntimeRequest) -> dict[str, object]:
    policy_summary = _verify_policy_contract(request)
    authority_summary = _verify_runtime_authority(request)
    base_request = request.base_request
    data_root_device = int(authority_summary["data_root_device"])
    data_root_inode = int(authority_summary["data_root_inode"])

    snapshots = _capture_runtime_inputs(base_request)
    input_hashes = _verify_captured_inputs(snapshots)
    input_hashes["experimental_runtime_policy"] = policy_summary["sha256"]
    input_hashes["runtime_authority_evidence"] = authority_summary[
        "evidence_sha256"
    ]

    modeling = _parquet_snapshot(snapshots["modeling_dataset"])
    validation_predictions = _parquet_snapshot(
        snapshots["m0_validation_predictions"]
    )
    eligible, validation_ids = base._identity_frames(
        modeling,
        validation_predictions,
    )
    license_passed, license_validation = base._license_access_validation(
        _json_snapshot(snapshots["license_access_evidence"])
    )
    pit_evidence = _json_snapshot(snapshots["pit_semantics_evidence"])
    pit_semantics_verified = base._pit_semantics_passed(pit_evidence)

    pairs: list[base.validation.FutoiDailyPair] = []
    schema_columns: tuple[str, ...] | None = None
    source_errors: list[dict[str, object]] = []
    if not license_passed:
        source_errors.append(
            _source_error_record(
                trade_date_value=None,
                blocker="provider_license_and_access_terms_not_documented",
                reason=(
                    "retrieval is authorized only for the exact experimental "
                    "invocation; production use and redistribution remain prohibited"
                ),
            )
        )
    if not pit_semantics_verified:
        source_errors.append(
            _source_error_record(
                trade_date_value=None,
                blocker="historical_pit_revision_semantics_not_proven",
                reason=(
                    "historical measurement may proceed, but G5 and production "
                    "entry remain blocked"
                ),
            )
        )

    _assert_data_root_identity(data_root_device, data_root_inode)
    token: str | None = None
    try:
        token = base.validation.algopack_http.load_algopack_token()
    except base.validation.algopack_http.AlgoPackHttpError as exc:
        source_errors.append(
            _source_error_record(
                trade_date_value=None,
                blocker=exc.transport_outcome,
                reason=str(exc),
            )
        )

    if token is not None:
        for value in sorted(eligible.prior_trade_date.unique()):
            source_date = date.fromisoformat(str(value))
            try:
                pair, columns = base.validation.load_futoi_daily_pair(
                    source_date,
                    bearer_token=token,
                )
            except base.validation.FutoiSiSourceValidationError as exc:
                source_errors.append(
                    _source_error_record(
                        trade_date_value=source_date.isoformat(),
                        blocker=exc.blocker,
                        reason=str(exc),
                    )
                )
                continue
            if schema_columns is None:
                schema_columns = columns
            elif schema_columns != columns:
                source_errors.append(
                    _source_error_record(
                        trade_date_value=source_date.isoformat(),
                        blocker="official_schema_not_stable",
                        reason="FUTOI schema changed across requests",
                    )
                )
                break
            pairs.append(pair)

    matrix, diagnostics = base.validation.build_futoi_pit_acceptance_matrix(
        eligible,
        pairs,
    )
    coverage = base.validation.coverage_by_source(matrix, validation_ids)
    transport_exercised = bool(pairs)
    route_validation = {
        "official_service": base.EXPECTED_LICENSE_PROVIDER,
        "host": base.validation.ALGOPACK_HOST,
        "exact_path": base.validation.FUTOI_PATH,
        "source_ticker": base.validation.SOURCE_TICKER,
        "target_security_id": base.validation.TARGET_SECURITY_ID,
        "storage_family_code": base.validation.STORAGE_FAMILY_CODE,
        "token_environment_variable": "MOEX_ALGOPACK_TOKEN",
        "moex_api_key_alias_allowed": False,
        "redirects_allowed": False,
        "fallback_used": False,
        "one_trade_date_per_request": True,
        "latest": 1,
        "request_attempted": token is not None,
        "successful_request_count": len(pairs),
        "transport_exercised": transport_exercised,
        "route_validated": transport_exercised,
        "runtime_mode": AUTHORITY_MODE,
        "runtime_authorization_id": authority_summary["authorization_id"],
    }
    schema_profile = {
        "required_fields": list(base.validation.RAW_REQUIRED_FIELDS),
        "observed_columns": list(schema_columns or ()),
        "participant_groups": list(base.validation.PARTICIPANT_GROUPS),
        "pair_key": ["trade_date", "moment", "sess_id"],
        "cross_group_seqnum_equality_required": False,
        "canonical_normalizer": (
            "moex_data.futures.futoi_raw_loader.normalize_futoi"
        ),
        "canonical_schema_version": "futures_futoi_5m_raw.v1",
        "daily_pair_count": len(pairs),
        "schema_stable": bool(schema_columns)
        and not any(
            item["blocker"] == "official_schema_not_stable"
            for item in source_errors
        ),
        "pit_evidence": pit_evidence,
        "runtime_mode": AUTHORITY_MODE,
    }
    numerical_integrity = not any(
        item["blocker"] == "numerical_or_chronology_integrity_failure"
        for item in source_errors
    )
    provenance_passed = not any(
        item["blocker"]
        in {"provenance_not_sufficient", "official_route_not_reproducible"}
        for item in source_errors
    )
    gates = base.validation.evaluate_gates(
        immutable_inputs_verified=True,
        eligible_identity_count=len(eligible),
        validation_identity_count=len(validation_ids),
        route_validated=transport_exercised,
        license_access_passed=license_passed,
        schema_stable=bool(schema_profile["schema_stable"]),
        pit_semantics_verified=pit_semantics_verified,
        matrix=matrix,
        coverage=coverage,
        diagnostics=diagnostics,
        numerical_integrity_passed=numerical_integrity,
        provenance_passed=provenance_passed,
    )
    failed_gates = [
        name for name, result in gates.items() if not bool(result["passed"])
    ]
    technical_gates_passed = all(
        bool(gates[name]["passed"]) for name in TECHNICAL_GATES
    )
    final_status = (
        EXPERIMENTAL_STATUS if technical_gates_passed else FAIL_STATUS
    )
    blockers = {
        "project": PROJECT,
        "task_id": TASK_ID,
        "run_id": base_request.run_id,
        "final_status": final_status,
        "failed_gates": failed_gates,
        "technical_gates_passed": technical_gates_passed,
        "experimental_runtime_policy": policy_summary,
        "runtime_authority": authority_summary,
        "blockers": source_errors,
        "historical_model_use_status": (
            "experimental_only" if technical_gates_passed else "blocked"
        ),
    }
    input_verification = {
        "project": PROJECT,
        "task_id": TASK_ID,
        "run_id": base_request.run_id,
        "git_commit_sha": base_request.git_commit_sha,
        "eligible_identity_count": len(eligible),
        "validation_identity_count": len(validation_ids),
        "input_hashes": input_hashes,
        "expected_frozen_sha256": base.EXPECTED_FROZEN_SHA256,
        "expected_parent_contract_git_blob_sha1": base.CONTRACT_GIT_BLOB_SHA1,
        "experimental_runtime_policy": policy_summary,
        "runtime_authority": authority_summary,
        "immutable_inputs_verified": True,
        "input_snapshot_count": len(snapshots),
    }
    artifact_names = _write_validation_artifacts_secure(
        base_request.output_dir,
        run_id=base_request.run_id,
        data_root_device=data_root_device,
        data_root_inode=data_root_inode,
        input_identity_verification=input_verification,
        route_validation=route_validation,
        license_validation=license_validation,
        schema_profile=schema_profile,
        pairs=pairs,
        matrix=matrix,
        coverage=coverage,
        diagnostics=diagnostics,
        blockers=blockers,
        gates={
            "project": PROJECT,
            "task_id": TASK_ID,
            "run_id": base_request.run_id,
            "final_status": final_status,
            "failed_gates": failed_gates,
            "technical_gates_passed": technical_gates_passed,
            "experimental_runtime_policy": policy_summary,
            "runtime_authority": authority_summary,
            "gates": gates,
        },
    )
    return {
        "project": PROJECT,
        "task_id": TASK_ID,
        "run_id": base_request.run_id,
        "output_dir": str(base_request.output_dir),
        "artifact_names": list(artifact_names),
        "artifact_count": len(artifact_names),
        "eligible_identity_count": len(eligible),
        "validation_identity_count": len(validation_ids),
        "daily_pair_count": len(pairs),
        "final_status": final_status,
        "failed_gates": failed_gates,
        "technical_gates_passed": technical_gates_passed,
        "runtime_authorization_id": authority_summary["authorization_id"],
        "production_use_allowed": False,
    }


def main(argv: list[str] | None = None) -> int:
    request = request_from_args(build_argument_parser().parse_args(argv))
    result = execute(request)
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
