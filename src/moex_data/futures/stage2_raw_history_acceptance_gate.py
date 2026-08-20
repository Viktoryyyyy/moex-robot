from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Sequence

from . import accepted_manifest
from . import stage2_raw_history_acceptance as acceptance


def _accepted_pointer_ref(repo_root: Path, contract_path: str, instrument_id: str) -> str:
    path = repo_root / contract_path
    if not path.exists() or not path.is_file():
        raise acceptance.RawHistoryAcceptanceError("target dataset contract does not exist")
    values: list[str] = []
    prefix = "accepted_pointer_path_contract:"
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if line.startswith(prefix):
            value = line[len(prefix) :].strip().strip('"').strip("'")
            if value:
                values.append(value)
    if len(values) != 1:
        raise acceptance.RawHistoryAcceptanceError(
            "target dataset contract must declare exactly one accepted_pointer_path_contract"
        )
    pointer_ref = values[0].replace("{INSTRUMENT_ID}", instrument_id)
    unresolved = pointer_ref.removeprefix("${MOEX_DATA_ROOT}/")
    if "{" in unresolved or "}" in unresolved:
        raise acceptance.RawHistoryAcceptanceError(
            "accepted pointer contract contains unresolved placeholders"
        )
    return pointer_ref


def _pointer_path(repo_root: Path, target_dataset_id: str, instrument_id: str) -> Path:
    if target_dataset_id == acceptance.QUOTE_DATASET_ID:
        contract_path = acceptance.QUOTE_CONTRACT_PATH
    elif target_dataset_id == acceptance.FUTOI_DATASET_ID:
        contract_path = acceptance.FUTOI_CONTRACT_PATH
    else:
        raise acceptance.RawHistoryAcceptanceError(
            "target_dataset_id is not part of Stage 2 raw history acceptance scope"
        )
    pointer_ref = _accepted_pointer_ref(repo_root, contract_path, instrument_id)
    try:
        return accepted_manifest._pointer_path(
            None, pointer_ref, target_dataset_id, instrument_id
        )
    except accepted_manifest.FuturesAcceptedManifestError as exc:
        raise acceptance.RawHistoryAcceptanceError(str(exc)) from exc


def run_gate(
    *,
    repo_root: str | Path,
    target_dataset_id: str,
    instrument_id: str,
    run_id: str,
) -> dict[str, object]:
    root = Path(repo_root)
    checked_dataset = acceptance._require_token(target_dataset_id, "target_dataset_id")
    checked_instrument = acceptance._require_token(instrument_id, "instrument_id")
    checked_run_id = acceptance._require_token(run_id, "run_id")

    pointer = _pointer_path(root, checked_dataset, checked_instrument)
    if pointer.exists():
        raise acceptance.RawHistoryAcceptanceError(
            "preexisting canonical accepted pointer must be absent before raw history acceptance"
        )

    report_path = acceptance.acceptance_report_path(
        repo_root=root,
        target_dataset_id=checked_dataset,
        instrument_id=checked_instrument,
        run_id=checked_run_id,
    )
    if report_path.exists():
        raise acceptance.RawHistoryAcceptanceError(
            "acceptance report already exists for explicit run_id"
        )

    result = acceptance.audit_history(
        repo_root=root,
        target_dataset_id=checked_dataset,
        instrument_id=checked_instrument,
        run_id=checked_run_id,
    )
    if Path(str(result["acceptance_report_reference"])) != report_path:
        raise acceptance.RawHistoryAcceptanceError(
            "acceptance report path changed during deterministic gate evaluation"
        )
    result["preexisting_accepted_pointer_present"] = False
    result["accepted_pointer_path_checked"] = pointer.as_posix()
    result["evidence_written"] = True
    acceptance._write_json_immutable(report_path, result)
    return result


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Canonical fail-closed Stage 2 raw history acceptance gate."
    )
    parser.add_argument(
        "--target-dataset-id",
        required=True,
        choices=(acceptance.QUOTE_DATASET_ID, acceptance.FUTOI_DATASET_ID),
    )
    parser.add_argument("--instrument-id", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--repo-root", default=Path.cwd().as_posix())
    parser.add_argument("--env-file", default=None)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        acceptance.quote_materializer.load_env_file(args.env_file)
        payload = run_gate(
            repo_root=args.repo_root,
            target_dataset_id=args.target_dataset_id,
            instrument_id=args.instrument_id,
            run_id=args.run_id,
        )
    except Exception as exc:
        print(
            json.dumps(
                {
                    "status": "failed",
                    "dataset_id": acceptance.ACCEPTANCE_DATASET_ID,
                    "error": str(exc),
                    "network_access_used": False,
                    "historical_backfill_used": False,
                    "accepted_pointer_written": False,
                    "evidence_written": False,
                },
                ensure_ascii=False,
                sort_keys=True,
            )
        )
        return 1
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str))
    return 0 if payload["acceptance_status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
