from __future__ import annotations

import argparse
import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Final, Sequence

from moex_data.futures.freeze_step7_accepted_raw_5m import freeze_accepted_quote_history
from moex_data.step7_rub_native_d1_w1_materializer import materialize_instrument

CANONICAL_ENV_PATH: Final[str] = "/home/trader/moex_bot/.env"
HISTORY: Final[dict[str, tuple[str, str, int]]] = {
    "usdrubf_futures_family": ("2022-04-26", "2026-08-17", 1100),
    "cnyrubf_futures_family": ("2022-04-26", "2026-08-17", 1100),
}


class Step7PilotError(ValueError):
    pass


def _fail(message: str) -> None:
    raise Step7PilotError(message)


def _safe_token(value: object, field: str) -> str:
    text = str(value or "").strip()
    if not text or text in {".", ".."} or "/" in text or "\\" in text or any(x in text for x in ("*", "{", "}", "$(", "`")):
        _fail(field + " must be explicit safe token")
    return text


def load_env_file(path: str | None) -> None:
    if not path:
        return
    env_path = Path(path)
    if not env_path.is_file():
        _fail("env_file does not exist")
    for raw in env_path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def _data_root() -> Path:
    text = str(os.environ.get("MOEX_DATA_ROOT", "")).strip()
    if not text:
        _fail("MOEX_DATA_ROOT is required")
    root = Path(text)
    if not root.is_absolute():
        _fail("MOEX_DATA_ROOT must be absolute")
    return root.resolve()


def _atomic_json(path: Path, values: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=path.parent, delete=False, suffix=".tmp") as handle:
        json.dump(values, handle, ensure_ascii=False, indent=2, sort_keys=True, default=str)
        handle.write("\n")
        temp = Path(handle.name)
    temp.replace(path)


def run_pilot(*, artifact_version: str, repo_root: str | Path = ".", env_file: str | None = CANONICAL_ENV_PATH) -> dict[str, object]:
    load_env_file(env_file)
    run_id = _safe_token(artifact_version, "artifact_version")
    root = _data_root()
    repo = Path(repo_root).resolve()
    if not (repo / "configs" / "datasets" / "futures_data_lake.v1.yaml").is_file():
        _fail("repo_root is not canonical MOEX Bot repository checkout")
    run_root = root / "runs" / "step7_rub_native_d1_w1" / ("run_id=" + run_id)
    evidence_dir = root / "state" / "acceptance" / "step7_rub_native_d1_w1" / ("run_id=" + run_id)
    if run_root.exists() or evidence_dir.exists():
        _fail("immutable Stage 7 run_id already exists")

    frozen_inputs: list[dict[str, object]] = []
    outputs: list[dict[str, object]] = []
    for instrument_id, (start_date, end_date, expected_d1_rows) in HISTORY.items():
        frozen = freeze_accepted_quote_history(
            repo_root=repo,
            data_root=root,
            run_root=run_root,
            instrument_id=instrument_id,
            start_date=start_date,
            end_date=end_date,
            run_id=run_id + "_" + instrument_id,
        )
        if int(frozen["partition_count"]) != expected_d1_rows:
            _fail("frozen accepted raw partition count mismatch for " + instrument_id)
        frozen_inputs.append(frozen)
        materialized = materialize_instrument(
            data_root=root,
            run_root=run_root,
            frozen_manifest_path=frozen["manifest_path"],
            instrument_id=instrument_id,
            history_start=start_date,
            history_end=end_date,
            run_id=run_id,
        )
        d1_rows = [row for row in materialized if row["dataset_id"] == "rub_native_ohlcv_htf" and row["timeframe"] == "1D"]
        if len(d1_rows) != 1 or int(d1_rows[0]["row_count"]) != expected_d1_rows:
            _fail("D1 row count mismatch for " + instrument_id)
        if len(materialized) != 4:
            _fail("expected four Stage 7 outputs per instrument")
        outputs.extend(materialized)

    if len(outputs) != 8:
        _fail("Stage 7 pilot output count must be eight")
    evidence: dict[str, object] = {
        "project": "MOEX_Bot",
        "step": 7,
        "status": "pilot_passed",
        "artifact_version": run_id,
        "run_id": run_id,
        "run_root": run_root.as_posix(),
        "run_artifacts_immutable": True,
        "run_id_reuse_allowed": False,
        "network_calls_used": False,
        "latest_autodetect_used": False,
        "continuous_series_used": False,
        "accepted_raw_history_required": True,
        "mutable_canonical_raw_read_after_freeze_allowed": False,
        "si_cr_continuous_ready": False,
        "weekly_oi_ready": False,
        "advanced_technical_policy_ready": False,
        "research_ready": False,
        "frozen_inputs": frozen_inputs,
        "outputs": outputs,
        "counts": {
            "frozen_input_manifests": len(frozen_inputs),
            "outputs": len(outputs),
            "expected_accepted_pointers": 8,
        },
        "created_at_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
    }
    evidence_path = evidence_dir / "pilot_evidence.json"
    _atomic_json(evidence_path, evidence)
    evidence["evidence_path"] = evidence_path.as_posix()
    return evidence


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run immutable Stage 7 native RUB D1/W1 technical pilot.")
    parser.add_argument("--artifact-version", required=True)
    parser.add_argument("--repo-root", default=".")
    parser.add_argument("--env-file", default=CANONICAL_ENV_PATH)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        result = run_pilot(artifact_version=args.artifact_version, repo_root=args.repo_root, env_file=args.env_file)
    except Exception as exc:
        print(json.dumps({"project": "MOEX_Bot", "step": 7, "status": "pilot_failed", "error": str(exc)}, ensure_ascii=False))
        return 1
    print(json.dumps(result, ensure_ascii=False, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
