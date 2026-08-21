from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from moex_data.futures import accepted_manifest
from moex_data.futures import stage2_raw_history_promotion as promotion


DATASET_ID = "futures_raw_5m"
INSTRUMENT_ID = "usdrubf_futures_family"
RUN_ID = "stage2_accept_usdrubf_20260821_v1"


def _write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _repo(tmp_path: Path) -> Path:
    repo = tmp_path / "repo"
    _write(
        repo / "contracts/datasets/futures_raw_history_acceptance.v1.yaml",
        "\n".join(
            (
                "dataset_id: futures_raw_history_acceptance",
                'path_pattern: "${MOEX_DATA_ROOT}/state/acceptance/target_dataset_id={TARGET_DATASET_ID}/instrument_id={INSTRUMENT_ID}/run_id={RUN_ID}/acceptance_report.json"',
                "",
            )
        ),
    )
    _write(
        repo / "contracts/datasets/futures_raw_history_accepted_manifest.v1.yaml",
        "\n".join(
            (
                "dataset_id: futures_raw_history_accepted_manifest",
                "schema_version: futures_raw_history_accepted_manifest.v1",
                'path_pattern: "${MOEX_DATA_ROOT}/state/accepted_manifests/target_dataset_id={TARGET_DATASET_ID}/instrument_id={INSTRUMENT_ID}/acceptance_run_id={ACCEPTANCE_RUN_ID}/accepted_manifest.json"',
                "",
            )
        ),
    )
    _write(
        repo / "contracts/datasets/futures_raw_5m.v1.yaml",
        "\n".join(
            (
                "dataset_id: futures_raw_5m",
                'accepted_pointer_path_contract: "${MOEX_DATA_ROOT}/state/datasets/dataset_id=futures_raw_5m/instrument_id={INSTRUMENT_ID}/current_accepted_manifest.json"',
                "",
            )
        ),
    )
    return repo


def _report_values(**overrides: object) -> dict[str, object]:
    values: dict[str, object] = {
        "dataset_id": "futures_raw_history_acceptance",
        "target_dataset_id": DATASET_ID,
        "instrument_id": INSTRUMENT_ID,
        "run_id": RUN_ID,
        "acceptance_status": "pass",
        "evidence_written": True,
        "accepted_pointer_written": False,
        "preexisting_accepted_pointer_present": False,
        "network_access_used": False,
        "historical_backfill_used": False,
        "implicit_partition_discovery_used": False,
        "latest_autodetect_used": False,
        "failed_partition_dates": [],
        "hard_check_failures": [],
        "expected_partition_count": 2,
        "actual_partition_count": 2,
        "expected_row_count": 3,
        "actual_row_count": 3,
        "expected_partition_dates_sha256": "a" * 64,
        "actual_partition_dates_sha256": "a" * 64,
        "expected_missing_dates_sha256": "b" * 64,
        "actual_missing_dates_sha256": "b" * 64,
        "expected_calendar_missing_partition_count": 1,
        "actual_calendar_missing_partition_count": 1,
        "missing_partition_dates": ["2022-04-30"],
        "source_id": "moex_algopack_fo_tradestats_5m",
        "secid_scope": ["USDRUBF"],
        "requested_from": "2022-04-26",
        "requested_till": "2022-04-30",
    }
    values.update(overrides)
    return values


def _report_path(data_root: Path) -> Path:
    return (
        data_root
        / "state/acceptance"
        / ("target_dataset_id=" + DATASET_ID)
        / ("instrument_id=" + INSTRUMENT_ID)
        / ("run_id=" + RUN_ID)
        / "acceptance_report.json"
    )


def _pointer_path(data_root: Path) -> Path:
    return (
        data_root
        / "state/datasets"
        / ("dataset_id=" + DATASET_ID)
        / ("instrument_id=" + INSTRUMENT_ID)
        / "current_accepted_manifest.json"
    )


def _write_report(data_root: Path, values: dict[str, object]) -> Path:
    path = _report_path(data_root)
    values = dict(values)
    values["producer"] = promotion.ACCEPTANCE_PRODUCER
    values["acceptance_contract_ref"] = (
        "contracts/datasets/futures_raw_history_acceptance.v1.yaml"
    )
    values["acceptance_report_reference"] = path.as_posix()
    values["accepted_pointer_path_checked"] = _pointer_path(data_root).as_posix()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(values, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return path


def _pointer_ref() -> str:
    return (
        "${MOEX_DATA_ROOT}/state/datasets/dataset_id="
        + DATASET_ID
        + "/instrument_id="
        + INSTRUMENT_ID
        + "/current_accepted_manifest.json"
    )


def test_pass_acceptance_promotes_immutable_manifest_and_pointer(tmp_path, monkeypatch) -> None:
    repo = _repo(tmp_path)
    data_root = tmp_path / "data"
    monkeypatch.setenv("MOEX_DATA_ROOT", str(data_root))
    report_path = _write_report(data_root, _report_values())

    result = promotion.promote_history(
        repo_root=repo,
        target_dataset_id=DATASET_ID,
        instrument_id=INSTRUMENT_ID,
        acceptance_run_id=RUN_ID,
    )

    assert result["status"] == "promoted"
    assert result["accepted_pointer_written"] is True
    assert result["acceptance_report_sha256"] == hashlib.sha256(
        report_path.read_bytes()
    ).hexdigest()

    manifest_path = promotion.accepted_manifest_path(
        repo_root=repo,
        target_dataset_id=DATASET_ID,
        instrument_id=INSTRUMENT_ID,
        acceptance_run_id=RUN_ID,
    )
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest["dataset_id"] == promotion.PROMOTION_DATASET_ID
    assert manifest["target_dataset_id"] == DATASET_ID
    assert manifest["acceptance_status"] == "pass"
    assert manifest["partition_count"] == 2
    assert manifest["row_count"] == 3
    assert manifest["missing_partition_dates"] == ["2022-04-30"]
    assert manifest["acceptance_report_sha256"] == result["acceptance_report_sha256"]

    pointer = json.loads(_pointer_path(data_root).read_text(encoding="utf-8"))
    assert pointer["promotion_basis"] == "raw_history_acceptance"
    assert pointer["quality_report_ref"] == manifest["acceptance_report_ref"]
    assert pointer["acceptance_report_ref"] == manifest["acceptance_report_ref"]
    assert pointer["manifest_ref"] == result["accepted_manifest_ref"]

    compatible = accepted_manifest.read_accepted_manifest_pointer(
        env={"MOEX_DATA_ROOT": str(data_root)},
        dataset_id=DATASET_ID,
        instrument_id=INSTRUMENT_ID,
        accepted_manifest_ref=_pointer_ref(),
    )
    assert compatible.run_id == RUN_ID
    assert compatible.manifest_ref == result["accepted_manifest_ref"]


def test_fail_acceptance_cannot_be_promoted(tmp_path, monkeypatch) -> None:
    repo = _repo(tmp_path)
    data_root = tmp_path / "data"
    monkeypatch.setenv("MOEX_DATA_ROOT", str(data_root))
    _write_report(data_root, _report_values(acceptance_status="fail"))

    with pytest.raises(promotion.RawHistoryPromotionError, match="acceptance_status"):
        promotion.promote_history(
            repo_root=repo,
            target_dataset_id=DATASET_ID,
            instrument_id=INSTRUMENT_ID,
            acceptance_run_id=RUN_ID,
        )

    assert not _pointer_path(data_root).exists()


def test_digest_or_count_mismatch_cannot_be_promoted(tmp_path, monkeypatch) -> None:
    repo = _repo(tmp_path)
    data_root = tmp_path / "data"
    monkeypatch.setenv("MOEX_DATA_ROOT", str(data_root))
    _write_report(data_root, _report_values(actual_partition_count=1))

    with pytest.raises(promotion.RawHistoryPromotionError, match="partition_count"):
        promotion.promote_history(
            repo_root=repo,
            target_dataset_id=DATASET_ID,
            instrument_id=INSTRUMENT_ID,
            acceptance_run_id=RUN_ID,
        )

    assert not _pointer_path(data_root).exists()


def test_preexisting_pointer_blocks_promotion(tmp_path, monkeypatch) -> None:
    repo = _repo(tmp_path)
    data_root = tmp_path / "data"
    monkeypatch.setenv("MOEX_DATA_ROOT", str(data_root))
    _write_report(data_root, _report_values())
    pointer = _pointer_path(data_root)
    pointer.parent.mkdir(parents=True, exist_ok=True)
    pointer.write_text("{}\n", encoding="utf-8")

    with pytest.raises(promotion.RawHistoryPromotionError, match="already exists"):
        promotion.promote_history(
            repo_root=repo,
            target_dataset_id=DATASET_ID,
            instrument_id=INSTRUMENT_ID,
            acceptance_run_id=RUN_ID,
        )


def test_conflicting_precreated_manifest_fails_without_pointer(tmp_path, monkeypatch) -> None:
    repo = _repo(tmp_path)
    data_root = tmp_path / "data"
    monkeypatch.setenv("MOEX_DATA_ROOT", str(data_root))
    _write_report(data_root, _report_values())
    manifest_path = promotion.accepted_manifest_path(
        repo_root=repo,
        target_dataset_id=DATASET_ID,
        instrument_id=INSTRUMENT_ID,
        acceptance_run_id=RUN_ID,
    )
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text('{"corrupt": true}\n', encoding="utf-8")

    with pytest.raises(promotion.RawHistoryPromotionError, match="conflicting content"):
        promotion.promote_history(
            repo_root=repo,
            target_dataset_id=DATASET_ID,
            instrument_id=INSTRUMENT_ID,
            acceptance_run_id=RUN_ID,
        )

    assert not _pointer_path(data_root).exists()


def test_identical_manifest_allows_recovery_before_pointer_creation(tmp_path, monkeypatch) -> None:
    repo = _repo(tmp_path)
    data_root = tmp_path / "data"
    monkeypatch.setenv("MOEX_DATA_ROOT", str(data_root))
    _write_report(data_root, _report_values())

    original_writer = promotion._write_json_create_only
    calls = {"count": 0}

    def interrupted(path, values, *, allow_identical_existing):
        calls["count"] += 1
        original_writer(
            path,
            values,
            allow_identical_existing=allow_identical_existing,
        )
        if calls["count"] == 1:
            raise RuntimeError("simulated interruption after manifest write")

    monkeypatch.setattr(promotion, "_write_json_create_only", interrupted)
    with pytest.raises(RuntimeError, match="simulated interruption"):
        promotion.promote_history(
            repo_root=repo,
            target_dataset_id=DATASET_ID,
            instrument_id=INSTRUMENT_ID,
            acceptance_run_id=RUN_ID,
        )
    assert not _pointer_path(data_root).exists()

    monkeypatch.setattr(promotion, "_write_json_create_only", original_writer)
    result = promotion.promote_history(
        repo_root=repo,
        target_dataset_id=DATASET_ID,
        instrument_id=INSTRUMENT_ID,
        acceptance_run_id=RUN_ID,
    )
    assert result["status"] == "promoted"
    assert _pointer_path(data_root).is_file()


def test_report_pointer_binding_mismatch_fails_closed(tmp_path, monkeypatch) -> None:
    repo = _repo(tmp_path)
    data_root = tmp_path / "data"
    monkeypatch.setenv("MOEX_DATA_ROOT", str(data_root))
    values = _report_values()
    path = _write_report(data_root, values)
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["accepted_pointer_path_checked"] = (data_root / "wrong.json").as_posix()
    path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")

    with pytest.raises(promotion.RawHistoryPromotionError, match="pointer path checked"):
        promotion.promote_history(
            repo_root=repo,
            target_dataset_id=DATASET_ID,
            instrument_id=INSTRUMENT_ID,
            acceptance_run_id=RUN_ID,
        )
    assert not _pointer_path(data_root).exists()
