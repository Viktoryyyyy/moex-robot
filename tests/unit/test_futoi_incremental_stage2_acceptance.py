from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pandas as pd
import pytest

from moex_data.futures import futoi_incremental_stage2_acceptance as subject


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _write_json(path: Path, values: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(values, ensure_ascii=False, sort_keys=True) + "\n", encoding="utf-8")


@pytest.fixture
def acceptance_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    root = tmp_path / "data"
    root.mkdir()
    repo = tmp_path / "repo"
    (repo / "configs" / "instruments").mkdir(parents=True)
    (repo / subject.REGISTRY_PATH).write_text("instruments: []\n", encoding="utf-8")
    monkeypatch.setenv("MOEX_DATA_ROOT", root.as_posix())

    base_dir = root / "state" / "accepted_manifests" / "raw_history_content_attestation" / "generation_id=base_v1"
    marker = base_dir / "current.json"
    manifest = base_dir / "manifest.json"
    _write_json(manifest, {"generation_id": "base_v1", "status": "pass"})
    _write_json(marker, {"generation_id": "base_v1", "manifest_ref": subject.ROOT_PREFIX + manifest.relative_to(root).as_posix()})

    base = {
        "generation_id": "base_v1",
        "marker_path": marker.as_posix(),
        "marker_sha256": _sha256(marker),
        "manifest_path": manifest.as_posix(),
        "manifest_sha256": _sha256(manifest),
        "accepted_dates": ["2026-08-15", "2026-08-17"],
    }

    historical_pointer = (
        root
        / "state"
        / "datasets"
        / "dataset_id=futures_futoi_raw"
        / "instrument_id=si_futures_family"
        / "current_accepted_manifest.json"
    )
    _write_json(historical_pointer, {"sentinel": "historical-must-not-change"})
    historical_bytes = historical_pointer.read_bytes()

    def base_resolver(**_kwargs):
        return dict(base)

    return root, repo, base, base_resolver, historical_pointer, historical_bytes


def _calendar_fetcher(start: str, end: str, **_kwargs):
    current = pd.Timestamp(start)
    last = pd.Timestamp(end)
    rows = []
    while current <= last:
        rows.append({"trade_date": current.date().isoformat(), "is_trading_day": True})
        current += pd.Timedelta(days=1)
    return rows


def _materializer_factory(root: Path, *, groups=("FIZ", "YUR")):
    calls: list[str] = []

    def materializer(*, trade_date: str, instrument_id: str, run_id: str, **_kwargs):
        calls.append(trade_date)
        partition = (
            root
            / "market"
            / "supplementary"
            / "dataset_id=futures_futoi_raw"
            / ("instrument_id=" + instrument_id)
            / ("trade_date=" + trade_date)
            / "source=moex_algopack_futoi"
            / "part.parquet"
        )
        partition.parent.mkdir(parents=True, exist_ok=True)
        rows = []
        for offset, group in enumerate(groups):
            rows.append(
                {
                    "instrument_id": instrument_id,
                    "source_id": subject.SOURCE_ID,
                    "trade_date": trade_date,
                    "secid": "SiU6",
                    "clgroup": group,
                    "sess_id": 3,
                    "seqnum": 100 + offset,
                    "ts": trade_date + " 23:50:00",
                    "systime": trade_date + " 23:50:07",
                    "availability_ts_utc": trade_date + "T21:00:00+00:00",
                    "pos": 10 if group == "FIZ" else -10,
                    "pos_long": 20 if group == "FIZ" else 10,
                    "pos_short": -10 if group == "FIZ" else -20,
                    "pos_long_num": 2,
                    "pos_short_num": 2,
                }
            )
        pd.DataFrame(rows).to_parquet(partition, index=False)

        quality = (
            root
            / "state"
            / "quality"
            / "dataset_id=futures_futoi_raw"
            / ("run_date=" + trade_date)
            / ("run_id=" + run_id)
            / "quality_report.json"
        )
        manifest = (
            root
            / "state"
            / "refresh"
            / "dataset_id=futures_futoi_raw"
            / ("run_date=" + trade_date)
            / ("run_id=" + run_id)
            / "manifest.json"
        )
        _write_json(
            quality,
            {
                "quality_status": "pass",
                "row_count": len(rows),
                "duplicate_key_count": 0,
                "invalid_position_count": 0,
                "null_required_count": 0,
            },
        )
        _write_json(manifest, {"refresh_status": "succeeded"})
        return {
            "status": "succeeded",
            "dataset_id": subject.TARGET_DATASET_ID,
            "instrument_id": instrument_id,
            "source_id": subject.SOURCE_ID,
            "trade_date": trade_date,
            "row_count": len(rows),
            "quality_status": "pass",
            "latest_autodetect_used": False,
            "storage_partition_path": partition.as_posix(),
            "quality_report_reference": quality.as_posix(),
            "manifest_reference": manifest.as_posix(),
        }

    return materializer, calls


def test_accepts_incremental_overlay_without_replacing_historical_pointer(acceptance_env):
    root, repo, _base, base_resolver, historical_pointer, historical_bytes = acceptance_env
    materializer, calls = _materializer_factory(root)

    result = subject.accept_incremental(
        instrument_id="si_futures_family",
        through_date="2026-08-20",
        run_id="incremental_v1",
        repo_root=repo,
        env_file=None,
        base_resolver=base_resolver,
        calendar_fetcher=_calendar_fetcher,
        partition_materializer=materializer,
    )

    assert result["status"] == "accepted"
    assert result["requested_trade_dates"] == ["2026-08-18", "2026-08-19", "2026-08-20"]
    assert calls == result["requested_trade_dates"]
    assert result["accepted_pointer_written"] is True
    assert result["historical_pointer_replaced"] is False
    assert historical_pointer.read_bytes() == historical_bytes

    pointer = subject._load_json(subject._pointer_path("si_futures_family"), "pointer")
    assert pointer["base_generation_id"] == "base_v1"
    assert pointer["last_incremental_trade_date"] == "2026-08-20"
    manifest = subject._load_json(subject._expand_root_ref(pointer["manifest_ref"], "manifest_ref"), "manifest")
    assert [row["trade_date"] for row in manifest["records"]] == result["requested_trade_dates"]
    for row in manifest["records"]:
        frozen = subject._expand_root_ref(row["frozen_partition_ref"], "frozen_partition_ref")
        canonical = subject._expand_root_ref(row["canonical_partition_ref"], "canonical_partition_ref")
        assert _sha256(frozen) == row["frozen_sha256"]
        assert _sha256(canonical) == row["canonical_sha256_at_acceptance"]
        assert frozen.stat().st_ino != canonical.stat().st_ino


def test_missing_fiz_yur_pair_fails_closed_without_pointer(acceptance_env):
    root, repo, _base, base_resolver, historical_pointer, historical_bytes = acceptance_env
    materializer, _calls = _materializer_factory(root, groups=("FIZ",))

    with pytest.raises(subject.FutoiIncrementalAcceptanceError, match="exactly FIZ and YUR"):
        subject.accept_incremental(
            instrument_id="si_futures_family",
            through_date="2026-08-18",
            run_id="incremental_bad_groups",
            repo_root=repo,
            env_file=None,
            base_resolver=base_resolver,
            calendar_fetcher=_calendar_fetcher,
            partition_materializer=materializer,
        )

    assert not subject._pointer_path("si_futures_family").exists()
    assert historical_pointer.read_bytes() == historical_bytes


def test_second_generation_extends_previous_incremental_records(acceptance_env):
    root, repo, _base, base_resolver, _historical_pointer, _historical_bytes = acceptance_env
    materializer, calls = _materializer_factory(root)

    first = subject.accept_incremental(
        instrument_id="si_futures_family",
        through_date="2026-08-18",
        run_id="incremental_first",
        repo_root=repo,
        env_file=None,
        base_resolver=base_resolver,
        calendar_fetcher=_calendar_fetcher,
        partition_materializer=materializer,
    )
    second = subject.accept_incremental(
        instrument_id="si_futures_family",
        through_date="2026-08-20",
        run_id="incremental_second",
        repo_root=repo,
        env_file=None,
        base_resolver=base_resolver,
        calendar_fetcher=_calendar_fetcher,
        partition_materializer=materializer,
    )

    assert first["requested_trade_dates"] == ["2026-08-18"]
    assert second["requested_trade_dates"] == ["2026-08-19", "2026-08-20"]
    assert calls == ["2026-08-18", "2026-08-19", "2026-08-20"]
    pointer = subject._load_json(subject._pointer_path("si_futures_family"), "pointer")
    manifest = subject._load_json(subject._expand_root_ref(pointer["manifest_ref"], "manifest_ref"), "manifest")
    assert [row["trade_date"] for row in manifest["records"]] == [
        "2026-08-18",
        "2026-08-19",
        "2026-08-20",
    ]
    assert manifest["previous_incremental_manifest_ref"] == first["manifest_ref"]


def test_base_generation_change_before_promotion_fails_closed(acceptance_env):
    root, repo, base, _base_resolver, historical_pointer, historical_bytes = acceptance_env
    materializer, _calls = _materializer_factory(root)
    calls = 0

    def changing_base_resolver(**_kwargs):
        nonlocal calls
        calls += 1
        values = dict(base)
        if calls >= 2:
            values["generation_id"] = "base_v2"
        return values

    with pytest.raises(subject.FutoiIncrementalAcceptanceError, match="base content-attestation generation changed"):
        subject.accept_incremental(
            instrument_id="si_futures_family",
            through_date="2026-08-18",
            run_id="incremental_base_changed",
            repo_root=repo,
            env_file=None,
            base_resolver=changing_base_resolver,
            calendar_fetcher=_calendar_fetcher,
            partition_materializer=materializer,
        )

    assert not subject._pointer_path("si_futures_family").exists()
    assert historical_pointer.read_bytes() == historical_bytes
