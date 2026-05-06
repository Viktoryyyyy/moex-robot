import csv
import json
from pathlib import Path

from moex_data.futures.limited_controlled_batch_classifier import classify


def test_limited_controlled_batch_classifier(tmp_path, monkeypatch):
    data_root = tmp_path / "data"
    candidate_dir = data_root / "futures" / "rfud_candidates" / "snapshot_date=2026-05-06"
    candidate_dir.mkdir(parents=True)

    rows = [
        {
            "family": "W",
            "raw_futoi_status": "complete",
            "liquidity_history_status": "review_required",
        },
        {
            "family": "MM",
            "raw_futoi_status": "complete",
            "liquidity_history_status": "review_required",
        },
        {
            "family": "MX",
            "raw_futoi_status": "complete",
            "liquidity_history_status": "review_required",
        },
        {
            "family": "CR",
            "raw_futoi_status": "complete",
            "liquidity_history_status": "accepted",
        },
        {
            "family": "SiH7",
            "raw_futoi_status": "complete",
            "liquidity_history_status": "review_required",
        },
    ]

    csv_path = candidate_dir / "rfud_candidates.csv"
    with csv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["family", "raw_futoi_status", "liquidity_history_status"],
        )
        writer.writeheader()
        writer.writerows(rows)

    monkeypatch.setenv("MOEX_DATA_ROOT", str(data_root))

    repo_root = Path(__file__).resolve().parents[4]
    config_dir = repo_root / "configs" / "datasets"
    config_dir.mkdir(parents=True, exist_ok=True)

    config_path = config_dir / "futures_limited_controlled_batch_config.json"
    if not config_path.exists():
        config_path.write_text(
            json.dumps(
                {
                    "controlled_batch_id": "test_batch",
                    "evidence": {
                        "input_patterns": [
                            "futures/rfud_candidates/snapshot_date={snapshot_date}/rfud_candidates.csv"
                        ]
                    },
                    "output": {
                        "path_pattern": "futures/out/{snapshot_date}/classification.csv",
                        "summary_path_pattern": "futures/out/{snapshot_date}/summary.json",
                    },
                }
            ),
            encoding="utf-8",
        )

    result_rows, metadata = classify("2026-05-06")

    assert len(result_rows) == 3
    assert [row["family"] for row in result_rows] == ["MM", "MX", "W"]
    assert all(row["classification_status"] == "controlled_provisional" for row in result_rows)
    assert all(row["continuous_eligibility_status"] == "not_accepted" for row in result_rows)
    assert "CR" not in metadata["summary"]["families"]
    assert "SiH7" not in metadata["summary"]["families"]
