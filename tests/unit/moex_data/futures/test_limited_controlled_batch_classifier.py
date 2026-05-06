import pandas as pd

from moex_data.futures.limited_controlled_batch_classifier import classify


def _write_parquet(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_parquet(path, index=False)


def test_limited_controlled_batch_classifier(tmp_path, monkeypatch):
    data_root = tmp_path / "data"
    snapshot_dir = "snapshot_date=2026-05-06"

    registry_rows = [
        {"family_code": "W", "secid": "W1", "board": "rfud"},
        {"family_code": "MM", "secid": "MM1", "board": "rfud"},
        {"family_code": "MX", "secid": "MX1", "board": "rfud"},
        {"family_code": "CR", "secid": "CR1", "board": "rfud"},
        {"family_code": "SiH7", "secid": "SiH7", "board": "rfud"},
    ]
    availability_rows = [
        {"secid": "W1", "board": "rfud", "availability_status": "available"},
        {"secid": "MM1", "board": "rfud", "availability_status": "available"},
        {"secid": "MX1", "board": "rfud", "availability_status": "available"},
        {"secid": "CR1", "board": "rfud", "availability_status": "available"},
        {"secid": "SiH7", "board": "rfud", "availability_status": "available"},
    ]
    liquidity_rows = [
        {"secid": "W1", "board": "rfud", "liquidity_status": "review_required"},
        {"secid": "MM1", "board": "rfud", "liquidity_status": "review_required"},
        {"secid": "MX1", "board": "rfud", "liquidity_status": "review_required"},
        {"secid": "CR1", "board": "rfud", "liquidity_status": "pass"},
        {"secid": "SiH7", "board": "rfud", "liquidity_status": "review_required"},
    ]
    history_rows = [
        {"secid": "W1", "board": "rfud", "history_depth_status": "review_required"},
        {"secid": "MM1", "board": "rfud", "history_depth_status": "review_required"},
        {"secid": "MX1", "board": "rfud", "history_depth_status": "review_required"},
        {"secid": "CR1", "board": "rfud", "history_depth_status": "pass"},
        {"secid": "SiH7", "board": "rfud", "history_depth_status": "review_required"},
    ]

    _write_parquet(
        data_root / "futures" / "registry" / "universe_scope=rfud_candidates" / snapshot_dir / "futures_normalized_instrument_registry.parquet",
        registry_rows,
    )
    _write_parquet(
        data_root / "futures" / "availability" / "universe_scope=rfud_candidates" / snapshot_dir / "futures_algopack_tradestats_availability_report.parquet",
        availability_rows,
    )
    _write_parquet(
        data_root / "futures" / "availability" / "universe_scope=rfud_candidates" / snapshot_dir / "futures_futoi_availability_report.parquet",
        availability_rows,
    )
    _write_parquet(
        data_root / "futures" / "screens" / "liquidity" / "universe_scope=rfud_candidates" / snapshot_dir / "futures_liquidity_screen.parquet",
        liquidity_rows,
    )
    _write_parquet(
        data_root / "futures" / "screens" / "history_depth" / "universe_scope=rfud_candidates" / snapshot_dir / "futures_history_depth_screen.parquet",
        history_rows,
    )

    monkeypatch.setenv("MOEX_DATA_ROOT", str(data_root))

    result_rows, metadata = classify("2026-05-06")

    assert len(result_rows) == 3
    assert [row["family"] for row in result_rows] == ["MM", "MX", "W"]
    assert all(row["classification_status"] == "controlled_provisional" for row in result_rows)
    assert all(row["continuous_eligibility_status"] == "not_accepted" for row in result_rows)
    assert metadata["summary"]["row_count"] == 3
    assert "CR" not in metadata["summary"]["families"]
    assert "SiH7" not in metadata["summary"]["families"]
