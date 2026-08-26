from pathlib import Path

import pandas as pd
import pytest

from moex_data.futures.validate_futoi_eod_from_frozen import (
    reconstruct_eod_row,
    validate_candidate_partition,
)


def _raw_day() -> pd.DataFrame:
    common = {
        "instrument_id": "si_futures_family",
        "trade_date": "2026-08-17",
        "ts": "2026-08-17 20:00:00",
        "sess_id": 1,
        "seqnum": 7,
        "availability_ts_utc": "2026-08-17T17:01:00+00:00",
    }
    return pd.DataFrame([
        {
            **common,
            "systime": "2026-08-17 20:00:10",
            "clgroup": "FIZ",
            "pos": 20,
            "pos_long": 60,
            "pos_short": -40,
            "pos_long_num": 10,
            "pos_short_num": 8,
        },
        {
            **common,
            "systime": "2026-08-17 20:00:12",
            "clgroup": "YUR",
            "pos": -20,
            "pos_long": 40,
            "pos_short": -60,
            "pos_long_num": 8,
            "pos_short_num": 12,
        },
    ])


def _fixture(tmp_path: Path) -> tuple[Path, Path, dict[str, dict[str, object]]]:
    raw_path = tmp_path / "frozen.parquet"
    raw = _raw_day()
    raw.to_parquet(raw_path, index=False)
    frozen_ref = "${MOEX_DATA_ROOT}/runs/frozen.parquet"
    canonical_ref = "${MOEX_DATA_ROOT}/market/raw.parquet"
    frozen_sha = "a" * 64
    expected = reconstruct_eod_row(
        raw,
        instrument_id="si_futures_family",
        trade_date="2026-08-17",
        frozen_partition_ref=frozen_ref,
        canonical_source_ref=canonical_ref,
        frozen_sha256=frozen_sha,
    )
    eod_path = tmp_path / "eod.parquet"
    pd.DataFrame([expected]).to_parquet(eod_path, index=False)
    records = {
        "2026-08-17": {
            "frozen_partition_ref": frozen_ref,
            "canonical_source_ref": canonical_ref,
            "frozen_sha256": frozen_sha,
        }
    }
    return raw_path, eod_path, records


def test_independent_frozen_raw_eod_oracle_accepts_exact_candidate(tmp_path: Path) -> None:
    raw_path, eod_path, records = _fixture(tmp_path)
    result = validate_candidate_partition(
        eod_path=eod_path,
        records_by_date=records,
        expand_frozen_ref=lambda _ref: raw_path,
    )
    assert result["reconstructed_eod_rows"] == 1
    assert result["reconstructed_from_frozen_raw_match"] is True
    assert result["independent_from_eod_producer"] is True


def test_independent_frozen_raw_eod_oracle_rejects_self_consistent_alternative_history(tmp_path: Path) -> None:
    raw_path, eod_path, records = _fixture(tmp_path)
    candidate = pd.read_parquet(eod_path)

    # Keep the candidate internally balance/formula-consistent while moving it away
    # from the frozen raw positions: FIZ 61/40 => +21, YUR 39/60 => -21.
    updates = {
        "phys_net": 21,
        "phys_long": 61,
        "phys_short_abs": 40,
        "legal_net": -21,
        "legal_long": 39,
        "legal_short_abs": 60,
        "total_open_interest": 100,
        "total_short_abs": 100,
        "phys_gross": 101,
        "legal_gross": 99,
        "phys_long_share_of_oi": 0.61,
        "phys_short_share_of_oi": 0.40,
        "phys_net_share_of_oi": 0.21,
        "legal_long_share_of_oi": 0.39,
        "legal_short_share_of_oi": 0.60,
        "legal_net_share_of_oi": -0.21,
        "phys_gross_share_of_two_sided_oi": 0.505,
        "legal_gross_share_of_two_sided_oi": 0.495,
        "phys_avg_long_per_participant": 6.1,
        "phys_avg_short_per_participant": 5.0,
        "legal_avg_long_per_participant": 4.875,
        "legal_avg_short_per_participant": 5.0,
    }
    for field, value in updates.items():
        candidate.loc[0, field] = value
    candidate.to_parquet(eod_path, index=False)

    with pytest.raises(ValueError, match="EOD frozen reconstruction mismatch"):
        validate_candidate_partition(
            eod_path=eod_path,
            records_by_date=records,
            expand_frozen_ref=lambda _ref: raw_path,
        )
