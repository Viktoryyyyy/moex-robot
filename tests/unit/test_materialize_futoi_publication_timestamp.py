from __future__ import annotations

import pandas as pd
import pytest

from moex_data.futures import materialize_futoi_instrument as target


def _normalized_rows() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "trade_date": "2021-04-06",
                "ts": pd.Timestamp("2021-04-06 18:30:00"),
                "moment": pd.Timestamp("2021-04-06 18:30:00"),
                "systime": pd.Timestamp("2021-04-06 18:30:05"),
                "secid": "SiU6",
                "clgroup": "FIZ",
                "sess_id": 6263,
                "seqnum": 195,
                "source_ticker": "SI",
                "pos": 53870,
                "pos_long": 668969,
                "pos_short": -615099,
                "pos_long_num": 14657,
                "pos_short_num": 9598,
            },
            {
                "trade_date": "2021-04-06",
                "ts": pd.Timestamp("2021-04-06 18:30:00"),
                "moment": pd.Timestamp("2021-04-06 18:30:00"),
                "systime": pd.Timestamp("2021-04-06 18:35:05"),
                "secid": "SiU6",
                "clgroup": "FIZ",
                "sess_id": 6263,
                "seqnum": 215,
                "source_ticker": "SI",
                "pos": 53415,
                "pos_long": 668882,
                "pos_short": -615467,
                "pos_long_num": 14665,
                "pos_short_num": 9613,
            },
        ]
    )


def test_publication_systime_becomes_canonical_ts_and_preserves_distinct_snapshots() -> None:
    frame = target._enforce_publication_timestamp(_normalized_rows())
    cleaned, dropped = target._deduplicate_exact_source_duplicates(frame)

    assert dropped == 0
    assert len(cleaned) == 2
    assert cleaned["moment"].nunique() == 1
    assert cleaned["ts"].nunique() == 2
    assert cleaned["ts"].tolist() == [
        pd.Timestamp("2021-04-06 18:30:05"),
        pd.Timestamp("2021-04-06 18:35:05"),
    ]


def test_missing_publication_systime_fails_closed() -> None:
    frame = _normalized_rows().drop(columns=["systime"])

    with pytest.raises(target.FutoiMaterializationError, match="missing systime publication timestamp"):
        target._enforce_publication_timestamp(frame)


def test_invalid_publication_systime_fails_closed() -> None:
    frame = _normalized_rows()
    frame["systime"] = frame["systime"].astype(object)
    frame.loc[frame.index[0], "systime"] = "not-a-timestamp"

    with pytest.raises(target.FutoiMaterializationError, match="invalid systime publication timestamp"):
        target._enforce_publication_timestamp(frame)


def test_publication_date_mismatch_fails_closed() -> None:
    frame = _normalized_rows()
    frame.loc[frame.index[0], "systime"] = pd.Timestamp("2021-04-07 00:00:05")

    with pytest.raises(target.FutoiMaterializationError, match="publication systime date does not match trade_date"):
        target._enforce_publication_timestamp(frame)


def test_fetch_exact_requires_official_publication_fields(monkeypatch) -> None:
    def fake_fetch(*args, **kwargs):
        return pd.DataFrame(
            [
                {
                    "sess_id": 1,
                    "seqnum": 1,
                    "tradedate": "2026-08-17",
                    "tradetime": "07:05:00",
                    "ticker": "SI",
                    "clgroup": "FIZ",
                    "pos": 1,
                    "pos_long": 2,
                    "pos_short": -1,
                    "pos_long_num": 2,
                    "pos_short_num": 1,
                }
            ]
        )

    monkeypatch.setenv("MOEX_API_KEY", "test-token")
    monkeypatch.setattr(target.availability, "fetch_paged_frame", fake_fetch)

    with pytest.raises(target.FutoiMaterializationError, match="FUTOI APIM schema mismatch"):
        target._fetch_exact("si", "2026-08-17", 5.0, "https://apim.moex.com")
