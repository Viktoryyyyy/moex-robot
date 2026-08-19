from __future__ import annotations

import pandas as pd
import pytest

from moex_data.futures import materialize_futoi_instrument as target


def _rows(pos_fiz: int = 422417) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "trade_date": "2021-03-02",
                "ts": pd.Timestamp("2021-03-02 18:44:50"),
                "moment": pd.Timestamp("2021-03-02 18:44:50"),
                "systime": pd.Timestamp("2025-06-21 16:22:46"),
                "secid": "SiU6",
                "clgroup": "FIZ",
                "sess_id": 6239,
                "seqnum": 217,
                "source_ticker": "SI",
                "pos": pos_fiz,
                "pos_long": 816793,
                "pos_short": -394376,
                "pos_long_num": 16320,
                "pos_short_num": 9519,
            },
            {
                "trade_date": "2021-03-02",
                "ts": pd.Timestamp("2021-03-02 18:44:50"),
                "moment": pd.Timestamp("2021-03-02 18:44:50"),
                "systime": pd.Timestamp("2025-06-21 16:22:46"),
                "secid": "SiU6",
                "clgroup": "FIZ",
                "sess_id": 6239,
                "seqnum": 218,
                "source_ticker": "SI",
                "pos": pos_fiz,
                "pos_long": 816793,
                "pos_short": -394376,
                "pos_long_num": 16320,
                "pos_short_num": 9519,
            },
            {
                "trade_date": "2021-03-02",
                "ts": pd.Timestamp("2021-03-02 18:44:50"),
                "moment": pd.Timestamp("2021-03-02 18:44:50"),
                "systime": pd.Timestamp("2025-06-21 16:22:46"),
                "secid": "SiU6",
                "clgroup": "YUR",
                "sess_id": 6239,
                "seqnum": 217,
                "source_ticker": "SI",
                "pos": -422417,
                "pos_long": 855621,
                "pos_short": -1278038,
                "pos_long_num": 245,
                "pos_short_num": 172,
            },
            {
                "trade_date": "2021-03-02",
                "ts": pd.Timestamp("2021-03-02 18:44:50"),
                "moment": pd.Timestamp("2021-03-02 18:44:50"),
                "systime": pd.Timestamp("2025-06-21 16:22:46"),
                "secid": "SiU6",
                "clgroup": "YUR",
                "sess_id": 6239,
                "seqnum": 218,
                "source_ticker": "SI",
                "pos": -422417,
                "pos_long": 855621,
                "pos_short": -1278038,
                "pos_long_num": 245,
                "pos_short_num": 172,
            },
        ]
    )


def test_same_moment_different_seqnum_source_records_are_preserved() -> None:
    cleaned, dropped = target._deduplicate_exact_source_duplicates(_rows())

    assert dropped == 0
    assert len(cleaned) == 4
    assert set(cleaned["seqnum"].astype(int)) == {217, 218}
    assert not cleaned.duplicated(subset=list(target.SOURCE_RECORD_KEY_FIELDS)).any()


def test_exact_duplicate_same_source_record_is_collapsed() -> None:
    row = _rows().iloc[[0]].copy()
    frame = pd.concat([row, row], ignore_index=True)

    cleaned, dropped = target._deduplicate_exact_source_duplicates(frame)

    assert dropped == 1
    assert len(cleaned) == 1


def test_conflicting_same_source_record_fails_closed() -> None:
    row = _rows().iloc[[0]].copy()
    conflict = row.copy()
    conflict.loc[conflict.index[0], "pos"] = 422418
    frame = pd.concat([row, conflict], ignore_index=True)

    with pytest.raises(target.FutoiMaterializationError, match="conflicting duplicate FUTOI source record"):
        target._deduplicate_exact_source_duplicates(frame)


def test_fetch_exact_retries_transient_401_then_succeeds(monkeypatch) -> None:
    calls = []

    class Response:
        status_code = 401

    class Transient401(Exception):
        response = Response()

    def fake_fetch(*args, **kwargs):
        calls.append((args, kwargs))
        if len(calls) == 1:
            raise Transient401("temporary unauthorized")
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
                    "systime": "2026-08-17 07:05:05",
                }
            ]
        )

    monkeypatch.setenv("MOEX_API_KEY", "test-token")
    monkeypatch.setattr(target.availability, "fetch_paged_frame", fake_fetch)
    monkeypatch.setattr(target.time, "sleep", lambda *_: None)

    frame, _ = target._fetch_exact("si", "2026-08-17", 5.0, "https://apim.moex.com")

    assert len(calls) == 2
    assert len(frame) == 1
