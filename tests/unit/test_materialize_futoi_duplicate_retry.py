from __future__ import annotations

import pandas as pd
import pytest

from moex_data.futures import materialize_futoi_instrument as target


def _rows(pos_fiz: int = 422417) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "trade_date": "2021-03-02",
                "ts": pd.Timestamp("2021-03-02 18:44:59"),
                "moment": pd.Timestamp("2021-03-02 18:44:50"),
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
                "ts": pd.Timestamp("2021-03-02 18:44:59"),
                "moment": pd.Timestamp("2021-03-02 18:44:50"),
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
                "ts": pd.Timestamp("2021-03-02 18:44:59"),
                "moment": pd.Timestamp("2021-03-02 18:44:50"),
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
                "ts": pd.Timestamp("2021-03-02 18:44:59"),
                "moment": pd.Timestamp("2021-03-02 18:44:50"),
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


def test_exact_source_duplicates_are_collapsed_to_latest_seqnum() -> None:
    cleaned, dropped = target._deduplicate_exact_source_duplicates(_rows())

    assert dropped == 2
    assert len(cleaned) == 2
    assert set(cleaned["seqnum"].astype(int)) == {218}
    assert not cleaned.duplicated(subset=list(target.CANONICAL_KEY_FIELDS)).any()


def test_conflicting_duplicate_canonical_key_fails_closed() -> None:
    frame = _rows().iloc[:2].copy()
    frame.loc[frame.index[1], "pos"] = 422418

    with pytest.raises(target.FutoiMaterializationError, match="conflicting duplicate canonical FUTOI key"):
        target._deduplicate_exact_source_duplicates(frame)


def test_conflicting_moment_at_same_publication_key_fails_closed() -> None:
    frame = _rows().iloc[:2].copy()
    frame.loc[frame.index[1], "moment"] = pd.Timestamp("2021-03-02 18:44:55")

    with pytest.raises(target.FutoiMaterializationError, match="conflicting duplicate canonical FUTOI key"):
        target._deduplicate_exact_source_duplicates(frame)


def test_duplicate_without_usable_seqnum_fails_closed() -> None:
    frame = _rows().iloc[:2].copy()
    frame["seqnum"] = [None, "not-a-number"]

    with pytest.raises(target.FutoiMaterializationError, match="missing usable seqnum"):
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
