import pandas as pd
import pytest

from moex_data.futures import continuous_bars_access as access
from moex_data.futures.continuous_bars_access import (
    FuturesContinuousBarsAccessError,
    load_futures_continuous_bars,
)


FAMILY_CODE = "Si"
ROLL_POLICY_ID = "expiration_minus_1_session.v1"
ADJUSTMENT_POLICY_ID = "unadjusted.v1"


@pytest.fixture
def parquet_store(monkeypatch):
    store = {}

    def fake_read_parquet(path):
        key = str(path)
        if key not in store:
            raise AssertionError("unexpected parquet path:" + key)
        return store[key].copy()

    monkeypatch.setattr(access.pd, "read_parquet", fake_read_parquet)
    return store


def make_5m_rows():
    ends = pd.to_datetime(["2026-05-04 10:05:00", "2026-05-04 10:10:00", "2026-05-04 10:15:00"])
    rows = []
    for idx, end in enumerate(ends):
        base = 1000.0 + idx
        rows.append(
            {
                "trade_date": "2026-05-04",
                "end": end,
                "session_date": "2026-05-04",
                "continuous_symbol": "Si_CONT",
                "family_code": FAMILY_CODE,
                "source_secid": "SiM6",
                "source_contract": "SiM6",
                "open": base,
                "high": base + 1.0,
                "low": base - 1.0,
                "close": base + 0.5,
                "volume": 10 + idx,
                "roll_policy_id": ROLL_POLICY_ID,
                "adjustment_policy_id": ADJUSTMENT_POLICY_ID,
                "adjustment_factor": 1.0,
                "is_roll_boundary": False,
                "roll_map_id": "Si.rollmap.2026-05-04",
                "schema_version": "futures_continuous_5m.v1",
                "ingest_ts": "2026-05-04 23:00:00",
            }
        )
    return pd.DataFrame(rows)


def make_d1_rows():
    return pd.DataFrame(
        [
            {
                "trade_date": "2026-05-04",
                "session_date": "2026-05-04",
                "continuous_symbol": "Si_CONT",
                "family_code": FAMILY_CODE,
                "source_contracts": ["SiM6"],
                "open": 1000.0,
                "high": 1005.0,
                "low": 999.0,
                "close": 1003.0,
                "volume": 1000,
                "roll_policy_id": ROLL_POLICY_ID,
                "adjustment_policy_id": ADJUSTMENT_POLICY_ID,
                "adjustment_factor": 1.0,
                "has_roll_boundary": False,
                "roll_map_id": "Si.rollmap.2026-05-04",
                "schema_version": "futures_continuous_d1.v1",
                "ingest_ts": "2026-05-04 23:00:00",
            }
        ]
    )


def make_htf_rows(timeframe):
    return pd.DataFrame(
        [
            {
                "trade_date": "2026-05-04",
                "session_date": "2026-05-04",
                "bucket_end": pd.Timestamp("2026-05-04 10:15:00"),
                "timeframe": timeframe,
                "continuous_symbol": "Si_CONT",
                "family_code": FAMILY_CODE,
                "source_secid": "SiM6",
                "source_contract": "SiM6",
                "open": 1000.0,
                "high": 1003.0,
                "low": 999.0,
                "close": 1002.5,
                "volume": 33,
                "roll_policy_id": ROLL_POLICY_ID,
                "adjustment_policy_id": ADJUSTMENT_POLICY_ID,
                "adjustment_factor": 1.0,
                "contains_roll_boundary": False,
                "source_bar_count": 3,
                "first_source_end": pd.Timestamp("2026-05-04 10:05:00"),
                "last_source_end": pd.Timestamp("2026-05-04 10:15:00"),
                "schema_version": "futures_continuous_htf_ondemand_resampling.v1",
            }
        ]
    )


def register_5m(root, store, frame):
    path = (
        root
        / "futures"
        / "continuous_5m"
        / ("roll_policy=" + ROLL_POLICY_ID)
        / ("adjustment_policy=" + ADJUSTMENT_POLICY_ID)
        / ("family=" + FAMILY_CODE)
        / "trade_date=2026-05-04"
        / "part.parquet"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.touch()
    store[str(path)] = frame


def register_d1(root, store, frame):
    path = (
        root
        / "futures"
        / "continuous_d1"
        / ("roll_policy=" + ROLL_POLICY_ID)
        / ("adjustment_policy=" + ADJUSTMENT_POLICY_ID)
        / ("family=" + FAMILY_CODE)
        / "trade_date=2026-05-04"
        / "part.parquet"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.touch()
    store[str(path)] = frame


def call_loader(root, timeframe="5m", **overrides):
    params = {
        "data_root": root,
        "family_code": FAMILY_CODE,
        "roll_policy_id": ROLL_POLICY_ID,
        "adjustment_policy_id": ADJUSTMENT_POLICY_ID,
        "timeframe": timeframe,
        "start": "2026-05-04",
        "end": "2026-05-04",
        "session_calendar_id": None,
        "session_calendar": None,
        "columns": None,
    }
    params.update(overrides)
    return load_futures_continuous_bars(**params)


def test_valid_5m_route(tmp_path, parquet_store):
    register_5m(tmp_path, parquet_store, make_5m_rows())

    result = call_loader(tmp_path, "5m", columns=["end", "close", "schema_version"])

    assert result.columns.tolist() == ["end", "close", "schema_version"]
    assert len(result) == 3
    assert result["schema_version"].unique().tolist() == ["futures_continuous_5m.v1"]


def test_valid_htf_route_calls_resample_continuous_htf(tmp_path, parquet_store, monkeypatch):
    register_5m(tmp_path, parquet_store, make_5m_rows())
    calls = {}

    def fake_resample(source_bars, session_calendar, timeframe, session_calendar_id=None):
        calls["source_rows"] = len(source_bars)
        calls["session_calendar"] = session_calendar
        calls["timeframe"] = timeframe
        calls["session_calendar_id"] = session_calendar_id
        return make_htf_rows(timeframe)

    monkeypatch.setattr(access, "resample_continuous_htf", fake_resample)
    calendar = pd.DataFrame({"calendar_id": ["moex_futures_session_calendar.v1"]})

    result = call_loader(
        tmp_path,
        "15m",
        session_calendar_id="moex_futures_session_calendar.v1",
        session_calendar=calendar,
    )

    assert calls["source_rows"] == 3
    assert calls["session_calendar"] is calendar
    assert calls["timeframe"] == "15m"
    assert calls["session_calendar_id"] == "moex_futures_session_calendar.v1"
    assert result["timeframe"].unique().tolist() == ["15m"]
    assert result["schema_version"].unique().tolist() == ["futures_continuous_htf_ondemand_resampling.v1"]


def test_valid_d1_route(tmp_path, parquet_store, monkeypatch):
    register_d1(tmp_path, parquet_store, make_d1_rows())

    def unexpected_resample(*args, **kwargs):
        raise AssertionError("D1 must not call HTF resampling")

    monkeypatch.setattr(access, "resample_continuous_htf", unexpected_resample)

    result = call_loader(tmp_path, "D1")

    assert len(result) == 1
    assert result["schema_version"].unique().tolist() == ["futures_continuous_d1.v1"]
    assert result.iloc[0]["source_contracts"] == ["SiM6"]


@pytest.mark.parametrize("timeframe", ["1d", "daily", "60m", "240m", "2h"])
def test_invalid_timeframe_failure(tmp_path, timeframe):
    with pytest.raises(FuturesContinuousBarsAccessError, match="unsupported_timeframe"):
        call_loader(tmp_path, timeframe)


def test_missing_adjustment_policy_id_failure(tmp_path):
    with pytest.raises(FuturesContinuousBarsAccessError, match="missing_adjustment_policy_id"):
        call_loader(tmp_path, "5m", adjustment_policy_id="")


def test_missing_htf_session_calendar_id_failure(tmp_path, parquet_store):
    register_5m(tmp_path, parquet_store, make_5m_rows())

    with pytest.raises(FuturesContinuousBarsAccessError, match="missing_htf_session_calendar_id"):
        call_loader(tmp_path, "15m", session_calendar_id=None, session_calendar=pd.DataFrame())


def test_missing_htf_session_calendar_failure(tmp_path, parquet_store):
    register_5m(tmp_path, parquet_store, make_5m_rows())

    with pytest.raises(FuturesContinuousBarsAccessError, match="missing_htf_session_calendar"):
        call_loader(
            tmp_path,
            "15m",
            session_calendar_id="moex_futures_session_calendar.v1",
            session_calendar=None,
        )


def test_source_schema_failure(tmp_path, parquet_store):
    register_5m(tmp_path, parquet_store, make_5m_rows().drop(columns=["source_contract"]))

    with pytest.raises(FuturesContinuousBarsAccessError, match="missing_source_columns:source_contract"):
        call_loader(tmp_path, "5m")


def test_duplicate_end_failure(tmp_path, parquet_store):
    frame = pd.concat([make_5m_rows(), make_5m_rows().iloc[[0]]], ignore_index=True)
    register_5m(tmp_path, parquet_store, frame)

    with pytest.raises(FuturesContinuousBarsAccessError, match="duplicate_source_primary_key"):
        call_loader(tmp_path, "5m")


def test_non_monotonic_end_failure(tmp_path, parquet_store):
    frame = make_5m_rows()
    frame = pd.concat([frame.iloc[[1]], frame.iloc[[0]], frame.iloc[2:]], ignore_index=True)
    register_5m(tmp_path, parquet_store, frame)

    with pytest.raises(FuturesContinuousBarsAccessError, match="non_monotonic_source_timestamps"):
        call_loader(tmp_path, "5m")


def test_invalid_selected_columns_failure(tmp_path, parquet_store):
    register_5m(tmp_path, parquet_store, make_5m_rows())

    with pytest.raises(FuturesContinuousBarsAccessError, match="unknown_requested_columns:not_a_column"):
        call_loader(tmp_path, "5m", columns=["end", "not_a_column"])


def test_deterministic_sorted_output(tmp_path, parquet_store):
    first = make_5m_rows().iloc[[0]].copy()
    first["continuous_symbol"] = "Si_CONT_B"
    second = make_5m_rows().iloc[[0]].copy()
    second["continuous_symbol"] = "Si_CONT_A"
    frame = pd.concat([first, second], ignore_index=True)
    frame.loc[1, "end"] = pd.Timestamp("2026-05-04 10:10:00")
    register_5m(tmp_path, parquet_store, frame)

    result = call_loader(tmp_path, "5m", columns=["continuous_symbol", "end"])

    assert result["continuous_symbol"].tolist() == ["Si_CONT_A", "Si_CONT_B"]
