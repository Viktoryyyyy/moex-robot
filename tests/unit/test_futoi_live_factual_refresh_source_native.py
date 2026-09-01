from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from moex_data.futures import futoi_live_factual_refresh_source_native as factual


IDENTITIES = {
    factual.SI_INSTRUMENT_ID: ("si", "SiU6"),
    factual.CR_INSTRUMENT_ID: ("cr", "CRU6"),
}


def _raw_frame(trade_date: str, ticker: str) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "sess_id": 1,
                "seqnum": 1,
                "tradedate": trade_date,
                "tradetime": "23:50:00",
                "ticker": ticker,
                "clgroup": "FIZ",
                "pos": 20,
                "pos_long": 100,
                "pos_short": -80,
                "pos_long_num": 10,
                "pos_short_num": 11,
                "systime": trade_date + " 23:55:00",
            },
            {
                "sess_id": 1,
                "seqnum": 1,
                "tradedate": trade_date,
                "tradetime": "23:50:00",
                "ticker": ticker,
                "clgroup": "YUR",
                "pos": -20,
                "pos_long": 80,
                "pos_short": -100,
                "pos_long_num": 12,
                "pos_short_num": 13,
                "systime": trade_date + " 23:55:00",
            },
        ]
    )


def _accepted_frame(
    trade_date: str,
    instrument_id: str,
    *,
    event_time: str = "23:50:00",
    seqnum: int = 1,
    sess_id: int = 1,
    fiz_long: int = 100,
    fiz_short: int = -80,
    fiz_net: int = 20,
    yur_long: int = 80,
    yur_short: int = -100,
    yur_net: int = -20,
) -> pd.DataFrame:
    ticker, secid = IDENTITIES[instrument_id]
    snapshot = trade_date + " " + event_time
    return pd.DataFrame(
        [
            {
                "trade_date": trade_date,
                "ts": snapshot,
                "systime": trade_date + " 23:55:00",
                "availability_ts_utc": trade_date + "T20:56:00+00:00",
                "ingest_ts": trade_date + "T20:57:00+00:00",
                "sess_id": sess_id,
                "seqnum": seqnum,
                "clgroup": "FIZ",
                "pos": fiz_net,
                "pos_long": fiz_long,
                "pos_short": fiz_short,
                "pos_long_num": 10,
                "pos_short_num": 11,
                "source_id": factual.SOURCE_ID,
                "instrument_id": instrument_id,
                "source_ticker": ticker,
                "secid": secid,
            },
            {
                "trade_date": trade_date,
                "ts": snapshot,
                "systime": trade_date + " 23:55:00",
                "availability_ts_utc": trade_date + "T20:56:00+00:00",
                "ingest_ts": trade_date + "T20:57:00+00:00",
                "sess_id": sess_id,
                "seqnum": seqnum,
                "clgroup": "YUR",
                "pos": yur_net,
                "pos_long": yur_long,
                "pos_short": yur_short,
                "pos_long_num": 12,
                "pos_short_num": 13,
                "source_id": factual.SOURCE_ID,
                "instrument_id": instrument_id,
                "source_ticker": ticker,
                "secid": secid,
            },
        ]
    )


def _identity(instrument_id: str) -> dict[str, str]:
    ticker, secid = IDENTITIES[instrument_id]
    return {
        "instrument_id": instrument_id,
        "source_id": factual.SOURCE_ID,
        "source_ticker": ticker,
        "secid": secid,
    }


def _latest(frame: pd.DataFrame, instrument_id: str, trade_date: str = "2026-08-28"):
    identity = _identity(instrument_id)
    return factual.latest_aligned_factual(
        frame,
        expected_trade_date=trade_date,
        expected_instrument_id=instrument_id,
        expected_source_ticker=identity["source_ticker"],
        expected_secid=identity["secid"],
    )


def _patch_binding(monkeypatch) -> None:
    def binding(instrument_id: str):
        identity = _identity(instrument_id)
        return {
            "instrument_id": instrument_id,
            "futoi.source_id": factual.SOURCE_ID,
            "futoi.ticker": identity["source_ticker"],
            "secid": identity["secid"],
        }

    monkeypatch.setattr(factual, "_binding", binding)


def _patch_validation_passthrough(monkeypatch) -> None:
    monkeypatch.setattr(
        factual.materializer,
        "_validate_required_source_identifiers",
        lambda frame: frame,
    )
    monkeypatch.setattr(
        factual.materializer,
        "_validate_raw_source_rows",
        lambda frame, trade_date, ticker: frame,
    )


def test_cr_registry_binding_resolves_expected_futoi_source_identity() -> None:
    identity = factual.source_identity(factual.CR_INSTRUMENT_ID)

    assert identity == {
        "instrument_id": "cr_futures_family",
        "source_id": "moex_algopack_futoi",
        "source_ticker": "cr",
        "secid": "CRU6",
    }


def test_explicit_si_and_cr_source_native_probe_works(monkeypatch) -> None:
    _patch_binding(monkeypatch)
    _patch_validation_passthrough(monkeypatch)

    def fetch(ticker, trade_date, timeout, apim_base_url):
        del timeout, apim_base_url
        return _raw_frame(trade_date, ticker), "https://apim.example/futoi/" + ticker + ".json"

    monkeypatch.setattr(factual.materializer, "_fetch_exact", fetch)

    for instrument_id in factual.LIVE_INSTRUMENT_IDS:
        target, observations = factual.discover_latest_source_trade_date(
            "2026-08-28",
            instrument_id=instrument_id,
            timeout=1.0,
        )
        assert target == "2026-08-28"
        assert observations[0]["status"] == "DATA"
        assert observations[0]["row_count"] == 2


def test_empty_exact_source_fails_closed_without_weekday_or_weekend_inference(monkeypatch) -> None:
    _patch_binding(monkeypatch)

    def fetch(ticker, trade_date, timeout, apim_base_url):
        del ticker, trade_date, timeout, apim_base_url
        raise factual.materializer.FutoiMaterializationError(factual.EXPLICIT_EMPTY_ERROR)

    monkeypatch.setattr(factual.materializer, "_fetch_exact", fetch)

    for trade_date in ("2026-08-28", "2026-08-29"):
        with pytest.raises(
            factual.FutoiSourceNativeRefreshError,
            match="trading-day status cannot be proven",
        ):
            factual.discover_latest_source_trade_date(
                trade_date,
                instrument_id=factual.SI_INSTRUMENT_ID,
                timeout=1.0,
            )


def test_source_native_probe_error_fails_closed(monkeypatch) -> None:
    _patch_binding(monkeypatch)

    def fetch(ticker, trade_date, timeout, apim_base_url):
        del ticker, trade_date, timeout, apim_base_url
        raise RuntimeError("transport failure")

    monkeypatch.setattr(factual.materializer, "_fetch_exact", fetch)

    with pytest.raises(factual.FutoiSourceNativeRefreshError, match="transport failure"):
        factual.discover_latest_source_trade_date(
            "2026-08-28",
            instrument_id=factual.CR_INSTRUMENT_ID,
            timeout=1.0,
        )


def test_latest_exact_aligned_fiz_yur_event_is_selected() -> None:
    older = _accepted_frame("2026-08-28", factual.SI_INSTRUMENT_ID, event_time="20:00:00")
    latest = _accepted_frame("2026-08-28", factual.SI_INSTRUMENT_ID, event_time="23:50:00")
    frame = pd.concat([older, latest], ignore_index=True)

    result = _latest(frame, factual.SI_INSTRUMENT_ID)

    assert result["snapshot_ts"] == "2026-08-28T20:50:00+00:00"
    assert result["fiz"]["net"] == 20
    assert result["yur"]["net"] == -20


def test_max_seqnum_revision_is_resolved_deterministically() -> None:
    first = _accepted_frame(
        "2026-08-28",
        factual.CR_INSTRUMENT_ID,
        seqnum=1,
        fiz_long=90,
        fiz_short=-80,
        fiz_net=10,
        yur_long=80,
        yur_short=-90,
        yur_net=-10,
    )
    revised = _accepted_frame("2026-08-28", factual.CR_INSTRUMENT_ID, seqnum=2)
    frame = pd.concat([first, revised], ignore_index=True)

    result = _latest(frame, factual.CR_INSTRUMENT_ID)

    assert result["fiz"]["long"] == 100
    assert result["fiz"]["net"] == 20
    assert result["yur"]["short"] == 100


def test_fiz_yur_sess_id_mismatch_fails_closed() -> None:
    frame = _accepted_frame("2026-08-28", factual.CR_INSTRUMENT_ID)
    frame.loc[frame["clgroup"] == "YUR", "sess_id"] = 2

    with pytest.raises(factual.FutoiSourceNativeRefreshError, match="must share sess_id"):
        _latest(frame, factual.CR_INSTRUMENT_ID)


def test_fiz_yur_net_imbalance_fails_closed() -> None:
    frame = _accepted_frame(
        "2026-08-28",
        factual.SI_INSTRUMENT_ID,
        yur_long=81,
        yur_short=-100,
        yur_net=-19,
    )

    with pytest.raises(factual.FutoiSourceNativeRefreshError, match="do not balance to zero"):
        _latest(frame, factual.SI_INSTRUMENT_ID)


def test_long_short_oi_identity_failure_fails_closed() -> None:
    frame = _accepted_frame("2026-08-28", factual.SI_INSTRUMENT_ID)
    frame.loc[frame["clgroup"] == "FIZ", "pos"] = 19

    with pytest.raises(factual.FutoiSourceNativeRefreshError, match="net position identity failed"):
        _latest(frame, factual.SI_INSTRUMENT_ID)


@pytest.mark.parametrize("field", ["instrument_id", "source_ticker", "secid"])
def test_wrong_source_or_instrument_identity_fails_closed(field: str) -> None:
    frame = _accepted_frame("2026-08-28", factual.CR_INSTRUMENT_ID)
    replacements = {
        "instrument_id": factual.SI_INSTRUMENT_ID,
        "source_ticker": "si",
        "secid": "SiU6",
    }
    frame[field] = replacements[field]

    with pytest.raises(factual.FutoiSourceNativeRefreshError, match=field + " mismatch"):
        _latest(frame, factual.CR_INSTRUMENT_ID)


def test_si_and_cr_refresh_write_separate_current_artifacts(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("MOEX_DATA_ROOT", str(tmp_path))
    monkeypatch.setattr(factual, "source_identity", _identity)
    monkeypatch.setattr(
        factual,
        "discover_latest_source_trade_date",
        lambda through_date, instrument_id, timeout: (through_date, [{"trade_date": through_date, "status": "DATA"}]),
    )

    def materialize(root, target_trade_date, run_id, *, instrument_id, timeout):
        del root, run_id, timeout
        return tmp_path / (instrument_id + ".parquet"), {"instrument_id": instrument_id}

    monkeypatch.setattr(factual, "_materialize_target", materialize)

    def read_parquet(path):
        instrument_id = Path(path).stem
        return _accepted_frame("2026-08-28", instrument_id)

    monkeypatch.setattr(factual.pd, "read_parquet", read_parquet)

    for instrument_id in factual.LIVE_INSTRUMENT_IDS:
        result = factual.run_refresh(
            through_date="2026-08-28",
            instrument_id=instrument_id,
            run_id="unit_" + instrument_id,
            timeout=1.0,
        )
        assert result["status"] == "PASS"
        assert result["instrument_id"] == instrument_id
        assert result["directional_authority"] is False
        assert result["action_authority"] is False
        assert result["standalone_buy_sell_authority"] is False
        assert result["stage5_full_mode_ready"] is False
        assert result["stage5_pointer_promotion_performed"] is False

    si_path = factual._current_path(tmp_path, factual.SI_INSTRUMENT_ID)
    cr_path = factual._current_path(tmp_path, factual.CR_INSTRUMENT_ID)
    assert si_path != cr_path
    assert json.loads(si_path.read_text(encoding="utf-8"))["instrument_id"] == factual.SI_INSTRUMENT_ID
    assert json.loads(cr_path.read_text(encoding="utf-8"))["instrument_id"] == factual.CR_INSTRUMENT_ID


def test_refresh_all_reports_one_instrument_failure_without_substitution(monkeypatch) -> None:
    def refresh(*, through_date, instrument_id, run_id, timeout):
        del through_date, run_id, timeout
        if instrument_id == factual.CR_INSTRUMENT_ID:
            raise RuntimeError("CR failed")
        return {"status": "PASS", "instrument_id": instrument_id}

    monkeypatch.setattr(factual, "run_refresh", refresh)
    result = factual.run_refresh_all(
        through_date="2026-08-28",
        run_id="aggregate_test",
        timeout=1.0,
    )

    assert result["status"] == "PARTIAL_FAILURE"
    assert result["failed_instrument_ids"] == [factual.CR_INSTRUMENT_ID]
    assert result["instrument_results"][factual.SI_INSTRUMENT_ID]["status"] == "PASS"
    assert result["instrument_results"][factual.CR_INSTRUMENT_ID]["status"] == "FAILED"
    assert result["instrument_results"][factual.CR_INSTRUMENT_ID]["instrument_id"] == factual.CR_INSTRUMENT_ID
    assert result["stage5_full_mode_ready"] is False
    assert result["stage5_pointer_promotion_performed"] is False


def test_no_moex_calendar_api_dependency_is_introduced() -> None:
    source = Path(factual.__file__).read_text(encoding="utf-8")
    assert "/iss/calendars.json" not in source
    assert "/iss/calendars" not in source
    assert ".weekday()" not in source
    assert date.fromisoformat("2026-08-29").weekday() == 5
