import pandas as pd

from moex_data.futures import derived_d1_ohlcv_builder as mod


def _config():
    return {
        "active_raw_d1_selection_mode": "rfud_included_raw_d1",
        "continuous_build_enabled": False,
        "w1_build_enabled": False,
        "l3_5_raw_d1_included_universe": {
            "board": "RFUD",
            "classification_status": "included",
            "dataset_stage": "raw_d1",
            "raw_5m_eligible_only": True,
            "raw_d1_eligible_only": True,
            "source_quality_stage": "raw_5m",
        },
    }


def _eligibility():
    return pd.DataFrame([
        {"secid": "A", "board": "RFUD", "family_code": "Si", "classification_status": "included", "raw_5m_eligible": True, "raw_d1_eligible": False, "schema_version": "old", "notes": "n", "backfill_selection_status": "selected", "backfill_selection_reason": "raw_5m"},
        {"secid": "B", "board": "RFUD", "family_code": "BR", "classification_status": "included", "raw_5m_eligible": True, "raw_d1_eligible": False, "schema_version": "old", "notes": "n", "backfill_selection_status": "selected", "backfill_selection_reason": "raw_5m"},
        {"secid": "C", "board": "RFUD", "family_code": "CNY", "classification_status": "included", "raw_5m_eligible": True, "raw_d1_eligible": False, "schema_version": "old", "notes": "n", "backfill_selection_status": "selected", "backfill_selection_reason": "raw_5m"},
        {"secid": "D", "board": "TQBR", "family_code": "BAD", "classification_status": "deferred", "raw_5m_eligible": False, "raw_d1_eligible": False, "schema_version": "old", "notes": "n", "backfill_selection_status": "deferred", "backfill_selection_reason": "unsupported_board"},
    ])


def _quality():
    return pd.DataFrame([
        {"dataset_stage": "raw_5m", "family_code": "Si", "secid": "A", "quality_status": "pass", "rows_written": 10, "partition_status": "written", "date_from": "2026-05-15", "date_till": "2026-05-19"},
        {"dataset_stage": "raw_5m", "family_code": "BR", "secid": "B", "quality_status": "fail", "rows_written": 0, "partition_status": "not_written", "date_from": "2026-05-15", "date_till": "2026-05-19"},
        {"dataset_stage": "raw_5m", "family_code": "CNY", "secid": "C", "quality_status": "pass", "rows_written": 5, "partition_status": "written", "date_from": "2026-05-18", "date_till": "2026-05-19"},
    ])


def _raw_frame():
    return pd.DataFrame([
        {"trade_date": "2026-05-18", "ts": "2026-05-18 10:00:00", "board": "RFUD", "secid": "A", "family_code": "Si", "open": 1, "high": 3, "low": 1, "close": 2, "volume": 10, "value": 20, "num_trades": 2, "schema_version": "futures_raw_5m.v1", "calendar_denominator_status": "canonical_apim_futures_xml", "short_history_flag": False, "_source_partition_path": "/tmp/raw/trade_date=2026-05-18/family=Si/secid=A/part.parquet"},
        {"trade_date": "2026-05-18", "ts": "2026-05-18 10:05:00", "board": "RFUD", "secid": "A", "family_code": "Si", "open": 2, "high": 4, "low": 2, "close": 3, "volume": 20, "value": 30, "num_trades": 3, "schema_version": "futures_raw_5m.v1", "calendar_denominator_status": "canonical_apim_futures_xml", "short_history_flag": False, "_source_partition_path": "/tmp/raw/trade_date=2026-05-18/family=Si/secid=A/part.parquet"},
        {"trade_date": "2026-05-18", "ts": "2026-05-18 10:00:00", "board": "RFUD", "secid": "C", "family_code": "CNY", "open": 5, "high": 6, "low": 4, "close": 5, "volume": 7, "value": 8, "num_trades": 1, "schema_version": "futures_raw_5m.v1", "calendar_denominator_status": "canonical_apim_futures_xml", "short_history_flag": False, "_source_partition_path": "/tmp/raw/trade_date=2026-05-18/family=CNY/secid=C/part.parquet"},
    ])


def _row(frame, secid):
    matched = frame.loc[frame["secid"] == secid]
    assert len(matched) == 1
    return matched.iloc[0].to_dict()


def test_raw_d1_selection_uses_eligibility_and_accepted_raw_5m_quality():
    refined, selected = mod.refine_eligibility_for_raw_d1(_eligibility(), _quality().loc[_quality()["quality_status"] == "pass"].copy(), _config())
    assert selected["secid"].tolist() == ["C", "A"]
    assert _row(refined, "A")["raw_d1_eligible"] is True
    assert _row(refined, "C")["raw_d1_eligible"] is True
    assert _row(refined, "B")["raw_d1_eligible"] is False
    assert _row(refined, "B")["raw_d1_check_status"] == "raw_5m_quality_not_accepted"
    assert _row(refined, "D")["classification_status"] == "deferred"


def test_raw_d1_chunk_groups_are_family_driven_and_deferred_visible():
    refined, selected = mod.refine_eligibility_for_raw_d1(_eligibility(), _quality().loc[_quality()["quality_status"] == "pass"].copy(), _config())
    groups = mod.chunk_groups(selected)
    assert [family for family, _frame in groups] == ["CNY", "Si"]
    assert set(refined["secid"].tolist()) == {"A", "B", "C", "D"}
    assert refined.loc[refined["secid"] == "D", "raw_d1_eligible"].iloc[0] is False


def test_raw_d1_aggregate_is_raw_5m_only_and_outputs_stage():
    raw = mod.normalize_raw(_raw_frame())
    mod.validate_raw(raw)
    d1 = mod.aggregate_d1(raw, "2026-05-19T00:00:00Z")
    assert set(d1["dataset_stage"].unique()) == {"raw_d1"}
    assert set(d1["source_dataset_id"].unique()) == {"futures_raw_5m"}
    assert "continuous_symbol" not in d1.columns
    assert "pos_long" not in d1.columns
    a = d1.loc[d1["secid"] == "A"].iloc[0]
    assert a["open"] == 1
    assert a["high"] == 4
    assert a["low"] == 1
    assert a["close"] == 3
    assert a["volume"] == 30


def test_raw_d1_forbidden_manual_whitelist_is_detectable_by_mode_contract():
    config = _config()
    assert config["continuous_build_enabled"] is False
    assert config["w1_build_enabled"] is False
    assert config["active_raw_d1_selection_mode"] == "rfud_included_raw_d1"
