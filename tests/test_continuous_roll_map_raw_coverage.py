import pandas as pd

from moex_data.futures import continuous_roll_map_builder as mod


def row(secid, first_trade):
    return {
        "secid": secid,
        "board": "RFUD",
        "family_code": "Si",
        "decision_source": "registry_expiration_date",
        "is_perpetual": False,
        "expiration_date": "2026-06-18" if secid == "SiM6" else "2026-09-17",
        "first_trade_date": first_trade,
        "expiration_status": "pass",
        "expiration_map_id": secid + "_exp",
    }


def make_raw_partition(root, trade_date, family, secid):
    path = root / "futures" / "raw_5m" / ("trade_date=" + trade_date) / ("family=" + family) / ("secid=" + secid) / "part.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("placeholder", encoding="utf-8")


def sessions(start="2025-04-01", end="2026-12-31"):
    return [str(x.date()) for x in pd.date_range(start, end, freq="B")]


def test_first_ordinary_uses_earliest_raw_partition_not_snapshot_date(tmp_path):
    make_raw_partition(tmp_path, "2025-04-29", "Si", "SiM6")
    source = pd.DataFrame([row("SiM6", None), row("SiU6", "2026-06-19")])
    out = mod.build_ordinary_rows(source, "2026-05-25", "run", sessions(), [], tmp_path)
    by_secid = {x["source_secid"]: x for x in out}
    assert by_secid["SiM6"]["valid_from_session"] == "2025-04-29"
    assert by_secid["SiM6"]["valid_from_session"] != "2026-05-25"


def test_snapshot_date_fallback_blocked_when_no_raw_or_backfill_coverage(tmp_path):
    source = pd.DataFrame([row("SiM6", None), row("SiU6", "2026-06-19")])
    try:
        mod.build_ordinary_rows(source, "2026-05-25", "run", sessions(), [], tmp_path)
    except RuntimeError as exc:
        assert "first_ordinary_valid_from_unresolved_for_SiM6" in str(exc)
    else:
        raise AssertionError("expected fail-closed unresolved first ordinary coverage")


def test_usdrubf_perpetual_identity_unchanged():
    out = mod.build_perpetual_row(pd.Series({"secid": "USDRUBF", "family_code": "USDRUBF", "board": "RFUD"}), "2026-05-25", "run")
    assert out["continuous_symbol"] == "USDRUBF"
    assert out["source_secid"] == "USDRUBF"
    assert out["roll_required"] is False
    assert out["roll_status"] == "perpetual_identity"
    assert out["valid_from_session"] == "2026-05-25"


def test_excluded_sih7_sim7_not_included_or_bridged(tmp_path):
    make_raw_partition(tmp_path, "2025-04-29", "Si", "SiM6")
    source = pd.DataFrame([row("SiM6", None), row("SiU6", "2026-06-19"), row("SiZ6", "2026-09-18"), row("SiU7", "2027-06-18")])
    out = mod.build_ordinary_rows(source, "2026-05-25", "run", sessions(), ["SiH7", "SiM7"], tmp_path)
    secids = [x["source_secid"] for x in out]
    nexts = [x["next_secid"] for x in out if x.get("next_secid")]
    assert "SiH7" not in secids
    assert "SiM7" not in secids
    assert "SiH7" not in nexts
    assert "SiM7" not in nexts
    assert {x["source_secid"]: x["roll_status"] for x in out}["SiZ6"] == "explicit_partial_chain_gap"
