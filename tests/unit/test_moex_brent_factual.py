"""Brent-only source and canonical snapshot acceptance regressions (offline fixtures)."""
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import hashlib
import json
from urllib.parse import parse_qs, urlsplit

import pytest

from moex_data import rub_production_source_matrix as matrix
from moex_data.rub_snapshot_read_freshness import apply_read_freshness
from moex_research.external_data import moex_brent_factual as brent
from src.moex_research.runners import usdrubf_s7_3_chat_analysis_snapshot as snapshot


NOW = datetime(2026, 9, 7, 14, 58, 58, tzinfo=timezone.utc)


def block(records):
    columns = list(records[0]) if records else []
    return {"columns": columns, "data": [[r[c] for c in columns] for r in records]}


def documents(*, secid="BRV6", expiry="2026-10-01", published="2026-09-04"):
    security = dict(SECID=secid, BOARDID="RFUD", ASSETCODE="BR", SHORTNAME="fixture",
                    LASTTRADEDATE=expiry, LASTDELDATE=expiry, LOTVOLUME=10)
    description = dict(SECID=secid, ASSETCODE="BR", SHORTNAME="fixture",
                       GROUP="futures_forts", TYPE="futures", UNIT=brent.UNIT_TEXT,
                       FRSTTRADE="2026-02-19", LSTTRADE=expiry,
                       LSTDELDATE=expiry, LOTSIZE="10")
    board = dict(secid=secid, boardid="RFUD", engine="futures", market="forts",
                 history_from="2026-02-19", history_till=published)
    history = dict(SECID=secid, BOARDID="RFUD", ASSETCODE="BR", TRADEDATE=published,
                   OPEN=95.97, HIGH=96.59, LOW=93.26, CLOSE=95.88, SETTLEPRICE=95.70)
    return [
        {"securities": block([security])},
        {"description": block([dict(name=k, value=v) for k, v in description.items()]),
         "boards": block([board])},
        {"history": block([history]),
         "history.cursor": block([dict(INDEX=0, TOTAL=1, PAGESIZE=100)])},
    ]


def set_value(document, name, field, value, index=0):
    document[name]["data"][index][document[name]["columns"].index(field)] = value


def set_description(document, field, value):
    for row in document["description"]["data"]:
        if row[0] == field:
            row[1] = value
            return
    raise AssertionError(field)


def collect(docs=None, *, now=NOW, **kwargs):
    docs = documents() if docs is None else docs
    raw = [json.dumps(d, ensure_ascii=False).encode() for d in docs]
    calls = []

    def transport(url):
        calls.append(url)
        assert len(calls) <= 3, "backfill/retry or unrelated source request"
        assert urlsplit(url).hostname == "iss.moex.com"
        assert not any(word in url.lower() for word in ("calendar", "candles", "marketdata"))
        return raw[len(calls) - 1]

    data = brent.load_factual_brent(
        transport=transport, clock=kwargs.pop("clock", lambda: now),
        monotonic=kwargs.pop("monotonic", lambda: 0.0), **kwargs)
    return data, calls, raw


def component(data=None):
    data = collect()[0] if data is None else deepcopy(data)
    return {"status": "READY", "data_as_of": data["received_at"], "data": data}


def use_default_producers(monkeypatch, data=None):
    data = collect()[0] if data is None else deepcopy(data)
    monkeypatch.setattr(brent, "load_factual_brent", lambda: deepcopy(data))
    producers = dict(snapshot.default_producers())
    assert producers["oil"] is snapshot._oil_component
    for name in producers:
        if name != "oil":
            producers[name] = lambda now: snapshot.ProducedComponent({}, now)
    monkeypatch.setattr(snapshot, "default_producers", lambda: producers)
    return producers


def brent_row(context):
    return next(row for row in matrix.build(context)["rows"] if row["block_id"] == "brent")


def test_source_semantics_provenance_and_bounded_exact_date_requests():
    data, calls, raw = collect()
    assert len(calls) == 3
    assert data["source_id"] == "moex_brent_futures_daily"
    assert data["secid"] == "BRV6" and data["expiry"] == "2026-10-01"
    assert data["price"] == 95.88 and data["price"] != 95.70
    assert data["price_field"] == "CLOSE"
    assert data["quote_currency"] == "USD" and data["price_unit"] == "USD/barrel"
    assert data["contract_size_barrels"] == 10
    assert data["source_trade_date"] == data["source_history_till"] == "2026-09-04"
    assert data["data_as_of"] == data["received_at"] == NOW.isoformat()
    assert data["source_event_time"] is None and data["source_published_at"] is None
    assert data["current_market_session"] == "UNKNOWN"
    assert all(data[key] is False for key in brent.FALSE_FLAGS)
    assert all(data[key] is True for key in brent.FACTUAL_FLAGS)
    assert [p["raw_payload_sha256"] for p in data["provenance"]] == [
        hashlib.sha256(body).hexdigest() for body in raw]
    query = parse_qs(urlsplit(calls[-1]).query)
    assert query["from"] == query["till"] == ["2026-09-04"]
    assert "BRV6.json" in calls[-1]


def test_repeatable_and_not_hard_coded_to_brv6():
    assert collect()[0] == collect()[0]
    data, calls, _ = collect(documents(secid="BRX6", expiry="2026-11-02"))
    assert data["secid"] == "BRX6"
    assert all("BRV6" not in url for url in calls)


def test_selection_reuses_existing_selector(monkeypatch):
    original = brent.select_nearest_contract
    calls = []

    def selector(candidates, *, target_trade_date):
        calls.append(target_trade_date)
        return original(candidates, target_trade_date=target_trade_date)

    monkeypatch.setattr(brent, "select_nearest_contract", selector)
    collect()
    assert calls == [NOW.date()]


def test_expired_and_near_expiry_contracts_excluded_independent_of_order():
    docs = documents()
    base = dict(zip(docs[0]["securities"]["columns"], docs[0]["securities"]["data"][0]))
    expired = {**base, "SECID": "BRU6", "LASTTRADEDATE": "2026-09-01"}
    too_near = {**base, "SECID": "BRTEST", "LASTTRADEDATE": "2026-09-13"}
    next_contract = {**base, "SECID": "BRX6", "LASTTRADEDATE": "2026-11-02"}
    for records in ([next_contract, expired, base, too_near],
                    [too_near, base, expired, next_contract]):
        docs[0]["securities"] = block(records)
        assert collect(docs)[0]["secid"] == "BRV6"


def test_exact_seven_day_boundary_eligible():
    docs = documents(expiry="2026-09-14")
    assert collect(docs)[0]["expiry"] == "2026-09-14"


@pytest.mark.parametrize("value", [None, "", "not-a-date"])
def test_missing_or_malformed_expiry_fails_closed(value):
    docs = documents()
    set_value(docs[0], "securities", "LASTTRADEDATE", value)
    with pytest.raises(ValueError):
        collect(docs)


def test_tied_expiry_and_duplicate_security_fail_closed():
    for duplicate in (False, True):
        docs = documents()
        row = dict(zip(docs[0]["securities"]["columns"], docs[0]["securities"]["data"][0]))
        docs[0]["securities"] = block([row, {**row, "SECID": row["SECID"] if duplicate else "BRX6"}])
        with pytest.raises(ValueError):
            collect(docs)


@pytest.mark.parametrize("unit", [None, "RUB/barrel", "USD/contract", "points"])
def test_unit_is_proven_not_assumed(unit):
    docs = documents()
    set_description(docs[1], "UNIT", unit)
    with pytest.raises(ValueError, match="unit"):
        collect(docs)


@pytest.mark.parametrize("field,value", [
    ("LSTTRADE", "2026-10-02"), ("LSTDELDATE", "2026-10-02"),
    ("LOTSIZE", "100"), ("TYPE", "stock"), ("SECID", "BRX6"),
])
def test_identity_expiry_size_inconsistency_fails(field, value):
    docs = documents()
    set_description(docs[1], field, value)
    with pytest.raises(ValueError):
        collect(docs)


@pytest.mark.parametrize("price", [None, 0, -1, True, "95.88", float("nan"), float("inf")])
def test_invalid_close_never_falls_back_to_settlement(price):
    docs = documents()
    set_value(docs[2], "history", "CLOSE", price)
    with pytest.raises(ValueError):
        collect(docs)


def test_inconsistent_ohlc_fails():
    docs = documents()
    set_value(docs[2], "history", "LOW", 96.0)
    with pytest.raises(ValueError, match="OHLC"):
        collect(docs)


@pytest.mark.parametrize("kind", ["empty", "duplicate", "incomplete_cursor", "wrong_date", "wrong_id"])
def test_history_must_be_one_exact_complete_source_result(kind):
    docs = documents()
    if kind == "empty":
        docs[2]["history"]["data"] = []
    elif kind == "duplicate":
        docs[2]["history"]["data"] *= 2
    elif kind == "incomplete_cursor":
        set_value(docs[2], "history.cursor", "TOTAL", 2)
    elif kind == "wrong_date":
        set_value(docs[2], "history", "TRADEDATE", "2026-09-03")
    else:
        set_value(docs[2], "history", "SECID", "BRX6")
    with pytest.raises(ValueError):
        collect(docs)


@pytest.mark.parametrize("kind", ["description", "boards", "columns"])
def test_duplicate_metadata_or_schema_fails(kind):
    docs = documents()
    if kind == "columns":
        docs[0]["securities"]["columns"][1] = docs[0]["securities"]["columns"][0]
    else:
        docs[1][kind]["data"] *= 2
    with pytest.raises(ValueError):
        collect(docs)


@pytest.mark.parametrize("published", [None, "2026-09-07", "2026-09-08", "2026-02-18"])
def test_native_history_bound_not_fabricated_or_current_partial_day(published):
    with pytest.raises(ValueError):
        collect(documents(published=published))


def test_source_observed_dates_not_weekday_calendar_inference():
    sunday = datetime(2026, 9, 6, 12, tzinfo=timezone.utc)
    data, _, _ = collect(documents(published="2026-09-05"), now=sunday)
    assert data["source_trade_date"] == "2026-09-05"
    assert data["current_market_session"] == "UNKNOWN"
    assert data["latest_completed_session_proven"] is False
    assert data["historical_pit_eligible"] is False


def test_budget_and_clock_regression_fail():
    ticks = iter([0.0, 0.0, 7.0])
    with pytest.raises(ValueError, match="budget"):
        collect(monotonic=lambda: next(ticks))
    clock = iter([NOW, NOW, NOW - timedelta(seconds=1)])
    with pytest.raises(ValueError, match="regressed"):
        collect(clock=lambda: next(clock))


def test_midnight_selection_transition_fails():
    before = NOW.replace(hour=20, minute=59, second=59)
    clock = iter([before] * 6 + [before + timedelta(seconds=2)])
    with pytest.raises(ValueError, match="date changed"):
        collect(clock=lambda: next(clock))


@pytest.mark.parametrize("url", [
    "http://iss.moex.com/iss/x", "https://example.org/iss/x",
    "https://user:password@iss.moex.com/iss/x", "https://iss.moex.com/other/x",
])
def test_non_official_routes_refused_without_network(url):
    with pytest.raises(ValueError, match="route"):
        brent._fetch(url)


def test_receipt_age_and_selection_date_downgrade_only():
    original = component()
    unchanged = deepcopy(original)
    fresh = brent.reconcile_component(original, now=NOW + timedelta(minutes=20))
    assert brent.factual_usable(fresh)
    stale = brent.reconcile_component(original, now=NOW + timedelta(minutes=20, seconds=1))
    assert stale["status"] == "UNAVAILABLE"
    assert not brent.factual_usable(stale)
    assert not brent.factual_usable(brent.reconcile_component(stale, now=NOW))
    assert original == unchanged

    late = collect(now=NOW.replace(hour=20, minute=59, second=59))[0]
    shifted = brent.reconcile_component(component(late),
                                        now=NOW.replace(hour=21, minute=0, second=1))
    assert shifted["status"] == "UNAVAILABLE"
    assert "date recheck" in shifted["data"]["read_freshness_reason"]


@pytest.mark.parametrize("field,value", [
    ("action_authority", True), ("historical_pit_eligible", True),
    ("factual_authority", False), ("price", 95.7), ("price_field", "SETTLEPRICE"),
    ("price_unit", "RUB/barrel"), ("received_at", None),
])
def test_persisted_bad_gate_cannot_be_upgraded(field, value):
    original = component()
    original["data"][field] = value
    result = brent.reconcile_component(original, now=NOW)
    assert result["status"] == "UNAVAILABLE"
    assert all(result["data"][key] is False for key in brent.FACTUAL_FLAGS)
    assert all(result["data"][key] is False for key in brent.FALSE_FLAGS)


def test_canonical_default_integration_and_matrix_only_brent(monkeypatch):
    use_default_producers(monkeypatch)
    result = snapshot.build_snapshot(now=NOW)
    assert result["schema_version"] == "rub_chat_analysis_snapshot.v1"
    assert brent.factual_usable(result["components"]["oil"])
    assert result["analysis_views"]["oil_component_ref"] == "oil"
    assert result["authority"]["broker_execution"] is False
    inventory = matrix.build(result)
    row = brent_row(result)
    assert row["collection_present"] and row["usable_for_full_forecast"]
    assert row["factual_context_usable"] and row["intraday_fresh"] is False
    assert all(inventory[key] is False for key in
               ("data_acceptance_complete", "analysis_ready", "model_validated", "training_authorized"))
    assert all(not row["collection_present"] for row in inventory["rows"]
               if row["block_id"] in {"wti", "urals"})


def test_source_failure_unavailable_and_no_governance_placeholder_retention(monkeypatch):
    producers = use_default_producers(monkeypatch)

    def failure():
        raise TimeoutError("fixture source unavailable")

    monkeypatch.setattr(brent, "load_factual_brent", failure)
    old = {"components": {"oil": {"status": "GOVERNED_BLOCKED",
                                  "data": {"reason": "not accepted"}}}}
    result = snapshot.build_snapshot(now=NOW, previous=old)
    assert result["components"]["oil"]["status"] == "UNAVAILABLE"
    assert result["components"]["oil"]["data"] is None
    assert not brent_row(result)["usable_for_full_forecast"]
    assert result["readiness"]["status"] == "PARTIAL"


def test_retention_preserves_price_receipt_not_current_authority(monkeypatch):
    use_default_producers(monkeypatch)
    previous = snapshot.build_snapshot(now=NOW)
    untouched = deepcopy(previous)

    def failure():
        raise TimeoutError("fixture source unavailable")

    monkeypatch.setattr(brent, "load_factual_brent", failure)
    for age in (10, 30):
        result = snapshot.build_snapshot(now=NOW + timedelta(minutes=age), previous=previous)
        oil = result["components"]["oil"]
        assert oil["status"] == "RETAINED_PREVIOUS"
        assert oil["data"]["price"] == 95.88
        assert oil["data"]["received_at"] == NOW.isoformat()
        assert not brent.factual_usable(oil)
        assert all(oil["data"][key] is False for key in brent.FACTUAL_FLAGS)
        assert not brent_row(result)["usable_for_full_forecast"]
        previous = result
    assert untouched["components"]["oil"]["data"]["consumer_factual_use_allowed"] is True


def test_final_collection_stamp_does_not_renew_source_receipt(monkeypatch):
    use_default_producers(monkeypatch)
    result = snapshot.build_snapshot(now=NOW)
    completed = NOW + timedelta(minutes=21)
    snapshot.finalize_snapshot_timing(result, started=NOW, completed=completed)
    assert result["identity"]["generated_at_utc"] == completed.isoformat()
    assert result["components"]["oil"]["data"]["received_at"] == NOW.isoformat()
    assert result["components"]["oil"]["status"] == "UNAVAILABLE"
    assert result["readiness"]["component_statuses"]["oil"] == "UNAVAILABLE"
    assert "oil" in result["readiness"]["unavailable_components"]


def test_receipt_after_collection_start_uses_completion_for_age(monkeypatch):
    data = collect(now=NOW + timedelta(seconds=43))[0]
    use_default_producers(monkeypatch, data)
    result = snapshot.build_snapshot(now=NOW)
    snapshot.finalize_snapshot_timing(result, started=NOW, completed=NOW + timedelta(seconds=44))
    assert brent.factual_usable(result["components"]["oil"])
    assert result["components"]["oil"]["data"]["receipt_age_seconds"] == 1


def test_matrix_rechecks_read_reference_not_file_generation():
    result = {"identity": {"generated_at_utc": NOW.isoformat()},
              "components": {"oil": component()},
              "live_read_freshness": {"read_at_utc": (NOW + timedelta(minutes=21)).isoformat()}}
    original = deepcopy(result)
    row = brent_row(result)
    assert row["collection_present"] and not row["usable_for_full_forecast"]
    assert result == original


def test_ready_without_acceptance_is_not_a_matrix_promotion():
    result = {"identity": {"generated_at_utc": NOW.isoformat()},
              "components": {"oil": {"status": "READY", "data": {"price": 95.88}}}}
    assert not brent_row(result)["usable_for_full_forecast"]
    result["components"]["oil"] = {"status": "UNAVAILABLE", "data": None}
    assert not brent_row(result)["collection_present"]


def test_canonical_reader_downgrades_without_fetch_or_file_write(monkeypatch, tmp_path):
    use_default_producers(monkeypatch)
    monkeypatch.setenv("MOEX_DATA_ROOT", str(tmp_path))
    monkeypatch.setattr(snapshot, "install_timestamp_policy", lambda: None)
    _, path = snapshot.refresh_snapshot(now_fn=lambda: NOW)
    original = path.read_bytes()

    def forbidden():
        raise AssertionError("reader must not fetch")

    monkeypatch.setattr(brent, "load_factual_brent", forbidden)
    result, read_path = snapshot.read_current_snapshot(
        now_fn=lambda: NOW + timedelta(minutes=21))
    assert read_path == path and path.read_bytes() == original
    assert result["components"]["oil"]["status"] == "UNAVAILABLE"
    assert not brent.factual_usable(result["components"]["oil"])
    assert result["live_read_freshness"]["additional_live_fetch_performed"] is False


def test_shared_reader_preserves_unrelated_factual_components():
    result = {"components": {"oil": component(), "official_news": {"status": "GOVERNED_BLOCKED",
                                                                   "data": {"marker": "unchanged"}}},
              "readiness": {"status": "READY"}}
    original = deepcopy(result)
    view = apply_read_freshness(result, now=NOW + timedelta(minutes=21))
    assert view["components"]["oil"]["status"] == "UNAVAILABLE"
    assert view["components"]["official_news"] == original["components"]["official_news"]
    assert result == original
