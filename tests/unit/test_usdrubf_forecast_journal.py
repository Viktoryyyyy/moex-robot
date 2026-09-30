"""Synthetic fixtures only; no real forecast or production outcome is claimed."""
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from hashlib import sha256
import json
import os
from pathlib import Path
import subprocess
import sys

import pytest

from src.moex_research.intelligence.usdrubf_forecast_journal import (
    ForecastJournal, ForecastJournalError, decode, encode, timestamp,
)
from src.moex_research.intelligence.usdrubf_forecast_evaluation import (
    FACTS_VERSION, FORECAST_VERSION, evaluate, validate_forecast,
)

BASE = datetime(2026, 1, 1, 9, tzinfo=timezone.utc)


def at(minutes):
    return (BASE + timedelta(minutes=minutes)).isoformat()


def metadata(end=-1):
    return {"source_ref": "synthetic-fixture-only", "schema_version": "synthetic.v1",
            "code_revision": "fixture-v1", "data_as_of": at(end),
            "available_at": at(end), "received_at": at(end), "quality_limitations": []}


def scenario(**updates):
    item = {"id": "up", "direction": "BULLISH_USD", "activation": None,
            "confirmation": None, "targets": ["103"], "invalidation": "97"}
    item.update(updates)
    return item


def forecast(inputs=None, **updates):
    spec = {"schema_version": FORECAST_VERSION, "instrument": "USDRUBF",
            "contract": "USDRUBF", "issued_at": at(-1), "horizon_start": at(0),
            "horizon_end": at(15), "reference_price": "100", "reference_price_at": at(-1),
            "bias": "BULLISH_USD", "neutral_band_bps": "0",
            "range": {"lower": "97", "upper": "104"}, "method_version": "synthetic-method-v1",
            "inputs": inputs or [{"kind": "input", "id": "fixture", "sha256": "a" * 64}],
            "observation_grid": [[at(n), at(n + 5)] for n in (0, 5, 10)],
            "scenarios": [scenario()], "supersedes": None, "revision_reason": None}
    spec.update(updates)
    return spec


def facts(prices=None):
    values = prices or [("100", "102", "99", "101"),
                        ("101", "104", "100", "103"), ("103", "104", "102", "103")]
    return {"schema_version": FACTS_VERSION, "instrument": "USDRUBF", "contract": "USDRUBF",
            "bars": [dict(zip(("open", "high", "low", "close"), row),
                         open_at=at(i * 5), close_at=at(i * 5 + 5))
                     for i, row in enumerate(values)]}


def score(spec=None, data=None, **kwargs):
    return evaluate(spec or forecast(), data or facts(),
                    evaluated_at=kwargs.get("evaluated_at", BASE + timedelta(minutes=20)),
                    source_as_of=kwargs.get("source_as_of", BASE + timedelta(minutes=15)),
                    limitations=kwargs.get("limitations", []))


def registered(tmp_path, *, late=False):
    clock = [BASE]
    journal = ForecastJournal(tmp_path / "journal", clock=lambda: clock[0])
    source_bytes = b'{ "synthetic": true, "value": 100 }\n'
    source = journal.capture("input", source_bytes, metadata())
    if late:
        clock[0] = BASE + timedelta(minutes=30)
    ref = journal.register("forecast", forecast([source]))
    return journal, ref, clock, source_bytes


def test_complete_round_trip_and_restart(tmp_path):
    journal, ref, clock, source_bytes = registered(tmp_path)
    source_ref = journal.read(ref)["payload"]["inputs"][0]
    captured = journal.read(source_ref)["payload"]
    assert journal.object_bytes(captured["object_sha256"]) == source_bytes
    clock[0] = BASE + timedelta(minutes=20)
    result = journal.evaluate("evaluation", ref, encode(facts()), metadata(15))
    other = ForecastJournal(journal.root)
    record = other.read(result)["payload"]
    assert record["registration_class"] == "PROSPECTIVE_LOCAL"
    assert len(record["evaluator_sha256"]) == 64
    report = other.reproduce(result)
    assert report["direction"]["correct"] is True
    assert report["range"]["status"] == "CONTAINED"
    assert report["scenarios"][0]["status"] == "TARGETS_REACHED"


def test_idempotent_retry_does_not_move_original_clock(tmp_path):
    journal, ref, clock, _ = registered(tmp_path)
    original = journal.read(ref)
    clock[0] = BASE + timedelta(days=10)
    assert journal.register("forecast", original["payload"]) == ref
    assert journal.read(ref) == original
    changed = deepcopy(original["payload"])
    changed["bias"] = "BEARISH_USD"
    with pytest.raises(ForecastJournalError, match="different content"):
        journal.register("forecast", changed)
    assert journal.read(ref) == original


def test_retrospective_import_cannot_be_upgraded(tmp_path):
    journal, ref, clock, _ = registered(tmp_path, late=True)
    output = journal.evaluate("evaluation", ref, encode(facts()), metadata(15))
    assert journal.read(output)["payload"]["registration_class"] == "RETROSPECTIVE"


def test_revision_preserves_original_and_requires_reason(tmp_path):
    journal, ref, clock, _ = registered(tmp_path)
    original = journal.read(ref)
    spec = deepcopy(original["payload"])
    spec.update(supersedes=ref, revision_reason="Changed hypothesis", bias="NEUTRAL")
    newer = journal.register("revision-2", spec)
    assert journal.read(newer)["payload"]["supersedes"] == ref
    assert journal.read(ref) == original
    spec["revision_reason"] = None
    with pytest.raises(ForecastJournalError):
        journal.register("revision-3", spec)


@pytest.mark.parametrize("kind", ["object", "forecast"])
def test_hash_tampering_is_detected(tmp_path, kind):
    journal, ref, clock, _ = registered(tmp_path)
    if kind == "forecast":
        path = journal._path("forecast", "forecast")
        path.chmod(0o644)
        path.write_bytes(path.read_bytes() + b" ")
        with pytest.raises(ForecastJournalError, match="hash mismatch"):
            journal.read(ref)
    else:
        input_ref = journal.read(ref)["payload"]["inputs"][0]
        key = journal.read(input_ref)["payload"]["object_sha256"]
        path = journal.root / "objects" / key
        path.chmod(0o644)
        path.write_bytes(b"changed")
        clock[0] = BASE + timedelta(minutes=20)
        with pytest.raises(ForecastJournalError, match="hash mismatch"):
            journal.evaluate("evaluation", ref, encode(facts()), metadata(15))


def test_concurrent_identical_capture(tmp_path):
    root = tmp_path / "journal"
    def put(i):
        journal = ForecastJournal(root, clock=lambda: BASE + timedelta(seconds=i))
        return journal.capture("one", b'{"x":1}', metadata())
    with ThreadPoolExecutor(max_workers=8) as pool:
        refs = list(pool.map(put, range(8)))
    assert len({ref["sha256"] for ref in refs}) == 1


def test_concurrent_conflicting_capture_has_one_winner(tmp_path):
    journal = ForecastJournal(tmp_path / "journal", clock=lambda: BASE)
    def put(i):
        try:
            return journal.capture("one", encode({"x": i}), metadata())
        except ForecastJournalError:
            return None
    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(put, range(8)))
    assert sum(result is not None for result in results) == 1


def test_symlink_root_and_record_rejected(tmp_path):
    actual = tmp_path / "actual"
    actual.mkdir()
    link = tmp_path / "link"
    link.symlink_to(actual)
    with pytest.raises(ForecastJournalError, match="symlink"):
        ForecastJournal(link)
    journal, ref, _, _ = registered(tmp_path)
    path = journal._path("forecast", "forecast")
    target = tmp_path / "target.json"
    target.write_bytes(path.read_bytes())
    path.unlink()
    path.symlink_to(target)
    with pytest.raises(OSError):
        journal.read(ref)


@pytest.mark.parametrize("data", [b'{"x":1,"x":2}', b'{"x":NaN}', b'{"x":1e999}', b'[] invalid', b"\xff"])
def test_strict_json(data):
    with pytest.raises(ForecastJournalError):
        decode(data)


def test_source_future_and_forecast_future_rejected(tmp_path):
    journal = ForecastJournal(tmp_path / "journal", clock=lambda: BASE)
    with pytest.raises(ForecastJournalError):
        journal.capture("future", b"{}", metadata(1))
    ref = journal.capture("current", b"{}", metadata(0))
    with pytest.raises(ForecastJournalError, match="future input"):
        journal.register("forecast", forecast([ref]))


@pytest.mark.parametrize("change", [
    {"reference_price": True}, {"reference_price": float("nan")},
    {"reference_price": "1e999"}, {"reference_price": "0"},
    {"neutral_band_bps": "-1"}, {"neutral_band_bps": "10001"},
    {"issued_at": "2026-01-01T09:00:00"}, {"issued_at": at(1)},
    {"horizon_end": at(0)}, {"contract": "Si"},
    {"range": {"lower": "104", "upper": "97"}},
    {"observation_grid": [[at(0), at(15)], [at(10), at(15)]]},
    {"bias": []}, {"confidence": 0.5},
])
def test_invalid_forecast(change):
    with pytest.raises(ForecastJournalError):
        validate_forecast(forecast(**change))


def test_horizon_must_be_completed():
    with pytest.raises(ForecastJournalError, match="has not finished"):
        score(evaluated_at=BASE + timedelta(minutes=14))


def test_missing_middle_bar_preserves_only_endpoint_direction():
    data = facts()
    del data["bars"][1]
    report = score(data=data)
    assert report["coverage"]["status"] == "PARTIAL"
    assert report["direction"]["correct"] is True
    assert report["range"]["status"] == "NOT_EVALUABLE"
    assert report["scenarios"][0]["status"] == "NOT_EVALUABLE"


def test_missing_terminal_bar_and_quality_limitations():
    data = facts()
    data["bars"].pop()
    assert score(data=data)["direction"]["status"] == "NOT_EVALUABLE"
    report = score(limitations=["unverified source coverage"])
    assert report["direction"]["correct"] is None
    assert report["scenarios"][0]["status"] == "NOT_EVALUABLE"


@pytest.mark.parametrize("fault", ["duplicate", "foreign", "future", "unordered", "ohlc", "extra"])
def test_invalid_facts(fault):
    data = facts()
    if fault == "duplicate":
        data["bars"].append(deepcopy(data["bars"][0]))
    elif fault == "foreign":
        data["instrument"] = "CNYRUBF"
    elif fault == "future":
        data["bars"][-1]["close_at"] = at(20)
    elif fault == "unordered":
        data["bars"].reverse()
    elif fault == "ohlc":
        data["bars"][0]["low"] = "500"
    else:
        data["future_labels"] = {}
    with pytest.raises(ForecastJournalError):
        score(data=data)


def test_bar_must_close_by_source_as_of():
    with pytest.raises(ForecastJournalError, match="after source"):
        score(source_as_of=BASE + timedelta(minutes=14))


def test_no_fabricated_probability_or_position():
    report = score(spec=forecast(bias=None, range=None, scenarios=[]))
    assert report["direction"]["correct"] is None
    assert report["direction"]["status"] == "NOT_REQUESTED"
    assert "confidence" not in encode(report).decode()
    assert "trade_state" not in encode(report).decode()


@pytest.mark.parametrize("bias,close,band", [
    ("BULLISH_USD", "101", "0"), ("BEARISH_USD", "99", "0"), ("NEUTRAL", "101", "100"),
])
def test_direction_and_neutral_boundary(bias, close, band):
    data = facts()
    data["bars"][-1].update(open="100", low="98", high="102", close=close)
    report = score(spec=forecast(bias=bias, neutral_band_bps=band), data=data)
    assert report["direction"]["correct"] is True


@pytest.mark.parametrize("direction,stop,target,ohlc", [
    ("BULLISH_USD", "97", "103", ("100", "104", "96", "100")),
    ("BEARISH_USD", "103", "97", ("100", "104", "96", "100")),
])
def test_same_bar_order_is_unknown(direction, stop, target, ohlc):
    spec = forecast(scenarios=[scenario(direction=direction, invalidation=stop, targets=[target])])
    report = score(spec=spec, data=facts([ohlc, ohlc, ohlc]))
    outcome = report["scenarios"][0]
    assert outcome["status"] == "AMBIGUOUS"
    assert outcome["targets"][0]["status"] == "UNKNOWN_ORDER"


def test_multiple_targets_and_later_invalidation():
    spec = forecast(scenarios=[scenario(targets=["102", "110"])])
    data = facts([("100", "103", "99", "101"), ("101", "104", "96", "98"), ("98", "99", "97", "98")])
    result = score(spec=spec, data=data)["scenarios"][0]
    assert [item["status"] for item in result["targets"]] == ["TARGET_FIRST", "INVALIDATION_FIRST"]
    assert result["status"] == "INVALIDATED"


def test_targets_before_later_invalidation():
    data = facts([("100", "104", "99", "101"), ("101", "104", "96", "98"), ("98", "99", "97", "98")])
    assert score(data=data)["scenarios"][0]["status"] == "TARGETS_BEFORE_INVALIDATION"


def test_activation_confirmation_and_intrabar_no_lookahead():
    spec = forecast(scenarios=[scenario(
        activation={"op": "GE", "level": "101", "closes": 1},
        confirmation={"op": "GE", "level": "101", "closes": 1})])
    data = facts([("100", "104", "99", "101"), ("101", "104", "100", "101"), ("101", "104", "100", "103")])
    result = score(spec=spec, data=data)["scenarios"][0]
    assert result["activated_at"] == at(5)
    assert result["confirmed_at"] == at(10)
    assert result["targets"][0]["first_touch_bar"] == [at(10), at(15)]


@pytest.mark.parametrize("closes,status", [(4, "NOT_ACTIVATED"), (3, "ACTIVE_AT_HORIZON_END")])
def test_consecutive_closes_and_not_activated_are_not_wins(closes, status):
    spec = forecast(scenarios=[scenario(targets=["110"], activation={"op": "GE", "level": "100", "closes": closes})])
    result = score(spec=spec)["scenarios"][0]
    assert result["status"] == status
    assert result["targets"][0]["status"] != "TARGET_FIRST"


def test_confirmation_requires_later_close():
    spec = forecast(scenarios=[scenario(targets=["110"],
        activation={"op": "GE", "level": "100", "closes": 3},
        confirmation={"op": "GE", "level": "100", "closes": 1})])
    assert score(spec=spec)["scenarios"][0]["status"] == "NOT_CONFIRMED"


def test_pre_activation_cancellation():
    spec = forecast(scenarios=[scenario(activation={"op": "GE", "level": "110", "closes": 1})])
    data = facts([("100", "104", "96", "100")] * 3)
    assert score(spec=spec, data=data)["scenarios"][0]["status"] == "CANCELLED_BEFORE_ACTIVATION"


def test_gap_past_target_at_confirmation_is_not_success():
    spec = forecast(scenarios=[scenario(activation={"op": "GE", "level": "103", "closes": 1})])
    assert score(spec=spec)["scenarios"][0]["status"] == "NOT_EVALUABLE"


def test_cli_round_trip_real_subprocess(tmp_path):
    root = Path(__file__).resolve().parents[2]
    env = dict(os.environ, PYTHONPATH=str(root))
    store = tmp_path / "store"
    source = tmp_path / "source.json"
    meta = tmp_path / "meta.json"
    spec_path = tmp_path / "spec.json"
    reference = tmp_path / "forecast-ref.json"
    actual = tmp_path / "facts.json"
    evaluation_ref = tmp_path / "evaluation-ref.json"
    source.write_bytes(b'{"synthetic":true}')
    meta.write_bytes(encode(metadata()))
    prefix = [sys.executable, "-m", "src.moex_research.intelligence.usdrubf_forecast_journal", "--root", str(store)]
    def run(*args):
        result = subprocess.run(prefix + list(args), cwd=root, env=env, capture_output=True, text=True, check=True)
        return json.loads(result.stdout)
    captured = run("capture", "--id", "cli-input", "--source", str(source), "--metadata", str(meta))
    spec_path.write_bytes(encode(forecast([captured])))
    registered_ref = run("register", "--id", "cli-forecast", "--spec", str(spec_path))
    reference.write_bytes(encode(registered_ref))
    meta.write_bytes(encode(metadata(15)))
    actual.write_bytes(encode(facts()))
    evaluated_ref = run("evaluate", "--id", "cli-evaluation", "--forecast-ref", str(reference),
                        "--facts", str(actual), "--metadata", str(meta))
    evaluation_ref.write_bytes(encode(evaluated_ref))
    result = run("reproduce", "--evaluation-ref", str(evaluation_ref))
    assert result["direction"]["correct"] is True


def test_decimal_context_does_not_change_evaluation():
    from decimal import Inexact, ROUND_DOWN, localcontext
    spec = forecast(reference_price="99")
    expected = score(spec=spec)
    with localcontext() as context:
        context.prec = 3
        context.rounding = ROUND_DOWN
        context.traps[Inexact] = True
        assert score(spec=spec) == expected


def test_factual_evaluation_is_idempotent_and_conflicts_cannot_replace_it(tmp_path):
    journal, ref, clock, _ = registered(tmp_path)
    clock[0] = BASE + timedelta(minutes=20)
    saved = journal.evaluate("eval", ref, encode(facts()), metadata(15))
    clock[0] = BASE + timedelta(minutes=30)
    assert journal.evaluate("eval", ref, encode(facts()), metadata(15)) == saved
    with pytest.raises(ForecastJournalError, match="different content"):
        journal.evaluate("eval", ref, encode(facts([("100", "101", "99", "100")] * 3)), metadata(15))
    assert journal.reproduce(saved)["direction"]["correct"] is True


def test_registration_clock_is_sampled_after_validation(tmp_path):
    journal, ref, clock, _ = registered(tmp_path)
    spec = journal.read(ref)["payload"]
    values = iter([BASE, BASE + timedelta(minutes=1)])
    journal.clock = lambda: next(values)
    newer = journal.register("late-clock", spec)
    assert journal.read(newer)["recorded_at"] == at(1)


def test_input_receipt_after_issued_is_rejected_even_when_horizon_is_old(tmp_path):
    clock = BASE + timedelta(minutes=30)
    journal = ForecastJournal(tmp_path / "journal", clock=lambda: clock)
    ref = journal.capture("future-to-forecast", b'{}', metadata(20))
    with pytest.raises(ForecastJournalError, match="future input"):
        journal.register("retro", forecast([ref]))


def test_replay_checks_fact_bytes_and_evaluator_fingerprint(tmp_path):
    journal, ref, clock, _ = registered(tmp_path)
    clock[0] = BASE + timedelta(minutes=20)
    saved = journal.evaluate("eval", ref, encode(facts()), metadata(15))
    record = journal.read(saved)
    record["payload"]["evaluator_sha256"] = "0" * 64
    path = journal._path("evaluation", "eval")
    path.chmod(0o644)
    path.write_bytes(encode(record))
    forged_ref = dict(saved, sha256=sha256(encode(record)).hexdigest())
    with pytest.raises(ForecastJournalError, match="evaluator version"):
        journal.reproduce(forged_ref)


def test_invalid_reference_and_self_revision(tmp_path):
    journal, ref, _, _ = registered(tmp_path)
    spec = journal.read(ref)["payload"]
    broken = deepcopy(spec)
    broken["inputs"][0]["sha256"] = "b" * 64
    with pytest.raises(ForecastJournalError, match="hash mismatch"):
        journal.register("wrong-ref", broken)
    broken = deepcopy(spec)
    broken.update(supersedes=ref, revision_reason="Self")
    with pytest.raises(ForecastJournalError, match="distinct"):
        journal.register("forecast", broken)


def test_pre_confirmation_cancellation_is_not_a_trade_loss():
    spec = forecast(scenarios=[scenario(
        activation={"op": "GE", "level": "101", "closes": 1},
        confirmation={"op": "GE", "level": "102", "closes": 1})])
    data = facts([("100", "102", "99", "101"), ("101", "104", "96", "103"), ("103", "104", "102", "103")])
    result = score(spec=spec, data=data)["scenarios"][0]
    assert result["status"] == "CANCELLED_BEFORE_CONFIRMATION"
    assert result["targets"][0]["status"] == "NOT_APPLICABLE"


def test_activation_counter_resets():
    spec = forecast(scenarios=[scenario(targets=["110"],
        activation={"op": "GE", "level": "101", "closes": 2})])
    data = facts([("100", "102", "99", "101"), ("101", "102", "99", "100"), ("100", "102", "99", "101")])
    assert score(spec=spec, data=data)["scenarios"][0]["status"] == "NOT_ACTIVATED"


def test_short_target_and_missing_optional_scenario_fields():
    spec = forecast(scenarios=[scenario(direction="BEARISH_USD", targets=["99"], invalidation=None)])
    data = facts([("100", "101", "98", "99")] * 3)
    assert score(spec=spec, data=data)["scenarios"][0]["status"] == "TARGETS_REACHED"
    spec["scenarios"][0]["targets"] = []
    assert score(spec=spec, data=data)["scenarios"][0]["status"] == "ACTIVE_AT_HORIZON_END"


def test_missing_all_bars_is_not_zero_performance():
    data = facts()
    data["bars"] = []
    report = score(data=data)
    assert report["coverage"]["observed_bars"] == 0
    assert report["direction"]["correct"] is None
    assert report["direction"]["return_bps"] is None
    assert report["range"]["status"] == "NOT_EVALUABLE"


def test_same_instant_with_different_timezone_is_duplicate():
    data = facts()
    copy = deepcopy(data["bars"][0])
    copy.update(open_at="2026-01-01T12:00:00+03:00", close_at="2026-01-01T12:05:00+03:00")
    data["bars"].append(copy)
    with pytest.raises(ForecastJournalError, match="duplicate"):
        score(data=data)


def test_registrar_clock_rollback_fails_without_forecast_commit(tmp_path):
    journal, ref, _, _ = registered(tmp_path)
    spec = journal.read(ref)["payload"]
    values = iter([BASE, BASE - timedelta(seconds=1)])
    journal.clock = lambda: next(values)
    with pytest.raises(ForecastJournalError, match="backwards"):
        journal.register("rollback", spec)
    assert not journal._path("forecast", "rollback").exists()


@pytest.mark.parametrize("inputs", [[{}], ["wrong"], [{"kind": [], "id": "x", "sha256": "a"*64}]])
def test_invalid_input_reference_shapes(inputs):
    with pytest.raises(ForecastJournalError):
        validate_forecast(forecast(inputs=inputs))


@pytest.mark.parametrize("invalid_spec", [{}, {"schema_version": "wrong"}])
def test_cli_validation_errors_are_clean_and_do_not_commit(tmp_path, invalid_spec):
    root = Path(__file__).resolve().parents[2]
    spec_path = tmp_path / "invalid.json"
    spec_path.write_bytes(encode(invalid_spec))
    store = tmp_path / "store"
    result = subprocess.run(
        [sys.executable, "-m", "src.moex_research.intelligence.usdrubf_forecast_journal",
         "--root", str(store), "register", "--id", "invalid", "--spec", str(spec_path)],
        cwd=root, env=dict(os.environ, PYTHONPATH=str(root)),
        capture_output=True, text=True, check=False,
    )
    assert result.returncode == 2
    assert result.stdout == ""
    assert result.stderr.startswith("forecast journal: ")
    assert "Traceback" not in result.stderr
    assert not (store / "records" / "forecast.invalid.json").exists()
