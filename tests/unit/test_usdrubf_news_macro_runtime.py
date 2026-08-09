from datetime import datetime, timezone
import json
from pathlib import Path

import pytest

from src.moex_research.intelligence.usdrubf_news_macro import (
    MacroObservation,
    NewsSourceRecord,
)
from src.moex_research.intelligence.usdrubf_news_macro_runtime import (
    FlowiseJsonAdapter,
    FlowiseTransportConfig,
    JsonSnapshotStore,
    NewsMacroRuntime,
    ProviderBinding,
    ProviderRegistry,
    RuntimeIntegrationError,
    provider_from_external_registry,
)


T0 = datetime(2026, 8, 9, 10, 0, tzinfo=timezone.utc)
T1 = datetime(2026, 8, 9, 10, 1, tzinfo=timezone.utc)
T2 = datetime(2026, 8, 9, 10, 2, tzinfo=timezone.utc)


def _news_provider(source_id: str = "news_official") -> ProviderBinding:
    return ProviderBinding(
        source_id=source_id,
        provider_kind="NEWS",
        source_tier="OFFICIAL_PRIMARY",
        official_routes=("https://example.test/news",),
        availability_policy="usable only after available_at",
        source_status="test_fixture",
    )


def _macro_provider(source_id: str = "macro_official") -> ProviderBinding:
    return ProviderBinding(
        source_id=source_id,
        provider_kind="MACRO",
        source_tier="OFFICIAL_PRIMARY",
        official_routes=("https://example.test/macro",),
        availability_policy="usable only after available_at",
        source_status="test_fixture",
    )


def _news_record(source_id: str = "news_official") -> NewsSourceRecord:
    return NewsSourceRecord(
        source_id=source_id,
        source_tier="OFFICIAL_PRIMARY",
        source_reference="https://example.test/news/1",
        published_at=T0,
        available_at=T1,
        ingested_at=T2,
        headline="CBR signals unchanged rate",
    )


def _macro_observation(source_id: str = "macro_official") -> MacroObservation:
    return MacroObservation(
        metric_id="cbr_rate",
        source_id=source_id,
        source_reference="https://example.test/macro/1",
        value=18.0,
        unit="percent",
        observed_or_effective_at=T0,
        published_at=T0,
        available_at=T1,
        ingested_at=T2,
        quality_status="OK",
    )


def _news_classification() -> dict:
    return {
        "event_type": "MONETARY_POLICY",
        "entities": ["CBR"],
        "rub_relevance": 0.9,
        "direction": "USD_BEARISH",
        "importance": "HIGH",
        "novelty": "NEW",
        "horizon": "SHORT_TERM",
        "confidence": 0.8,
        "mechanism": "Rate expectations affect RUB carry.",
    }


def _macro_interpretation() -> dict:
    return {
        "overall_direction": "USD_BEARISH",
        "confidence": 0.7,
        "dominant_drivers": ["cbr_rate"],
    }


def test_provider_registry_reuses_existing_external_source_metadata() -> None:
    provider = provider_from_external_registry("cbr_key_rate_daily")
    assert provider.source_id == "cbr_key_rate_daily"
    assert provider.provider_kind == "MACRO"
    assert provider.source_tier == "OFFICIAL_PRIMARY"
    assert provider.official_routes == (
        "https://www.cbr.ru/eng/hd_base/ProcStav/IR_CHG_MPO/",
    )
    assert "effective_date" in provider.availability_policy


def test_provider_registry_fails_closed_on_unknown_source() -> None:
    registry = ProviderRegistry([_news_provider()])
    with pytest.raises(RuntimeIntegrationError, match="unregistered"):
        registry.require("unknown", "NEWS")


def test_runtime_rejects_blocked_external_provider_before_interpretation(tmp_path: Path) -> None:
    blocked_provider = provider_from_external_registry("cbr_banking_liquidity_daily")
    calls = []
    runtime = NewsMacroRuntime(
        provider_registry=ProviderRegistry([blocked_provider]),
        news_classifier=lambda _payload: _news_classification(),
        macro_interpreter=lambda payload: calls.append(payload) or _macro_interpretation(),
        store=JsonSnapshotStore(tmp_path),
    )
    with pytest.raises(RuntimeIntegrationError, match="blocked provider"):
        runtime.run(
            news_records=[],
            macro_observations=[_macro_observation("cbr_banking_liquidity_daily")],
            as_of_timestamp=T2,
        )
    assert calls == []


class _FakeResponse:
    def __init__(self, payload: object) -> None:
        self._raw = json.dumps(payload).encode("utf-8")

    def read(self) -> bytes:
        return self._raw


def test_flowise_adapter_uses_explicit_envelope_and_parses_json_field() -> None:
    captured = {}

    def opener(request, timeout):
        captured["url"] = request.full_url
        captured["timeout"] = timeout
        captured["body"] = json.loads(request.data.decode("utf-8"))
        return _FakeResponse({"text": json.dumps(_news_classification())})

    adapter = FlowiseJsonAdapter(
        FlowiseTransportConfig(
            endpoint="https://flowise.example.test/classify",
            request_field="question",
            response_field="text",
            timeout_seconds=7.5,
        ),
        opener=opener,
    )
    result = adapter({"instrument": "USDRUBF", "cluster_id": "c1"})

    assert result["direction"] == "USD_BEARISH"
    assert captured["url"] == "https://flowise.example.test/classify"
    assert captured["timeout"] == 7.5
    nested = json.loads(captured["body"]["question"])
    assert nested["instrument"] == "USDRUBF"
    assert nested["cluster_id"] == "c1"


def test_flowise_adapter_does_not_guess_or_allow_plain_http_endpoint() -> None:
    with pytest.raises(RuntimeIntegrationError, match="HTTPS"):
        FlowiseTransportConfig(
            endpoint="http://flowise.example.test/classify",
            request_field="question",
            response_field="text",
        )


def test_flowise_adapter_fails_closed_on_invalid_response_shape() -> None:
    adapter = FlowiseJsonAdapter(
        FlowiseTransportConfig(
            endpoint="https://flowise.example.test/classify",
            request_field="question",
            response_field="text",
        ),
        opener=lambda *_args, **_kwargs: _FakeResponse({"text": "not-json"}),
    )
    with pytest.raises(RuntimeIntegrationError, match="not JSON"):
        adapter({"instrument": "USDRUBF"})


def test_snapshot_store_uses_only_explicit_root_and_atomic_files(tmp_path: Path) -> None:
    store = JsonSnapshotStore(tmp_path)
    path = store.save_news_events(())
    assert path == tmp_path / "news_events.json"
    assert json.loads(path.read_text(encoding="utf-8")) == []
    assert not list(tmp_path.glob("*.tmp"))


def test_snapshot_store_rejects_path_in_filename(tmp_path: Path) -> None:
    with pytest.raises(RuntimeIntegrationError, match="basename"):
        JsonSnapshotStore(tmp_path, news_filename="../news.json")


def test_snapshot_store_requires_distinct_filenames(tmp_path: Path) -> None:
    with pytest.raises(RuntimeIntegrationError, match="distinct"):
        JsonSnapshotStore(
            tmp_path,
            news_filename="state.json",
            macro_filename="state.json",
        )


def test_runtime_rejects_unregistered_source_before_classification(tmp_path: Path) -> None:
    called = []

    runtime = NewsMacroRuntime(
        provider_registry=ProviderRegistry([_macro_provider()]),
        news_classifier=lambda payload: called.append(payload) or _news_classification(),
        macro_interpreter=lambda _payload: _macro_interpretation(),
        store=JsonSnapshotStore(tmp_path),
    )

    with pytest.raises(RuntimeIntegrationError, match="unregistered"):
        runtime.run(
            news_records=[_news_record()],
            macro_observations=[_macro_observation()],
            as_of_timestamp=T2,
        )
    assert called == []


def test_runtime_wires_bounded_pipeline_and_persists_snapshots(tmp_path: Path) -> None:
    news_payloads = []
    macro_payloads = []

    runtime = NewsMacroRuntime(
        provider_registry=ProviderRegistry([_news_provider(), _macro_provider()]),
        news_classifier=lambda payload: news_payloads.append(payload) or _news_classification(),
        macro_interpreter=lambda payload: macro_payloads.append(payload) or _macro_interpretation(),
        store=JsonSnapshotStore(tmp_path),
    )
    result = runtime.run(
        news_records=[_news_record()],
        macro_observations=[_macro_observation()],
        as_of_timestamp=T2,
    )

    assert result.news.clusters_classified == 1
    assert result.news.events[0].source_id == "news_official"
    assert result.macro.dominant_drivers == ("cbr_rate",)
    assert len(news_payloads) == 1
    assert len(macro_payloads) == 1
    news_disk = json.loads(result.news_snapshot_path.read_text(encoding="utf-8"))
    macro_disk = json.loads(result.macro_snapshot_path.read_text(encoding="utf-8"))
    assert news_disk[0]["direction"] == "USD_BEARISH"
    assert "body" not in news_disk[0]
    assert macro_disk["observations"][0]["value"] == 18.0
    assert macro_disk["observations"][0]["available_at"] == T1.isoformat()


def test_runtime_enforces_provider_kind(tmp_path: Path) -> None:
    runtime = NewsMacroRuntime(
        provider_registry=ProviderRegistry([_news_provider("macro_official")]),
        news_classifier=lambda _payload: _news_classification(),
        macro_interpreter=lambda _payload: _macro_interpretation(),
        store=JsonSnapshotStore(tmp_path),
    )
    with pytest.raises(RuntimeIntegrationError, match="kind mismatch"):
        runtime.run(
            news_records=[],
            macro_observations=[_macro_observation()],
            as_of_timestamp=T2,
        )


def test_runtime_rejects_news_source_tier_mismatch(tmp_path: Path) -> None:
    runtime = NewsMacroRuntime(
        provider_registry=ProviderRegistry([_news_provider(), _macro_provider()]),
        news_classifier=lambda _payload: _news_classification(),
        macro_interpreter=lambda _payload: _macro_interpretation(),
        store=JsonSnapshotStore(tmp_path),
    )
    mismatched = NewsSourceRecord(
        source_id="news_official",
        source_tier="MAJOR_AGENCY_OR_FINANCIAL_MEDIA",
        source_reference="https://example.test/news/2",
        published_at=T0,
        available_at=T1,
        ingested_at=T2,
        headline="CBR rate update",
    )
    with pytest.raises(RuntimeIntegrationError, match="tier mismatch"):
        runtime.run(
            news_records=[mismatched],
            macro_observations=[_macro_observation()],
            as_of_timestamp=T2,
        )
