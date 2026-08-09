from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
import json
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Callable, Iterable, Mapping, Sequence
from urllib.request import Request, urlopen

from ..external_data.registry import SOURCE_REGISTRY
from .usdrubf_news_macro import (
    MacroObservation,
    MacroState,
    NewsEvent,
    NewsPipelineResult,
    NewsSourceRecord,
    build_macro_state,
    process_news_batch,
)


_ALLOWED_PROVIDER_KINDS = {"NEWS", "MACRO"}
_ALLOWED_SOURCE_TIERS = {
    "OFFICIAL_PRIMARY",
    "OFFICIAL_SECONDARY",
    "MAJOR_AGENCY_OR_FINANCIAL_MEDIA",
}


class RuntimeIntegrationError(ValueError):
    """Raised when runtime wiring violates the integration boundary."""


@dataclass(frozen=True)
class ProviderBinding:
    source_id: str
    provider_kind: str
    source_tier: str
    official_routes: tuple[str, ...]
    availability_policy: str
    source_status: str

    def __post_init__(self) -> None:
        if not isinstance(self.source_id, str) or not self.source_id.strip():
            raise RuntimeIntegrationError("source_id must be non-empty")
        if self.provider_kind not in _ALLOWED_PROVIDER_KINDS:
            raise RuntimeIntegrationError("invalid provider_kind")
        if self.source_tier not in _ALLOWED_SOURCE_TIERS:
            raise RuntimeIntegrationError("invalid source_tier")
        if not self.official_routes or any(
            not isinstance(route, str) or not route.startswith("https://")
            for route in self.official_routes
        ):
            raise RuntimeIntegrationError("official_routes must be non-empty HTTPS routes")
        if not isinstance(self.availability_policy, str) or not self.availability_policy.strip():
            raise RuntimeIntegrationError("availability_policy must be non-empty")
        if not isinstance(self.source_status, str) or not self.source_status.strip():
            raise RuntimeIntegrationError("source_status must be non-empty")


class ProviderRegistry:
    def __init__(self, providers: Iterable[ProviderBinding]) -> None:
        items = tuple(providers)
        by_id = {item.source_id: item for item in items}
        if len(by_id) != len(items):
            raise RuntimeIntegrationError("duplicate provider source_id")
        self._providers = by_id

    def require(self, source_id: str, provider_kind: str) -> ProviderBinding:
        try:
            provider = self._providers[source_id]
        except KeyError as exc:
            raise RuntimeIntegrationError(f"unregistered provider: {source_id}") from exc
        if provider.provider_kind != provider_kind:
            raise RuntimeIntegrationError(
                f"provider kind mismatch for {source_id}: expected {provider_kind}"
            )
        return provider

    @property
    def providers(self) -> tuple[ProviderBinding, ...]:
        return tuple(self._providers[key] for key in sorted(self._providers))


def provider_from_external_registry(
    source_id: str,
    *,
    provider_kind: str = "MACRO",
    source_tier: str = "OFFICIAL_PRIMARY",
) -> ProviderBinding:
    try:
        definition = SOURCE_REGISTRY[source_id]
    except KeyError as exc:
        raise RuntimeIntegrationError(f"unknown external_data source: {source_id}") from exc
    return ProviderBinding(
        source_id=definition.source_id,
        provider_kind=provider_kind,
        source_tier=source_tier,
        official_routes=definition.official_routes,
        availability_policy=definition.availability_policy,
        source_status=definition.historical_model_use_status,
    )


@dataclass(frozen=True)
class FlowiseTransportConfig:
    endpoint: str
    request_field: str
    response_field: str
    timeout_seconds: float = 20.0

    def __post_init__(self) -> None:
        if not isinstance(self.endpoint, str) or not self.endpoint.startswith("https://"):
            raise RuntimeIntegrationError("Flowise endpoint must be explicit HTTPS URL")
        if not isinstance(self.request_field, str) or not self.request_field.strip():
            raise RuntimeIntegrationError("request_field must be non-empty")
        if not isinstance(self.response_field, str) or not self.response_field.strip():
            raise RuntimeIntegrationError("response_field must be non-empty")
        if self.timeout_seconds <= 0:
            raise RuntimeIntegrationError("timeout_seconds must be positive")


class FlowiseJsonAdapter:
    """Bounded JSON-over-HTTP adapter.

    Endpoint and envelope field names are explicit runtime configuration. This
    module does not guess a Flowise endpoint or assume a repository-global schema.
    """

    def __init__(
        self,
        config: FlowiseTransportConfig,
        *,
        opener: Callable[..., object] = urlopen,
    ) -> None:
        self._config = config
        self._opener = opener

    def __call__(self, payload: Mapping[str, object]) -> Mapping[str, object]:
        serialized_payload = json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        request_body = json.dumps(
            {self._config.request_field: serialized_payload},
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8")
        request = Request(
            self._config.endpoint,
            data=request_body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            response = self._opener(request, timeout=self._config.timeout_seconds)
            raw = response.read()
        except Exception as exc:
            raise RuntimeIntegrationError("Flowise transport failed") from exc

        if not isinstance(raw, (bytes, bytearray)):
            raise RuntimeIntegrationError("Flowise response body must be bytes")
        try:
            envelope = json.loads(bytes(raw).decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise RuntimeIntegrationError("Flowise response is not valid UTF-8 JSON") from exc
        if not isinstance(envelope, Mapping):
            raise RuntimeIntegrationError("Flowise response envelope must be a mapping")
        if self._config.response_field not in envelope:
            raise RuntimeIntegrationError("Flowise response field missing")
        value = envelope[self._config.response_field]
        if isinstance(value, str):
            try:
                value = json.loads(value)
            except json.JSONDecodeError as exc:
                raise RuntimeIntegrationError(
                    "Flowise configured response field is not JSON"
                ) from exc
        if not isinstance(value, Mapping):
            raise RuntimeIntegrationError(
                "Flowise configured response field must decode to a mapping"
            )
        return dict(value)


class JsonSnapshotStore:
    """Atomic snapshot persistence under an explicit caller-supplied root."""

    def __init__(
        self,
        root: Path | str,
        *,
        news_filename: str = "news_events.json",
        macro_filename: str = "macro_state.json",
    ) -> None:
        self.root = Path(root)
        self.news_filename = self._validate_filename(news_filename)
        self.macro_filename = self._validate_filename(macro_filename)

    @staticmethod
    def _validate_filename(value: str) -> str:
        if not isinstance(value, str) or not value.strip():
            raise RuntimeIntegrationError("snapshot filename must be a plain basename")
        path = Path(value)
        if path.name != value or value in {".", ".."}:
            raise RuntimeIntegrationError("snapshot filename must be a plain basename")
        return value

    def _write_atomic(self, filename: str, payload: object) -> Path:
        self.root.mkdir(parents=True, exist_ok=True)
        target = self.root / filename
        serialized = json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        with NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=self.root,
            prefix=f".{filename}.",
            suffix=".tmp",
            delete=False,
        ) as handle:
            handle.write(serialized)
            handle.flush()
            temp_path = Path(handle.name)
        temp_path.replace(target)
        return target

    def save_news_events(self, events: Sequence[NewsEvent]) -> Path:
        return self._write_atomic(
            self.news_filename,
            [asdict(event) for event in events],
        )

    def save_macro_state(self, state: MacroState) -> Path:
        payload = asdict(state)
        payload["observations"] = [
            {
                **asdict(item),
                "observed_or_effective_at": item.observed_or_effective_at.isoformat(),
                "published_at": item.published_at.isoformat(),
                "available_at": item.available_at.isoformat(),
                "ingested_at": item.ingested_at.isoformat(),
            }
            for item in state.observations
        ]
        return self._write_atomic(self.macro_filename, payload)


@dataclass(frozen=True)
class RuntimeResult:
    news: NewsPipelineResult
    macro: MacroState
    news_snapshot_path: Path
    macro_snapshot_path: Path


class NewsMacroRuntime:
    def __init__(
        self,
        *,
        provider_registry: ProviderRegistry,
        news_classifier: Callable[[Mapping[str, object]], Mapping[str, object]],
        macro_interpreter: Callable[[Mapping[str, object]], Mapping[str, object]] | None,
        store: JsonSnapshotStore,
    ) -> None:
        self._provider_registry = provider_registry
        self._news_classifier = news_classifier
        self._macro_interpreter = macro_interpreter
        self._store = store

    def run(
        self,
        *,
        news_records: Iterable[NewsSourceRecord],
        macro_observations: Iterable[MacroObservation],
        as_of_timestamp: datetime | str,
        prior_clusters: Mapping[str, Sequence[str]] | None = None,
        prior_event_history: Mapping[str, Sequence[Mapping[str, object]]] | None = None,
    ) -> RuntimeResult:
        news_records_tuple = tuple(news_records)
        macro_observations_tuple = tuple(macro_observations)

        for item in news_records_tuple:
            self._provider_registry.require(item.source_id, "NEWS")
        for item in macro_observations_tuple:
            self._provider_registry.require(item.source_id, "MACRO")

        news_result = process_news_batch(
            news_records_tuple,
            as_of_timestamp=as_of_timestamp,
            classifier=self._news_classifier,
            prior_clusters=prior_clusters,
            prior_event_history=prior_event_history,
        )
        macro_state = build_macro_state(
            macro_observations_tuple,
            as_of_timestamp=as_of_timestamp,
            interpreter=self._macro_interpreter,
        )
        news_path = self._store.save_news_events(news_result.events)
        macro_path = self._store.save_macro_state(macro_state)
        return RuntimeResult(
            news=news_result,
            macro=macro_state,
            news_snapshot_path=news_path,
            macro_snapshot_path=macro_path,
        )
