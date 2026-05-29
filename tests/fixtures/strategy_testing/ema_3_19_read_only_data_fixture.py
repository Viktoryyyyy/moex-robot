from __future__ import annotations

from moex_research.runners.data_binding import (
    ReadOnlyDataBinding,
    ReadOnlyDataRequest,
    ReadOnlyDatasetSchema,
)

EMA_3_19_READ_ONLY_ROWS = (
    {"timestamp": "2026-01-05", "instrument_id": "SYNTH_FIXTURE", "close": 100.0},
    {"timestamp": "2026-01-06", "instrument_id": "SYNTH_FIXTURE", "close": 101.0},
    {"timestamp": "2026-01-07", "instrument_id": "SYNTH_FIXTURE", "close": 102.5},
    {"timestamp": "2026-01-08", "instrument_id": "SYNTH_FIXTURE", "close": 101.5},
    {"timestamp": "2026-01-09", "instrument_id": "SYNTH_FIXTURE", "close": 103.0},
)

EMA_3_19_READ_ONLY_BINDING = ReadOnlyDataBinding(
    binding_id="ema_3_19.read_only.binding.v1",
    dataset_ref_id="dataset.ema_3_19.fixture.v1",
    strategy_id="ema_3_19",
    strategy_test_id="ema_3_19.strategy_test.fixture.v1",
    artifact_class="temporary_test_path",
    artifact_ref="test_fixture://ema_3_19/read_only_rows.v1",
    schema_ref="schema.ema_3_19.read_only.v1",
    read_mode="test_fixture_read_only",
)

EMA_3_19_READ_ONLY_SCHEMA = ReadOnlyDatasetSchema(
    schema_id="schema.ema_3_19.read_only.v1",
    schema_version="read_only_schema.v1",
    required_columns=("timestamp", "instrument_id", "close"),
    timestamp_column="timestamp",
    instrument_column="instrument_id",
    price_columns=("close",),
)

EMA_3_19_READ_ONLY_REQUEST = ReadOnlyDataRequest(
    request_id="ema_3_19.read_only.request.v1",
    strategy_id="ema_3_19",
    strategy_test_id="ema_3_19.strategy_test.fixture.v1",
    dataset_binding=EMA_3_19_READ_ONLY_BINDING,
    dataset_schema=EMA_3_19_READ_ONLY_SCHEMA,
    read_mode="test_fixture_read_only",
)

__all__ = [
    "EMA_3_19_READ_ONLY_BINDING",
    "EMA_3_19_READ_ONLY_REQUEST",
    "EMA_3_19_READ_ONLY_ROWS",
    "EMA_3_19_READ_ONLY_SCHEMA",
]
