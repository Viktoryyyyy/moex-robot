from __future__ import annotations

from pathlib import Path

import pytest

from moex_data.futures import futoi_live_factual_refresh_source_native as factual


def test_materialize_target_raw_run_id_is_instrument_scoped(monkeypatch, tmp_path: Path) -> None:
    captured_run_ids: list[str] = []

    monkeypatch.setattr(
        factual,
        "source_identity",
        lambda instrument_id: {
            "instrument_id": instrument_id,
            "source_id": factual.SOURCE_ID,
            "source_ticker": "si" if instrument_id == factual.SI_INSTRUMENT_ID else "cr",
            "secid": "SiU6" if instrument_id == factual.SI_INSTRUMENT_ID else "CRU6",
        },
    )

    def stop_after_capture(**kwargs):
        captured_run_ids.append(str(kwargs["run_id"]))
        raise RuntimeError("captured raw run id")

    monkeypatch.setattr(factual.materializer, "materialize_futoi_partition", stop_after_capture)

    for instrument_id in factual.LIVE_INSTRUMENT_IDS:
        with pytest.raises(RuntimeError, match="captured raw run id"):
            factual._materialize_target(
                tmp_path,
                "2026-08-28",
                "same_operational_run",
                instrument_id=instrument_id,
                timeout=1.0,
            )

    assert captured_run_ids == [
        "same_operational_run_si_futures_family_raw_20260828",
        "same_operational_run_cr_futures_family_raw_20260828",
    ]
    assert len(set(captured_run_ids)) == len(factual.LIVE_INSTRUMENT_IDS)
