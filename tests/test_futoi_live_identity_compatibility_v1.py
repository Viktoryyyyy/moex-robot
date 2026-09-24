"""Repository-wide compatibility boundaries for issue #547.

This is a source-only inventory guard, not a production data probe.
New identity-sensitive consumers require explicit review before a live schema
change may be treated as complete. Historical v1 defaults remain protected.
"""
from pathlib import Path
import re

from moex_data.futures import materialize_futoi_instrument as materializer


# These concrete consumers were inspected in the repository before this guard
# was introduced. A match outside this set is a review requirement, not a
# reason to silently broaden v2 admission or to alter historical evidence.
REVIEWED_IDENTITY_CONSUMERS = frozenset({
    "src/moex_data/futures/materialize_futoi_instrument.py",
    "src/moex_data/futures/futoi_live_factual_refresh_source_native.py",
    "src/moex_data/futures/futoi_intraday_previous_session_context.py",
    "src/moex_data/futures/futoi_intraday_previous_session_context_fast.py",
    "src/moex_data/futures/futoi_delta_statistics_context.py",
    "src/moex_data/futures/futoi_publication_audit.py",
    "src/moex_data/rub_si_futoi_dated_context.py",
    "src/moex_data/rub_cr_futoi_dated_context.py",
    "src/moex_research/runners/usdrubf_s7_3_chat_analysis_snapshot_current_context.py",
    "src/moex_research/runners/usdrubf_s7_3_chat_analysis_snapshot_futoi.py",
})
IDENTITY_SYMBOL = re.compile(r"\b(?:source_identity|latest_aligned_factual|_materialize_target)\b")


def test_futoi_identity_consumer_inventory_is_explicit():
    root = Path(__file__).resolve().parents[1]
    assert (root / "src").is_dir(), "complete repository source checkout is required"
    unreviewed = {}
    for path in sorted((root / "src").rglob("*")):
        if not path.is_file() or path.suffix not in {".py", ".inc"}:
            continue
        text = path.read_text(encoding="utf-8")
        if "futoi" not in text.lower():
            continue
        matches = [f"{number}: {line.strip()}"
                   for number, line in enumerate(text.splitlines(), 1)
                   if IDENTITY_SYMBOL.search(line)]
        relative = path.relative_to(root).as_posix()
        if matches and relative not in REVIEWED_IDENTITY_CONSUMERS:
            unreviewed[relative] = matches
    assert not unreviewed, (
        "Unreviewed FUTOI identity-sensitive consumers; inspect before changing live schemas:\n"
        + "\n".join(path + "\n  " + "\n  ".join(lines)
                    for path, lines in unreviewed.items())
    )


def test_historical_raw_default_key_and_paths_remain_v1(monkeypatch, tmp_path):
    monkeypatch.setenv("MOEX_DATA_ROOT", str(tmp_path))
    assert materializer.SOURCE_RECORD_KEY_FIELDS == (
        "trade_date", "sess_id", "seqnum", "secid", "clgroup",
    )
    assert materializer.RAW_CONTRACT_REF == "contracts/datasets/futures_futoi_raw.v1.yaml"
    assert materializer.SOURCE_CONTRACT_REF == "contracts/sources/futures/moex_algopack_futoi.v1.yaml"
    expected = (tmp_path / "market" / "supplementary"
                / "dataset_id=futures_futoi_raw" / "instrument_id=si_futures_family"
                / "trade_date=2026-09-17" / "source=moex_algopack_futoi" / "part.parquet")
    assert materializer._partition_path("2026-09-17", "si_futures_family", "moex_algopack_futoi") == expected
    assert materializer._quality_path("2026-09-17", "legacy") == (
        tmp_path / "state" / "quality" / "dataset_id=futures_futoi_raw"
        / "run_date=2026-09-17" / "run_id=legacy" / "quality_report.json")
    assert materializer._manifest_path("2026-09-17", "legacy") == (
        tmp_path / "state" / "refresh" / "dataset_id=futures_futoi_raw"
        / "run_date=2026-09-17" / "run_id=legacy" / "manifest.json")
