from __future__ import annotations

import argparse
from collections import Counter
from typing import Sequence

from ..intelligence.usdrubf_news_live_rss import (
    FIRST_SLICE_SOURCE_IDS,
    fetch_official_rss_batch,
)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="One-shot official RSS acquisition smoke for USDRUBF RUB Intelligence.",
    )
    parser.add_argument(
        "--source-id",
        action="append",
        dest="source_ids",
        choices=FIRST_SLICE_SOURCE_IDS,
        help="Fetch only this registered source; may be repeated.",
    )
    parser.add_argument("--timeout-seconds", type=float, default=10.0)
    return parser


def _latest_timestamp(records) -> str:
    if not records:
        return "NONE"
    return max(record.published_at for record in records).isoformat()


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    source_ids = tuple(args.source_ids or FIRST_SLICE_SOURCE_IDS)
    result = fetch_official_rss_batch(
        source_ids=source_ids,
        timeout_seconds=args.timeout_seconds,
    )

    status_counts = Counter(item.quality_status for item in result.source_results)
    if result.ok_source_count == len(result.source_results):
        status = "COMPLETED"
        exit_code = 0
    elif result.ok_source_count == 0:
        status = "BLOCKED"
        exit_code = 2
    else:
        status = "PARTIAL"
        exit_code = 2

    print("PROJECT=MOEX_Bot")
    print("MODE=official_rss_live_ingestion_smoke")
    print(f"STATUS={status}")
    print(f"SOURCE_COUNT={len(result.source_results)}")
    print(f"OK_SOURCE_COUNT={result.ok_source_count}")
    print(f"FAILED_SOURCE_COUNT={len(result.failures)}")
    print(f"RECORD_COUNT={len(result.records)}")
    print(f"LATEST_PUBLISHED_AT={_latest_timestamp(result.records)}")
    print(
        "QUALITY_COUNTS="
        + ",".join(f"{key}:{status_counts[key]}" for key in sorted(status_counts))
    )
    for item in result.source_results:
        print(
            f"SOURCE={item.source_id} QUALITY={item.quality_status} "
            f"RECORDS={len(item.records)} FUTURE_SKIPPED={item.future_items_skipped}"
        )
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
