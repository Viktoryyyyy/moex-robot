from __future__ import annotations

import argparse

from src.moex_research.intelligence.usdrubf_news_live_treasury import (
    TreasuryAcquisitionError,
    fetch_treasury_press_releases,
)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--timeout-seconds", type=float, default=20.0)
    parser.add_argument("--max-detail-pages", type=int, default=10)
    args = parser.parse_args()

    print("PROJECT=MOEX_Bot")
    print("MODE=treasury_html_live_ingestion_smoke")
    try:
        result = fetch_treasury_press_releases(
            timeout_seconds=args.timeout_seconds,
            max_detail_pages=args.max_detail_pages,
        )
    except TreasuryAcquisitionError as exc:
        print("STATUS=BLOCKED")
        print("SOURCE=us_treasury_press_releases")
        print(f"QUALITY={exc.code}")
        print(f"ERROR={exc}")
        return 1

    latest = max((record.published_at for record in result.records), default=None)
    print("STATUS=COMPLETED")
    print(f"SOURCE={result.source_id}")
    print(f"QUALITY={result.quality_status}")
    print(f"RECORD_COUNT={len(result.records)}")
    print(f"FUTURE_SKIPPED={result.future_items_skipped}")
    print(f"LATEST_PUBLISHED_AT={latest.isoformat() if latest is not None else 'NONE'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
