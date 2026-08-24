from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path

from dotenv import load_dotenv

from moex_research.runners.usdrubf_phase8_6a_algopack_cnyrubf_source_validation import (
    main as validation_main,
)


PROJECT_ENV_PATH = Path(__file__).resolve().parents[4] / ".env"


def main(argv: Sequence[str] | None = None) -> int:
    load_dotenv(PROJECT_ENV_PATH, override=False)
    return validation_main(argv)


if __name__ == "__main__":
    raise SystemExit(main())
