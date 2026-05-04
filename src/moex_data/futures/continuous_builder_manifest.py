#!/usr/bin/env python3
import sys
from pathlib import Path

sys.path.insert(0, str(Path.cwd() / "src"))

from moex_data.futures.continuous_quality_report import main

if __name__ == "__main__":
    raise SystemExit(main())
