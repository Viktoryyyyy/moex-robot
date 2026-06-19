from __future__ import annotations

import base64
import urllib.request
import zlib

BASE = "https://raw.githubusercontent.com/Viktoryyyyy/moex-robot/b70edd1747cbcf001ffd59bca5e75b205351e5ae/"
PATHS = (
    "docs/sot/MOEX_ROUTE_B_WORKER_POLLER_UNIVERSAL_ROLE_RUNNER_V0_1_TARGET.json",
    "tests/contract/test_route_b_worker_poller_universal_role_runner_target_workflow.py",
)

messages = []
for path in PATHS:
    with urllib.request.urlopen(BASE + path, timeout=30) as response:
        data = response.read()
    payload = base64.b64encode(zlib.compress(data, 9)).decode("ascii")
    label = "WORKFLOW" if path.endswith(".json") else "TEST"
    messages.append(f"RECOVER_{label}_ZLIB_B64={payload}")

raise RuntimeError("\n".join(messages))
