from __future__ import annotations

import base64
import urllib.request
import zlib

URL = "https://raw.githubusercontent.com/Viktoryyyyy/moex-robot/b70edd1747cbcf001ffd59bca5e75b205351e5ae/docs/sot/MOEX_ROUTE_B_WORKER_POLLER_UNIVERSAL_ROLE_RUNNER_V0_1_TARGET.json"

with urllib.request.urlopen(URL, timeout=30) as response:
    data = response.read()

payload = base64.b64encode(zlib.compress(data, 9)).decode("ascii")
raise RuntimeError("RECOVER_WORKFLOW_ZLIB_B64=" + payload)
