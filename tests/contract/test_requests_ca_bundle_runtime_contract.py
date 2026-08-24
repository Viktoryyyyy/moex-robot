from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).parents[2]
ENV_EXAMPLE = (ROOT / ".env.example").read_text(encoding="utf-8")
RUNTIME_ENVIRONMENT = (ROOT / "docs/data/runtime_environment.md").read_text(encoding="utf-8")
SERVER_LAYOUT = (ROOT / "docs/sot/runtime/server_layout.v1.md").read_text(encoding="utf-8")


def test_requests_ca_bundle_is_canonical_for_moex_runtime() -> None:
    assignment = "REQUESTS_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt"
    assert ENV_EXAMPLE.count(assignment) == 1
    assert assignment in RUNTIME_ENVIRONMENT
    assert assignment in SERVER_LAYOUT
    assert "verify=False" in RUNTIME_ENVIRONMENT
    assert "verify=False" in SERVER_LAYOUT
    assert "forbidden" in SERVER_LAYOUT
