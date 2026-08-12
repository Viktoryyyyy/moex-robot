import json
from pathlib import Path


REGISTRY_PATH = Path("contracts/intelligence/usdrubf_news_macro_source_registry_v1.json")


def test_opec_remains_fail_closed_without_provable_publication_timestamp() -> None:
    registry = json.loads(REGISTRY_PATH.read_text(encoding="utf-8"))
    sources = {item["source_id"]: item for item in registry["primary_sources"]}
    opec = sources["opec_press_releases"]

    assert opec["tier"] == "OFFICIAL_PRIMARY"
    assert opec["transport"] == "HTML_INDEX"
    assert opec["references"] == ["https://www.opec.org/press-releases.html"]
    assert opec["stage12b_status"] == "BLOCKED_PENDING_PROVABLE_PUBLICATION_TIMESTAMP"
    assert "no proven timezone-aware publication timestamp" in opec["available_at_policy"]
    assert "meeting schedule does not imply outcome availability" in opec["available_at_policy"]
