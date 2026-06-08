from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
OPERATING_MODEL_DOC = REPO_ROOT / "docs/sot/route_b/route_b_n8n_operating_model.v1.md"


def _operating_model_text() -> str:
    return OPERATING_MODEL_DOC.read_text(encoding="utf-8")


def test_route_b_n8n_operating_model_doc_exists() -> None:
    assert OPERATING_MODEL_DOC.is_file()


def test_route_b_n8n_operating_model_names_all_seven_workflows() -> None:
    text = _operating_model_text()
    workflows = (
        "MOEX_ROUTE_B_INTAKE_ACK_V1_10_3",
        "MOEX_ROUTE_B_WORKER_POLLER_V1_10_3",
        "MOEX_ROUTE_B_STATUS_QUERY_V1_10_3",
        "MOEX_ROUTE_B_WATCHDOG_ERROR_HANDLER_V1_10_3",
        "MOEX_ROUTE_B_GITHUB_BRANCH_PR_EXECUTOR_V1_10_3",
        "MOEX_ROUTE_B_RESULT_QUERY_V1_10_3",
        "MOEX_ROUTE_B_PM_L3_RETURN_INTAKE_V1_10_3",
    )

    for workflow in workflows:
        assert workflow in text


def test_route_b_n8n_operating_model_documents_sot_state_and_authority_boundaries() -> None:
    text = _operating_model_text()

    required_phrases = (
        "GitHub/repo is Source of Truth",
        "Postgres is workflow state/evidence store",
        "PM L2 owns merge approval authority",
        "PM L3 returns to PM L2",
        "Sub-chat returns to PM L3",
        "direct main write is forbidden",
        "n8n merge is forbidden",
        "force push is forbidden",
        "file delete is forbidden",
        "CI passed is not merge approval",
    )

    for phrase in required_phrases:
        assert phrase in text


def test_route_b_n8n_operating_model_documents_result_and_return_endpoints() -> None:
    text = _operating_model_text()

    assert "Branch/PR Executor" in text
    assert "Result Query v2" in text
    assert "PM L3 Return Intake" in text
    assert "GET /webhook/moex/route-b/result" in text
    assert "POST /webhook/moex/route-b/pm-l3-return" in text
