import json

import pytest

from moex_data import step10_rub_refresh_scheduler as scheduler


@pytest.mark.parametrize("explicit", [None, False, "true", True])
@pytest.mark.parametrize("gates_pass", [False, True])
def test_factual_acceptance_does_not_imply_stage5_promotion(tmp_path, explicit, gates_pass):
    path = tmp_path / scheduler.FUTOI_GOVERNANCE_RELATIVE_PATH
    path.parent.mkdir(parents=True)
    authority = {"factual_live_authority": True}
    if explicit is not None:
        authority["stage5_promotion_authority"] = explicit
    path.write_text(json.dumps({
        "project": "MOEX_Bot", "contract_id": scheduler.FUTOI_GOVERNANCE_CONTRACT_ID,
        "gates": [{"gate_id": "acceptance", "required": True,
                   "status": "PASS" if gates_pass else "BLOCKED"}], "authority": authority}))
    result = scheduler._futoi_stage5_promotion_governance(tmp_path)
    assert result["promotion_allowed"] is (gates_pass and explicit is True)
