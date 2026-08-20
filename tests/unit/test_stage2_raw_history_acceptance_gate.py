from pathlib import Path

import pytest

from moex_data.futures import stage2_raw_history_acceptance as acceptance
from moex_data.futures import stage2_raw_history_acceptance_gate as gate


def test_preexisting_accepted_pointer_blocks_before_history_audit(tmp_path, monkeypatch) -> None:
    repo = tmp_path / "repo"
    contract = repo / acceptance.QUOTE_CONTRACT_PATH
    contract.parent.mkdir(parents=True, exist_ok=True)
    contract.write_text(
        "\n".join(
            [
                "dataset_id: futures_raw_5m",
                'accepted_pointer_path_contract: "${MOEX_DATA_ROOT}/state/datasets/dataset_id=futures_raw_5m/instrument_id={INSTRUMENT_ID}/current_accepted_manifest.json"',
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    data_root = tmp_path / "data"
    monkeypatch.setenv("MOEX_DATA_ROOT", str(data_root))
    pointer = (
        data_root
        / "state/datasets/dataset_id=futures_raw_5m/instrument_id=usdrubf_futures_family/current_accepted_manifest.json"
    )
    pointer.parent.mkdir(parents=True, exist_ok=True)
    pointer.write_text("{}\n", encoding="utf-8")

    def should_not_run(**kwargs):
        raise AssertionError("history audit must not run when a pointer already exists")

    monkeypatch.setattr(gate.acceptance, "audit_history", should_not_run)
    with pytest.raises(acceptance.RawHistoryAcceptanceError, match="must be absent"):
        gate.run_gate(
            repo_root=repo,
            target_dataset_id="futures_raw_5m",
            instrument_id="usdrubf_futures_family",
            run_id="blocked_pointer",
        )
