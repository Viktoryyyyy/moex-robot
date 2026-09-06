import tempfile
import unittest
from datetime import date
from pathlib import Path

from moex_data.rub_contract_history_capture import validate_scope, reserve, EMPTY_ERRORS


class ContractCaptureTests(unittest.TestCase):
    def entry(self):
        return {"instrument_id":"si_futures_family", "secid":"SiU6",
            "source_id":"moex_algopack_fo_tradestats_5m", "evidence_status":"pilot_passed",
            "contract_binding":{"type":"expiring_current_explicit", "observed_secid":"SiU6",
                                "observed_last_trade_date":"2026-09-17"}}

    def check(self, entry=None, secid="SiU6", start="2026-07-20", end="2026-09-05", today="2026-09-06"):
        validate_scope(entry or self.entry(), "si_futures_family", secid,
                       date.fromisoformat(start),date.fromisoformat(end),date.fromisoformat(today))

    def test_current_explicit_capture_keeps_global_loading_disabled(self):
        entry = self.entry()
        entry["enabled_for_raw_5m_materialization"] = False
        self.check(entry)
        self.assertFalse(entry["enabled_for_raw_5m_materialization"])

    def test_other_contract_is_rejected(self):
        with self.assertRaisesRegex(ValueError,"identity mismatch"):
            self.check(secid="SiZ6")

    def test_full_history_request_is_rejected(self):
        with self.assertRaisesRegex(ValueError,"60 calendar"):
            self.check(start="2020-01-01")

    def test_open_date_is_rejected(self):
        with self.assertRaisesRegex(ValueError,"completed dates"):
            self.check(end="2026-09-06")

    def test_after_expiry_is_rejected(self):
        with self.assertRaisesRegex(ValueError,"contract lifetime"):
            self.check(start="2026-09-01",end="2026-09-18",today="2026-09-20")

    def test_unproven_source_is_rejected(self):
        entry=self.entry();entry["evidence_status"]="not_proven"
        with self.assertRaisesRegex(ValueError,"pilot evidence"):
            self.check(entry)

    def test_run_cannot_be_reused(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            run=reserve(root,"one_run")
            self.assertTrue(run.resolve().is_relative_to(root.resolve()))
            with self.assertRaises(FileExistsError):
                reserve(root,"one_run")
            with self.assertRaises(ValueError):
                reserve(root,"../outside")

    def test_transport_error_is_not_empty_date(self):
        self.assertNotIn("timeout: source returned no rows", EMPTY_ERRORS)


if __name__ == "__main__":
    unittest.main()
