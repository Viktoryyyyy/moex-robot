import json
import tempfile
import unittest
from pathlib import Path

from moex_data import rub_history_inventory as audit


class HistoryInventoryTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name).resolve()

    def pointer(self):
        values = {"dataset_id": "example", "instrument_id": "si_futures_family"}
        for name in ("partition", "manifest", "quality_report"):
            path = self.root / (name + ".json")
            path.write_text(json.dumps({"quality_status": "pass", "refresh_status": "succeeded"}))
            values[name + "_ref"] = audit.PREFIX + path.name
            values[name + "_sha256"] = audit.sha256(path)
        path = self.root / "current_accepted_manifest.json"
        path.write_text(json.dumps(values))
        return path

    def test_intact_hashes_do_not_grant_completeness(self):
        result = audit.audit_pointer(self.root, self.pointer(), lambda path: {"row_count": 10})
        self.assertEqual(result["status"], "INTEGRITY_VERIFIED_INVENTORIED")
        self.assertEqual(result["source_completeness"], "NOT_VERIFIED")
        self.assertFalse(result["model_readiness_granted"])

    def test_bad_hash_blocks_reading(self):
        path = self.pointer()
        (self.root / "partition.json").write_text("changed")
        def reader(path):
            self.fail("unverified partition must not be read")
        result = audit.audit_pointer(self.root, path, reader)
        self.assertEqual(result["status"], "ERROR")
        self.assertIn("SHA mismatch", result["error"])

    def test_pointer_race_is_detected(self):
        path = self.pointer()
        def reader(partition):
            path.write_text("{}")
            return {"row_count": 10}
        result = audit.audit_pointer(self.root, path, reader)
        self.assertIn("pointer changed", result["error"])

    def test_partition_race_is_detected(self):
        path = self.pointer()
        def reader(partition):
            partition.write_text("changed")
            return {"row_count": 10}
        result = audit.audit_pointer(self.root, path, reader)
        self.assertIn("partition changed", result["error"])

    def test_path_escape_is_rejected(self):
        with self.assertRaises(ValueError):
            audit.rooted(self.root, audit.PREFIX + "../other")

    def test_empty_continuous_storage_is_explicit(self):
        rows = audit.continuous_inventory(self.root)
        self.assertEqual(len(rows), 6)
        self.assertTrue(all(row["status"] == "NO_FILES" for row in rows))

    def test_different_roll_policies_are_not_combined(self):
        for policy in ("first", "second"):
            path = self.root / "futures/continuous_d1" / ("roll_policy=" + policy) / "adjustment_policy=raw/family=Si/trade_date=2026-09-04/part.parquet"
            path.parent.mkdir(parents=True)
            path.write_bytes(b"sample")
        def reader(path):
            return {"row_count": 1, "date_ranges": {"trade_date": {"first": "2026-09-04", "last": "2026-09-04"}}}
        rows = [row for row in audit.continuous_inventory(self.root, reader) if row["status"] != "NO_FILES"]
        self.assertEqual(len(rows), 2)
        self.assertTrue(all(row["row_count"] == 1 and row["acceptance"] == "NOT_VERIFIED" for row in rows))


if __name__ == "__main__":
    unittest.main()
