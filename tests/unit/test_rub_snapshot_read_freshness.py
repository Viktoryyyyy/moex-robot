from copy import deepcopy
from datetime import datetime, timedelta, timezone
import unittest

from moex_data.rub_snapshot_read_freshness import apply_read_freshness, LIVE, BASIS


NOW = datetime(2026, 9, 6, 12, 42, 58, tzinfo=timezone.utc)


def fixture():
    keys = ("usdrubf", "cnyrubf", "si_front", "si_next", "cr_front", "cr_next")
    instruments = {key: {"timestamp": NOW.isoformat(), "stale": False,
                         "age_seconds": 0.831, "price_oi_usable": True} for key in keys}
    instruments["cnyrub_tom"] = {"timestamp": NOW.isoformat(), "stale": False, "age_seconds": 0.831}
    return {
        "components": {
            LIVE: {"status": "READY", "data": {
                "instruments": instruments,
                "synchronization": {"synchronized": True, "all_instruments_fresh": True,
                                    "futures_synchronized": True, "futures_all_fresh": True},
                "quality": {"analysis_usable": True, "factual_context_usable": True,
                            "price_oi_all_futures_usable": True, "spot_price_usable": True,
                            "price_oi_usable_by_instrument": dict.fromkeys(keys, True)},
            }},
            BASIS: {"status": "READY", "data": {
                "status": "PARTIAL", "current_live_scope_status": "READY", "ready_metric_count": 1,
                "pairs": {"usd_rub": {"pair_id": "USD/RUB", "legs": {}, "metrics": [
                    {"metric_id": "usd_rub.front_next", "legs": ["si_front", "si_next"],
                     "status": "READY", "value": 1.5, "synchronized": True,
                     "freshness": {"status": "FRESH"}}
                ]}},
            }},
            "futoi_live": {"status": "UNAVAILABLE", "data": {"factual_authority": False}},
        },
        "authority": {"live_market_oi_factual_authority": True, "live_basis_carry_factual_authority": True,
                      "futoi_factual_authority": False},
        "readiness": {"status": "PARTIAL"},
    }


class ReadFreshnessTests(unittest.TestCase):
    def test_expired_sources_downgrade_live_and_basis_without_mutating_archive(self):
        source = fixture()
        original = deepcopy(source)
        read = apply_read_freshness(source, now=NOW + timedelta(seconds=825))
        self.assertEqual(source, original)
        self.assertEqual(read["components"][LIVE]["status"], "UNAVAILABLE")
        self.assertEqual(read["components"][BASIS]["status"], "UNAVAILABLE")
        metric = read["components"][BASIS]["data"]["pairs"]["usd_rub"]["metrics"][0]
        self.assertIsNone(metric["value"])
        self.assertFalse(metric["synchronized"])
        self.assertEqual(metric["freshness"]["status"], "UNAVAILABLE")
        self.assertEqual(metric["freshness"]["age_seconds_by_leg"]["si_front"], 825)
        self.assertFalse(read["authority"]["live_market_oi_factual_authority"])
        self.assertFalse(read["authority"]["live_basis_carry_factual_authority"])

    def test_boundary_uses_unrounded_age(self):
        for seconds, expected in ((60, "READY"), (60.000001, "UNAVAILABLE")):
            with self.subTest(seconds=seconds):
                read = apply_read_freshness(fixture(), now=NOW + timedelta(seconds=seconds))
                self.assertEqual(read["components"][LIVE]["status"], expected)

    def test_closed_spot_preserves_only_fresh_futures_and_their_basis(self):
        source = fixture()
        source["components"][LIVE]["data"]["instruments"]["cnyrub_tom"]["timestamp"] = (NOW - timedelta(days=2)).isoformat()
        read = apply_read_freshness(source, now=NOW)
        data = read["components"][LIVE]["data"]
        self.assertEqual(read["components"][LIVE]["status"], "PARTIAL")
        self.assertTrue(data["synchronization"]["futures_synchronized"])
        self.assertFalse(data["synchronization"]["synchronized"])
        self.assertFalse(data["quality"]["analysis_usable"])
        self.assertEqual(read["components"][BASIS]["data"]["ready_metric_count"], 1)

    def test_bad_timestamp_never_authorizes_its_instrument(self):
        for timestamp in (None, "bad", "2026-09-06T12:42:58", (NOW + timedelta(seconds=6)).isoformat()):
            with self.subTest(timestamp=timestamp):
                source = fixture()
                source["components"][LIVE]["data"]["instruments"]["si_front"]["timestamp"] = timestamp
                read = apply_read_freshness(source, now=NOW)
                self.assertFalse(read["components"][LIVE]["data"]["instruments"]["si_front"]["price_oi_usable"])
                self.assertFalse(read["components"][LIVE]["data"]["synchronization"]["futures_synchronized"])
                self.assertEqual(read["components"][BASIS]["data"]["ready_metric_count"], 0)

    def test_read_cannot_promote_governance_or_source_quality(self):
        source = fixture()
        source["components"][LIVE]["data"]["instruments"]["si_front"]["stale"] = True
        read = apply_read_freshness(source, now=NOW)
        self.assertTrue(read["components"][LIVE]["data"]["instruments"]["si_front"]["stale"])
        self.assertEqual(read["components"]["futoi_live"], source["components"]["futoi_live"])
        self.assertFalse(read["authority"]["futoi_factual_authority"])

    def test_missing_live_source_disables_previously_ready_basis(self):
        source = fixture()
        del source["components"][LIVE]
        read = apply_read_freshness(source, now=NOW)
        self.assertEqual(read["components"][BASIS]["data"]["ready_metric_count"], 0)
        self.assertFalse(read["authority"]["live_basis_carry_factual_authority"])


if __name__ == "__main__":
    unittest.main()
