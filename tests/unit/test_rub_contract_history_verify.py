import unittest
from moex_data.rub_contract_history_verify import verify_rows


class VerifyHistoryTests(unittest.TestCase):
    def row(self):
        return dict(instrument_id="si_futures_family",secid="SiU6",trade_date="2026-08-03",
            ts="2026-08-03T10:05:00",open=100,high=102,low=99,close=101,volume=12)

    def check(self,rows):
        return verify_rows(rows,instrument="si_futures_family",secid="SiU6",day="2026-08-03")

    def test_session_break_is_reported_without_fabricating_bars(self):
        other=self.row();other["ts"]="2026-08-03T14:05:00"
        result=self.check([self.row(),other])
        self.assertEqual(result["row_count"],2)
        self.assertEqual(result["intervals_over_5m"],1)

    def test_wrong_identity_dates_prices_and_volume_are_rejected(self):
        for field,value in [("secid","SiZ6"),("trade_date","2026-08-04"),("ts","2026-08-03T10:06:00"),
                            ("close",103),("open",float("nan")),("high",float("inf")),("volume",-1)]:
            with self.subTest(field=field):
                row=self.row();row[field]=value
                with self.assertRaises(ValueError):self.check([row])

    def test_duplicate_bars_are_rejected(self):
        with self.assertRaisesRegex(ValueError,"duplicate"):
            self.check([self.row(),self.row()])


if __name__ == "__main__":
    unittest.main()
