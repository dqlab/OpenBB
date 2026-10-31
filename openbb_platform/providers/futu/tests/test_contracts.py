"""Offline Futu provider tests."""

import json
import unittest

from openbb_futu.contracts import business_frame, plan


class ContractTests(unittest.TestCase):
    """Handle ContractTests."""

    def test_frame_is_lossless_including_duplicates_and_large_integer(self):
        """Test frame is lossless including duplicates and large integer."""
        raw = (
            '{"type":"TICKER","symbol":"HK.00700","data":{"ticker_list":['
            '{"sequence":9223372036854775807},{"sequence":9223372036854775807}],'
            '"unknown":1.234567890123456789}}'
        )
        result = business_frame(raw, run_id="r", connection_id="c", local_seq=1)
        self.assertEqual(result["raw_payload"], raw)
        self.assertEqual(len(json.loads(result["raw_payload"])["data"]["ticker_list"]), 2)
        self.assertEqual(json.loads(result["raw_payload"])["data"]["ticker_list"][0]["sequence"], 9223372036854775807)
        self.assertEqual(result["data_kind"], "trade_tick")

    def test_controls_and_secrets_rejected(self):
        """Test controls and secrets rejected."""
        for raw in (
            '{"action":"auth","data":{"authorization":"secret"}}',
            '{"type":"QUOTE","access_token":"secret"}',
            '{"type":"QUOTE","id":"auth","data":{}}',
            '{"type":"QUOTE","type":"TICKER","data":{}}',
            "not json",
        ):
            with self.assertRaises(ValueError):
                business_frame(raw, run_id="r", connection_id="c", local_seq=1)

    def test_kline_and_unknown(self):
        """Test kline and unknown."""
        for raw, kind in [
            ('{"type":"KLINE","data":{"period":"1m","adjust":"none"}}', "native_bar_1m"),
            ('{"type":"KLINE","data":{"period":"1D"}}', None),
        ]:
            self.assertEqual(business_frame(raw, run_id="r", connection_id="c", local_seq=1)["data_kind"], kind)

    def test_budget_and_asset_denominators(self):
        """Test budget and asset denominators."""
        entries = [
            {"asset": asset, "symbol": "X." + asset, "channel": "ticker"}
            for asset in ("equity", "etf", "option", "future")
        ]
        report = plan(entries, budget=None, pilot_limit=1)
        self.assertFalse(report["discovery_complete"])
        self.assertEqual(len(report["selected"]), 1)
        self.assertEqual(len(report["not_selected"]), 3)
        self.assertTrue(all(x["discovered_dimensions"] == 1 for x in report["by_asset"].values()))
        self.assertTrue(report["coverage_partial"])
        with self.assertRaises(ValueError):
            plan(entries + entries[:1], budget=4)


if __name__ == "__main__":
    unittest.main()
