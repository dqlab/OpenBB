"""Bounded offline validation of raw abbreviated-name retention."""

import asyncio
import unittest
from unittest.mock import patch

from openbb_yfinance.models.equity_profile import YFinanceEquityProfileFetcher as F


class ProfileNamesTest(unittest.TestCase):
    """Exercise names without contacting Yahoo."""

    def test_preserves_long_and_short_names_without_inventing_long_name(self):
        """Retain the exact short label without claiming a full legal name."""
        with patch("yfinance.Ticker") as ticker:
            ticker.return_value.get_info.return_value = {
                "symbol": "TEST",
                "shortName": "Test Index",
                "quoteType": "INDEX",
                "exchange": "ZRH",
            }
            q = F.transform_query({"symbol": "TEST"})
            result = asyncio.run(F.aextract_data(q, None))
        self.assertEqual(result[0]["shortName"], "Test Index")
        self.assertNotIn("longName", result[0])
        self.assertIsNone(F.transform_data(q, result)[0].name)


if __name__ == "__main__":
    unittest.main()
