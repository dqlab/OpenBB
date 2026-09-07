"""Exercise installed entry points and public commands without a gateway."""

from importlib.metadata import distribution
from unittest.mock import patch


def test_installed_entry_points():
    """Both extension types must survive monorepo packaging."""
    entries = {
        (entry.group, entry.name): entry
        for entry in distribution("openbb-ibkr").entry_points
    }
    provider = entries[("openbb_provider_extension", "ibkr")].load()
    router = entries[("openbb_core_extension", "ibkr")].load()
    assert provider.name == "ibkr"
    assert {"EquityQuote", "EquityHistorical"} <= provider.fetcher_dict.keys()
    assert router is not None


def test_public_equity_quote():
    """The generated standard command dispatches to the IBKR client."""
    from openbb import obb

    with patch("openbb_ibkr.utils.client.IbkrClient.get_quote") as quote:
        quote.return_value = {
            "symbol": "AAPL", "bid": 179.5, "ask": 180.5,
            "last_price": 180.0, "volume": 100,
        }
        result = obb.equity.price.quote(symbol="AAPL", provider="ibkr", delayed=True)
    quote.assert_called_once_with("AAPL", delayed=True)
    assert result.provider == "ibkr"
    assert result.results[0].symbol == "AAPL"
    assert result.results[0].bid == 179.5


def test_public_ibkr_router():
    """The generated custom router is available without connecting to TWS."""
    from openbb import obb

    with patch("openbb_ibkr.utils.client.IbkrClient.is_connected", return_value=False):
        result = obb.ibkr.is_connected()
    assert result.results == {"connected": False}


def test_public_equity_historical():
    """The standard historical command accepts OpenBB's execution context."""
    from openbb import obb

    with patch("openbb_ibkr.utils.client.IbkrClient.get_historical") as historical:
        historical.return_value = [{
            "date": "2026-09-04", "open": 179.0, "high": 181.0,
            "low": 178.0, "close": 180.0, "volume": 100,
        }]
        result = obb.equity.price.historical(symbol="AAPL", provider="ibkr", delayed=True)
    historical.assert_called_once_with("AAPL", delayed=True)
    assert result.provider == "ibkr"
    assert result.results[0].close == 180.0
