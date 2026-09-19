"""Offline derivative contracts and exposed command integration."""

import asyncio
from datetime import date

import duckdb
import pytest
from openbb_core.app.model.abstract.error import OpenBBError
from openbb_core.provider.utils.errors import EmptyDataError
from openbb_duckdb.models.futures_curve import DuckDBFuturesCurveFetcher as Curve
from openbb_duckdb.models.futures_historical import DuckDBFuturesHistoricalFetcher as History
from openbb_duckdb.models.options_chains import DuckDBOptionsChainsFetcher as Options


@pytest.fixture()
def market(tmp_path):
    """Small immutable daily snapshots with nullable quotes and multiple expiries."""
    path = str(tmp_path / "derivatives.duckdb")
    with duckdb.connect(path) as con:
        con.execute("""
            CREATE TABLE options_chains (
                underlying_symbol VARCHAR, eod_date DATE, contract_symbol VARCHAR,
                expiration DATE, strike DOUBLE, option_type VARCHAR,
                bid DOUBLE, ask DOUBLE, implied_volatility DOUBLE,
                delta DOUBLE, currency VARCHAR, contract_size INTEGER
            );
            INSERT INTO options_chains VALUES
            ('SPX','2026-09-01','C1','2026-09-18',6500,'call',10,11,0.2,0.5,'USD',100),
            ('SPX','2026-09-02','C1','2026-09-18',6500,'call',12,13,0.21,0.51,'USD',100),
            ('SPX','2026-09-02','P1','2026-09-18',6500,'put',NULL,14,NULL,-0.49,'USD',100),
            ('SPX','2026-09-02','C2','2026-10-16',6600,'call',20,21,0.22,0.4,'USD',100),
            ('OTHER','2026-09-03','X','2026-09-18',10,'call',1,2,0.3,0.5,'USD',100);
            CREATE TABLE futures_historical (
                symbol VARCHAR, date DATE, contract_symbol VARCHAR, expiration DATE,
                close DOUBLE, volume BIGINT, open_interest BIGINT, currency VARCHAR
            );
            INSERT INTO futures_historical VALUES
            ('SR3','2026-09-01','SR3U6','2026-09-16',96.2,100,200,'USD'),
            ('SR3','2026-09-02','SR3U6','2026-09-16',96.3,NULL,210,'USD'),
            ('SR3','2026-09-02','SR3Z6','2026-12-16',96.4,120,300,'USD'),
            ('CL','2026-09-02','CLU6','2026-09-20',-1.5,10,20,'USD');
            CREATE VIEW futures_curve AS
            SELECT symbol, date, contract_symbol, expiration, close AS price, currency
            FROM futures_historical;
        """)
    return path


def fetch(model, market, **params):
    """Exercise the complete fetcher pipeline."""
    return asyncio.run(model.fetch_data({"database_path": market, **params}))


def test_options_snapshot_and_column_alignment(market):
    """No later other-underlying data or older observations leak into the chain."""
    chain = fetch(Options, market, symbol="spx")
    assert chain.contract_symbol == ["C1", "P1", "C2"]
    assert chain.eod_date == [date(2026, 9, 2)] * 3
    assert chain.bid == [12, None, 20]
    assert chain.implied_volatility == [0.21, None, 0.22]
    assert chain.contract_size == [100] * 3
    assert chain.currency == ["USD"] * 3
    assert len(chain.dataframe) == 3
    assert fetch(Options, market, symbol="SPX", date="2026-09-01").bid == [10]
    narrowed = fetch(Options, market, symbol="SPX", expiration="2026-09-18", option_type="put")
    assert narrowed.contract_symbol == ["P1"]


def test_futures_history_filters_preserve_contracts(market):
    """Expiration filters do not mix deliveries or adjust quote units."""
    result = fetch(History, market, symbol="sr3", start_date="2026-09-02", end_date="2026-09-02", expiration="2026-09")
    assert len(result) == 1
    assert result[0].contract_symbol == "SR3U6"
    assert result[0].close == 96.3
    assert result[0].volume is None
    assert result[0].open_interest == 210
    assert result[0].currency == "USD"
    assert len(fetch(History, market, symbol="SR3,CL", start_date="2026-09-02")) == 3
    assert fetch(History, market, symbol="CL")[0].close == -1.5


def test_curve_exact_and_latest(market):
    """A view provides latest or requested dates with each maturity retained."""
    latest = fetch(Curve, market, symbol="SR3")
    assert [row.price for row in latest] == [96.3, 96.4]
    assert [row.contract_symbol for row in latest] == ["SR3U6", "SR3Z6"]
    assert len(fetch(Curve, market, symbol="SR3", date="2026-09-01")) == 1
    assert len(fetch(Curve, market, symbol="SR3", date="2026-09-01,2026-09-02")) == 3


@pytest.mark.parametrize("model,symbol", [(Options, "SPX"), (History, "SR3"), (Curve, "SR3")])
def test_bounds_and_empty_results(market, model, symbol):
    """Never silently publish a truncated chain or curve."""
    with pytest.raises(OpenBBError, match="exceeds limit"):
        fetch(model, market, symbol=symbol, limit=1)
    with pytest.raises(EmptyDataError):
        fetch(model, market, symbol="MISSING")
    with pytest.raises(EmptyDataError):
        fetch(model, market, symbol="X' OR 1=1 --")


def test_exact_snapshot_has_no_fallback(market):
    """No future observation fills a missing requested snapshot."""
    with pytest.raises(EmptyDataError):
        fetch(Options, market, symbol="SPX", date="2026-08-31")
    with pytest.raises(EmptyDataError):
        fetch(Curve, market, symbol="SR3", date="2026-08-31")


def test_validation_and_identifier_safety(market):
    """Invalid queries and injected identifiers cannot widen scope or mutate tables."""
    with pytest.raises(ValueError):
        History.transform_query({"symbol": "SR3", "start_date": "2026-09-02", "end_date": "2026-09-01"})
    with pytest.raises(ValueError):
        History.transform_query({"symbol": "SR3", "expiration": "2026-13"})
    with pytest.raises(OpenBBError):
        fetch(Options, market, symbol="SPX", table='options_chains"; DROP TABLE futures_historical; --')
    assert len(fetch(History, market, symbol="SR3")) == 3
    with pytest.raises(OpenBBError, match="Missing required"):
        fetch(Options, market, symbol="SPX", table="futures_historical")


def test_installed_derivatives_commands(market):
    """Opt-in check after editable installation and openbb-build."""
    import os

    if os.environ.get("OPENBB_DUCKDB_INTERFACE_TEST") != "1":
        pytest.skip("Requires installed derivatives router and openbb-build.")
    from openbb import obb

    params = {"provider": "duckdb", "database_path": market}
    chain = obb.derivatives.options.chains(symbol="SPX", **params)
    assert len(chain.to_dataframe()) == 3
    history = obb.derivatives.futures.historical(symbol="SR3", start_date="2026-09-02", end_date="2026-09-02", **params)
    assert len(history.results) == 2
    curve = obb.derivatives.futures.curve(symbol="SR3", date="2026-09-01", **params)
    assert len(curve.results) == 1
    assert curve.results[0].price == 96.2


def test_options_missing_identifiers(market):
    """Surface the source underlying through the standard field when it is renamed."""
    with duckdb.connect(market) as con:
        con.execute("""
            CREATE VIEW local_option_chain AS
            SELECT underlying_symbol AS underlying, eod_date, expiration,
                   strike, option_type, bid, ask
            FROM options_chains
        """)
    chain = fetch(Options, market, symbol="SPX", table="local_option_chain")
    assert chain.contract_symbol == [None] * 3
    assert chain.underlying_symbol == ["SPX"] * 3
    assert chain.underlying == ["SPX"] * 3
    assert len(chain.dataframe) == 3
    assert "underlying_symbol" in chain.dataframe.columns
    assert chain.bid == [12, None, 20]


@pytest.mark.parametrize("call,put", [("C", "P"), ("c", "p"), ("CALL", "PUT"), (" call ", " put ")])
def test_option_side_normalization(market, call, put):
    """Normalize stored sides consistently in output and SQL filters."""
    with duckdb.connect(market) as con:
        con.execute(
            "UPDATE options_chains SET option_type = CASE option_type WHEN 'call' THEN ? ELSE ? END",
            [call, put],
        )
    chain = fetch(Options, market, symbol="SPX")
    assert chain.option_type == ["call", "put", "call"]
    assert fetch(Options, market, symbol="SPX", option_type="call").contract_symbol == ["C1", "C2"]
    assert fetch(Options, market, symbol="SPX", option_type="put").contract_symbol == ["P1"]


def test_invalid_option_side(market):
    """Unknown sides remain errors rather than silently becoming puts."""
    with duckdb.connect(market) as con:
        con.execute("UPDATE options_chains SET option_type = 'unknown'")
    with pytest.raises(ValueError, match="Stored option_type"):
        fetch(Options, market, symbol="SPX")
