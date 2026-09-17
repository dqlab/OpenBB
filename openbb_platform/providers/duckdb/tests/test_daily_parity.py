"""Yahoo-compatible daily semantics without fetching from the network."""

from datetime import date

import duckdb
import pytest
from openbb_core.app.model.abstract.error import OpenBBError
from openbb_duckdb.models.equity_historical import DuckDBEquityHistoricalFetcher as F
from pydantic import ValidationError


@pytest.fixture
def database(tmp_path):
    """Create two split-adjusted daily bars with corporate-action fields."""
    p = tmp_path / "daily.duckdb"
    with duckdb.connect(str(p)) as c:
        c.execute("""CREATE TABLE equity_historical AS
        SELECT 'AAPL' symbol, DATE '2026-09-08' date,100.0 open,110.0 high,90.0 low,
        105.0 AS close,1000::BIGINT volume,95.0 adjusted_open,104.5 adjusted_high,
        85.5 adjusted_low,99.75 adjusted_close,0.5 dividend,0.0 split_ratio
        UNION ALL SELECT 'MSFT',DATE '2026-09-08',200,220,180,210,2000,190,209,171,199.5,0,2""")
    return str(p)


def fetch(database, **kwargs):
    """Query the local daily fixture through the complete fetcher contract."""
    q = F.transform_query(
        dict(
            database_path=database,
            symbol="AAPL,MSFT",
            start_date="2026-09-08",
            end_date="2026-09-08",
            **kwargs,
        )
    )
    return F.transform_data(q, F.extract_data(q, None))


def test_adjustment_action_and_volume_parity(database):
    """Preserve adjustment, action and volume semantics across both price modes."""
    base = fetch(database, adjustment="splits_only")
    adj = fetch(database, adjustment="splits_and_dividends", interval="1d")
    assert [r.symbol for r in adj] == ["AAPL", "MSFT"]
    assert adj[0].date == date(2026, 9, 8)
    assert base[0].close == 105
    assert adj[0].close == 99.75
    assert adj[0].open == 95
    assert adj[0].volume == base[0].volume == 1000
    assert adj[0].dividend == 0.5
    assert adj[1].split_ratio == 2
    assert adj[0].price_adjustment == "splits_and_dividends"


def test_actions_can_be_excluded(database):
    """Omit action fields when the caller disables them."""
    assert all(r.dividend is None and r.split_ratio is None for r in fetch(database, include_actions=False))


def test_adjustment_missing_is_error(database):
    """Reject a request for adjusted prices when the stored field is missing."""
    with duckdb.connect(database) as c:
        c.execute("update equity_historical set adjusted_close=NULL")
    with pytest.raises(OpenBBError, match="adjusted OHLC is unavailable"):
        fetch(database, adjustment="splits_and_dividends")


def test_bound_does_not_silently_truncate(database):
    """Raise when the result exceeds the requested row bound."""
    with pytest.raises(OpenBBError, match="exceeds limit"):
        fetch(database, limit=1)


def test_duplicate_revisions_rejected(database):
    """Reject multiple revisions for the same symbol and session."""
    with duckdb.connect(database) as c:
        c.execute("insert into equity_historical select * from equity_historical")
    with pytest.raises(OpenBBError, match="duplicate"):
        fetch(database)


@pytest.mark.parametrize("params", [{"interval": "1m"}, {"extended_hours": True}])
def test_unsupported_session_settings_are_not_ignored(params):
    """Reject intraday and extended-session parameters."""
    with pytest.raises(ValidationError):
        F.transform_query(dict(symbol="AAPL", **params))


def test_existing_market_lock_is_honored(tmp_path):
    """Respect an exclusive catalog lock held by another process."""
    import os

    if os.name == "nt":
        pytest.skip("POSIX deployment lock test")
    import subprocess
    import sys

    from openbb_duckdb.utils.helpers import market_read_lock

    db = tmp_path / "locked.duckdb"
    lock = db.with_suffix(".market.lock")
    lock.touch()
    code = (
        'import fcntl,sys,time; f=open(sys.argv[1],"r+b"); '
        'fcntl.flock(f,fcntl.LOCK_EX); print("locked",flush=True); time.sleep(20)'
    )
    p = subprocess.Popen(  # noqa: S603 - fixed test script and fixture path
        [sys.executable, "-c", code, str(lock)], stdout=subprocess.PIPE, text=True
    )
    try:
        assert p.stdout.readline().strip() == "locked"
        with pytest.raises(OpenBBError, match="busy"), market_read_lock(str(db), timeout=0.1):
            pytest.fail("Lock was bypassed")
    finally:
        p.terminate()
        p.wait(timeout=5)
