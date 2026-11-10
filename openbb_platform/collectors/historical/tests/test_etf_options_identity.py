"""Offline semantic identity regressions using the real provider's master shape.

The provider must be *this checkout*, not whatever ``openbb_akshare`` is installed:
the module this file needs (``models.etf_options``) exists in the repository
and not in the installed 1.1.0 wheel, and the collector it drives must be this
checkout too -- an ambient ``dq_historical_market_data_collector`` 0.1.7 has no
``etf_options`` package at all, so an unwitting run would test the wrong collector.

The import guard below therefore puts the repository provider first and then proves
what it got.  Absent, or resolved somewhere else: a skip that names the path, never
an ``ImportError`` during collection -- one missing environment entry must not abort
the whole selection.  ``test_raw_import_guard_provider_module_is_the_repo_one``
states the same fact as a node, and the freeze harness asserts the module origins
of every receipt independently (``run_evidence.module_origins``).
"""

import json
import runpy
import sys
from copy import deepcopy
from pathlib import Path

import pytest

CHECKOUT = Path(__file__).resolve().parents[1]
PROVIDER_ROOT = Path(__file__).resolve().parents[3] / "providers" / "akshare"
if str(PROVIDER_ROOT) not in sys.path:
    sys.path.insert(0, str(PROVIDER_ROOT))

provider = pytest.importorskip(
    "openbb_akshare.models.etf_options",
    reason=f"the repository provider at {PROVIDER_ROOT} is not importable here",
)
if not Path(provider.__file__).resolve().is_relative_to(PROVIDER_ROOT.resolve()):
    pytest.skip(
        f"ambient openbb_akshare.models.etf_options is {provider.__file__}, "
        f"not {PROVIDER_ROOT}",
        allow_module_level=True,
    )

from dq_historical_market_data_collector.etf_options.runtime import (  # noqa: E402
    Collector,
)
from dq_historical_market_data_collector.etf_options.storage import Journal  # noqa: E402

from .test_etf_options import NOW, FakeClient, calendar, config, quote  # noqa: E402

FIRST = "2026-10-14T07:20:00+00:00"
SECOND = "2026-10-14T08:20:00+00:00"
PROVIDER_TESTS = (
    Path(__file__).resolve().parents[3] / "providers/akshare/tests/test_etf_option_contracts.py"
)
if not PROVIDER_TESTS.is_file():
    pytest.skip(
        f"the provider's master fixtures are absent: {PROVIDER_TESTS}", allow_module_level=True
    )


def test_raw_import_guard_provider_module_is_the_repo_one():
    """The guard resolved this checkout -- the provider and the collector both.

    Without this node the environment dependence is implicit: a stale
    ``openbb_akshare`` wheel or an installed ``dq_historical_market_data_collector``
    would turn these regressions into a test of other bytes (or into a skip that
    looks like absence).  Here it is a named, checkable fact.
    """
    import dq_historical_market_data_collector as collector
    from dq_historical_market_data_collector.etf_options import runtime as lane_runtime

    assert Path(provider.__file__).resolve().is_relative_to(PROVIDER_ROOT.resolve())
    assert str(PROVIDER_ROOT) in sys.path
    for module in (collector, lane_runtime):
        assert Path(module.__file__).resolve().is_relative_to(CHECKOUT.resolve()), module.__file__


@pytest.fixture
def master_at():
    fixtures = runpy.run_path(str(PROVIDER_TESTS))

    def fetch(stamp):
        with pytest.MonkeyPatch.context() as patch:
            fixtures["install_sina"](patch)
            original = provider.fetch_quotes

            def fetch_quotes(symbols):
                rows = original(symbols)
                for row in rows:
                    row["fetched_at"] = stamp
                return rows

            patch.setattr(provider, "fetch_quotes", fetch_quotes)
            return provider.fetch_contracts("SSE", "sina", "510050")

    return fetch


def basket(metadata):
    return [deepcopy(row["source_row"]["quote"]) for row in metadata]


def collect(cfg, journal, client, *, later=False):
    clock = NOW.replace(hour=16) if later else NOW
    return Collector(cfg, journal, calendar(), client, clock=lambda: clock).collect()


def identities(journal):
    return [
        row[0] for row in journal.db.execute("SELECT universe_hash FROM captures ORDER BY rowid")
    ]


def revision_count(journal):
    return journal.db.execute("SELECT count(*) FROM revisions").fetchone()[0]


def test_real_nested_acquisition_is_idempotent_and_audited(tmp_path, master_at):
    first, second = master_at(FIRST), master_at(SECOND)
    assert first != second
    first_quotes, second_quotes = basket(first), basket(second)
    # Also exercise documented acquisition fields outside the embedded quote.
    for row in first + first_quotes:
        row["observed_at"] = FIRST
        row["fetched_at"] = FIRST
    for row in second + second_quotes:
        row["observed_at"] = SECOND
        row["fetched_at"] = SECOND
    before = deepcopy((first, second, first_quotes, second_quotes))
    cfg = config(tmp_path)
    with Journal(cfg.root) as journal:
        initial = collect(cfg, journal, FakeClient(first, [first_quotes]))
        repeated = collect(cfg, journal, FakeClient(second, [second_quotes]), later=True)
        assert initial["status"] == repeated["status"] == "complete"
        assert repeated["changed_snapshots"] == 0
        assert revision_count(journal) == 1
        assert len(set(identities(journal))) == 1
        assert journal.db.execute("SELECT count(*) FROM current_quotes").fetchone()[0] == 2
        captures = list(journal.db.execute("SELECT * FROM captures ORDER BY rowid"))
        assert len(captures) == 2 and captures[0]["observed_at"] != captures[1]["observed_at"]
        for capture, stamp in zip(captures, (FIRST, SECOND), strict=True):
            metadata = json.loads(capture["contracts_json"])
            assert metadata[0]["source_row"]["quote"]["fetched_at"] == stamp
            assert json.loads(capture["quotes_json"])[0]["fetched_at"] == stamp
            stored = journal.db.execute(
                "SELECT metadata_json FROM contract_metadata WHERE capture_id=? ORDER BY symbol",
                (capture["id"],),
            ).fetchone()[0]
            assert json.loads(stored) == metadata[0]
        attempts = list(journal.db.execute("SELECT * FROM attempts ORDER BY rowid"))
        assert len(attempts) == 4
        for attempt, stamp in zip(attempts, (FIRST, FIRST, SECOND, SECOND), strict=True):
            raw = json.loads((cfg.root / attempt["raw_path"]).read_text())
            assert raw["observed_at"] == attempt["observed_at"]
            assert raw["rows"][0]["fetched_at"] == stamp
            if attempt["model"] == "AkshareEtfOptionContracts":
                assert raw["rows"][0]["source_row"]["quote"]["fetched_at"] == stamp
    assert (first, second, first_quotes, second_quotes) == before


@pytest.mark.parametrize("ancillary_change", [False, True])
def test_failed_block_resumes_despite_new_acquisition_and_metadata_quote(
    tmp_path, master_at, ancillary_change
):
    first, second = master_at(FIRST), master_at(SECOND)
    if ancillary_change:
        for row in second:
            row["source_row"]["quote"].update(bid=0.7, source_time="2026-10-14T15:01:00+08:00")
            row["source_row"]["quote"]["raw_fields"]["买价"] = "0.7"
    rows = basket(first)
    cfg = config(tmp_path, block_size=1)
    with Journal(cfg.root) as journal:
        failed = collect(cfg, journal, FakeClient(first, [[rows[0]], []]))
        assert failed["status"] == "failed"
        assert journal.db.execute("SELECT count(*) FROM checkpoints").fetchone()[0] == 1
    # Prove durability through reopening, not only the in-process resume cache.
    with Journal(cfg.root) as journal:
        pending = deepcopy(rows[1])
        pending["fetched_at"] = SECOND
        client = FakeClient(second, [[pending]])
        resumed = collect(cfg, journal, client, later=True)
        assert resumed["status"] == "complete"
        assert resumed["resumed_rows"] == 1
        assert [params for model, params in client.calls if model == "AkshareEtfOptionQuotes"] == [
            {"symbols": rows[1]["symbol"]}
        ]
        assert len(set(identities(journal))) == 1
        assert journal.db.execute("SELECT count(*) FROM checkpoints").fetchone()[0] == 0
        assert revision_count(journal) == 1
        capture = journal.db.execute(
            "SELECT * FROM captures ORDER BY rowid DESC LIMIT 1"
        ).fetchone()
        assert json.loads(capture["contracts_json"]) == [
            dict(row, quality_flags=["unknown_contract_unit"]) for row in second
        ]
        assert [row["fetched_at"] for row in json.loads(capture["quotes_json"])] == [FIRST, SECOND]


def test_ancillary_master_quote_does_not_create_revision(tmp_path, master_at):
    first, second = master_at(FIRST), master_at(SECOND)
    second[0]["source_row"]["quote"].update(bid=0.7, source_time="2026-10-14T15:01:00+08:00")
    second[0]["source_row"]["quote"]["raw_payload"] = "different ancillary live payload"
    cfg = config(tmp_path)
    with Journal(cfg.root) as journal:
        assert collect(cfg, journal, FakeClient(first, [basket(first)]))["status"] == "complete"
        report = collect(cfg, journal, FakeClient(second, [basket(first)]), later=True)
        assert report["status"] == "complete" and report["changed_snapshots"] == 0
        assert revision_count(journal) == 1
        assert len(set(identities(journal))) == 1


@pytest.mark.parametrize("change", ["bid_ask", "source_time", "source_event_time", "raw_timestamp"])
def test_actual_quote_content_creates_revision_not_new_universe(tmp_path, master_at, change):
    first, second = master_at(FIRST), master_at(SECOND)
    changed = basket(second)
    if change == "bid_ask":
        changed[0].update(bid=0.7, ask=0.8)
    elif change == "source_time":
        changed[0]["source_time"] = "2026-10-14T15:01:00+08:00"
    elif change == "source_event_time":
        changed[0]["source_event_time"] = "2026-10-14T15:01:00+08:00"
    else:
        # Never recursively discard arbitrary source fields by timestamp-like name.
        changed[0]["raw_fields"]["fetched_at"] = "source field, not collector provenance"
    cfg = config(tmp_path)
    with Journal(cfg.root) as journal:
        assert collect(cfg, journal, FakeClient(first, [basket(first)]))["status"] == "complete"
        report = collect(cfg, journal, FakeClient(second, [changed]), later=True)
        assert report["status"] == "complete" and report["changed_snapshots"] == 1
        assert revision_count(journal) == 2
        assert len(set(identities(journal))) == 1


def change_terms(metadata, key):
    result = deepcopy(metadata)
    value = {
        "strike": 6.9,
        "contract_unit": 10003.0,
        "expiration": "2026-12-24",
        "adjustment_marker": "B",
    }[key]
    result[0][key] = value
    if key != "contract_unit":
        result[0]["source_row"]["quote"][key] = value
    return result


@pytest.mark.parametrize("key", ["strike", "contract_unit", "expiration", "adjustment_marker"])
def test_effective_terms_change_universe_and_revision(tmp_path, master_at, key):
    first = master_at(FIRST)
    second = change_terms(master_at(SECOND), key)
    cfg = config(tmp_path)
    with Journal(cfg.root) as journal:
        assert collect(cfg, journal, FakeClient(first, [basket(first)]))["status"] == "complete"
        report = collect(cfg, journal, FakeClient(second, [basket(second)]), later=True)
        assert report["status"] == "complete" and report["changed_snapshots"] == 1
        assert revision_count(journal) == 2
        assert len(set(identities(journal))) == 2


@pytest.mark.parametrize("key", ["strike", "contract_unit", "expiration", "adjustment_marker"])
def test_effective_terms_change_cannot_resume_old_checkpoint(tmp_path, master_at, key):
    first = master_at(FIRST)
    second = change_terms(master_at(SECOND), key)
    rows = basket(first)
    cfg = config(tmp_path, block_size=1)
    with Journal(cfg.root) as journal:
        assert collect(cfg, journal, FakeClient(first, [[rows[0]], []]))["status"] == "failed"
        client = FakeClient(second, [[row] for row in basket(second)])
        report = collect(cfg, journal, client, later=True)
        assert report["status"] == "complete" and report["resumed_rows"] == 0
        assert len(set(identities(journal))) == 2
        assert len([model for model, _ in client.calls if model == "AkshareEtfOptionQuotes"]) == 2


@pytest.mark.parametrize("kind", ["discovery", "official_adjustment", "source_event_time"])
def test_meaningful_source_metadata_remains_in_identity(tmp_path, master_at, kind):
    if kind == "official_adjustment":
        fixtures = runpy.run_path(str(PROVIDER_TESTS))
        raw = fixtures["sse_row"](TRADE_DATE="2026-10-14", ADJUSTMENT_DATE="2026-10-13")
        first = provider.parse_sse_official({"result": [raw]})
        second = deepcopy(first)
        second[0]["source_row"]["ADJUSTMENT_DATE"] = "2026-10-14"
        cfg = config(tmp_path, exchanges={"SSE": {"source": "official"}})
        rows = [quote(first[0]["symbol"], underlying="510500", strike=6.876)]
    else:
        first = master_at(FIRST)
        second = deepcopy(first)
        cfg = config(tmp_path)
        rows = basket(first)
        if kind == "discovery":
            second[0]["source_row"]["discovery"]["name"] += "_corrected"
        else:
            second[0]["source_row"]["source_event_time"] = "2026-10-14T15:01:00+08:00"
    with Journal(cfg.root) as journal:
        assert collect(cfg, journal, FakeClient(first, [rows]))["status"] == "complete"
        report = collect(cfg, journal, FakeClient(second, [rows]), later=True)
        assert report["status"] == "complete" and report["changed_snapshots"] == 1
        assert revision_count(journal) == 2
        assert len(set(identities(journal))) == 2
