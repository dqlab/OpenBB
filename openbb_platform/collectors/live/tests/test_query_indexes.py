from openbb_collector_core.delivery import DeliveryConfig
from openbb_collector_core.incremental import heartbeat

from dq_live_market_data_collector.storage import Journal


def test_old_journal_gets_indexed_export_and_status_lookups(rig):
    config, clock, provider, journal, service = rig
    service.step()
    identities = [
        tuple(row) for row in journal.db.execute("SELECT seq,record_id FROM observations")
    ]
    for name in (
        "observations_export_batch",
        "observations_session_exported",
        "observations_collection_available",
    ):
        journal.db.execute("DROP INDEX " + name)
    journal.db.commit()
    journal.close()
    with Journal(config.storage) as upgraded:
        assert [
            tuple(row) for row in upgraded.db.execute("SELECT seq,record_id FROM observations")
        ] == identities
        queries = [
            ("SELECT payload FROM observations WHERE export_batch=? ORDER BY seq", ("batch",)),
            ("UPDATE observations SET exported=1 WHERE export_batch=?", ("batch",)),
            ("SELECT max(available_at) FROM observations WHERE collection_id=?", ("quotes",)),
            ("SELECT 1 FROM observations WHERE session_id=? AND exported=0 LIMIT 1", ("session",)),
        ]
        for sql, args in queries:
            plan = " ".join(
                row[3] for row in upgraded.db.execute("EXPLAIN QUERY PLAN " + sql, args)
            )
            assert "USING" in plan and "INDEX" in plan


def test_live_heartbeat_does_not_aggregate_all_instruments(rig, tmp_path):
    config, clock, provider, journal, service = rig
    service.step()
    config.delivery = DeliveryConfig(
        collector_id="edge",
        transport="local",
        destination_root=str(tmp_path / "master"),
        incremental=True,
    )
    queries = []
    journal.db.set_trace_callback(queries.append)
    heartbeat(config, journal, "live", clock(), {"quotes": "open"})
    journal.db.set_trace_callback(None)
    assert not any("GROUP BY" in sql or "WHERE instrument_id" in sql for sql in queries)
    assert any("max(available_at)" in sql for sql in queries)


def test_local_heartbeat_uses_flush_completion_time(rig, monkeypatch):
    config, clock, provider, journal, service = rig
    original = journal.export_pending

    def slow_export():
        clock.advance(17)
        return original()

    monkeypatch.setattr(journal, "export_pending", slow_export)
    service._flush(clock())
    assert journal.get_meta("heartbeat") == clock().isoformat()
