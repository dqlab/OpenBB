"""Real loopback WebSocket/HTTP servers and collector subprocess; no live account."""

import asyncio
import hashlib
import json
import os
import signal
import subprocess
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest
from websockets.asyncio.server import serve


class OAuthHandler(BaseHTTPRequestHandler):
    """Handle OAuthHandler."""

    calls = 0

    def do_POST(self):
        """Handle do POST."""
        self.rfile.read(int(self.headers["Content-Length"]))
        type(self).calls += 1
        body = json.dumps({"access_token": "new-secret", "refresh_token": "rotated-secret", "expires_in": 3}).encode()  # noqa: S105 -- synthetic test credential
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        """Return synthetic discovery evidence, including an HTTP denial."""
        if self.path.endswith("/option-chain"):
            status, body = 200, b'{"ret_code":0,"data":{"option_chain":[{"code":"US.OPTION"}]}}'
        else:
            status, body = 403, b'{"ret_code":403,"message":"synthetic denial"}'
        self.send_response(status)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        """Handle log message."""
        pass


@pytest.fixture
def servers():
    """Handle servers."""
    http = ThreadingHTTPServer(("127.0.0.1", 0), OAuthHandler)
    OAuthHandler.calls = 0
    threading.Thread(target=http.serve_forever, daemon=True).start()
    loop = asyncio.new_event_loop()
    ready = threading.Event()
    state = {"connections": 0, "refreshes": 0, "subscriptions": 0, "pings": 0}
    ticker = '{"type":"TICKER","symbol":"US.AAPL","data":{"ticker_list":[{"sequence":1},{"sequence":1}]}}'

    async def handler(ws):
        """Handle handler."""
        state["connections"] += 1
        conn = state["connections"]
        original_recv_frame = ws.protocol.recv_frame

        def observe(frame):
            """Handle observe."""
            if frame.opcode == 9:
                state["pings"] += 1
            original_recv_frame(frame)

        ws.protocol.recv_frame = observe
        async for raw in ws:
            req = json.loads(raw)
            action = req["action"]
            if action == "auth":
                await ws.send(ticker)  # business arrives while auth acknowledgement is pending
                await ws.send(json.dumps({"id": req["id"], "session_id": "test"}))
            elif action == "subscribe":
                state["subscriptions"] += 1
                if state.get("sensitive"):
                    await ws.send('{"type":"UNKNOWN","data":{"refresh_token":"do-not-log"}}')
                    await asyncio.sleep(0.1)
                    return
                if state.get("valid_tickers"):
                    for symbol in ("US.AAPL", "US.SPY"):
                        await ws.send(
                            json.dumps(
                                {
                                    "type": "TICKER",
                                    "symbol": symbol,
                                    "data": {
                                        "ticker_list": [
                                            {"time_ms": 9223372036854775807, "sequence": 9223372036854775806},
                                            {"time_ms": 2, "sequence": 3},
                                        ]
                                    },
                                }
                            )
                        )
                    bar = json.dumps({"type": "KLINE", "symbol": "US.AAPL", "data": {"period": "1m", "adjust": "none"}})
                    await ws.send(bar)
                    await ws.send(bar)
                for kind in ("QUOTE", "ORDER_BOOK", "KLINE", "BROKER_QUEUE"):
                    await ws.send(json.dumps({"type": kind, "symbol": "US.AAPL", "data": {}}))
                await ws.send(ticker)
                await ws.send(json.dumps({"id": req["id"], "code": 0}))
                if conn == 1:
                    await ws.close()
            elif action == "refresh":
                state["refreshes"] += 1
                await ws.send(ticker)
                await ws.send(json.dumps({"id": req["id"], "session_id": "test"}))

    async def start():
        """Handle start."""
        server = await serve(handler, "127.0.0.1", 0)
        state["port"] = server.sockets[0].getsockname()[1]
        state["server"] = server
        ready.set()

    def runner():
        """Handle runner."""
        asyncio.set_event_loop(loop)
        loop.run_until_complete(start())
        loop.run_forever()
        state["server"].close()
        loop.run_until_complete(state["server"].wait_closed())
        loop.close()

    thread = threading.Thread(target=runner, daemon=True)
    thread.start()
    assert ready.wait(5)
    yield state, http.server_port, ticker
    http.shutdown()
    http.server_close()
    loop.call_soon_threadsafe(loop.stop)
    thread.join(5)


def wait_for(predicate, seconds=12):
    """Handle wait for."""
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.05)
    raise AssertionError("timed out")


def test_collector_real_services_reconnect_refresh_and_sigterm(tmp_path, servers):
    """Test collector real services reconnect refresh and sigterm."""
    state, port, ticker = servers
    token = tmp_path / "token.json"
    token.write_text(
        json.dumps(
            {"client_id": "client-secret", "access_token": "old-secret", "refresh_token": "old-refresh", "expires_at": 0}
        )
    )
    token.chmod(0o600)
    config = tmp_path / "config.json"
    config.write_text(
        json.dumps(
            {
                "channels": {
                    "quote": ["US.AAPL"],
                    "order_book": ["US.AAPL"],
                    "ticker": ["US.AAPL"],
                    "kline": [{"symbol": "US.AAPL", "period": "1m", "adjust": "none"}],
                },
                "backoff_base": 0.05,
                "backoff_max": 0.1,
                "channel_refresh": 0.2,
                "resubscribe_interval": 0.4,
                "token_margin": 0.5,
                "ping_interval": 0.1,
                "ping_timeout": 1,
            }
        )
    )
    output = tmp_path / "out"
    env = dict(
        os.environ,
        FUTU_LOOPBACK_PORT=str(state["port"]),
        FUTU_COORDINATION_DIR=str(tmp_path / "coord"),
        FUTU_TEST_HTTP_BASE=f"http://127.0.0.1:{port}",
    )
    process = subprocess.Popen(  # noqa: S603 -- fixed local test command
        [
            os.sys.executable,
            str(Path(__file__).with_name("loopback_runner.py")),
            "--config",
            str(config),
            "--token-file",
            str(token),
            "--output",
            str(output),
        ],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        wait_for(
            lambda: (
                state["connections"] >= 2
                and state["refreshes"] >= 1
                and OAuthHandler.calls >= 2
                and state["pings"] >= 1
                or process.poll() is not None
            )
        )
        assert process.poll() is None, process.communicate()
        # A second process must not acquire the session's owner lock.
        other = subprocess.run(  # noqa: S603, PLW1510 -- fixed local test command; status asserted
            [
                os.sys.executable,
                str(Path(__file__).with_name("loopback_runner.py")),
                "--config",
                str(config),
                "--token-file",
                str(token),
                "--output",
                str(tmp_path / "other"),
            ],
            env=env,
            capture_output=True,
            text=True,
            timeout=5,
        )
        assert other.returncode != 0
        process.send_signal(signal.SIGTERM)
        stdout, stderr = process.communicate(timeout=10)
        assert process.returncode == 0, (stdout, stderr)
    finally:
        if process.poll() is None:
            process.kill()
            process.wait()
    assert OAuthHandler.calls >= 2
    assert json.loads(token.read_text())["refresh_token"] == "rotated-secret"  # noqa: S105 -- synthetic test credential
    assert not token.with_name(token.name + ".refresh-pending").exists()
    assert state["subscriptions"] > 2
    events = [json.loads(line) for line in next(output.glob("futu-events-*.jsonl")).read_text().splitlines()]
    assert {"gap_start", "gap_end", "channel_refreshed", "archive_closed"} <= {x["event"] for x in events}
    manifest = json.loads(next((output / "futu-manifests").glob("*.json")).read_text())
    records = [json.loads(line) for f in manifest["files"] for line in Path(f["file"]).read_text().splitlines()]
    assert manifest["durable_seq"] == len(records) >= 12
    assert {r["message_type"] for r in records} >= {"QUOTE", "ORDER_BOOK", "TICKER", "KLINE", "BROKER_QUEUE"}
    assert sum(r["raw_payload"] == ticker for r in records) >= 4
    for path in output.rglob("*"):
        if path.is_file():
            assert "secret" not in path.read_text()


def test_token_store_fails_closed_after_uncertain_rotation(tmp_path, monkeypatch):
    """Test token store fails closed after uncertain rotation."""
    from openbb_futu.client import FutuError
    from openbb_futu.collector import TokenStore
    from openbb_futu.coordination import Owner

    token = tmp_path / "token"
    token.write_text(json.dumps({"client_id": "c", "access_token": "a", "refresh_token": "r", "expires_at": 0}))
    token.chmod(0o600)
    store = TokenStore(token)
    monkeypatch.setattr(
        "openbb_futu.collector.refresh_tokens", lambda *a, **k: (_ for _ in ()).throw(FutuError("transport"))
    )
    with Owner(tmp_path / "lock") as owner, pytest.raises(FutuError, match="transport"):
        store.rotate(owner)
    with pytest.raises(FutuError, match="token_rotation_uncertain"):
        store.read()


def test_unknown_business_preserved_sensitive_rejected():
    """Test unknown business preserved sensitive rejected."""
    from openbb_futu.contracts import business_frame

    raw = '{"type":"NEW_BUSINESS","data":{"value":123}}'
    assert business_frame(raw, run_id="r", connection_id="c", local_seq=1)["raw_payload"] == raw
    with pytest.raises(ValueError):
        business_frame(
            '{"type":"NEW_BUSINESS","data":{"refresh_token":"secret"}}', run_id="r", connection_id="c", local_seq=1
        )


def test_sensitive_frame_fails_explicitly_without_logging_payload(tmp_path, servers):
    """Test sensitive frame fails explicitly without logging payload."""
    state, port, _ = servers
    state["sensitive"] = True
    token = tmp_path / "token"
    token.write_text(
        json.dumps({"client_id": "c", "access_token": "a", "refresh_token": "r", "expires_at": time.time() + 100})
    )
    token.chmod(0o600)
    config = tmp_path / "config"
    config.write_text(json.dumps({"channels": {"ticker": ["US.AAPL"]}}))
    output = tmp_path / "output"
    env = dict(
        os.environ,
        FUTU_LOOPBACK_PORT=str(state["port"]),
        FUTU_COORDINATION_DIR=str(tmp_path / "coord"),
        FUTU_TEST_HTTP_BASE=f"http://127.0.0.1:{port}",
    )
    result = subprocess.run(  # noqa: S603, PLW1510 -- fixed local test command; status asserted
        [
            os.sys.executable,
            str(Path(__file__).with_name("loopback_runner.py")),
            "--config",
            str(config),
            "--token-file",
            str(token),
            "--output",
            str(output),
        ],
        env=env,
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert result.returncode != 0
    contents = "".join(p.read_text() for p in output.rglob("*") if p.is_file())
    assert "archive_rejected" in contents
    assert "gap_unresolved" in contents
    assert "do-not-log" not in contents + result.stdout + result.stderr


def test_disconnect_exhaustion_is_explicit(tmp_path, servers):
    """Test disconnect exhaustion is explicit."""
    state, _, _ = servers
    token = tmp_path / "token"
    token.write_text(
        json.dumps({"client_id": "c", "access_token": "a", "refresh_token": "r", "expires_at": time.time() + 100})
    )
    token.chmod(0o600)
    config = tmp_path / "config"
    config.write_text(json.dumps({"channels": {"ticker": ["US.AAPL"]}, "max_reconnects": 0}))
    output = tmp_path / "output"
    env = dict(os.environ, FUTU_LOOPBACK_PORT=str(state["port"]), FUTU_COORDINATION_DIR=str(tmp_path / "coord"))
    result = subprocess.run(  # noqa: S603, PLW1510 -- fixed local test command; status asserted
        [
            os.sys.executable,
            str(Path(__file__).with_name("loopback_runner.py")),
            "--config",
            str(config),
            "--token-file",
            str(token),
            "--output",
            str(output),
        ],
        env=env,
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )
    assert result.returncode != 0
    events = [json.loads(line) for line in next(output.glob("futu-events-*.jsonl")).read_text().splitlines()]
    assert any(event["event"] == "gap_unresolved" for event in events)
    assert events[-1]["status"] == "failed"
    assert state["connections"] == 1


def test_sigkill_restart_recovers_active_segment(tmp_path, servers):
    """A killed collector leaves evidence which its next process records as a gap."""
    state, port, _ = servers
    token = tmp_path / "token.json"
    token.write_text(
        json.dumps(
            {
                "client_id": "test",
                "access_token": "synthetic",
                "refresh_token": "synthetic",
                "expires_at": time.time() + 3600,
            }
        )
    )
    token.chmod(0o600)
    config = tmp_path / "config.json"
    config.write_text(json.dumps({"channels": {"ticker": ["US.AAPL"]}, "backoff_base": 0.05}))
    output = tmp_path / "out"
    env = dict(
        os.environ,
        FUTU_LOOPBACK_PORT=str(state["port"]),
        FUTU_COORDINATION_DIR=str(tmp_path / "coord"),
        FUTU_TEST_HTTP_BASE=f"http://127.0.0.1:{port}",
    )
    command = [
        os.sys.executable,
        str(Path(__file__).with_name("loopback_runner.py")),
        "--config",
        str(config),
        "--token-file",
        str(token),
        "--output",
        str(output),
    ]
    first = subprocess.Popen(command, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE)  # noqa: S603 -- fixed local test command
    try:
        wait_for(
            lambda: (
                any(p.stat().st_size for p in output.glob("source_id=futu/recv_date=*/session=*/*.open"))
                or first.poll() is not None
            )
        )
        assert first.poll() is None
        first.kill()
        first.wait(5)
        active = next(output.glob("source_id=futu/recv_date=*/session=*/*.open"))
        with active.open("ab") as stream:
            stream.write(b'{"local_seq":')
        second = subprocess.Popen(command, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE)  # noqa: S603 -- fixed local test command
        try:
            wait_for(lambda: bool(list(output.glob("futu-recovery/*/recovery.json"))) or second.poll() is not None)
            assert list(output.glob("futu-recovery/*/tail.bin"))[0].read_bytes() == b'{"local_seq":'
            second.send_signal(signal.SIGTERM)
            second.communicate(timeout=10)
            events = [
                json.loads(line) for file in output.glob("futu-events-*.jsonl") for line in file.read_text().splitlines()
            ]
            assert any(event["event"] == "interrupted_segment" and event["gap"] == "unresolved" for event in events)
        finally:
            if second.poll() is None:
                second.kill()
                second.wait()
    finally:
        if first.poll() is None:
            first.kill()
            first.wait()


def test_collector_killed_between_seal_and_manifest(tmp_path, servers):
    """Observe the real collector rename, then kill it before manifest creation."""
    import select

    from openbb_futu.archive import recover_open_files

    state, port, _ = servers
    token = tmp_path / "token.json"
    token.write_text(
        json.dumps(
            {
                "client_id": "test",
                "access_token": "synthetic",
                "refresh_token": "synthetic",
                "expires_at": time.time() + 3600,
            }
        )
    )
    token.chmod(0o600)
    config = tmp_path / "config.json"
    config.write_text(json.dumps({"channels": {"ticker": ["US.AAPL"]}, "backoff_base": 0.05}))
    output = tmp_path / "out"
    env = dict(
        os.environ,
        FUTU_LOOPBACK_PORT=str(state["port"]),
        FUTU_COORDINATION_DIR=str(tmp_path / "coord"),
        FUTU_TEST_HTTP_BASE=f"http://127.0.0.1:{port}",
        FUTU_TEST_SEAL_HANDSHAKE="1",
    )
    process = subprocess.Popen(  # noqa: S603
        [
            os.sys.executable,
            str(Path(__file__).with_name("loopback_runner.py")),
            "--config",
            str(config),
            "--token-file",
            str(token),
            "--output",
            str(output),
        ],
        env=env,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        wait_for(lambda: state["connections"] >= 2 or process.poll() is not None)
        assert process.poll() is None
        process.send_signal(signal.SIGTERM)
        ready, _, _ = select.select([process.stdout], [], [], 10)
        assert ready and process.stdout.readline() == b"sealed\n"
        process.kill()
        process.wait(timeout=10)
        assert process.returncode == -signal.SIGKILL
        sealed = list(output.glob("source_id=futu/recv_date=*/session=*/*.jsonl"))
        assert len(sealed) == 1
        original = sealed[0].read_bytes()
        assert not list((output / "futu-manifests").glob("*.json"))
        report = recover_open_files(output)[0]
        assert len(report["run_ids"]) == 1 and report["run_ids"][0] != "unknown"
        assert report["gap"] == "unresolved_after_process_interruption"
        assert sealed[0].read_bytes() == original
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=10)
        for stream in (process.stdin, process.stdout, process.stderr):
            if stream:
                stream.close()


def test_planned_cli_coverage_rebuild(tmp_path, servers):
    """Real subprocess, partial returns, reconnect, and interrupted report reconstruction."""
    state, _, _ = servers
    state["valid_tickers"] = True
    token = tmp_path / "token"
    token.write_text(
        json.dumps(
            {
                "client_id": "client",
                "access_token": "access",
                "refresh_token": "refresh",
                "expires_at": time.time() + 3600,
            }
        )
    )
    token.chmod(0o600)
    config = tmp_path / "config.json"
    dimensions = [
        {"asset": "equity", "symbol": "US.AAPL", "channel": "quote"},
        {"asset": "equity", "symbol": "US.AAPL", "channel": "ticker"},
        {"asset": "etf", "symbol": "US.SPY", "channel": "ticker"},
        {"asset": "equity", "symbol": "US.AAPL", "channel": "kline"},
        {"asset": "option", "symbol": "US.OPTION", "channel": "ticker"},
        {"asset": "future", "symbol": "US.FUTURE", "channel": "order_book"},
    ]
    config.write_text(json.dumps({"dimensions": dimensions, "subscription_budget": 5, "backoff_base": 0.05}))
    output = tmp_path / "out"
    env = dict(os.environ, FUTU_LOOPBACK_PORT=str(state["port"]), FUTU_COORDINATION_DIR=str(tmp_path / "coord"))
    cmd = [
        os.sys.executable,
        str(Path(__file__).with_name("loopback_runner.py")),
        "--config",
        str(config),
        "--token-file",
        str(token),
        "--output",
        str(output),
    ]
    process = subprocess.Popen(cmd, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)  # noqa: S603
    try:
        wait_for(lambda: state["connections"] >= 2 or process.poll() is not None)
        assert process.poll() is None, process.communicate()
        process.send_signal(signal.SIGTERM)
        stdout, stderr = process.communicate(timeout=10)
        assert process.returncode == 0, (stdout, stderr)
    finally:
        if process.poll() is None:
            process.kill()
            process.wait()
    coverage = json.loads(next(output.glob("futu-coverage-*.json")).read_text())
    assert coverage["discovery"]["universe"] == "unknown"
    assert len(coverage["selected"]) == 5 and len(coverage["not_selected"]) == 1
    assert {event["event"] for event in coverage["events"]} >= {"gap_start", "gap_end"}
    quote = next(d for d in coverage["dimensions"] if d["symbol"] == "US.AAPL" and d["channel"] == "quote")
    ticker = next(d for d in coverage["dimensions"] if d["symbol"] == "US.AAPL" and d["channel"] == "ticker")
    assert quote["durable_observations"]
    assert ticker["durable_observation_count"] >= 4
    assert ticker["durable_observations"][0]["sequence"] == 9223372036854775806
    spy = next(d for d in coverage["dimensions"] if d["symbol"] == "US.SPY")
    assert spy["durable_observation_count"] >= 4
    bar = next(d for d in coverage["dimensions"] if d["channel"] == "kline")
    assert bar["durable_observation_count"] >= 4
    run_id = coverage["run_id"]
    read = subprocess.run(  # noqa: S603
        [os.sys.executable, "-m", "openbb_futu.evidence_cli", "coverage", "--output", str(output), "--run-id", run_id],
        env=env,
        check=True,
        capture_output=True,
        text=True,
    )  # noqa: S603
    assert json.loads(read.stdout)["dimensions"] == coverage["dimensions"]
    assert coverage["not_selected"][0]["not_selected_reason"] == "budget_shortfall"
    assert all(d["subscription_ack"] in {"unknown", "not_requested"} for d in coverage["dimensions"])


def test_discovery_to_kill_recovery_cli(tmp_path, servers):
    """Bind HTTP evidence to a real collector plan and recover in another process."""
    state, port, _ = servers
    state["valid_tickers"] = True
    token = tmp_path / "token.json"
    token.write_text(
        json.dumps(
            {
                "client_id": "client",
                "access_token": "access",
                "refresh_token": "refresh",
                "expires_at": time.time() + 3600,
            }
        )
    )
    token.chmod(0o600)
    seeds = tmp_path / "seeds.json"
    seeds.write_text(json.dumps([{"asset": "option", "symbol": "US.AAPL"}, {"asset": "future", "symbol": "US.AAPL"}]))
    env = dict(
        os.environ,
        FUTU_LOOPBACK_PORT=str(state["port"]),
        FUTU_COORDINATION_DIR=str(tmp_path / "coord"),
        FUTU_TEST_HTTP_BASE=f"http://127.0.0.1:{port}",
    )
    discovery = subprocess.run(  # noqa: S603 -- fixed test command
        [
            os.sys.executable,
            "-m",
            "openbb_futu.evidence_cli",
            "discover",
            "--seeds",
            str(seeds),
            "--token-file",
            str(token),
            "--output",
            str(tmp_path / "evidence"),
        ],
        env=env,
        capture_output=True,
        text=True,
        check=True,
    )  # noqa: S603
    evidence = json.loads(discovery.stdout)
    assert [r["http_status"] for r in evidence["requests"]] == [200, 403]
    assert Path(evidence["requests"][1]["raw_file"]).read_bytes() == b'{"ret_code":403,"message":"synthetic denial"}'
    dimensions = [
        {"asset": "equity", "symbol": "US.AAPL", "channel": "ticker"},
        {"asset": "etf", "symbol": "US.SPY", "channel": "ticker"},
        {"asset": "equity", "symbol": "US.AAPL", "channel": "kline"},
        {"asset": "option", "symbol": "US.OPTION", "channel": "ticker"},
        {"asset": "future", "symbol": "US.FUTURE", "channel": "order_book"},
    ]
    config = tmp_path / "config.json"
    config.write_text(
        json.dumps(
            {
                "dimensions": dimensions,
                "subscription_budget": 4,
                "discovery_report": evidence["report_file"],
                "backoff_base": 0.05,
            }
        )
    )
    output = tmp_path / "out"
    cmd = [
        os.sys.executable,
        str(Path(__file__).with_name("loopback_runner.py")),
        "--config",
        str(config),
        "--token-file",
        str(token),
        "--output",
        str(output),
    ]
    first = subprocess.Popen(cmd, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)  # noqa: S603
    try:
        wait_for(lambda: state["connections"] >= 2 or first.poll() is not None)
        assert first.poll() is None
        wait_for(lambda: bool(list(output.glob("**/*.jsonl.open"))))
        wait_for(lambda: any('"gap_end"' in p.read_text() for p in output.glob("futu-events-*.jsonl")))
        first.kill()
        first.communicate(timeout=10)
        assert first.returncode == -signal.SIGKILL
        first_plan = next(output.glob("futu-plan-*.json"))
        first_id = first_plan.stem.removeprefix("futu-plan-")
        second = subprocess.Popen(cmd, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)  # noqa: S603
        try:
            wait_for(lambda: len(list(output.glob("futu-plan-*.json"))) == 2 or second.poll() is not None)
            wait_for(lambda: state["connections"] >= 3 or second.poll() is not None)
            assert second.poll() is None
            wait_for(
                lambda: any(
                    '"mode": "streaming"' in p.read_text()
                    for p in output.glob("futu-events-*.jsonl")
                    if first_id not in p.name
                )
            )
            second.send_signal(signal.SIGTERM)
            stdout, stderr = second.communicate(timeout=10)
            assert second.returncode == 0, (stdout, stderr)
        finally:
            if second.poll() is None:
                second.kill()
                second.communicate()
        read = subprocess.run(  # noqa: S603 -- fixed test command
            [
                os.sys.executable,
                "-m",
                "openbb_futu.evidence_cli",
                "coverage",
                "--output",
                str(output),
                "--run-id",
                first_id,
            ],
            env=env,
            capture_output=True,
            text=True,
            check=True,
        )  # noqa: S603
        prior = json.loads(read.stdout)
        assert prior["run_status"] == "interrupted" and prior["archive_manifest"] is None
        assert all(d["synced_rows"] == 0 and d["entitlement"] == "unknown" for d in prior["dimensions"])
        assert len(prior["selected"]) == 4 and len(prior["not_selected"]) == 1
        assert any(d.get("discovery_refs") == [0] for d in prior["dimensions"] if d["symbol"] == "US.OPTION")
        assert prior["discovery"]["requests"][1]["http_status"] == 403
        assert {e["event"] for e in prior["events"]} >= {"mode_change", "gap_start", "gap_end"}
        current_id = next(p.stem.removeprefix("futu-plan-") for p in output.glob("futu-plan-*.json") if p != first_plan)
        read_current = subprocess.run(  # noqa: S603
            [
                os.sys.executable,
                "-m",
                "openbb_futu.evidence_cli",
                "coverage",
                "--output",
                str(output),
                "--run-id",
                current_id,
            ],
            env=env,
            capture_output=True,
            text=True,
            check=True,
        )
        current = json.loads(read_current.stdout)
        assert any(e["event"] == "interrupted_segment" and e["gap"] == "unresolved" for e in current["events"])
        assert all(d["subscription_ack"] in {"unknown", "not_requested"} for d in current["dimensions"])
        manifest = current["archive_manifest"]
        assert manifest and manifest["run_id"] == current_id and manifest["error"] is None
        disk_manifest = json.loads((output / "futu-manifests" / f"{manifest['session_id']}.json").read_text())
        assert disk_manifest == manifest
        counts = {("US.AAPL", "ticker"): 0, ("US.SPY", "ticker"): 0, ("US.AAPL", "kline"): 0}
        for entry in disk_manifest["files"]:
            raw = Path(entry["file"]).read_bytes()
            assert hashlib.sha256(raw).hexdigest() == entry["sha256"]
            for line in raw.splitlines():
                record = json.loads(line)
                assert record["collector_run_id"] == current_id
                if record["local_seq"] > disk_manifest["durable_seq"]:
                    continue
                frame = json.loads(record["raw_payload"])
                key = (frame.get("symbol"), frame["type"].lower())
                if key in counts:
                    if key[1] == "ticker":
                        counts[key] += sum(
                            type(row.get("time_ms")) is int and type(row.get("sequence")) is int
                            for row in frame["data"].get("ticker_list", [])
                        )
                    elif (frame["data"].get("period"), frame["data"].get("adjust")) == ("1m", "none"):
                        counts[key] += 1
        assert all(counts.values())
        for dimension in current["dimensions"]:
            assert dimension["synced_rows"] == counts.get((dimension["symbol"], dimension["channel"]), 0)
            assert dimension["entitlement"] == "unknown"
    finally:
        if first.poll() is None:
            first.kill()
            first.communicate()


@pytest.mark.parametrize(
    ("fault", "expected"), [("queue", "ingress failure: Full"), ("enospc", "writer failure: OSError errno=28")]
)
def test_cli_archive_and_event_failure(tmp_path, servers, fault, expected):
    """Real CLI process reports both injected failure modes without relying on events."""
    state, _, _ = servers
    token = tmp_path / "token"
    token.write_text(
        json.dumps(
            {
                "client_id": "client",
                "access_token": "access",
                "refresh_token": "refresh",
                "expires_at": time.time() + 3600,
            }
        )
    )
    token.chmod(0o600)
    config = tmp_path / "config.json"
    config.write_text(json.dumps({"channels": {"quote": ["US.AAPL"]}, "capacity": 1, "backoff_base": 0.05}))
    env = dict(
        os.environ,
        FUTU_LOOPBACK_PORT=str(state["port"]),
        FUTU_COORDINATION_DIR=str(tmp_path / "coord"),
        FUTU_TEST_ARCHIVE_FAULT=fault,
    )
    result = subprocess.run(  # noqa: S603
        [
            os.sys.executable,
            str(Path(__file__).with_name("loopback_runner.py")),
            "--config",
            str(config),
            "--token-file",
            str(token),
            "--output",
            str(tmp_path / "out"),
        ],
        env=env,
        capture_output=True,
        text=True,
        timeout=15,
        check=False,
    )
    assert result.returncode != 0
    assert expected in result.stderr
    assert "fatal errno=5" in result.stderr
