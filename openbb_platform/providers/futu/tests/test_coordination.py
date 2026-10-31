"""Cross-process coordination uses a shared local filesystem."""

import os
import subprocess
import sys

import pytest
from openbb_futu.coordination import Owner, RequestBudget


def test_budget_shared_and_configuration_fixed(tmp_path):
    """Test budget shared and configuration fixed."""
    budget = RequestBudget(tmp_path, limit=2, window=60)
    assert budget.acquire() == 0
    script = (
        "from pathlib import Path; from openbb_futu.coordination import RequestBudget; "
        "import sys; print(RequestBudget(Path(sys.argv[1]), 2, 60).acquire())"
    )
    result = subprocess.check_output([sys.executable, "-c", script, str(tmp_path)], env=os.environ)  # noqa: S603 -- fixed local test command
    assert float(result) == 0
    assert budget.acquire() > 0
    with pytest.raises(ValueError, match="configuration mismatch"):
        RequestBudget(tmp_path, limit=3, window=60)


def test_owner_is_released_when_process_exits(tmp_path):
    """Test owner is released when process exits."""
    script = (
        "from pathlib import Path; from openbb_futu.coordination import Owner; "
        "import sys; owner=Owner(Path(sys.argv[1])); print('locked', flush=True); "
        "sys.stdin.read()"
    )
    process = subprocess.Popen(  # noqa: S603 -- fixed local test command
        [sys.executable, "-c", script, str(tmp_path)],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        text=True,
        env=os.environ,
    )
    try:
        assert process.stdout.readline().strip() == "locked"
        with pytest.raises(RuntimeError, match="already owned"):
            Owner(tmp_path)
    finally:
        process.kill()
        process.wait(timeout=5)
        process.stdin.close()
        process.stdout.close()
    with Owner(tmp_path):
        pass


def test_rest_obeys_shared_budget_and_owner(tmp_path, monkeypatch):
    """Test rest obeys shared budget and owner."""
    from unittest.mock import patch

    from openbb_futu.client import FutuError, snapshot

    monkeypatch.setenv("FUTU_COORDINATION_DIR", str(tmp_path))
    budget = RequestBudget(tmp_path)
    for _ in range(70):
        assert budget.acquire() == 0
    with patch("openbb_futu.client.make_request") as request:
        with pytest.raises(FutuError, match="local_budget_exhausted"):
            snapshot(["US.AAPL"], "secret")
        request.assert_not_called()
        with Owner(tmp_path), pytest.raises(FutuError, match="coordination"):
            snapshot(["US.AAPL"], "secret")
        request.assert_not_called()
