"""Host-local coordination for cooperating FUTUNN processes.

All participants must use the same local directory and configuration. This is
not an IP-wide service and cannot account for other hosts or applications.
"""

from __future__ import annotations

import fcntl
import os
import sqlite3
import time
from contextlib import contextmanager
from pathlib import Path


def coordination_directory():
    """Return the directory all cooperating local clients must share."""
    return Path(
        os.environ.get(
            "FUTU_COORDINATION_DIR", Path.home() / ".local/state/openbb-futu"
        )
    )


class Owner:
    """Hold a kernel lock for the entire upstream session, including reconnects."""

    def __init__(self, directory: Path):
        directory = Path(directory).resolve()
        self.directory = directory
        directory.mkdir(mode=0o700, parents=True, exist_ok=True)
        self._file = (directory / "upstream.lock").open("a+")
        try:
            fcntl.flock(self._file, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            self._file.close()
            raise RuntimeError("upstream already owned") from None

    @contextmanager
    def borrow(self, directory):
        """Use an existing live owner without reacquiring or releasing its lock."""
        if self._file.closed or Path(directory).resolve() != self.directory:
            raise RuntimeError("invalid owner")
        yield self

    def close(self):
        """Handle close."""
        if not self._file.closed:
            fcntl.flock(self._file, fcntl.LOCK_UN)
            self._file.close()

    def __enter__(self):
        """Enter the ownership context."""
        return self

    def __exit__(self, *_):
        """Release the ownership context."""
        self.close()


class RequestBudget:
    """A persisted sliding window, serialized with SQLite across local processes."""

    def __init__(self, directory: Path, limit: int = 70, window: float = 60):
        if isinstance(limit, bool) or limit < 1 or window <= 0:
            raise ValueError("invalid budget")
        directory.mkdir(mode=0o700, parents=True, exist_ok=True)
        self.path = directory / "requests.sqlite3"
        self.limit, self.window = limit, window
        # Restrict the database even when the invoking process has a lax umask.
        fd = os.open(self.path, os.O_CREAT | os.O_RDWR, 0o600)
        os.close(fd)
        with sqlite3.connect(self.path, timeout=10) as db:
            db.execute(
                "CREATE TABLE IF NOT EXISTS config (limit_value INTEGER, window REAL)"
            )
            db.execute("CREATE TABLE IF NOT EXISTS requests (at REAL NOT NULL)")
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT limit_value, window FROM config").fetchone()
            if row is None:
                db.execute("INSERT INTO config VALUES (?, ?)", (limit, window))
            elif row != (limit, window):
                raise ValueError("shared budget configuration mismatch")

    def acquire(self):
        """Reserve one request or return seconds until the next reservation."""
        with sqlite3.connect(self.path, timeout=10, isolation_level=None) as db:
            db.execute("BEGIN IMMEDIATE")
            now = time.time()
            # Future timestamps after clock rollback deliberately keep the budget closed.
            db.execute("DELETE FROM requests WHERE at <= ?", (now - self.window,))
            count, oldest = db.execute(
                "SELECT COUNT(*), MIN(at) FROM requests"
            ).fetchone()
            if count >= self.limit:
                db.commit()
                return max(0.001, oldest + self.window - now)
            db.execute("INSERT INTO requests VALUES (?)", (now,))
            db.commit()
            return 0.0
