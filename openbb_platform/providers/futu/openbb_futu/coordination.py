"""Host-local coordination in a private, trusted local directory.

The effective user and all same-UID processes are trusted, as are root and the
filesystem. Other users must not be able to modify the directory or its
ancestors; a root-owned sticky ancestor (such as /tmp) is allowed. ACLs,
mounts and network filesystem semantics must enforce the same restrictions.
SQLite uses pathnames for its journal: a held descriptor alone cannot secure
those opens against a malicious same-UID process or privileged mount changes.
"""

from __future__ import annotations

import fcntl
import math
import os
import stat
import threading
import time
from collections import deque
from contextlib import contextmanager, suppress
from pathlib import Path

from openbb_futu.root_handle import RootHandle


def coordination_directory():
    """Return the directory all cooperating local clients must share."""
    return Path(
        os.environ.get(
            "FUTU_COORDINATION_DIR", Path.home() / ".local/state/openbb-futu"
        )
    )


def _trusted_directory(info, *, leaf=False):
    uid = os.geteuid()
    if not stat.S_ISDIR(info.st_mode) or info.st_uid not in (0, uid):
        raise PermissionError("untrusted coordination directory owner")
    if leaf:
        if info.st_uid != uid or info.st_mode & 0o077:
            raise PermissionError("coordination directory must be private")
    elif info.st_mode & 0o022 and not (
        info.st_uid == 0 and info.st_mode & stat.S_ISVTX
    ):
        raise PermissionError("writable coordination ancestor")


def _root(directory):
    raw = os.fspath(directory)
    if ".." in raw.split(os.sep):
        raise ValueError("parent directory component is not allowed")
    path = Path(os.path.abspath(raw))
    if path == Path("/"):
        raise PermissionError("coordination directory must be private")
    fd = os.open("/", os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        _trusted_directory(os.fstat(fd))
        for i, name in enumerate(path.parts[1:]):
            with suppress(FileExistsError):
                os.mkdir(name, 0o700, dir_fd=fd)
            child = os.open(
                name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd
            )
            os.close(fd)
            fd = child
            _trusted_directory(os.fstat(fd), leaf=i == len(path.parts) - 2)
        root = RootHandle(path)
        if (os.fstat(fd).st_dev, os.fstat(fd).st_ino) != (
            os.fstat(root.fds[-1]).st_dev,
            os.fstat(root.fds[-1]).st_ino,
        ):
            root.close()
            raise PermissionError("coordination directory changed")
        try:
            _check_root(root)
        except BaseException:
            root.close()
            raise
        return root
    finally:
        os.close(fd)


def _check_root(root):
    root.check()
    for i, fd in enumerate(root.fds):
        _trusted_directory(os.fstat(fd), leaf=i == len(root.fds) - 1)


def _regular(info):
    if (
        not stat.S_ISREG(info.st_mode)
        or info.st_uid != os.geteuid()
        or info.st_nlink != 1
        or info.st_mode & 0o077
    ):
        raise PermissionError("unsafe coordination file")


def _open_file(root, name):
    _check_root(root)
    fd = os.open(
        name,
        os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | os.O_NONBLOCK,
        0o600,
        dir_fd=root.fds[-1],
    )
    try:
        _regular(os.fstat(fd))
        _same_file(root, name, fd)
        return fd
    except BaseException:
        os.close(fd)
        raise


def _same_file(root, name, fd):
    _check_root(root)
    actual = os.stat(name, dir_fd=root.fds[-1], follow_symlinks=False)
    _regular(actual)
    held = os.fstat(fd)
    _regular(held)
    if (actual.st_dev, actual.st_ino) != (held.st_dev, held.st_ino):
        raise PermissionError("coordination file replaced")


def _sidecars(root):
    for name in (
        "requests.sqlite3-journal",
        "requests.sqlite3-wal",
        "requests.sqlite3-shm",
    ):
        try:
            info = os.stat(name, dir_fd=root.fds[-1], follow_symlinks=False)
        except FileNotFoundError:
            continue
        _regular(info)
        if name.endswith(("-wal", "-shm")):
            raise PermissionError("unsupported SQLite sidecar")


BUDGET_LIMIT = 70
BUDGET_WINDOW = 60.0


class Owner:
    """Hold a kernel lock for the entire upstream session, including reconnects."""

    def __init__(self, directory: Path):
        self._pid = os.getpid()
        self._mutex = threading.RLock()
        self._budget_config: tuple[int, float] | None = None
        self._requests: deque[float] = deque()
        self._root = _root(directory)
        self.directory = self._root.path
        self._fd = None
        try:
            self._fd = _open_file(self._root, "upstream.lock")
            try:
                fcntl.flock(self._fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise RuntimeError("upstream already owned") from None
            _same_file(self._root, "upstream.lock", self._fd)
            self._check()
            self._acquired_at = time.monotonic()
        except BaseException:
            self.close()
            raise

    def _check(self):
        if self._fd is None or os.getpid() != self._pid:
            raise RuntimeError("invalid owner")
        _same_file(self._root, "upstream.lock", self._fd)
        # Linux fdinfo reports locks attached to this open file description;
        # contention alone could be caused by a different owner.
        try:
            with open(f"/proc/self/fdinfo/{self._fd}", encoding="ascii") as info:
                lines = info.read().splitlines()
        except (OSError, UnicodeError) as exc:
            raise RuntimeError("owner lock unavailable") from exc
        if not any(
            line.startswith("lock:") and " FLOCK " in line and " WRITE " in line
            for line in lines
        ):
            raise RuntimeError("owner lock lost or unsupported")

    @contextmanager
    def borrow(self, directory):
        """Hold the owner alive for the complete operation, including sends."""
        if os.getpid() != self._pid:
            raise RuntimeError("invalid owner")
        with self._mutex:
            if Path(os.path.abspath(directory)) != self.directory:
                raise RuntimeError("invalid owner")
            self._check()
            yield self

    def budget(self, limit=None, window=None):
        """Return this owner's shared monotonic sliding-window budget."""
        limit = BUDGET_LIMIT if limit is None else limit
        window = BUDGET_WINDOW if window is None else window
        valid_limit = (
            isinstance(limit, int) and not isinstance(limit, bool) and limit >= 1
        )
        valid_window = (
            isinstance(window, (int, float))
            and not isinstance(window, bool)
            and math.isfinite(window)
            and window > 0
        )
        if not valid_limit or not valid_window:
            raise ValueError("invalid budget")
        if os.getpid() != self._pid:
            raise RuntimeError("invalid owner")
        with self._mutex:
            self._check()
            config = (limit, float(window))
            if config != (BUDGET_LIMIT, BUDGET_WINDOW):
                raise ValueError("shared budget configuration mismatch")
            if self._budget_config is None:
                self._budget_config = config
            elif self._budget_config != config:
                raise ValueError("shared budget configuration mismatch")
            return RequestBudget(self, *config)

    @property
    def acquired_at(self):
        """Return the monotonic start of this owner's initial cooldown."""
        return self._acquired_at

    @property
    def descriptor(self):
        """Return the lock descriptor while the caller holds a borrowed operation."""
        self._check()
        return self._fd

    def record_dispatch(self):
        """Keep the latest reservation charged through the borrowed request's exit."""
        with self._mutex:
            self._requests[-1] = time.monotonic()

    def reserve_request(self, limit, window):
        """Reserve a slot under this owner's lock and monotonic request history."""
        if os.getpid() != self._pid:
            raise RuntimeError("invalid owner")
        with self._mutex:
            self._check()
            now = time.monotonic()
            remaining = self._acquired_at + window - now
            if remaining > 0:
                return remaining
            while self._requests and self._requests[0] <= now - window:
                self._requests.popleft()
            if len(self._requests) >= limit:
                return max(0.000001, self._requests[0] + window - now)
            self._requests.append(now)
            return 0.0

    def close(self):
        """Release only after all borrowed operations have finished."""
        if os.getpid() != self._pid:
            raise RuntimeError("invalid owner")
        with self._mutex:
            if self._fd is not None:
                os.close(self._fd)
                self._fd = None
            self._root.close()

    def __enter__(self):
        """Return this owner."""
        return self

    def __exit__(self, *_):
        """Release the owner."""
        self.close()


class RequestBudget:
    """Owner-scoped budget; reservations never touch persistent storage."""

    def __init__(self, owner, limit, window):
        self._owner = owner
        self.limit, self.window = limit, window

    def acquire(self):
        """Reserve a slot, or return the positive wait until one is available."""
        return self._owner.reserve_request(self.limit, self.window)
