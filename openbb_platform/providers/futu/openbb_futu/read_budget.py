"""Shared bounded reads of regular evidence files.

Limits bound userspace work, not time spent in a blocking filesystem syscall.
"""

import os
import stat


def open_directory(path, *, identities=None):
    """Open each component without following links and check directory identities."""
    path = os.fspath(path)
    if ".." in path.split(os.sep):
        raise ValueError("parent directory component is not allowed")
    path = os.path.abspath(path)
    components = [part for part in path.split(os.sep) if part]
    fds = [os.open("/", os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)]
    paths = ["/"]
    try:
        for component in components:
            fds.append(os.open(component, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                               dir_fd=fds[-1]))
            paths.append(os.path.join(paths[-1], component))
        for index, fd in enumerate(fds):
            info = os.fstat(fd)
            identity = (info.st_dev, info.st_ino)
            if index:
                current = os.stat(components[index - 1], dir_fd=fds[index - 1],
                                  follow_symlinks=False)
                if (current.st_dev, current.st_ino) != identity or not stat.S_ISDIR(current.st_mode):
                    raise EvidenceChanged("directory replaced")
            if identities is not None:
                previous = identities.setdefault(paths[index], identity)
                if previous != identity:
                    raise EvidenceChanged("directory replaced")
        return os.dup(fds[-1])
    finally:
        for fd in reversed(fds):
            os.close(fd)


def open_regular(path, *, identities=None):
    """Open a regular file through no-follow directory descriptors."""
    parent, name = os.path.split(os.fspath(path))
    parent_fd = open_directory(parent or ".", identities=identities)
    try:
        fd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=parent_fd)
    finally:
        os.close(parent_fd)
    try:
        if not stat.S_ISREG(os.fstat(fd).st_mode):
            raise ValueError("evidence is not a regular file")
        return fd
    except BaseException:
        os.close(fd)
        raise


class EvidenceChanged(ValueError):
    """An evidence identity changed during reconstruction."""


class BudgetExceeded(ValueError):
    """An evidence read exceeded its shared budget."""


class Budget:
    """Mutable report-wide byte and line allowance, injectable by callers."""

    def __init__(self, *, bytes_limit=64 * 1024 * 1024, lines_limit=100_000,
                 chunk_size=64 * 1024, line_limit=1024 * 1024, entries_limit=100_000):
        for value in (bytes_limit, lines_limit, chunk_size, line_limit, entries_limit):
            if type(value) is not int or value <= 0:
                raise ValueError("budget limits must be positive integers")
        self._directory_identities = {}
        self._root_entry_identities = {}
        self.entries_left = entries_limit
        self.bytes_left = bytes_limit
        self.lines_left = lines_limit
        self.chunk_size = chunk_size
        self.line_limit = line_limit

    def entries(self, path):
        """Enumerate and charge every directory entry using a no-follow fd."""
        try:
            fd = open_directory(path, identities=self._directory_identities)
        except FileNotFoundError:
            return
        try:
            with os.scandir(fd) as entries:
                for entry in entries:
                    if self.entries_left <= 0:
                        raise BudgetExceeded("directory entry budget exceeded")
                    self.entries_left -= 1
                    yield entry.name
            check_fd = open_directory(path, identities=self._directory_identities)
            os.close(check_fd)
        finally:
            os.close(fd)

    def entries_at(self, handle):
        """Enumerate the held root, charging every entry and checking its name."""
        handle.check()
        fd = os.open(".", os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=handle.fds[-1])
        try:
            with os.scandir(fd) as entries:
                for entry in entries:
                    if self.entries_left <= 0:
                        raise BudgetExceeded("directory entry budget exceeded")
                    self.entries_left -= 1
                    st = entry.stat(follow_symlinks=False)
                    self._root_entry_identities[(id(handle), entry.name)] = (
                        st.st_dev, st.st_ino, st.st_mode, st.st_size, st.st_mtime_ns, st.st_ctime_ns)
                    yield entry.name
            handle.check()
        finally:
            os.close(fd)

    def open_regular_at(self, handle, name):
        """Open a single root entry without following links or blocking on FIFOs."""
        if name in ("", ".", "..") or "/" in name:
            raise ValueError("invalid evidence name")
        handle.check()
        fd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK,
                     dir_fd=handle.fds[-1])
        try:
            if not stat.S_ISREG(os.fstat(fd).st_mode):
                raise ValueError("evidence is not a regular file")
            return fd
        except BaseException:
            os.close(fd)
            raise

    def read_identity_at(self, handle, name, *, max_bytes):
        """Read and revalidate an entry relative to the same held root."""
        if type(max_bytes) is not int or max_bytes <= 0:
            raise ValueError("max_bytes must be positive")
        fd = self.open_regular_at(handle, name)
        try:
            def identity(st):
                return (st.st_dev, st.st_ino, st.st_mode, st.st_size, st.st_mtime_ns, st.st_ctime_ns)
            initial = identity(os.fstat(fd))
            discovered = self._root_entry_identities.get((id(handle), name))
            if discovered is not None and discovered != initial:
                raise EvidenceChanged("discovered evidence changed")
            parts = []
            total = 0
            while True:
                chunk = os.read(fd, min(self.chunk_size, max_bytes - total + 1, self.bytes_left + 1))
                if not chunk:
                    if identity(os.fstat(fd)) != initial:
                        raise EvidenceChanged("evidence changed")
                    self.check_identity_at(handle, name, initial)
                    return b"".join(parts), initial
                total += len(chunk)
                self.charge_read(len(chunk))
                if total > max_bytes:
                    raise BudgetExceeded("evidence byte budget exceeded")
                parts.append(chunk)
        finally:
            os.close(fd)

    def check_identity_at(self, handle, name, identity):
        """Revalidate an evidence name against the original descriptor identity."""
        fd = self.open_regular_at(handle, name)
        try:
            st = os.fstat(fd)
            if (st.st_dev, st.st_ino, st.st_mode, st.st_size, st.st_mtime_ns, st.st_ctime_ns) != identity:
                raise EvidenceChanged("evidence replaced or changed")
            handle.check()
        finally:
            os.close(fd)

    def open_regular(self, path):
        """Open using the directory identities shared by this budget."""
        return open_regular(path, identities=self._directory_identities)

    def charge_read(self, size):
        """Account for a chunk read by the verifier."""
        self.bytes_left -= size
        if self.bytes_left < 0:
            raise BudgetExceeded("evidence byte budget exceeded")

    def charge_line(self):
        """Charge one complete record."""
        if self.lines_left <= 0:
            raise BudgetExceeded("evidence line budget exceeded")
        self.lines_left -= 1

    def read(self, path, *, max_bytes):
        """Read one regular file, checking both per-file and shared byte limits."""
        return self.read_identity(path, max_bytes=max_bytes)[0]

    def read_identity(self, path, *, max_bytes):
        """Read a file and return its checked identity for later revalidation."""
        if type(max_bytes) is not int or max_bytes <= 0:
            raise ValueError("max_bytes must be positive")
        fd = self.open_regular(path)
        try:
            if not stat.S_ISREG(os.fstat(fd).st_mode):
                raise ValueError("evidence is not a regular file")
            initial = os.fstat(fd)
            def identity(st):
                return (st.st_dev, st.st_ino, st.st_mode, st.st_size, st.st_mtime_ns, st.st_ctime_ns)
            parts = []
            total = 0
            while True:
                size = min(self.chunk_size, max_bytes - total + 1, self.bytes_left + 1)
                chunk = os.read(fd, size)
                if not chunk:
                    if identity(os.fstat(fd)) != identity(initial):
                        raise ValueError("evidence changed")
                    check_fd = self.open_regular(path)
                    try:
                        if identity(os.fstat(check_fd)) != identity(initial):
                            raise ValueError("evidence replaced")
                    finally:
                        os.close(check_fd)
                    return b"".join(parts), identity(initial)
                total += len(chunk)
                self.bytes_left -= len(chunk)
                if total > max_bytes or self.bytes_left < 0:
                    raise BudgetExceeded("evidence byte budget exceeded")
                parts.append(chunk)
        finally:
            os.close(fd)

    def lines(self, path):
        """Yield bounded newline-terminated records from a regular file."""
        fd = self.open_regular(path)
        try:
            if not stat.S_ISREG(os.fstat(fd).st_mode):
                raise ValueError("evidence is not a regular file")
            initial = os.fstat(fd)
            pending = b""
            while True:
                chunk = os.read(fd, min(self.chunk_size, self.bytes_left + 1))
                self.bytes_left -= len(chunk)
                if self.bytes_left < 0:
                    raise BudgetExceeded("evidence byte budget exceeded")
                if not chunk:
                    if pending:
                        raise ValueError("incomplete event")
                    check_fd = self.open_regular(path)
                    try:
                        current = os.fstat(check_fd)
                        fields = ("st_dev", "st_ino", "st_size", "st_mtime_ns", "st_ctime_ns")
                        if any(getattr(current, key) != getattr(initial, key)
                               or getattr(os.fstat(fd), key) != getattr(initial, key)
                               for key in fields):
                            raise EvidenceChanged("event evidence changed")
                    finally:
                        os.close(check_fd)
                    return
                pending += chunk
                while b"\n" in pending:
                    line, pending = pending.split(b"\n", 1)
                    if len(line) + 1 > self.line_limit or self.lines_left <= 0:
                        raise BudgetExceeded("event line budget exceeded")
                    self.lines_left -= 1
                    yield line + b"\n"
                if len(pending) >= self.line_limit:
                    raise BudgetExceeded("event line too large")
        finally:
            os.close(fd)
