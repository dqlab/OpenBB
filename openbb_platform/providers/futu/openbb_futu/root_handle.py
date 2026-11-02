"""Descriptor-anchored, no-follow publication under an existing root."""

import os
import secrets
import stat
from contextlib import suppress
from pathlib import Path

from openbb_futu.read_budget import EvidenceChanged


class PublicationDurabilityUnknown(OSError):
    """The name was published, but directory durability could not be confirmed."""


TEMP_ATTEMPTS = 32


class RootHandle:
    """Keep every ancestor open and verify that the original names still match."""

    @classmethod
    def create(cls, root):
        """Create missing components without traversing symlinks."""
        raw = os.fspath(root)
        if ".." in raw.split(os.sep):
            raise ValueError("parent directory component is not allowed")
        path = Path(os.path.abspath(raw))
        fds = [os.open("/", os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)]
        names: list[str] = []
        try:
            for name in path.parts[1:]:
                for parent, held, component in zip(fds, fds[1:], names):
                    actual = os.stat(component, dir_fd=parent, follow_symlinks=False)
                    info = os.fstat(held)
                    if not stat.S_ISDIR(actual.st_mode) or (
                        actual.st_dev,
                        actual.st_ino,
                    ) != (info.st_dev, info.st_ino):
                        raise EvidenceChanged("root or ancestor replaced")
                try:
                    os.mkdir(name, 0o700, dir_fd=fds[-1])
                except FileExistsError:
                    pass
                else:
                    os.fsync(fds[-1])
                fds.append(
                    os.open(
                        name,
                        os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                        dir_fd=fds[-1],
                    )
                )
                names.append(name)
            result = cls(path)
            actual, original = os.fstat(result.fds[-1]), os.fstat(fds[-1])
            if (actual.st_dev, actual.st_ino) != (original.st_dev, original.st_ino):
                result.close()
                raise EvidenceChanged("root replaced")
            return result
        finally:
            for fd in reversed(fds):
                os.close(fd)

    def __init__(self, root):
        raw = os.fspath(root)
        if ".." in raw.split(os.sep):
            raise ValueError("parent directory component is not allowed")
        self.path = Path(os.path.abspath(raw))
        self.names = self.path.parts[1:]
        self.fds = [os.open("/", os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)]
        try:
            for name in self.names:
                self.fds.append(
                    os.open(
                        name,
                        os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                        dir_fd=self.fds[-1],
                    )
                )
            self.check()
        except BaseException:
            self.close()
            raise

    def __enter__(self):
        """Return the held root."""
        return self

    def __exit__(self, *args):
        """Close all held descriptors."""
        self.close()

    def close(self):
        """Close all held descriptors."""
        while self.fds:
            os.close(self.fds.pop())

    def check(self):
        """Reject a changed root or ancestor."""
        root = os.fstat(self.fds[-1])
        if (
            not stat.S_ISDIR(root.st_mode)
            or root.st_uid != os.geteuid()
            or root.st_mode & 0o022
        ):
            raise PermissionError(
                "output root must be owned by the effective user and not group/other writable"
            )
        try:
            for parent, fd, name in zip(self.fds, self.fds[1:], self.names):
                actual = os.stat(name, dir_fd=parent, follow_symlinks=False)
                held = os.fstat(fd)
                if not stat.S_ISDIR(actual.st_mode) or (
                    actual.st_dev,
                    actual.st_ino,
                ) != (held.st_dev, held.st_ino):
                    raise EvidenceChanged("root or ancestor replaced")
        except OSError as exc:
            raise EvidenceChanged("root or ancestor changed") from exc

    def directory(self, parts, *, create=False):
        """Open descendants by component; caller closes the returned fd."""
        fd = os.dup(self.fds[-1])
        try:
            for name in parts:
                if name in ("", ".", "..") or "/" in name:
                    raise ValueError("invalid directory component")
                if create:
                    with suppress(FileExistsError):
                        os.mkdir(name, 0o700, dir_fd=fd)
                    self.check()
                child = os.open(
                    name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd
                )
                os.close(fd)
                fd = child
            return fd
        except BaseException:
            os.close(fd)
            raise

    def publish(self, name, content, *, replace=False):
        """Publish through an exclusive temporary, optionally replacing a report."""
        if name in ("", ".", "..") or "/" in name:
            raise ValueError("invalid file name")
        self.check()
        parent = self.fds[-1]
        for _ in range(TEMP_ATTEMPTS):
            temp = ".tmp-" + secrets.token_hex(16)
            try:
                fd = os.open(
                    temp,
                    os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                    0o600,
                    dir_fd=parent,
                )
                break
            except FileExistsError:
                continue
        else:
            raise FileExistsError("temporary name collision limit reached")
        published = False
        try:
            try:
                with os.fdopen(fd, "wb") as stream:
                    stream.write(content)
                    stream.flush()
                    os.fsync(stream.fileno())
                self.check()
                if replace:
                    os.replace(temp, name, src_dir_fd=parent, dst_dir_fd=parent)
                else:
                    os.link(
                        temp,
                        name,
                        src_dir_fd=parent,
                        dst_dir_fd=parent,
                        follow_symlinks=False,
                    )
                published = True
                os.fsync(parent)
                self.check()
            finally:
                with suppress(FileNotFoundError):
                    os.unlink(temp, dir_fd=parent)
        except Exception as exc:
            if published:
                raise PublicationDurabilityUnknown(
                    getattr(exc, "errno", None),
                    "published; durability or location unknown",
                ) from exc
            raise
