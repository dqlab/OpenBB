"""Held, identity-checked descendants of a Futu evidence root.

Identity checks detect replacement; they cannot make pathname moves atomic with
subsequent filesystem operations. Callers must retain evidence on failure.
"""

import os
import stat

from openbb_futu.read_budget import EvidenceChanged


class DirectoryChain:
    """Hold every descendant descriptor and verify each parent/name binding."""

    def __init__(self, root, parts, *, create=False):
        self.root = root
        self.fds = [os.dup(root.fds[-1])]
        self.names = []
        try:
            for name in parts:
                if not isinstance(name, str) or name in ("", ".", "..") or "/" in name:
                    raise ValueError("invalid directory component")
                self.check()
                parent = self.fds[-1]
                if create:
                    try:
                        os.mkdir(name, 0o700, dir_fd=parent)
                    except FileExistsError:
                        pass
                    else:
                        os.fsync(parent)
                fd = os.open(
                    name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=parent
                )
                self.fds.append(fd)
                self.names.append(name)
                self.check()
        except BaseException:
            self.close()
            raise

    @property
    def fd(self):
        """Return the held leaf directory descriptor."""
        return self.fds[-1]

    def check(self):
        """Reject a changed root, descendant name, or directory identity."""
        self.root.check()
        try:
            for parent, fd, name in zip(self.fds, self.fds[1:], self.names):
                actual = os.stat(name, dir_fd=parent, follow_symlinks=False)
                held = os.fstat(fd)
                if not stat.S_ISDIR(actual.st_mode) or (
                    actual.st_dev,
                    actual.st_ino,
                ) != (held.st_dev, held.st_ino):
                    raise EvidenceChanged("directory replaced")
        except OSError as exc:
            raise EvidenceChanged("directory changed") from exc

    def close(self):
        """Close the held descendant descriptors without modifying evidence."""
        while self.fds:
            os.close(self.fds.pop())

    def __enter__(self):
        """Return the held chain."""
        return self

    def __exit__(self, *args):
        """Release held descriptors."""
        self.close()
