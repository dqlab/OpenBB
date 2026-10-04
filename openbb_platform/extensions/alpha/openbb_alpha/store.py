"""Private per-principal, bounded, immutable content-addressed artifact storage."""

import hashlib
import io
import json
import os
import re
import stat
import tempfile
from pathlib import Path

from openbb_core.provider.standard_models.alpha_research import ArtifactRef

MAX_BYTES = 64 * 1024 * 1024
MAX_ROWS = 600000


def canonical(value) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def digest(value) -> str:
    return hashlib.sha256(canonical(value)).hexdigest()


class Store:
    """One OS principal per root; HTTP callers cannot choose a root or tenant.

    Deploy separate authenticated OpenBB processes/roots per principal. This is
    deliberately not a shared multi-tenant artifact server.
    """

    def __init__(self):
        configured = os.environ.get("OPENBB_ALPHA_ROOT")
        if not configured:
            raise ValueError("Set OPENBB_ALPHA_ROOT to a private research artifact directory")
        root = Path(configured).absolute()
        if any(p.is_symlink() for p in (root, *root.parents)):
            raise ValueError("artifact root must not contain symlinks")
        root.mkdir(mode=0o700, parents=True, exist_ok=True)
        info = root.stat()
        if info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) & 0o077:
            raise ValueError("artifact root must be owned by this principal with mode 0700")
        self.root = root

    @staticmethod
    def _key(key):
        if not isinstance(key, str) or not re.fullmatch(r"[0-9a-f]{64}", key):
            raise ValueError("invalid artifact ID")
        return key

    def read(self, key: str) -> bytes:
        key = self._key(key)
        directory = os.open(self.root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            fd = os.open(key, os.O_RDONLY | os.O_NOFOLLOW, dir_fd=directory)
            with os.fdopen(fd, "rb") as stream:
                info = os.fstat(stream.fileno())
                if not stat.S_ISREG(info.st_mode) or info.st_size > MAX_BYTES:
                    raise ValueError("artifact is not a bounded regular file")
                if info.st_uid != os.getuid() or info.st_nlink != 1:
                    raise ValueError("artifact owner/link policy failed")
                data = stream.read(MAX_BYTES + 1)
        except OSError as exc:
            raise ValueError("artifact unavailable or unsafe") from exc
        finally:
            os.close(directory)
        if len(data) > MAX_BYTES or hashlib.sha256(data).hexdigest() != key:
            raise ValueError("artifact integrity check failed")
        return data

    def put(self, data: bytes) -> str:
        if len(data) > MAX_BYTES:
            raise ValueError("artifact exceeds 64 MiB limit")
        key = hashlib.sha256(data).hexdigest()
        fd, temporary = tempfile.mkstemp(prefix=".pending-", dir=self.root)
        try:
            with os.fdopen(fd, "wb") as stream:
                stream.write(data)
                stream.flush()
                os.fsync(stream.fileno())
            try:
                os.link(temporary, self.root / key, follow_symlinks=False)
            except FileExistsError:
                if self.read(key) != data:
                    raise ValueError("artifact collision") from None
        finally:
            os.unlink(temporary)
        return key

    def put_json(self, kind: str, payload: dict) -> str:
        return self.put(canonical({"kind": kind, "payload": payload}))

    def get_json(self, key: str, kind: str) -> dict:
        value = json.loads(self.read(key))
        if value.get("kind") != kind:
            raise ValueError(f"expected {kind} artifact")
        return value["payload"]

    def put_table(self, table) -> ArtifactRef:
        import pyarrow.parquet as pq

        if table.num_rows > MAX_ROWS:
            raise ValueError("panel exceeds 600000 result rows")
        sink = io.BytesIO()
        pq.write_table(table, sink, compression="zstd", version="2.6")
        preview = json.loads(json.dumps(table.slice(0, 10).to_pylist(), default=str))
        return ArtifactRef(
            id=self.put(sink.getvalue()),
            media_type="application/vnd.apache.parquet",
            rows=table.num_rows,
            columns=tuple(table.column_names),
            preview=tuple(preview),
        )

    def table(self, ref: ArtifactRef):
        import pyarrow.parquet as pq

        data = self.read(ref.id)
        file = pq.ParquetFile(io.BytesIO(data))
        if file.metadata.num_rows != ref.rows or ref.rows > MAX_ROWS:
            raise ValueError("artifact row count mismatch or overflow")
        if (
            sum(
                file.metadata.row_group(i).total_byte_size
                for i in range(file.metadata.num_row_groups)
            )
            > 256 * 1024 * 1024
        ):
            raise ValueError("uncompressed panel exceeds 256 MiB")
        result = file.read()
        if tuple(result.column_names) != ref.columns:
            raise ValueError("artifact schema mismatch")
        return result
