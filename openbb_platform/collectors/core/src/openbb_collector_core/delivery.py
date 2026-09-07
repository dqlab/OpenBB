"""Durable, verified session delivery to a local directory or an SSH/SFTP master.

The source journal remains local. Objects and committed manifests are immutable;
only individually acknowledged data files may be removed from the collector.
"""

from __future__ import annotations

import calendar
import contextlib
import hashlib
import io
import json
import os
import re
import sqlite3
import stat
import time
import uuid
from collections.abc import Iterable
from datetime import UTC, datetime, timedelta
from pathlib import Path, PurePosixPath
from typing import Any, BinaryIO, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

_CHUNK = 1024 * 1024
_IDENTIFIER = r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,99}$"


class DeliveryError(RuntimeError):
    """Safe diagnostic code, without endpoint, credential or filesystem details."""


class SSHConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    host: str = Field(min_length=1, max_length=253)
    user: str = Field(min_length=1, max_length=64)
    port: int = Field(default=22, ge=1, le=65535)
    known_hosts: Path
    private_key: Path | None = None

    @field_validator("host", "user")
    @classmethod
    def no_control_characters(cls, value: str) -> str:
        if any(c.isspace() or ord(c) < 32 for c in value):
            raise ValueError("SSH endpoint values cannot contain whitespace or control characters")
        return value


class DeliveryConfig(BaseModel):
    """Destination and explicit retention policy; omitted delivery keeps local behavior."""

    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)
    collector_id: str = Field(pattern=_IDENTIFIER)
    transport: Literal["local", "sftp"]
    destination_root: str = Field(min_length=1, max_length=2048)
    ssh: SSHConfig | None = None
    remove_local_after_verification: bool = False
    local_retention: str | None = Field(default=None, max_length=32)
    retry_seconds: float = Field(default=60, ge=1, le=86400)
    timeout_seconds: float = Field(default=20, ge=0.1, le=300)
    max_attempt_seconds: float = Field(default=300, ge=1, le=86400)
    max_files: int = Field(default=100000, ge=1, le=1000000)
    max_bytes: int = Field(default=100_000_000_000, ge=1, le=10_000_000_000_000)

    @field_validator("local_retention")
    @classmethod
    def valid_local_retention(cls, value: str | None) -> str | None:
        if value is None or value in {"after_verification", "forever"}:
            return value
        match = re.fullmatch(r"([1-9][0-9]*)(h|d|w|mo)", value)
        maximum = {"h": 876000, "d": 36500, "w": 5200, "mo": 1200}
        if not match or int(match[1]) > maximum[match[2]]:
            raise ValueError(
                "Use after_verification, forever, or a positive duration such as 1d, 1w, 1mo "
                "(at most 100 years)"
            )
        return value

    def retention_policy(self) -> str:
        if self.local_retention is not None:
            return self.local_retention
        return "after_verification" if self.remove_local_after_verification else "forever"

    def cleanup_after(self, first_verified_at: float | None) -> float | None:
        """Expiry starts at the first durable local acknowledgment, in UTC."""
        policy = self.retention_policy()
        if policy == "forever" or first_verified_at is None:
            return None
        if policy == "after_verification":
            return first_verified_at
        match = re.fullmatch(r"([1-9][0-9]*)(h|d|w|mo)", policy)
        count, unit = int(match[1]), match[2]
        start = datetime.fromtimestamp(first_verified_at, UTC)
        if unit == "mo":
            month_index = start.year * 12 + start.month - 1 + count
            year, month = divmod(month_index, 12)
            month += 1
            day = min(start.day, calendar.monthrange(year, month)[1])
            return start.replace(year=year, month=month, day=day).timestamp()
        seconds = count * {"h": 3600, "d": 86400, "w": 604800}[unit]
        return (start + timedelta(seconds=seconds)).timestamp()

    @model_validator(mode="after")
    def connection_contract(self) -> DeliveryConfig:
        if self.local_retention is not None and self.remove_local_after_verification:
            raise ValueError("Use local_retention instead of remove_local_after_verification")
        if (self.transport == "sftp") != (self.ssh is not None):
            raise ValueError("SFTP requires SSH settings; local delivery does not use them")
        if any(ord(c) < 32 for c in self.destination_root):
            raise ValueError("Destination cannot contain control characters")
        if self.transport == "sftp":
            path = PurePosixPath(self.destination_root)
            if not path.is_absolute() or str(path) == "/" or ".." in path.parts:
                raise ValueError("SFTP destination must be a bounded absolute directory")
        return self

    def resolve_paths(self, config_directory: Path) -> None:
        if self.transport == "local":
            path = Path(self.destination_root).expanduser()
            self.destination_root = str((config_directory / path).resolve())
        if self.ssh:
            for name in ("known_hosts", "private_key"):
                path = getattr(self.ssh, name)
                if path is not None:
                    setattr(self.ssh, name, (config_directory / path.expanduser()).resolve())

    def target_id(self) -> str:
        value = {
            "transport": self.transport,
            "root": self.destination_root,
            "collector": self.collector_id,
        }
        if self.ssh:
            value["endpoint"] = [self.ssh.host, self.ssh.port, self.ssh.user]
        return hashlib.sha256(json_bytes(value)).hexdigest()


def json_bytes(value: Any) -> bytes:
    return (
        json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n"
    ).encode()


def relative_path(value: str) -> str:
    path = PurePosixPath(value)
    if (
        not value
        or path.is_absolute()
        or ".." in path.parts
        or "\\" in value
        or ":" in value
        or any(ord(c) < 32 for c in value)
        or path.as_posix() != value
        or str(path) == "."
    ):
        raise DeliveryError("unsafe_artifact_path")
    return value


def local_path(root: Path, relative: str) -> Path:
    path = root
    for part in PurePosixPath(relative_path(relative)).parts:
        path = path / part
        if path.is_symlink():
            raise DeliveryError("symlink_artifact")
    if not path.resolve().is_relative_to(root.resolve()):
        raise DeliveryError("artifact_outside_root")
    return path


def stream_hash(stream: BinaryIO, limit: int, deadline: float = float("inf")) -> tuple[str, int]:
    digest = hashlib.sha256()
    size = 0
    while block := stream.read(_CHUNK):
        if time.monotonic() > deadline:
            raise DeliveryError("delivery_timeout")
        size += len(block)
        if size > limit:
            raise DeliveryError("artifact_size_limit")
        digest.update(block)
    return digest.hexdigest(), size


def file_hash(path: Path, limit: int) -> tuple[str, int]:
    if not path.is_file() or path.is_symlink():
        raise DeliveryError("artifact_not_regular_file")
    with path.open("rb") as stream:
        return stream_hash(stream, limit)


def atomic_local(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    try:
        with temporary.open("xb") as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        if os.name != "nt":
            descriptor = os.open(path.parent, os.O_RDONLY)
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
    finally:
        temporary.unlink(missing_ok=True)


class LocalDestination:
    def __init__(self, config: DeliveryConfig, source_root: Path):
        self.root = Path(config.destination_root).expanduser().resolve()
        if self.root == Path(self.root.anchor) or self.root.is_relative_to(source_root):
            raise DeliveryError("destination_overlaps_collector")
        if source_root.is_relative_to(self.root):
            raise DeliveryError("destination_overlaps_collector")
        self.root.mkdir(parents=True, exist_ok=True)

    def open_read(self, name: str):
        return local_path(self.root, name).open("rb")

    def open_write(self, name: str):
        path = local_path(self.root, name)
        path.parent.mkdir(parents=True, exist_ok=True)
        return path.open("xb")

    def exists(self, name: str) -> bool:
        path = local_path(self.root, name)
        if path.exists() and not path.is_file():
            raise DeliveryError("destination_not_regular_file")
        return path.exists()

    def finish(self, temporary: str, final: str) -> None:
        source, target = local_path(self.root, temporary), local_path(self.root, final)
        # Hard-link publication is atomic and never overwrites an existing object.
        try:
            os.link(source, target)
        except FileExistsError:
            pass
        source.unlink()
        if os.name != "nt":
            descriptor = os.open(target.parent, os.O_RDONLY)
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)

    def discard(self, name: str) -> None:
        local_path(self.root, name).unlink(missing_ok=True)

    def close(self) -> None:
        pass


class SFTPDestination:
    """Optional portable SSH transport; no shell commands or remote Python required."""

    def __init__(self, config: DeliveryConfig, source_root: Path):
        try:
            import paramiko
        except ImportError as exc:
            raise DeliveryError("install_collector_ssh_extra") from exc
        self.client = paramiko.SSHClient()
        self.sftp = None
        self.root = PurePosixPath(config.destination_root)
        ssh = config.ssh
        try:
            self.client.load_host_keys(str(ssh.known_hosts))
            self.client.set_missing_host_key_policy(paramiko.RejectPolicy())
            self.client.connect(
                hostname=ssh.host,
                port=ssh.port,
                username=ssh.user,
                key_filename=str(ssh.private_key) if ssh.private_key else None,
                allow_agent=ssh.private_key is None,
                look_for_keys=False,
                timeout=config.timeout_seconds,
                banner_timeout=config.timeout_seconds,
                auth_timeout=config.timeout_seconds,
            )
            self.sftp = self.client.open_sftp()
            self.sftp.get_channel().settimeout(config.timeout_seconds)
            self._directory(self.root)
        except Exception:
            self.close()
            raise

    def _directory(self, path: PurePosixPath) -> None:
        current = PurePosixPath("/")
        for part in path.parts[1:]:
            current /= part
            try:
                attrs = self.sftp.lstat(str(current))
            except FileNotFoundError:
                self.sftp.mkdir(str(current), mode=0o750)
                attrs = self.sftp.lstat(str(current))
            if not stat.S_ISDIR(attrs.st_mode):
                raise DeliveryError("unsafe_remote_directory")

    def _path(self, name: str) -> str:
        path = self.root / relative_path(name)
        self._directory(path.parent)
        try:
            attrs = self.sftp.lstat(str(path))
        except FileNotFoundError:
            return str(path)
        if not stat.S_ISREG(attrs.st_mode):
            raise DeliveryError("unsafe_remote_artifact")
        return str(path)

    def open_read(self, name: str):
        return self.sftp.open(self._path(name), "rb")

    def open_write(self, name: str):
        return self.sftp.open(self._path(name), "wx")

    def exists(self, name: str) -> bool:
        path = self._path(name)
        try:
            self.sftp.stat(path)
            return True
        except FileNotFoundError:
            return False

    def finish(self, temporary: str, final: str) -> None:
        # SFTP rename, unlike posix_rename, must not replace an existing file.
        try:
            self.sftp.rename(self._path(temporary), self._path(final))
        except OSError:
            if not self.exists(final):
                raise
            self.discard(temporary)

    def discard(self, name: str) -> None:
        with contextlib.suppress(FileNotFoundError):
            self.sftp.remove(self._path(name))

    def close(self) -> None:
        if self.sftp:
            self.sftp.close()
        self.client.close()


def destination(config: DeliveryConfig, source_root: Path):
    cls = LocalDestination if config.transport == "local" else SFTPDestination
    return cls(config, source_root)


def put_verified(
    backend, name: str, source: BinaryIO | None, sha256: str, size: int, deadline: float
) -> None:
    def verify(key):
        with backend.open_read(key) as stream:
            if stream_hash(stream, size, deadline) != (sha256, size):
                raise DeliveryError("central_checksum_conflict")

    if backend.exists(name):
        verify(name)
        return
    if source is None:
        raise DeliveryError("missing_local_and_central_artifact")
    temporary = name + "." + uuid.uuid4().hex + ".incomplete"
    try:
        with backend.open_write(temporary) as stream:
            digest = hashlib.sha256()
            count = 0
            while block := source.read(_CHUNK):
                if time.monotonic() > deadline:
                    raise DeliveryError("delivery_timeout")
                count += len(block)
                if count > size:
                    raise DeliveryError("local_artifact_changed")
                digest.update(block)
                stream.write(block)
            stream.flush()
            if backend.__class__ is LocalDestination:
                os.fsync(stream.fileno())
            if (digest.hexdigest(), count) != (sha256, size):
                raise DeliveryError("local_artifact_changed")
        verify(temporary)
        backend.finish(temporary, name)
        verify(name)
    finally:
        with contextlib.suppress(Exception):
            backend.discard(temporary)


class DeliveryStore:
    """Outbox and file receipts are separate from the engine's acquisition journal."""

    def __init__(self, root: Path, config: DeliveryConfig):
        self.root, self.config = root.resolve(), config
        self.state_root = local_path(self.root, "delivery")
        self.state_root.mkdir(parents=True, exist_ok=True)
        self.target = config.target_id()
        self.db = sqlite3.connect(self.state_root / "state.sqlite3")
        self.db.row_factory = sqlite3.Row
        self.db.executescript("""
            PRAGMA journal_mode=WAL;
            PRAGMA synchronous=FULL;
            CREATE TABLE IF NOT EXISTS jobs (
                id TEXT PRIMARY KEY, target TEXT NOT NULL, engine TEXT NOT NULL,
                session_id TEXT NOT NULL, manifest TEXT NOT NULL,
                status TEXT NOT NULL, local_removed INTEGER NOT NULL DEFAULT 0,
                attempts INTEGER NOT NULL DEFAULT 0,
                next_attempt REAL NOT NULL DEFAULT 0, error TEXT, receipt TEXT);
            CREATE TABLE IF NOT EXISTS files (
                target TEXT NOT NULL, path TEXT NOT NULL, sha256 TEXT NOT NULL,
                size INTEGER NOT NULL, verified INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY(target,path));
            CREATE TABLE IF NOT EXISTS staged_sessions (
                target TEXT NOT NULL, engine TEXT NOT NULL, session_id TEXT NOT NULL,
                revision TEXT NOT NULL, job_id TEXT NOT NULL,
                PRIMARY KEY(target,engine,session_id,revision));
        """)
        columns = {row[1] for row in self.db.execute("PRAGMA table_info(jobs)")}
        if "first_verified_at" not in columns:
            with self.db:
                self.db.execute("ALTER TABLE jobs ADD COLUMN first_verified_at REAL")
        self.db.create_function("cleanup_after", 1, self.config.cleanup_after)

    def close(self) -> None:
        self.db.close()

    def staged(self, engine: str, session_id: str, revision: str) -> bool:
        return (
            self.db.execute(
                "SELECT 1 FROM staged_sessions WHERE target=? AND engine=? "
                "AND session_id=? AND revision=?",
                (self.target, engine, session_id, revision),
            ).fetchone()
            is not None
        )

    def snapshot(
        self, engine: str, session_id: str, revision: str, name: str, rows: Iterable[Any]
    ) -> dict[str, Any]:
        for identifier in (engine, session_id, revision, name):
            if not re.fullmatch(_IDENTIFIER, identifier):
                raise DeliveryError("invalid_snapshot_identity")
        relative = f"delivery/snapshots/{engine}/{session_id}/{revision}/{name}.jsonl"
        path = local_path(self.root, relative)
        previous = self.db.execute(
            "SELECT verified FROM files WHERE target=? AND path=?", (self.target, relative)
        ).fetchone()
        if not path.exists() and not (previous and previous[0]):
            path.parent.mkdir(parents=True, exist_ok=True)
            temporary = path.with_name(f".{name}.{uuid.uuid4().hex}.tmp")
            try:
                size = 0
                with temporary.open("xb") as stream:
                    for row in rows:
                        encoded = json_bytes(row)
                        size += len(encoded)
                        if size > self.config.max_bytes:
                            raise DeliveryError("snapshot_byte_limit")
                        stream.write(encoded)
                    stream.flush()
                    os.fsync(stream.fileno())
                os.replace(temporary, path)
            finally:
                temporary.unlink(missing_ok=True)
        return {"path": relative, "role": name, "remove": name == "observations"}

    def stage(
        self,
        engine: str,
        session_id: str,
        files: Iterable[dict[str, Any]],
        metadata: dict[str, Any],
    ) -> str:
        if not re.fullmatch(_IDENTIFIER, engine) or not re.fullmatch(_IDENTIFIER, session_id):
            raise DeliveryError("invalid_session_identity")
        items = []
        total = 0
        paths = set()
        for value in files:
            name = relative_path(value["path"])
            if name in paths:
                continue
            if len(items) >= self.config.max_files:
                raise DeliveryError("delivery_file_limit")
            paths.add(name)
            path = local_path(self.root, name)
            previous = self.db.execute(
                "SELECT * FROM files WHERE target=? AND path=?", (self.target, name)
            ).fetchone()
            if path.exists():
                sha, size = file_hash(path, self.config.max_bytes)
                if previous and (previous["sha256"], previous["size"]) != (sha, size):
                    raise DeliveryError("immutable_local_artifact_changed")
            elif previous and previous["verified"]:
                sha, size = previous["sha256"], previous["size"]
            else:
                raise DeliveryError("missing_local_artifact")
            total += size
            if total > self.config.max_bytes:
                raise DeliveryError("delivery_byte_limit")
            role = value["role"]
            if role not in {"raw", "observations", "export", "report", "attempts", "quarantine"}:
                raise DeliveryError("invalid_artifact_role")
            allowed = {
                "raw": ("bronze/",),
                "export": ("records/", "observations/"),
                "observations": ("delivery/snapshots/",),
                "attempts": ("delivery/snapshots/",),
                "report": ("reports/", "delivery/snapshots/"),
                "quarantine": ("quarantine/",),
            }
            if not name.startswith(allowed[role]):
                raise DeliveryError("protected_local_artifact")
            removable = bool(value.get("remove", False)) and role in {
                "raw",
                "observations",
                "export",
                "quarantine",
            }
            items.append(
                {"path": name, "sha256": sha, "size": size, "role": role, "remove": removable}
            )
            with self.db:
                self.db.execute(
                    "INSERT OR IGNORE INTO files(target,path,sha256,size) VALUES(?,?,?,?)",
                    (self.target, name, sha, size),
                )
        manifest = {
            "schema_version": 1,
            "collector_id": self.config.collector_id,
            "destination_id": self.target,
            "engine": engine,
            "session_id": session_id,
            "metadata": metadata,
            "files": sorted(items, key=lambda item: item["path"]),
            "total_bytes": total,
        }
        encoded = json_bytes(manifest)
        if len(encoded) > 64_000_000:
            raise DeliveryError("manifest_size_limit")
        identifier = hashlib.sha256(encoded).hexdigest()
        with self.db:
            self.db.execute(
                "INSERT OR IGNORE INTO jobs(id,target,engine,session_id,manifest,status) "
                "VALUES(?,?,?,?,?,'pending')",
                (identifier, self.target, engine, session_id, encoded.decode()),
            )
            self.db.execute(
                "INSERT OR IGNORE INTO staged_sessions VALUES(?,?,?,?,?)",
                (self.target, engine, session_id, metadata["revision"], identifier),
            )
        atomic_local(local_path(self.root, f"delivery/manifests/{identifier}.json"), encoded)
        return identifier

    def status(self, limit: int = 20) -> dict[str, Any]:
        return _delivery_status(self.db, self.config, limit)

    def send_pending(self, *, force: bool = False, limit: int = 1) -> list[dict[str, Any]]:
        if not 1 <= limit <= 100:
            raise ValueError("Delivery attempt limit must be 1..100")
        rows = self.db.execute(
            "SELECT * FROM jobs WHERE target=? "
            "AND (status<>'delivered' OR (? AND local_removed=0 "
            "AND (first_verified_at IS NULL OR cleanup_after(first_verified_at)<=?))) "
            "AND (? OR next_attempt<=?) ORDER BY rowid LIMIT ?",
            (
                self.target,
                self.config.retention_policy() != "forever",
                time.time(),
                force,
                time.time(),
                limit,
            ),
        ).fetchall()
        results = []
        deadline = time.monotonic() + self.config.max_attempt_seconds
        for job in rows:
            if time.monotonic() >= deadline:
                break
            try:
                receipt = self._send(job, deadline)
                local_removed = self._cleanup(job)
                with self.db:
                    self.db.execute(
                        "UPDATE jobs SET status='delivered',error=NULL,receipt=?,"
                        "local_removed=?,attempts=attempts+1 WHERE id=?",
                        (
                            json_bytes(receipt).decode(),
                            local_removed,
                            job["id"],
                        ),
                    )
                outcome = {
                    "event": "session_delivered",
                    "delivery_id": job["id"],
                    "session_id": job["session_id"],
                    "status": "delivered",
                    "local_removed": local_removed,
                    "local_retention": self.config.retention_policy(),
                }
            except Exception as exc:
                code = str(exc) if isinstance(exc, DeliveryError) else "delivery_transport_error"
                with self.db:
                    self.db.execute(
                        "UPDATE jobs SET status='pending',error=?,attempts=attempts+1,"
                        "next_attempt=? WHERE id=?",
                        (code, time.time() + self.config.retry_seconds, job["id"]),
                    )
                outcome = {
                    "event": "session_delivery_pending",
                    "delivery_id": job["id"],
                    "session_id": job["session_id"],
                    "status": "pending",
                    "error": code,
                }
            results.append(outcome)
        return results

    def _send(self, job, deadline: float) -> dict[str, Any]:
        manifest = json.loads(job["manifest"])
        prefix = f"remote_collectors/{self.config.collector_id}"
        session_prefix = f"{prefix}/sessions/{job['engine']}/{job['session_id']}/{job['id']}"
        backend = destination(self.config, self.root)
        try:
            for item in manifest["files"]:
                path = local_path(self.root, item["path"])
                name = f"{prefix}/objects/sha256/{item['sha256'][:2]}/{item['sha256']}"
                with contextlib.ExitStack() as stack:
                    stream = stack.enter_context(path.open("rb")) if path.exists() else None
                    put_verified(backend, name, stream, item["sha256"], item["size"], deadline)
            encoded = job["manifest"].encode()
            put_verified(
                backend,
                session_prefix + ".manifest.json",
                io.BytesIO(encoded),
                job["id"],
                len(encoded),
                deadline,
            )
            receipt_name = session_prefix + ".received.json"
            if backend.exists(receipt_name):
                with backend.open_read(receipt_name) as stream:
                    encoded_receipt = stream.read(65537)
                if len(encoded_receipt) > 65536:
                    raise DeliveryError("invalid_central_receipt")
                receipt = json.loads(encoded_receipt)
                expected = {
                    "schema_version": 1,
                    "manifest_sha256": job["id"],
                    "collector_id": self.config.collector_id,
                    "engine": job["engine"],
                    "session_id": job["session_id"],
                    "files": len(manifest["files"]),
                    "bytes": manifest["total_bytes"],
                }
                if not isinstance(receipt, dict) or any(
                    receipt.get(k) != v for k, v in expected.items()
                ):
                    raise DeliveryError("invalid_central_receipt")
            else:
                receipt = {
                    "schema_version": 1,
                    "manifest_sha256": job["id"],
                    "collector_id": self.config.collector_id,
                    "engine": job["engine"],
                    "session_id": job["session_id"],
                    "files": len(manifest["files"]),
                    "bytes": manifest["total_bytes"],
                    "verified_at": datetime.now(UTC).isoformat(),
                }
                encoded_receipt = json_bytes(receipt)
                put_verified(
                    backend,
                    receipt_name,
                    io.BytesIO(encoded_receipt),
                    hashlib.sha256(encoded_receipt).hexdigest(),
                    len(encoded_receipt),
                    deadline,
                )
            # Durably save acknowledgment before removing any source file.
            atomic_local(
                local_path(self.root, f"delivery/receipts/{job['id']}.json"), json_bytes(receipt)
            )
            with self.db:
                for item in manifest["files"]:
                    self.db.execute(
                        "UPDATE files SET verified=1 WHERE target=? AND path=?",
                        (self.target, item["path"]),
                    )
                self.db.execute(
                    "UPDATE jobs SET receipt=?,first_verified_at=COALESCE(first_verified_at,?) "
                    "WHERE id=?",
                    (json_bytes(receipt).decode(), time.time(), job["id"]),
                )
            return receipt
        finally:
            backend.close()

    def _cleanup(self, job) -> bool:
        first_verified_at = self.db.execute(
            "SELECT first_verified_at FROM jobs WHERE id=?", (job["id"],)
        ).fetchone()[0]
        expiry = self.config.cleanup_after(first_verified_at)
        if expiry is None or time.time() < expiry:
            return False
        for item in json.loads(job["manifest"])["files"]:
            if not item["remove"]:
                continue
            path = local_path(self.root, item["path"])
            if path.exists():
                if file_hash(path, item["size"]) != (item["sha256"], item["size"]):
                    raise DeliveryError("local_artifact_changed_before_cleanup")
                path.unlink()
        return True


def deliver_sessions(config, journal, sessions, *, force=False, send=True, limit=10):
    """Seal closed sessions and drain the outbox under the engine's writer lock."""
    if config.delivery is None:
        return {"enabled": False, "counts": {}, "jobs": [], "events": []}
    store = DeliveryStore(journal.root, config.delivery)
    events = []
    try:
        staged = 0
        for engine, session_id, revision, artifacts, metadata in sessions(store):
            if store.staged(engine, session_id, revision):
                continue
            try:
                store.stage(engine, session_id, artifacts(), {**metadata, "revision": revision})
            except Exception as exc:
                code = str(exc) if isinstance(exc, DeliveryError) else "session_sealing_failed"
                events.append(
                    {
                        "event": "session_delivery_pending",
                        "session_id": session_id,
                        "status": "pending",
                        "error": code,
                    }
                )
            staged += 1
            if staged >= limit:
                break
        if send:
            events.extend(store.send_pending(force=force, limit=limit))
        return {
            "enabled": True,
            **store.status(),
            "events": events,
            "batch_limit_reached": staged >= limit,
        }
    finally:
        store.close()


def _delivery_status(db, config: DeliveryConfig | None, limit=20):
    if not 1 <= limit <= 100:
        raise ValueError("Delivery status limit must be 1..100")
    # The read-only command can inspect a pre-retention outbox without migrating it.
    columns = {row[1] for row in db.execute("PRAGMA table_info(jobs)")}
    verified_column = "first_verified_at" if "first_verified_at" in columns else "NULL"
    jobs = []
    for row in db.execute(
        "SELECT id,target,engine,session_id,status,attempts,error,local_removed,"
        f"{verified_column} AS first_verified_at FROM jobs ORDER BY rowid DESC LIMIT ?",
        (limit,),
    ):
        job = dict(row)
        first = job["first_verified_at"]
        active = config is not None and job["target"] == config.target_id()
        expiry = config.cleanup_after(first) if active else None
        state = (
            "removed"
            if job["local_removed"]
            else "awaiting_verification"
            if first is None
            else "cleanup_due"
            if expiry is not None and time.time() >= expiry
            else "retained"
        )
        job.update(
            first_verified_at=datetime.fromtimestamp(first, UTC).isoformat()
            if first is not None
            else None,
            cleanup_after=datetime.fromtimestamp(expiry, UTC).isoformat()
            if expiry is not None
            else None,
            local_state=state,
        )
        jobs.append(job)
    return {
        "counts": dict(db.execute("SELECT status,count(*) FROM jobs GROUP BY status")),
        "local_retention": config.retention_policy() if config else None,
        "jobs": jobs,
    }


def read_delivery_status(root: Path, config: DeliveryConfig | None, limit=20):
    """Inspect without taking a writer lock, loading providers or opening SSH."""
    if not 1 <= limit <= 100:
        raise ValueError("Delivery status limit must be 1..100")
    path = local_path(root.resolve(), "delivery/state.sqlite3")
    if not path.exists():
        return {
            "enabled": config is not None,
            "counts": {},
            "jobs": [],
            "local_retention": config.retention_policy() if config else None,
        }
    with contextlib.closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)) as db:
        db.row_factory = sqlite3.Row
        return {"enabled": config is not None, **_delivery_status(db, config, limit)}


def read_received_session(
    root: Path, manifest_path: str, *, max_files=100000, max_bytes=100_000_000_000
) -> dict[str, Any]:
    """Verify a committed master-side session and resolve its content-addressed files.

    This verifies delivery only. Canonical normalization/publication belongs to the
    data platform and must apply its own dataset and quality contracts.
    """
    root = root.resolve()
    parts = PurePosixPath(relative_path(manifest_path)).parts
    if (
        len(parts) != 6
        or parts[0] != "remote_collectors"
        or parts[2] != "sessions"
        or any(not re.fullmatch(_IDENTIFIER, value) for value in (parts[1], parts[3], parts[4]))
        or not re.fullmatch(r"[0-9a-f]{64}\.manifest\.json", parts[5])
    ):
        raise DeliveryError("invalid_session_manifest_path")
    path = local_path(root, manifest_path)
    with path.open("rb") as stream:
        encoded = stream.read(64_000_001)
    if len(encoded) > 64_000_000:
        raise DeliveryError("manifest_size_limit")
    identifier = parts[5].split(".")[0]
    if hashlib.sha256(encoded).hexdigest() != identifier:
        raise DeliveryError("manifest_checksum_conflict")
    manifest = json.loads(encoded)
    if (
        not isinstance(manifest, dict)
        or manifest.get("schema_version") != 1
        or manifest.get("collector_id") != parts[1]
        or manifest.get("engine") != parts[3]
        or manifest.get("session_id") != parts[4]
    ):
        raise DeliveryError("invalid_session_manifest")
    files = manifest.get("files")
    if not isinstance(files, list) or len(files) > max_files:
        raise DeliveryError("delivery_file_limit")
    receipt_path = manifest_path.removesuffix(".manifest.json") + ".received.json"
    with local_path(root, receipt_path).open("rb") as stream:
        encoded_receipt = stream.read(65537)
    if len(encoded_receipt) > 65536:
        raise DeliveryError("invalid_central_receipt")
    receipt = json.loads(encoded_receipt)
    expected = {
        "schema_version": 1,
        "manifest_sha256": identifier,
        "collector_id": parts[1],
        "engine": parts[3],
        "session_id": parts[4],
        "files": len(files),
        "bytes": manifest.get("total_bytes"),
    }
    if not isinstance(receipt, dict) or any(receipt.get(k) != v for k, v in expected.items()):
        raise DeliveryError("invalid_central_receipt")
    resolved = []
    seen = set()
    total = 0
    for item in files:
        if not isinstance(item, dict):
            raise DeliveryError("invalid_manifest_artifact")
        name = relative_path(item.get("path", ""))
        if name in seen:
            raise DeliveryError("duplicate_manifest_artifact")
        seen.add(name)
        sha, size = item.get("sha256"), item.get("size")
        if (
            not isinstance(sha, str)
            or not re.fullmatch(r"[0-9a-f]{64}", sha)
            or type(size) is not int
            or size < 0
        ):
            raise DeliveryError("invalid_manifest_artifact")
        total += size
        if total > max_bytes:
            raise DeliveryError("delivery_byte_limit")
        object_path = f"remote_collectors/{parts[1]}/objects/sha256/{sha[:2]}/{sha}"
        if file_hash(local_path(root, object_path), size) != (sha, size):
            raise DeliveryError("central_checksum_conflict")
        resolved.append({**item, "object_path": object_path})
    if type(manifest.get("total_bytes")) is not int or total != manifest["total_bytes"]:
        raise DeliveryError("manifest_reconciliation_failed")
    return {**manifest, "files": resolved, "receipt": receipt}
