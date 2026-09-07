"""Protocol integration against a temporary loopback SFTP server; no external account."""

import contextlib
import os
import socket
import threading
from pathlib import PurePosixPath

import pytest

from openbb_collector_core.delivery import DeliveryConfig, DeliveryStore

paramiko = pytest.importorskip("paramiko", reason="install the core ssh extra for SFTP tests")


@pytest.fixture
def sftp_server(tmp_path):
    root = tmp_path / "remote"
    root.mkdir()
    server_key = paramiko.RSAKey.generate(2048)
    client_key = paramiko.RSAKey.generate(2048)
    private_key = tmp_path / "client-key"
    client_key.write_private_key_file(str(private_key))

    class Authentication(paramiko.ServerInterface):
        def get_allowed_auths(self, username):
            return "publickey"

        def check_auth_publickey(self, username, key):
            if username == "collector" and key == client_key:
                return paramiko.AUTH_SUCCESSFUL
            return paramiko.AUTH_FAILED

        def check_channel_request(self, kind, chanid):
            return (
                paramiko.OPEN_SUCCEEDED
                if kind == "session"
                else paramiko.OPEN_FAILED_ADMINISTRATIVELY_PROHIBITED
            )

    class Files(paramiko.SFTPServerInterface):
        def local(self, path):
            parts = PurePosixPath(path).parts
            if ".." in parts:
                raise PermissionError("bounded test root")
            return root.joinpath(*parts[1:])

        def stat(self, path):
            try:
                return paramiko.SFTPAttributes.from_stat(self.local(path).stat())
            except OSError as exc:
                return paramiko.SFTPServer.convert_errno(exc.errno)

        def lstat(self, path):
            try:
                return paramiko.SFTPAttributes.from_stat(self.local(path).lstat())
            except OSError as exc:
                return paramiko.SFTPServer.convert_errno(exc.errno)

        def mkdir(self, path, attr):
            try:
                self.local(path).mkdir(mode=attr.st_mode or 0o750)
                return paramiko.SFTP_OK
            except OSError as exc:
                return paramiko.SFTPServer.convert_errno(exc.errno)

        def open(self, path, flags, attr):
            try:
                fd = os.open(self.local(path), flags, 0o600)
                mode = "wb" if flags & os.O_WRONLY else "rb"
                stream = os.fdopen(fd, mode)
                handle = paramiko.SFTPHandle(flags)
                if mode == "wb":
                    handle.writefile = stream
                else:
                    handle.readfile = stream
                return handle
            except OSError as exc:
                return paramiko.SFTPServer.convert_errno(exc.errno)

        def remove(self, path):
            try:
                self.local(path).unlink()
                return paramiko.SFTP_OK
            except OSError as exc:
                return paramiko.SFTPServer.convert_errno(exc.errno)

        def rename(self, oldpath, newpath):
            try:
                os.link(self.local(oldpath), self.local(newpath))
                self.local(oldpath).unlink()
                return paramiko.SFTP_OK
            except OSError as exc:
                return paramiko.SFTPServer.convert_errno(exc.errno)

    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(4)
    listener.settimeout(0.1)
    port = listener.getsockname()[1]
    known_hosts = tmp_path / "known_hosts"
    known_hosts.write_text(
        f"[127.0.0.1]:{port} {server_key.get_name()} {server_key.get_base64()}\n"
    )
    stop = threading.Event()
    transports = []

    def serve():
        while not stop.is_set():
            try:
                connection, _ = listener.accept()
            except TimeoutError:
                continue
            except OSError:
                break
            transport = paramiko.Transport(connection)
            transports.append(transport)
            transport.add_server_key(server_key)
            transport.set_subsystem_handler("sftp", paramiko.SFTPServer, Files)
            with contextlib.suppress(EOFError, paramiko.SSHException):
                transport.start_server(server=Authentication())

    thread = threading.Thread(target=serve, daemon=True)
    thread.start()
    config = DeliveryConfig(
        collector_id="ssh-edge",
        transport="sftp",
        destination_root="/master",
        ssh={
            "host": "127.0.0.1",
            "port": port,
            "user": "collector",
            "known_hosts": known_hosts,
            "private_key": private_key,
        },
        timeout_seconds=2,
        max_attempt_seconds=20,
        remove_local_after_verification=True,
    )
    try:
        yield config, root
    finally:
        stop.set()
        listener.close()
        for transport in transports:
            transport.close()
        thread.join(timeout=5)
        assert not thread.is_alive()


def make_store(tmp_path, config):
    source = tmp_path / "source"
    raw = source / "bronze/ab/test.json"
    raw.parent.mkdir(parents=True)
    raw.write_bytes(b'{"data":[1,2,3]}\n')
    store = DeliveryStore(source, config)
    store.stage(
        "live",
        "session",
        [{"path": "bronze/ab/test.json", "role": "raw", "remove": True}],
        {"revision": "v1"},
    )
    return store, raw


def test_sftp_verified_move_with_pinned_host_and_key_auth(sftp_server, tmp_path):
    config, remote = sftp_server
    store, raw = make_store(tmp_path, config)
    expected = raw.read_bytes()
    try:
        assert store.send_pending()[0]["status"] == "delivered"
        assert not raw.exists()
        assert (
            next(remote.glob("master/remote_collectors/ssh-edge/objects/sha256/*/*")).read_bytes()
            == expected
        )
        assert len(list(remote.rglob("*.received.json"))) == 1
        assert not list(remote.rglob("*.incomplete"))
    finally:
        store.close()


def test_unknown_ssh_host_is_rejected_and_local_data_kept(sftp_server, tmp_path):
    config, remote = sftp_server
    config.ssh.known_hosts.write_text("")
    store, raw = make_store(tmp_path, config)
    try:
        assert store.send_pending()[0]["status"] == "pending"
        assert raw.exists()
        assert not list(remote.rglob("*.received.json"))
    finally:
        store.close()


def test_remote_symlink_is_rejected(sftp_server, tmp_path):
    config, remote = sftp_server
    (remote / "other").mkdir()
    (remote / "master").symlink_to(remote / "other", target_is_directory=True)
    store, raw = make_store(tmp_path, config)
    try:
        assert store.send_pending()[0]["error"] == "unsafe_remote_directory"
        assert raw.exists()
    finally:
        store.close()
