"""Bounded interrupted-segment inspection and atomic recovery publication primitives."""

import ctypes
import errno
import io
import json
import os
import sys


def rename_noreplace(dir_fd, source, destination):
    """Atomically publish without aliases or replacement; fail closed elsewhere."""
    if sys.platform != "linux":
        raise OSError(errno.ENOTSUP, "atomic no-replace rename unavailable")
    libc = ctypes.CDLL(None, use_errno=True)
    try:
        rename = libc.renameat2
    except AttributeError as exc:
        raise OSError(errno.ENOTSUP, "renameat2 unavailable") from exc
    rename.argtypes = [
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_uint,
    ]
    rename.restype = ctypes.c_int
    if rename(dir_fd, os.fsencode(source), dir_fd, os.fsencode(destination), 1):
        code = ctypes.get_errno()
        raise OSError(code, os.strerror(code), destination)


def inspect_recovery(path, data, budget):
    """Inspect a bounded snapshot using the tick file prefix semantics."""
    count = offset = 0
    first_seq = last_seq = None
    error = None
    run_ids = set()
    for line in io.BytesIO(data):
        budget.charge_line()
        if len(line) > budget.line_limit:
            raise ValueError("recovery line too large")
        if not line.endswith(b"\n"):
            error = "incomplete_line"
            break
        try:
            record = json.loads(
                line, parse_constant=lambda _: (_ for _ in ()).throw(ValueError())
            )
        except (ValueError, UnicodeDecodeError):
            error = "invalid_json"
            break
        if not isinstance(record, dict) or "local_seq" not in record:
            error = "missing_sequence"
            break
        seq = record["local_seq"]
        if not (isinstance(seq, int) and seq.__class__ is int) or seq < 1:
            error = "invalid_sequence"
            break
        if last_seq is not None and seq != last_seq + 1:
            error = "noncontiguous_sequence"
            break
        run_id = record.get("collector_run_id")
        if not isinstance(run_id, str) or not run_id:
            run_ids.add("unknown")
        else:
            run_ids.add(run_id)
        first_seq = seq if first_seq is None else first_seq
        last_seq = seq
        count += 1
        offset += len(line)
    return (
        {
            "file": str(path),
            "records": count,
            "first_seq": first_seq,
            "last_seq": last_seq,
            "valid_bytes": offset,
            "unread_bytes": len(data) - offset,
            "error": error,
        },
        run_ids,
    )
