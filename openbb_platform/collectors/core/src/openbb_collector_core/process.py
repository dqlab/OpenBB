"""Shared killable worker boundary, source pacing and bounded byte protocol."""

from __future__ import annotations

import contextlib
import json
import multiprocessing as mp
import os
import sys
import threading
import time
import traceback
from dataclasses import dataclass
from multiprocessing.connection import Connection
from typing import Any

from .models import ProviderFailure, Response
from .providers import ProviderDispatcher, json_value


def _response_payload(response: Response, source: dict[str, Any]) -> dict[str, Any]:
    if source.get("_collector"):
        response.metadata["collector"] = source["_collector"]
    rows = json_value(response.rows)
    if not isinstance(rows, list) or len(rows) > source["max_rows"]:
        raise ProviderFailure("response_row_limit")
    if not all(isinstance(row, dict) for row in rows):
        raise ProviderFailure("invalid_response")
    return {
        "rows": rows, "warning_count": response.warning_count,
        "metadata": json_value(response.metadata),
    }


def _worker(connection: Connection, source: dict[str, Any]) -> None:
    dispatcher = None
    with (
        open(os.devnull, "w") as sink,
        contextlib.redirect_stdout(sink),
        contextlib.redirect_stderr(sink),
    ):
        try:
            dispatcher = ProviderDispatcher(source)
            while True:
                request = connection.recv()
                if request is None:
                    return
                try:
                    if isinstance(request, list):
                        if not 1 <= len(request) <= source.get("batch_size", 1):
                            raise ProviderFailure("batch_request_limit")
                        responses = dispatcher.fetch_batch(request)
                        payload = {"responses": [
                            _response_payload(response, source) for response in responses
                        ]}
                    else:
                        payload = _response_payload(dispatcher.fetch(request), source)
                    encoded = json.dumps(payload, allow_nan=False).encode()
                    if len(encoded) > source["max_response_bytes"]:
                        raise ProviderFailure("response_byte_limit")
                    connection.send_bytes(encoded)
                except ProviderFailure as exc:
                    connection.send_bytes(json.dumps({"error": exc.code}).encode())
                except Exception as exc:
                    # Preserve diagnostic code locations, never vendor messages,
                    # request parameters, credentials or traceback local values.
                    print(
                        json.dumps({
                            "event": "provider_request_failed",
                            "error_type": type(exc).__name__,
                            "frames": [
                                [os.path.basename(frame.filename), frame.lineno, frame.name]
                                for frame in traceback.extract_tb(exc.__traceback__)
                            ],
                        }),
                        file=sys.__stderr__,
                        flush=True,
                    )
                    connection.send_bytes(b'{"error":"provider_error"}')
        except ProviderFailure as exc:
            connection.send_bytes(json.dumps({"error": exc.code}).encode())
        except (ImportError, AttributeError):
            connection.send_bytes(b'{"error":"provider_not_installed"}')
        except (EOFError, BrokenPipeError):
            pass
        except Exception:
            with contextlib.suppress(OSError):
                connection.send_bytes(b'{"error":"provider_initialization_failed"}')
        finally:
            if dispatcher:
                with contextlib.suppress(Exception):
                    dispatcher.close()
            connection.close()


@dataclass
class Worker:
    process: Any
    connection: Connection
    source_signature: str


class ProcessClient:
    """One worker per source; serial requests, persistent pacing and bounded waits."""

    collector_metadata: dict[str, Any] = {}

    def __init__(self, stop: threading.Event | None = None) -> None:
        self.stop = stop or threading.Event()
        self._workers: dict[str, Worker] = {}
        self._last_call: dict[str, float] = {}
        self._context = mp.get_context("spawn")

    def _stop(self, source_id: str) -> None:
        worker = self._workers.pop(source_id, None)
        if worker is None:
            return
        if worker.process.is_alive():
            with contextlib.suppress(OSError):
                worker.connection.send(None)
            worker.process.join(timeout=0.2)
        worker.connection.close()
        if worker.process.is_alive():
            worker.process.terminate()
        worker.process.join(timeout=1)
        if worker.process.is_alive():
            worker.process.kill()
            worker.process.join(timeout=1)
        worker.process.close()

    def fetch(
        self,
        source_id: str,
        source: Any,
        request: dict[str, Any],
    ) -> Response:
        return Response(**self._request(source_id, source, request))

    def fetch_batch(
        self, source_id: str, source: Any, requests: list[dict[str, Any]],
    ) -> list[Response]:
        if not 1 <= len(requests) <= getattr(source, "batch_size", 1):
            raise ProviderFailure("batch_request_limit")
        result = self._request(source_id, source, requests)
        responses = result.get("responses")
        if not isinstance(responses, list) or len(responses) != len(requests):
            self._stop(source_id)
            raise ProviderFailure("invalid_batch_response")
        return [Response(**response) for response in responses]

    def _request(self, source_id: str, source: Any, request: Any) -> dict[str, Any]:
        delay = source.min_interval_seconds - (
            time.monotonic() - self._last_call.get(source_id, -float("inf"))
        )
        if self.stop.wait(max(0, delay)):
            raise ProviderFailure("cancelled")
        self._last_call[source_id] = time.monotonic()
        source_payload = source.model_dump(mode="json")
        source_payload["_collector"] = self.collector_metadata
        signature = json.dumps(source_payload, sort_keys=True)
        worker = self._workers.get(source_id)
        if worker is None or not worker.process.is_alive() or worker.source_signature != signature:
            self._stop(source_id)
            parent, child = self._context.Pipe()
            process = self._context.Process(
                target=_worker,
                args=(child, source_payload),
                daemon=True,
            )
            process.start()
            child.close()
            worker = Worker(process, parent, signature)
            self._workers[source_id] = worker
        try:
            worker.connection.send(request)
            deadline = time.monotonic() + source.timeout_seconds
            while not worker.connection.poll(min(0.1, max(0, deadline - time.monotonic()))):
                if self.stop.is_set():
                    raise ProviderFailure("cancelled")
                if time.monotonic() >= deadline:
                    raise ProviderFailure("timeout")
            result = json.loads(worker.connection.recv_bytes(source.max_response_bytes))
            if "error" in result:
                raise ProviderFailure(result["error"])
            return result
        except (EOFError, OSError, ValueError) as exc:
            self._stop(source_id)
            raise ProviderFailure("provider_process_failed") from exc
        except ProviderFailure:
            self._stop(source_id)
            raise

    def reset(self) -> None:
        for source_id in list(self._workers):
            self._stop(source_id)
        # Never reset request pacing at a calendar boundary.

    def close(self) -> None:
        self.reset()
