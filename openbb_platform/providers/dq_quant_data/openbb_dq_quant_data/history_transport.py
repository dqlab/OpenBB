"""Fixed history HTTP bridge over an explicitly configured SSH command channel.

No forwarding, shell configuration, arbitrary command, target URL or write API is
accepted. The remote HTTP client runs inside the already authorized container.
"""

import json
import re
import shlex
import subprocess
from pathlib import Path
from urllib.parse import urlsplit

from pydantic import BaseModel, ConfigDict, Field
from requests import PreparedRequest, Response
from requests.adapters import BaseAdapter

from openbb_dq_quant_data.native import NativeQueryError


class HistoryConnection(BaseModel):
    """Operator configuration, separate from the existing DQ endpoint settings."""

    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)
    transport: str = Field(pattern=r"^ssh-command$")
    ssh_host: str = Field(pattern=r"^[a-zA-Z0-9][a-zA-Z0-9.-]{0,252}$")
    ssh_user: str = Field(pattern=r"^[a-zA-Z_][a-zA-Z0-9_-]{0,63}$")
    identity_file: str = Field(pattern=r"^/[^\x00\r\n]+$")
    docker_path: str = Field(pattern=r"^/[^\x00\r\n]+$")
    container_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    releases: dict[str, str] = Field(min_length=1, max_length=16)


# This constant is the entire remote program. Input contains the key and one
# bounded request, never code. Remote failures never print exception contents.
REMOTE = r"""
import json,re,sys,urllib.request,urllib.error
try:
 v=json.load(sys.stdin)
 assert re.fullmatch(r'/v2/history/[0-9a-f]{64}(/(scopes|coverage|pages))?',v['path'])
 assert v['method']==('GET' if v['body'] is None else 'POST')
 body=None if v['body'] is None else json.dumps(v['body'],allow_nan=False).encode()
 req=urllib.request.Request('http://127.0.0.1:8000'+v['path'],data=body,
     headers={'X-API-Key':v['key'],'Content-Type':'application/json'},method=v['method'])
 class NoRedirect(urllib.request.HTTPRedirectHandler):
  def redirect_request(self,*args,**kwargs): return None
 opener=urllib.request.build_opener(urllib.request.ProxyHandler({}),NoRedirect())
 try: r=opener.open(req,timeout=12)
 except urllib.error.HTTPError as e: r=e
 with r:
  content=r.read(16*1024*1024+1)
  assert len(content)<=16*1024*1024
  body=content.decode('utf-8')
  assert not v['key'] or v['key'] not in body
  print(json.dumps({'status':r.code,'body':body,'retry_after':r.headers.get('Retry-After')}))
except Exception:
 sys.exit(1)
"""


class HistorySSHAdapter(BaseAdapter):
    """Bridge only the pinned history paths; the key travels exclusively on stdin."""

    def __init__(self, config: HistoryConnection, key: str) -> None:
        super().__init__()
        self.config = config
        self._key = key

    # Adapter options cannot loosen the fixed timeout or enable another transport.
    # pylint: disable=unused-argument,too-many-boolean-expressions
    def send(self, request: PreparedRequest, *args, **kwargs) -> Response:
        """Execute once with strict host checking, no forwarding and a fixed timeout."""
        url = urlsplit(request.url or "")
        match = re.fullmatch(
            r"/v2/history/([0-9a-f]{64})(/(scopes|coverage|pages))?", url.path
        )
        if (
            url.scheme != "http"
            or url.netloc != "dq-fixed-history.invalid"
            or url.query
            or url.fragment
            or not match
            or match[1] not in self.config.releases.values()
            or request.method != ("POST" if match[2] else "GET")
        ):
            raise NativeQueryError(None)
        remote = [
            self.config.docker_path,
            "exec",
            "-i",
            "-e",
            "PYTHONDONTWRITEBYTECODE=1",
            self.config.container_id,
            "python",
            "-c",
            REMOTE,
        ]
        command = [
            "ssh",
            "-F",
            "/dev/null",
            "-o",
            "BatchMode=yes",
            "-o",
            "IdentitiesOnly=yes",
            "-o",
            "StrictHostKeyChecking=yes",
            "-o",
            "ConnectTimeout=12",
            "-o",
            "ClearAllForwardings=yes",
            "-o",
            "ControlMaster=no",
            "-o",
            "ControlPath=none",
            "-i",
            str(Path(self.config.identity_file)),
            f"{self.config.ssh_user}@{self.config.ssh_host}",
            shlex.join(remote),
        ]
        try:
            payload = json.dumps(
                {
                    "key": self._key,
                    "method": request.method,
                    "path": url.path,
                    "body": json.loads(request.body) if request.body else None,
                },
                allow_nan=False,
            ).encode()
            result = subprocess.run(  # noqa: S603
                command,
                input=payload,
                capture_output=True,
                timeout=30,
                check=False,
            )
            if result.returncode or (
                self._key and self._key.encode() in result.stdout + result.stderr
            ):
                raise NativeQueryError(None)
            if len(result.stdout) > 32 * 1024**2:
                raise NativeQueryError(None)
            value = json.loads(result.stdout)
            response = Response()
            response.status_code = value["status"]
            # requests adapters supply already buffered response bytes.
            content = value["body"].encode()
            response._content = content  # pylint: disable=protected-access
            if value.get("retry_after"):
                response.headers["Retry-After"] = value["retry_after"]
            return response
        except (OSError, subprocess.SubprocessError, ValueError, KeyError, TypeError):
            raise NativeQueryError(None) from None

    def close(self) -> None:
        """The bridge keeps no persistent connection or background process."""
