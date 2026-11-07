"""The Reelspan API client. Standard library only.

Robust by default: network errors, 429s and 5xx answers are retried with backoff (honouring Retry-After), and every
post carries an Idempotency-Key, so a retried post is never published twice.
"""

from __future__ import annotations

import hashlib
import hmac
import io
import json
import mimetypes
import os
import random
import socket
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, Callable, Dict, Iterator, List, Mapping, Optional, Sequence, Tuple, Union
from urllib import error, parse, request

from ._version import __version__

__all__ = ["Reelspan", "ReelspanError", "ReelspanConnectionError", "RateLimit", "verify_webhook", "DIRECT_UPLOAD_BYTES"]

USER_AGENT = f"reelspan-python/{__version__}"

# A file to upload: a path, raw bytes with a name as (name, bytes) or (name, bytes, content_type), or a public https
# URL (Google Drive and Dropbox share links work).
FileInput = Union[str, "os.PathLike[str]", Tuple[Any, ...]]
Fields = Mapping[str, Any]

#: Files at or above this size go straight to storage in parts (POST /v1/media/uploads) instead of one request.
DIRECT_UPLOAD_BYTES = 90 * 1024 * 1024

_TERMINAL = ("completed", "failed", "cancelled")


class ReelspanError(Exception):
    """An error answer from the API, with its HTTP status, machine-readable code and the full body."""

    def __init__(self, status: int, body: Mapping[str, Any], retry_after: Optional[float] = None):
        self.status = status
        self.body = dict(body)
        self.code: Optional[str] = body.get("error_code") if isinstance(body.get("error_code"), str) else None
        #: seconds the API asked to wait (429 and 503), when it said
        self.retry_after = retry_after
        super().__init__(body.get("message") if isinstance(body.get("message"), str) else f"Reelspan API error {status}")

    @property
    def is_rate_limited(self) -> bool:
        return self.status == 429


class ReelspanConnectionError(Exception):
    """The request never got an answer: the network failed or it timed out (after the retries)."""


class RateLimit(dict):
    """The rate limit the API reported on the last answer: limit, remaining, reset (a Unix time)."""


def _is_url(value: Any) -> bool:
    return isinstance(value, str) and value.startswith(("https://", "http://"))


class _Source:
    """A local file to upload, read lazily so large files never sit in memory whole."""

    def __init__(self, value: FileInput):
        if isinstance(value, tuple):
            self.name = str(value[0])
            self._data: Optional[bytes] = bytes(value[1])
            self.path: Optional[Path] = None
            self.type = value[2] if len(value) > 2 else (mimetypes.guess_type(self.name)[0] or "application/octet-stream")
            self.size = len(self._data)
        else:
            self.path = Path(value)
            self._data = None
            self.name = self.path.name
            self.type = mimetypes.guess_type(self.name)[0] or "application/octet-stream"
            self.size = self.path.stat().st_size

    def read(self, start: int = 0, length: Optional[int] = None) -> bytes:
        if self._data is not None:
            return self._data[start : None if length is None else start + length]
        assert self.path is not None
        with self.path.open("rb") as f:
            f.seek(start)
            return f.read() if length is None else f.read(length)


def _multipart(fields: Sequence[Tuple[str, str]], files: Sequence[Tuple[str, _Source]]) -> Tuple[bytes, str]:
    boundary = f"reelspan-{uuid.uuid4().hex}"
    out = io.BytesIO()
    for name, value in fields:
        out.write(f'--{boundary}\r\nContent-Disposition: form-data; name="{name}"\r\n\r\n'.encode())
        out.write(str(value).encode() + b"\r\n")
    for name, src in files:
        safe = src.name.replace('"', "")
        out.write(f'--{boundary}\r\nContent-Disposition: form-data; name="{name}"; filename="{safe}"\r\nContent-Type: {src.type}\r\n\r\n'.encode())
        out.write(src.read() + b"\r\n")
    out.write(f"--{boundary}--\r\n".encode())
    return out.getvalue(), f"multipart/form-data; boundary={boundary}"


def _flatten(fields: Fields) -> List[Tuple[str, str]]:
    pairs: List[Tuple[str, str]] = []
    for key, value in fields.items():
        if value is None:
            continue
        if isinstance(value, (list, tuple)):
            name = key if key.endswith("[]") else f"{key}[]"
            pairs.extend((name, str(v)) for v in value)
        elif isinstance(value, bool):
            pairs.append((key, "true" if value else "false"))
        else:
            pairs.append((key, str(value)))
    return pairs


def _backoff(attempt: int) -> float:
    """Exponential backoff with jitter: about 0.5 s, 1 s, 2 s… up to 8 s."""
    base = min(8.0, 0.5 * 2 ** (attempt - 1))
    return base / 2 + random.random() * base / 2


class _NoRedirect(request.HTTPRedirectHandler):
    def redirect_request(self, *args: Any, **kwargs: Any) -> None:  # noqa: D401
        return None


class Reelspan:
    """A client for one API key.

    >>> reelspan = Reelspan()  # reads REELSPAN_API_KEY
    >>> reelspan.posts.video(user="my-brand", platforms=["tiktok", "youtube"], video="clip.mp4", title="Launch day")
    """

    def __init__(
        self,
        api_key: Optional[str] = None,
        base_url: Optional[str] = None,
        timeout: float = 120.0,
        max_retries: int = 2,
        headers: Optional[Mapping[str, str]] = None,
        direct_upload_bytes: int = DIRECT_UPLOAD_BYTES,
    ):
        key = api_key or os.environ.get("REELSPAN_API_KEY")
        if not key:
            raise ValueError("Reelspan: pass api_key or set REELSPAN_API_KEY")
        self._key = key
        self._base = (base_url or os.environ.get("REELSPAN_BASE_URL") or "https://api.reelspan.com").rstrip("/")
        self._timeout = timeout
        self._max_retries = max(0, max_retries)
        self._headers = dict(headers or {})
        self._direct_upload_bytes = direct_upload_bytes
        #: the rate limit the API reported on the last answer, if any
        self.rate_limit: Optional[RateLimit] = None
        self.profiles = _Profiles(self)
        self.accounts = _Accounts(self)
        self.connect = _Connect(self)
        self.posts = _Posts(self)
        self.scheduled = _Scheduled(self)
        self.queue = _Queue(self)
        self.media = _Media(self)
        self.library = _Library(self)
        self.ai = _Ai(self)
        self.pinterest = _Pinterest(self)
        self.analytics = _Analytics(self)
        self.approvals = _Approvals(self)
        self.webhooks = _Webhooks(self)

    def __repr__(self) -> str:
        return f"Reelspan(base_url={self._base!r})"

    # ─── transport ─────────────────────────────────────────────────────────────────────────────────

    def _send(self, method: str, url: str, data: Optional[bytes], headers: Mapping[str, str], timeout: float) -> Tuple[int, Mapping[str, str], bytes]:
        req = request.Request(url, data=data, method=method, headers=dict(headers))
        try:
            with request.urlopen(req, timeout=timeout) as res:
                return res.status, res.headers, res.read()
        except error.HTTPError as e:
            try:
                return e.code, e.headers, e.read()
            finally:
                e.close()

    def request(
        self,
        method: str,
        path: str,
        *,
        query: Optional[Fields] = None,
        json_body: Any = None,
        form: Optional[Fields] = None,
        files: Optional[Sequence[Tuple[str, FileInput]]] = None,
        idempotency_key: Optional[str] = None,
        max_retries: Optional[int] = None,
        timeout: Optional[float] = None,
    ) -> Dict[str, Any]:
        """Any endpoint: ``request("GET", "/v1/me")``. Returns the parsed JSON; raises ReelspanError on an error status."""
        url = self._base + path
        if query:
            q = {k: (",".join(map(str, v)) if isinstance(v, (list, tuple)) else ("true" if v is True else "false" if v is False else v)) for k, v in query.items() if v is not None}
            if q:
                url += "?" + parse.urlencode(q)
        headers = {**self._headers, "Authorization": f"Bearer {self._key}", "User-Agent": USER_AGENT, "Accept": "application/json", "X-Reelspan-SDK": f"python/{__version__}"}
        if idempotency_key:
            headers["Idempotency-Key"] = idempotency_key
        data: Optional[bytes] = None
        if form is not None or files:
            data, headers["Content-Type"] = _multipart(_flatten(form or {}), [(name, _Source(f)) for name, f in (files or [])])
        elif json_body is not None:
            data = json.dumps({k: v for k, v in json_body.items() if v is not None} if isinstance(json_body, dict) else json_body).encode()
            headers["Content-Type"] = "application/json"
        # a GET, PUT or DELETE repeats safely; a POST or PATCH only with an idempotency key
        safe = method.upper() in ("GET", "HEAD", "PUT", "DELETE") or bool(idempotency_key)
        tries = (self._max_retries if max_retries is None else max_retries) + 1
        attempt = 0
        while True:
            attempt += 1
            try:
                status, res_headers, raw = self._send(method, url, data, headers, timeout or self._timeout)
            except (error.URLError, socket.timeout, ConnectionError, TimeoutError) as e:
                if safe and attempt < tries:
                    time.sleep(_backoff(attempt))
                    continue
                raise ReelspanConnectionError(f"Reelspan: could not reach {self._base} ({e})") from e
            self._note_rate_limit(res_headers)
            text = raw.decode(errors="replace")
            try:
                body = json.loads(text) if text else {}
            except ValueError:
                body = {"message": text[:500]}
            if 200 <= status < 300:
                return body
            retry_after = _float(res_headers.get("Retry-After"))
            retryable = status in (408, 409, 429) or status >= 500
            if retryable and (safe or status == 429) and attempt < tries:
                time.sleep(min(retry_after, 60) if retry_after else _backoff(attempt))
                continue
            raise ReelspanError(status, body if isinstance(body, dict) else {"message": str(body)}, retry_after)

    def _note_rate_limit(self, headers: Mapping[str, str]) -> None:
        limit = _float(headers.get("X-RateLimit-Limit"))
        if limit:
            self.rate_limit = RateLimit(limit=int(limit), remaining=int(_float(headers.get("X-RateLimit-Remaining")) or 0), reset=int(_float(headers.get("X-RateLimit-Reset")) or 0))

    # ─── account ───────────────────────────────────────────────────────────────────────────────────

    def me(self) -> Dict[str, Any]:
        """The key's account: plan, usage and limits."""
        return self.request("GET", "/v1/me")

    def platforms(self) -> Dict[str, Any]:
        """Every network and whether it is open, with what it takes."""
        return self.request("GET", "/platforms")


def _float(value: Any) -> Optional[float]:
    try:
        return float(value) if value not in (None, "") else None
    except (TypeError, ValueError):
        return None


def _common(user: str, platforms: Sequence[str], title: Optional[str], kw: Dict[str, Any]) -> Dict[str, Any]:
    extra = kw.pop("extra", None) or {}
    out = {
        "user": user,
        "platform[]": list(platforms),
        "title": title,
        "description": kw.pop("description", None),
        "scheduled_date": kw.pop("scheduled_date", None),
        "timezone": kw.pop("timezone", None),
        "add_to_queue": "true" if kw.pop("add_to_queue", False) else None,
        "async_upload": "true" if (kw.pop("async_upload", False) or kw.pop("run_async", False)) else None,
        "first_comment": kw.pop("first_comment", None),
        "repeat_every": kw.pop("repeat_every", None),
        "repeat_times": kw.pop("repeat_times", None),
        "external_id": kw.pop("external_id", None),
    }
    if kw:
        raise TypeError(f"Unknown arguments: {', '.join(kw)}. Put other API fields in extra={{...}}.")
    out.update(extra)
    return out


class _Group:
    def __init__(self, client: Reelspan):
        self._c = client


class _Profiles(_Group):
    def list(self) -> Dict[str, Any]:
        return self._c.request("GET", "/v1/profiles")

    def get(self, username: str) -> Dict[str, Any]:
        return self._c.request("GET", f"/v1/profiles/{parse.quote(username)}")

    def create(self, username: str, display_name: Optional[str] = None) -> Dict[str, Any]:
        return self._c.request("POST", "/v1/profiles", json_body={"username": username, "display_name": display_name})

    def rename(self, username: str, display_name: Optional[str]) -> Dict[str, Any]:
        return self._c.request("PATCH", f"/v1/profiles/{parse.quote(username)}", json_body={"display_name": display_name})

    def delete(self, username: str) -> Dict[str, Any]:
        return self._c.request("DELETE", "/v1/profiles", json_body={"username": username})

    def connect_link(self, username: str, **branding: Any) -> Dict[str, Any]:
        """Deprecated: use ``connect.link``."""
        return self._c.connect.link(username, **branding)

    def disconnect(self, username: str, platform: str) -> Dict[str, Any]:
        """Deprecated: use ``accounts.disconnect``."""
        return self._c.accounts.disconnect(username, platform)


class _Accounts(_Group):
    def connect(self, username: str, platform: str, **credentials: Any) -> Dict[str, Any]:
        """Connects a network that uses credentials instead of an OAuth sign-in.

        Telegram, Discord, Slack, Mastodon, WordPress, Dev.to, Hashnode, Lemmy, Listmonk, Whop, Nostr and Bluesky. The
        fields are checked with the network first; Upload-Post's names work too, e.g. ``chat_id=``, ``instance_url=``.
        """
        return self._c.request("POST", f"/v1/accounts/{parse.quote(platform)}", json_body={"profile_username": username, **credentials})

    def disconnect(self, username: str, platform: str) -> Dict[str, Any]:
        return self._c.request("DELETE", "/v1/accounts", json_body={"profile_username": username, "social_platform": platform})


class _Connect(_Group):
    def link(self, username: str, **branding: Any) -> Dict[str, Any]:
        """A hosted, branded page where someone without a Reelspan login connects networks to this profile."""
        return self._c.request("POST", "/v1/connect-links", json_body={"username": username, **branding})

    def validate(self, token: str) -> Dict[str, Any]:
        """Checks a connect token from a link (the token, not your API key, is the credential here)."""
        status, headers, raw = self._c._send("GET", f"{self._c._base}/v1/connect-links/validate", None, {"Authorization": f"Bearer {token}", "Accept": "application/json", "User-Agent": USER_AGENT}, self._c._timeout)
        body = json.loads(raw.decode() or "{}")
        if status >= 400:
            raise ReelspanError(status, body)
        return body

    def oauth_start(self, platform: str, profile: Optional[str] = None, redirect_url: Optional[str] = None) -> Dict[str, Any]:
        """Starts an OAuth connection; send the person to the returned authorize_url."""
        return self._c.request("POST", f"/v1/oauth/{parse.quote(platform)}/start", json_body={"profile": profile, "redirect_url": redirect_url})


class _Posts(_Group):
    def _key(self, kw: Dict[str, Any]) -> str:
        return kw.pop("idempotency_key", None) or str(uuid.uuid4())

    def video(self, user: str, platforms: Sequence[str], video: Optional[FileInput] = None, title: Optional[str] = None, *, video_media_id: Optional[str] = None, thumbnail: Optional[FileInput] = None, **kw: Any) -> Dict[str, Any]:
        """A video path, (name, bytes), or a public URL; or video_media_id from media.upload / the library.

        Files from 90 MB go up to storage in parts first.
        """
        key = self._key(kw)
        form = _common(user, platforms, title, kw)
        files: List[Tuple[str, FileInput]] = []
        if video is not None and not video_media_id and not _is_url(video) and _Source(video).size >= self._c._direct_upload_bytes:
            video_media_id = self._c.media.upload(video, kind="video")["media_id"]
        elif _is_url(video):
            form["video"] = video
        elif video is not None and not video_media_id:
            files.append(("video", video))
        form["video_media_id"] = video_media_id
        if _is_url(thumbnail):
            form["thumbnail_url"] = thumbnail
        elif thumbnail is not None:
            files.append(("thumbnail", thumbnail))
        return self._c.request("POST", "/v1/posts/video", form=form, files=files, idempotency_key=key)

    def photos(self, user: str, platforms: Sequence[str], photos: Sequence[FileInput] = (), title: Optional[str] = None, *, photo_media_ids: Optional[Sequence[str]] = None, alt_texts: Optional[Sequence[str]] = None, **kw: Any) -> Dict[str, Any]:
        """One photo or a carousel, in order: paths, (name, bytes) or public URLs."""
        key = self._key(kw)
        form = _common(user, platforms, title, kw)
        form["photo_media_ids[]"] = list(photo_media_ids) if photo_media_ids else None
        form["alt_text[]"] = list(alt_texts) if alt_texts else None
        urls = [p for p in photos if _is_url(p)]
        if urls:
            form["photos[]"] = urls
        files = [("photos[]", p) for p in photos if not _is_url(p)]
        return self._c.request("POST", "/v1/posts/photos", form=form, files=files, idempotency_key=key)

    def text(self, user: str, platforms: Sequence[str], text: str, **kw: Any) -> Dict[str, Any]:
        """Text only. Long text becomes a thread on X and Bluesky."""
        key = self._key(kw)
        body = _common(user, platforms, text, kw)
        body["platform"] = body.pop("platform[]")
        return self._c.request("POST", "/v1/posts/text", json_body=body, idempotency_key=key)

    def document(self, user: str, document: FileInput, title: str, *, document_media_id: Optional[str] = None, **kw: Any) -> Dict[str, Any]:
        """A PDF to LinkedIn, shown as a swipeable document."""
        key = self._key(kw)
        form = _common(user, ["linkedin"], title, kw)
        form["document_media_id"] = document_media_id
        files: List[Tuple[str, FileInput]] = []
        if _is_url(document):
            form["document"] = document
        elif document is not None and not document_media_id:
            files.append(("document", document))
        return self._c.request("POST", "/v1/posts/document", form=form, files=files, idempotency_key=key)

    def status(self, request_id: Optional[str] = None, job_id: Optional[str] = None) -> Dict[str, Any]:
        return self._c.request("GET", "/v1/posts/status", query={"request_id": request_id, "job_id": job_id})

    def wait(self, request_id: str, timeout: float = 600.0, interval: float = 3.0) -> Dict[str, Any]:
        """Polls the status until every network has finished, or the timeout passes."""
        end = time.monotonic() + timeout
        while True:
            s = self.status(request_id=request_id)
            if s.get("status") in _TERMINAL or time.monotonic() > end:
                return s
            time.sleep(interval)

    def retry(self, request_id: str, platforms: Optional[Sequence[str]] = None) -> Dict[str, Any]:
        """Sends the failed networks again, with the media it already has."""
        return self._c.request("POST", "/v1/posts/retry", json_body={"request_id": request_id, "platforms": list(platforms) if platforms else None}, idempotency_key=str(uuid.uuid4()))

    def history(self, **query: Any) -> Dict[str, Any]:
        """One page of history (page=, limit= 10/20/50/100, platform=, status=, profile_username=, start=, end=…)."""
        return self._c.request("GET", "/v1/posts", query=query)

    def all(self, **query: Any) -> Iterator[Dict[str, Any]]:
        """Every history row, page by page: ``for row in reelspan.posts.all(platform="tiktok"): ...``"""
        query.pop("page", None)
        query["limit"] = 100
        page = 1
        while True:
            r = self.history(page=page, **query)
            rows = r.get("history") or []
            yield from rows
            if not rows or page * int(r.get("limit") or 100) >= int(r.get("total") or 0):
                return
            page += 1


class _Scheduled(_Group):
    def list(self, **query: Any) -> Dict[str, Any]:
        return self._c.request("GET", "/v1/scheduled", query=query)

    def get(self, job_id: str) -> Dict[str, Any]:
        return self._c.request("GET", f"/v1/scheduled/{parse.quote(job_id)}")

    def edit(self, job_id: str, **patch: Any) -> Dict[str, Any]:
        return self._c.request("PATCH", f"/v1/scheduled/{parse.quote(job_id)}", json_body=patch)

    def cancel(self, job_id: str, with_later_repeats: bool = False) -> Dict[str, Any]:
        """with_later_repeats also cancels the later posts of a repeating series."""
        return self._c.request("DELETE", f"/v1/scheduled/{parse.quote(job_id)}", query={"series": "rest" if with_later_repeats else None})


class _Queue(_Group):
    def settings(self, username: str) -> Dict[str, Any]:
        return self._c.request("GET", "/v1/queue/settings", query={"profile_username": username})

    def set_settings(self, username: str, **settings: Any) -> Dict[str, Any]:
        return self._c.request("POST", "/v1/queue/settings", json_body={"profile_username": username, **settings})

    def preview(self, username: str) -> Dict[str, Any]:
        return self._c.request("GET", "/v1/queue/preview", query={"profile_username": username})

    def next_slot(self, username: str) -> Dict[str, Any]:
        return self._c.request("GET", "/v1/queue/next-slot", query={"profile_username": username})

    def mark_full(self, username: str, slot: str) -> Dict[str, Any]:
        """Keeps the queue from using this slot (ISO-8601 with an offset)."""
        return self._c.request("POST", "/v1/queue/slot-full", json_body={"profile_username": username, "slot_datetime": slot})

    def mark_open(self, username: str, slot: str) -> Dict[str, Any]:
        return self._c.request("DELETE", "/v1/queue/slot-full", json_body={"profile_username": username, "slot_datetime": slot})


class _Media(_Group):
    def upload(self, file: FileInput, kind: Optional[str] = None, save: bool = False, title: Optional[str] = None, on_progress: Optional[Callable[[int, int], None]] = None) -> Dict[str, Any]:
        """Uploads a file once and returns its media_id, to post or keep in the library.

        Files from 90 MB go straight to storage in parts (with retries per part), up to your plan's limit.
        """
        src = _Source(file)
        if src.size < self._c._direct_upload_bytes:
            r = self._c.request("POST", "/v1/media/upload", query={"kind": kind, "save": "true" if save else None, "title": title}, form={}, files=[("file", file)])
            if on_progress:
                on_progress(src.size, src.size)
            return r
        r = self._in_parts(src, kind or "video", on_progress)
        if save:
            self._c.library.keep([r["media_id"]], title)
        return r

    def _in_parts(self, src: _Source, kind: str, on_progress: Optional[Callable[[int, int], None]]) -> Dict[str, Any]:
        c = self._c
        start = c.request("POST", "/v1/media/uploads", json_body={"kind": kind, "size_bytes": src.size, "filename": src.name}, idempotency_key=str(uuid.uuid4()))
        part_size = int(start["part_size"])
        done: List[Dict[str, Any]] = []
        sent = [0]
        lock = threading.Lock()

        def put(part: Mapping[str, Any]) -> None:
            n = int(part["part_number"])
            chunk = src.read((n - 1) * part_size, part_size)
            attempt = 0
            while True:
                attempt += 1
                try:
                    status, headers, _ = c._send("PUT", part["url"], chunk, {"Content-Length": str(len(chunk))}, c._timeout)
                    etag = headers.get("ETag")
                    if status >= 300 or not etag:
                        raise ReelspanConnectionError(f"storage answered {status}")
                    with lock:
                        done.append({"part_number": n, "etag": etag})
                        sent[0] += len(chunk)
                        if on_progress:
                            on_progress(sent[0], src.size)
                    return
                except (ReelspanConnectionError, error.URLError, socket.timeout, ConnectionError) as e:
                    if attempt > c._max_retries + 1:
                        raise ReelspanConnectionError(f"Reelspan: part {n} of the upload failed ({e})") from e
                    time.sleep(_backoff(attempt))

        try:
            with ThreadPoolExecutor(max_workers=3) as pool:
                list(pool.map(put, start["parts"]))
            return c.request("POST", f"/v1/media/uploads/{parse.quote(start['media_id'])}/complete", json_body={"parts": sorted(done, key=lambda p: p["part_number"])}, idempotency_key=str(uuid.uuid4()))
        except BaseException:
            try:
                c.request("POST", f"/v1/media/uploads/{parse.quote(start['media_id'])}/abort", max_retries=0)
            except Exception:  # noqa: BLE001 - the upload failed already; this is cleanup
                pass
            raise


class _Library(_Group):
    def list(self, kind: Optional[str] = None, limit: Optional[int] = None, before: Optional[str] = None) -> Dict[str, Any]:
        return self._c.request("GET", "/v1/library", query={"kind": kind, "limit": limit, "before": before})

    def keep(self, media_ids: Sequence[str], title: Optional[str] = None) -> Dict[str, Any]:
        return self._c.request("POST", "/v1/library", json_body={"media_ids": list(media_ids), "title": title})

    def import_url(self, url: str, title: Optional[str] = None) -> Dict[str, Any]:
        """From a Google Drive or Dropbox share link, or any public https link."""
        return self._c.request("POST", "/v1/library/import", json_body={"url": url, "title": title})

    def remove(self, media_id: str) -> Dict[str, Any]:
        return self._c.request("DELETE", f"/v1/library/{parse.quote(media_id)}")

    def file_url(self, media_id: str) -> str:
        """A short-lived link to download the file."""
        opener = request.build_opener(_NoRedirect)
        req = request.Request(f"{self._c._base}/v1/library/{parse.quote(media_id)}/file", headers={"Authorization": f"Bearer {self._c._key}", "User-Agent": USER_AGENT})
        try:
            opener.open(req, timeout=self._c._timeout)
        except error.HTTPError as e:
            location = e.headers.get("Location")
            if location:
                return location
            raise ReelspanError(e.code, json.loads(e.read().decode() or "{}")) from None
        raise ReelspanError(500, {"message": "No download link was returned"})


class _Ai(_Group):
    """Captions written by AI (each call uses one AI credit)."""

    def captions_from_video(self, video: FileInput, platforms: Optional[Sequence[str]] = None, language: Optional[str] = None, context: Optional[str] = None) -> Dict[str, Any]:
        """Captions for each network from a video file or public URL: frames and speech are read on the server."""
        form: Dict[str, Any] = {"platforms": ",".join(platforms) if platforms else None, "language": language, "context": context}
        files: List[Tuple[str, FileInput]] = []
        if _is_url(video):
            form["video"] = video
        else:
            files.append(("video", video))
        return self._c.request("POST", "/v1/ai/video-captions", form=form, files=files)

    def write_captions(self, *, text: Optional[str] = None, notes: Optional[str] = None, media_id: Optional[str] = None, photo_media_ids: Optional[Sequence[str]] = None, platforms: Optional[Sequence[str]] = None, kind: Optional[str] = None, language: Optional[str] = None) -> Dict[str, Any]:
        """Captions for each network from whatever the post has: your text, an uploaded video or photos."""
        return self._c.request("POST", "/v1/ai/adapt-captions", json_body={"text": text, "notes": notes, "media_id": media_id, "photo_media_ids": list(photo_media_ids) if photo_media_ids else None, "platforms": list(platforms) if platforms else None, "kind": kind, "language": language})

    def rewrite_captions(self, content: Mapping[str, Any], platforms: Optional[Sequence[str]] = None, language: Optional[str] = None, instructions: Optional[str] = None) -> Dict[str, Any]:
        """Your captions, adapted to each network's style and limits: ``{"youtube": {"title": "..."}}``."""
        return self._c.request("POST", "/v1/ai/rewrite-captions", json_body={"content": dict(content), "platforms": list(platforms) if platforms else None, "language": language, "instructions": instructions})


class _Pinterest(_Group):
    def create_board(self, username: str, name: str) -> Dict[str, Any]:
        """Creates a board on the profile's Pinterest account."""
        return self._c.request("POST", "/v1/pinterest/boards", json_body={"profile_username": username, "name": name})


class _Analytics(_Group):
    def account(self, username: str, platforms: Optional[Sequence[str]] = None) -> Dict[str, Any]:
        return self._c.request("GET", f"/v1/analytics/{parse.quote(username)}", query={"platforms": platforms})

    def history(self, username: str, days: Optional[int] = None, platforms: Optional[Sequence[str]] = None) -> Dict[str, Any]:
        return self._c.request("GET", f"/v1/analytics/{parse.quote(username)}/history", query={"days": days, "platforms": platforms})

    def post(self, request_id: str, platform: Optional[str] = None) -> Dict[str, Any]:
        return self._c.request("GET", f"/v1/post-analytics/{parse.quote(request_id)}", query={"platform": platform})

    def best_times(self, username: str, timezone: Optional[str] = None, platforms: Optional[Sequence[str]] = None) -> Dict[str, Any]:
        return self._c.request("GET", f"/v1/best-times/{parse.quote(username)}", query={"timezone": timezone, "platforms": platforms})

    def report(self, username: str, days: int = 30) -> Dict[str, Any]:
        return self._c.request("GET", f"/v1/reports/{parse.quote(username)}", query={"days": days})


class _Approvals(_Group):
    def create(self, user: str, job_ids: Sequence[str], title: Optional[str] = None, note: Optional[str] = None, expires_in_days: Optional[int] = None) -> Dict[str, Any]:
        return self._c.request("POST", "/v1/approval-links", json_body={"profile_username": user, "job_ids": list(job_ids), "title": title, "note": note, "expires_in_days": expires_in_days})

    def list(self, username: Optional[str] = None) -> Dict[str, Any]:
        return self._c.request("GET", "/v1/approval-links", query={"profile_username": username})

    def delete(self, link_id: str) -> Dict[str, Any]:
        return self._c.request("DELETE", f"/v1/approval-links/{parse.quote(link_id)}")


class _ProfileWebhooks(_Group):
    def get(self, username: str) -> Dict[str, Any]:
        return self._c.request("GET", "/v1/profile-webhook", query={"profile_username": username})

    def set(self, username: str, url: str, events: Optional[Sequence[str]] = None) -> Dict[str, Any]:
        return self._c.request("POST", "/v1/profile-webhook", json_body={"profile_username": username, "webhook_url": url, "webhook_events": list(events) if events else None})

    def delete(self, username: str) -> Dict[str, Any]:
        return self._c.request("DELETE", "/v1/profile-webhook", json_body={"profile_username": username})


class _Webhooks(_Group):
    def __init__(self, client: Reelspan):
        super().__init__(client)
        #: a profile's own webhook, sent in addition to the account one
        self.profile = _ProfileWebhooks(client)

    def get(self) -> Dict[str, Any]:
        return self._c.request("GET", "/v1/webhook")

    def set(self, url: str, events: Optional[Sequence[str]] = None) -> Dict[str, Any]:
        return self._c.request("POST", "/v1/webhook", json_body={"webhook_url": url, "webhook_events": list(events) if events else None})

    def delete(self) -> Dict[str, Any]:
        return self._c.request("DELETE", "/v1/webhook")

    def test(self) -> Dict[str, Any]:
        return self._c.request("POST", "/v1/webhook/test")

    def deliveries(self) -> Dict[str, Any]:
        return self._c.request("GET", "/v1/webhook/deliveries")

    def redeliver(self, delivery_id: str) -> Dict[str, Any]:
        return self._c.request("POST", f"/v1/webhook/deliveries/{parse.quote(delivery_id)}/redeliver")

    def rotate_secret(self) -> Dict[str, Any]:
        """A new signing secret; the old one stops working."""
        return self._c.request("POST", "/v1/webhook/secret")


def verify_webhook(raw_body: Union[str, bytes], signature: Optional[str], timestamp: Optional[str], secret: str, tolerance: int = 300) -> bool:
    """Checks a webhook really came from Reelspan.

    Pass the raw body exactly as received, the X-Reelspan-Signature and X-Reelspan-Timestamp headers and your signing
    secret. Anything older than ``tolerance`` seconds (default 5 minutes) is rejected.
    """
    if not signature or not timestamp or not str(timestamp).isdigit():
        return False
    if abs(time.time() - int(timestamp)) > tolerance:
        return False
    body = raw_body.decode() if isinstance(raw_body, bytes) else raw_body
    expected = "sha256=" + hmac.new(secret.encode(), f"{timestamp}.{body}".encode(), hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, signature)

