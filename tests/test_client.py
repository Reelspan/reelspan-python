"""Tests against a local stand-in server: the requests the client sends, its errors, and webhook checks."""

import hashlib
import hmac
import json
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, HTTPServer

from reelspan import Reelspan, ReelspanConnectionError, ReelspanError, verify_webhook

SEEN: list = []
STATE = {"flaky": 0}


class Handler(BaseHTTPRequestHandler):
    def _reply(self, status: int, body: dict, headers: dict = None) -> None:
        data = json.dumps(body).encode()
        self.send_response(status)
        for k, v in (headers or {}).items():
            self.send_header(k, v)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _record(self) -> None:
        length = int(self.headers.get("Content-Length") or 0)
        SEEN.append({"method": self.command, "path": self.path, "headers": dict(self.headers), "body": self.rfile.read(length)})

    def do_GET(self):  # noqa: N802
        self._record()
        if self.path.startswith("/v1/profiles/nobody"):
            return self._reply(404, {"success": False, "message": "Profile not found", "error_code": "PROFILE_NOT_FOUND"})
        if self.path.startswith("/v1/me-flaky"):
            STATE["flaky"] += 1
            if STATE["flaky"] < 3:
                return self._reply(503, {"message": "busy"}, {"Retry-After": "0"})
            return self._reply(200, {"success": True}, {"X-RateLimit-Limit": "600", "X-RateLimit-Remaining": "598", "X-RateLimit-Reset": "1"})
        if self.path.startswith("/v1/posts?"):
            page = int(dict(p.split("=") for p in self.path.split("?")[1].split("&"))["page"])
            rows = [{"n": i} for i in range((page - 1) * 100, min(page * 100, 230))]
            return self._reply(200, {"history": rows, "total": 230, "page": page, "limit": 100})
        if self.path.startswith("/v1/posts/status"):
            return self._reply(200, {"success": True, "status": "completed", "results": [{"platform": "x", "success": True}]})
        self._reply(200, {"success": True})

    def do_POST(self):  # noqa: N802
        self._record()
        if self.path == "/v1/media/uploads":
            size = json.loads(SEEN[-1]["body"])["size_bytes"]
            port = self.server.server_port
            parts = [{"part_number": i + 1, "url": f"http://127.0.0.1:{port}/storage/{i + 1}"} for i in range((size + 9) // 10)]
            return self._reply(200, {"success": True, "media_id": "med_big", "part_size": 10, "parts": parts})
        if self.path.endswith("/complete"):
            return self._reply(200, {"success": True, "media_id": "med_big", "kind": "video"})
        self._reply(200, {"success": True, "request_id": "req_1"})

    def do_PUT(self):  # noqa: N802
        self._record()
        self._reply(200, {}, {"ETag": f'"etag-{self.path.rsplit("/", 1)[1]}"'})

    do_DELETE = do_POST
    do_PATCH = do_POST

    def log_message(self, *args):  # quiet
        pass


class ClientTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.server = HTTPServer(("127.0.0.1", 0), Handler)
        threading.Thread(target=cls.server.serve_forever, daemon=True).start()
        cls.client = Reelspan(api_key="rs_test_abc", base_url=f"http://127.0.0.1:{cls.server.server_port}")

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()

    def setUp(self):
        SEEN.clear()

    def test_sends_the_key_and_json(self):
        self.client.profiles.create("brand", "Brand")
        req = SEEN[0]
        self.assertEqual(req["headers"]["Authorization"], "Bearer rs_test_abc")
        self.assertEqual(json.loads(req["body"]), {"username": "brand", "display_name": "Brand"})

    def test_uploads_a_video_with_its_fields(self):
        self.client.posts.video("brand", ["tiktok", "youtube"], ("clip.mp4", b"\x00\x01video", "video/mp4"), "Launch", scheduled_date="2026-12-01T09:00:00", repeat_every="week", repeat_times=3)
        req = SEEN[0]
        self.assertEqual(req["path"], "/v1/posts/video")
        body = req["body"]
        self.assertIn(b'name="platform[]"\r\n\r\ntiktok', body)
        self.assertIn(b'name="platform[]"\r\n\r\nyoutube', body)
        self.assertIn(b'name="repeat_times"\r\n\r\n3', body)
        self.assertIn(b'filename="clip.mp4"\r\nContent-Type: video/mp4\r\n\r\n\x00\x01video', body)

    def test_a_url_is_sent_as_a_field(self):
        self.client.posts.photos("brand", ["instagram"], ["https://example.com/a.jpg"], "Hi")
        self.assertIn(b'name="photos[]"\r\n\r\nhttps://example.com/a.jpg', SEEN[0]["body"])

    def test_text_posts_and_cancel_series(self):
        self.client.posts.text("brand", ["x"], "Hello")
        self.assertEqual(json.loads(SEEN[0]["body"])["platform"], ["x"])
        self.client.scheduled.cancel("sched_1", with_later_repeats=True)
        self.assertEqual(SEEN[1]["path"], "/v1/scheduled/sched_1?series=rest")

    def test_errors_carry_status_and_code(self):
        with self.assertRaises(ReelspanError) as ctx:
            self.client.profiles.get("nobody")
        self.assertEqual(ctx.exception.status, 404)
        self.assertEqual(ctx.exception.code, "PROFILE_NOT_FOUND")

    def test_wait_returns_when_done(self):
        self.assertEqual(self.client.posts.wait("req_1", interval=0)["status"], "completed")

    def test_rejects_unknown_arguments(self):
        with self.assertRaises(TypeError):
            self.client.posts.text("brand", ["x"], "Hello", schedule="tomorrow")


    def test_posts_carry_an_idempotency_key(self):
        self.client.posts.text("brand", ["x"], "Hello")
        self.client.posts.text("brand", ["x"], "Hello", idempotency_key="mine")
        first, second = SEEN[0]["headers"]["Idempotency-Key"], SEEN[1]["headers"]["Idempotency-Key"]
        self.assertTrue(first)
        self.assertEqual(second, "mine")

    def test_retries_503_then_reads_the_rate_limit(self):
        STATE["flaky"] = 0
        self.assertEqual(self.client.request("GET", "/v1/me-flaky"), {"success": True})
        self.assertEqual(STATE["flaky"], 3)
        self.assertEqual(self.client.rate_limit, {"limit": 600, "remaining": 598, "reset": 1})

    def test_gives_up_with_a_connection_error(self):
        dead = Reelspan(api_key="rs_x", base_url="http://127.0.0.1:9", max_retries=1, timeout=2)
        with self.assertRaises(ReelspanConnectionError):
            dead.me()

    def test_pages_through_history(self):
        rows = list(self.client.posts.all(platform="x"))
        self.assertEqual(len(rows), 230)
        self.assertEqual(rows[-1], {"n": 229})

    def test_large_files_go_up_in_parts(self):
        client = Reelspan(api_key="rs_test_abc", base_url=f"http://127.0.0.1:{self.server.server_port}", direct_upload_bytes=16)
        seen = []
        out = client.media.upload(("big.mp4", b"0123456789" * 3 + b"xy", "video/mp4"), on_progress=lambda sent, total: seen.append((sent, total)))
        self.assertEqual(out["media_id"], "med_big")
        puts = [r for r in SEEN if r["method"] == "PUT"]
        self.assertEqual(sorted(r["body"] for r in puts), [b"0123456789", b"0123456789", b"0123456789", b"xy"])
        complete = json.loads([r for r in SEEN if r["path"].endswith("/complete")][0]["body"])
        self.assertEqual([p["part_number"] for p in complete["parts"]], [1, 2, 3, 4])
        self.assertEqual(seen[-1], (32, 32))

    def test_connects_an_account_with_credentials(self):
        self.client.accounts.connect("brand", "telegram", bot_token="1:abc", chat_id="@news")
        self.assertEqual(SEEN[0]["path"], "/v1/accounts/telegram")
        self.assertEqual(json.loads(SEEN[0]["body"]), {"profile_username": "brand", "bot_token": "1:abc", "chat_id": "@news"})


class WebhookTest(unittest.TestCase):
    def sign(self, ts: str, body: str, secret: str = "whsec_1") -> str:
        return "sha256=" + hmac.new(secret.encode(), f"{ts}.{body}".encode(), hashlib.sha256).hexdigest()

    def test_verifies(self):
        ts = str(int(time.time()))
        body = '{"event":"upload_completed"}'
        self.assertTrue(verify_webhook(body, self.sign(ts, body), ts, "whsec_1"))
        self.assertFalse(verify_webhook(body + " ", self.sign(ts, body), ts, "whsec_1"))
        self.assertFalse(verify_webhook(body, self.sign("1000", body), "1000", "whsec_1"))


if __name__ == "__main__":
    unittest.main()
