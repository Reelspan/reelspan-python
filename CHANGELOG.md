# Changelog

## 1.0.0

First public release.

- Every endpoint of the v1 API: posts (video, photos, text, document), status, retry, history with paging, scheduled
  posts, the queue, media and the library, analytics and reports, approvals, AI captions, Pinterest boards, account and
  profile webhooks, connect links, OAuth starts and connecting credential networks.
- All 22 networks.
- Retries with backoff on network errors, 408, 409, 429 and 5xx, honouring `Retry-After`; POSTs are retried only with
  an idempotency key, and every post gets one automatically.
- Large files go to storage in parts, three at a time, with progress and per-part retries.
- Standard library only, fully typed (py.typed); Python 3.10+. Large files are read from disk in parts, never whole.
- `ReelspanError` (status, code, retry_after) and `ReelspanConnectionError`; `verify_webhook` for signed webhooks.
