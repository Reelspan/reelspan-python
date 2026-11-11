# Changelog

## 1.0.2

- Removed captions from a video file (`POST /v1/ai/video-captions` is gone). Upload the video, then write captions
  from its media id with `ai.write_captions(media_id=...)`.

## 1.0.1

- Dropped Whop, Listmonk, Nostr and Lemmy, which Reelspan no longer supports.

## 1.0.0

First public release.

- Every endpoint of the v1 API: posts (video, photos, text, document), status, retry, history with paging, scheduled
  posts, the queue, media and the library, analytics and reports, approvals, AI captions, Pinterest boards, account and
  profile webhooks, connect links, OAuth starts and connecting credential networks.
- Every network Reelspan supports.
- Retries with backoff on network errors, 408, 409, 429 and 5xx, honouring `Retry-After`; POSTs are retried only with
  an idempotency key, and every post gets one automatically.
- Large files go to storage in parts, three at a time, with progress and per-part retries.
- Standard library only, fully typed (py.typed); Python 3.10+. Large files are read from disk in parts, never whole.
- `ReelspanError` (status, code, retry_after) and `ReelspanConnectionError`; `verify_webhook` for signed webhooks.
