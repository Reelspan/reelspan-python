# reelspan

The official Python client for the [Reelspan API](https://reelspan.com/docs). Publish one video, photo set or text
post to TikTok, YouTube, Instagram, Facebook, X, Threads, LinkedIn, Pinterest, Bluesky, Reddit, Google Business,
Telegram, Discord, Slack, Mastodon, Dev.to, Hashnode, WordPress and more, schedule it, and read
how it did.

- Standard library only. Python 3.10+, fully typed.
- Retries network errors, 429s and 5xx answers with backoff, honouring `Retry-After`.
- Every post carries an `Idempotency-Key`, so a retried post is never published twice.
- Large files go straight to storage in parts, read from disk piece by piece, with progress and per-part retries.

```bash
pip install reelspan
```

## Quick start

```python
from reelspan import Reelspan

reelspan = Reelspan()  # reads REELSPAN_API_KEY

post = reelspan.posts.video(
    user="my-brand",  # a profile's username
    platforms=["tiktok", "youtube", "instagram"],
    video="launch.mp4",  # a path, (name, bytes), or a public https URL
    title="Launch day",
)
for platform, result in post["results"].items():
    print(platform, result.get("url") if result["success"] else result.get("error"))
```

## Posting

```python
# photos or a carousel, in order, with alt text
reelspan.posts.photos("my-brand", ["instagram", "pinterest"], ["front.jpg", "back.jpg"], "New colours", alt_texts=["Front", "Back"], extra={"pinterest_board_id": "123"})

# text: long posts become threads on X and Bluesky
reelspan.posts.text("my-brand", ["x", "bluesky", "threads", "linkedin"], "We shipped it.")

# a PDF to LinkedIn as a swipeable document
reelspan.posts.document("my-brand", "q3-report.pdf", "Q3 report")

# schedule, queue or repeat
reelspan.posts.video("my-brand", ["tiktok"], "clip.mp4", scheduled_date="2026-12-01T09:00:00Z")
reelspan.posts.video("my-brand", ["tiktok"], "clip.mp4", add_to_queue=True)
reelspan.posts.text("my-brand", ["x"], "Weekly tip", scheduled_date=when, repeat_every="week", repeat_times=6)

# network settings go in extra
reelspan.posts.video("my-brand", ["reddit", "tiktok"], "clip.mp4", "Clip", extra={"reddit_subreddit": "videos", "privacy_level": "PUBLIC_TO_EVERYONE"})
```

Pass `run_async=True` to get a `request_id` at once, then:

```python
request_id = reelspan.posts.video("my-brand", ["youtube"], "long.mp4", run_async=True)["request_id"]
done = reelspan.posts.wait(request_id)  # polls until every network has finished
reelspan.posts.retry(request_id)  # sends the failed networks again
```

## Large files

Files from 90 MB go up in parts on their own. To upload once and reuse:

```python
media = reelspan.media.upload("film.mp4", kind="video", on_progress=lambda sent, total: print(f"{sent * 100 // total}%"))
reelspan.posts.video("my-brand", ["youtube"], video_media_id=media["media_id"], title="The film")
```

## Profiles and accounts

```python
reelspan.profiles.create("client-a", "Client A")

# networks that sign in with OAuth: send your customer to a hosted, branded page
link = reelspan.connect.link("client-a", logo_image="https://…", redirect_url="https://you.com/done")["access_url"]

# networks that use credentials: connect them straight from your code
reelspan.accounts.connect("client-a", "discord", webhook_url="https://discord.com/api/webhooks/…")
reelspan.accounts.connect("client-a", "mastodon", instance_url="mastodon.social", access_token="…")

reelspan.accounts.disconnect("client-a", "discord")
```

## History, analytics and scheduling

```python
for row in reelspan.posts.all(platform="tiktok", status="failed"):
    print(row)
reelspan.analytics.account("my-brand")
reelspan.analytics.best_times("my-brand", timezone="Asia/Karachi")
reelspan.scheduled.list(profile_username="my-brand")
reelspan.scheduled.cancel(job_id, with_later_repeats=True)
```

Also: `queue`, `library`, `approvals`, `ai` (captions from a video or your text), `pinterest.create_board`, `webhooks`
(account and per-profile), and `request(method, path)` for anything else.

## Webhooks

```python
from reelspan import verify_webhook

@app.post("/reelspan")
def hook(request):
    ok = verify_webhook(request.body, request.headers.get("X-Reelspan-Signature"), request.headers.get("X-Reelspan-Timestamp"), SECRET)
    if not ok:
        return Response(status=401)
    ...
```

## Errors

```python
from reelspan import ReelspanError, ReelspanConnectionError

try:
    reelspan.posts.text("my-brand", ["x"], "Hi")
except ReelspanError as e:
    print(e.status, e.code, e, e.retry_after)
except ReelspanConnectionError as e:
    print("no answer:", e)
```

## Options

```python
Reelspan(
    api_key="rs_live_…",  # or REELSPAN_API_KEY
    base_url="https://api.reelspan.com",
    timeout=120,  # seconds per attempt
    max_retries=2,
    headers={"X-My-App": "scheduler"},
)
```

Use a test key (`rs_test_…`) to try everything without publishing. `reelspan.rate_limit` holds the limit the API
reported last. Full reference: [reelspan.com/docs](https://reelspan.com/docs).

MIT licensed.
