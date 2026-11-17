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

## Live posts

```python
reelspan.posts.edit(request_id, "x", text="Fixed the typo")
reelspan.posts.repost(platform="linkedin", user="my-brand", post_id="urn:li:share:…", commentary="Worth a read")
reelspan.posts.unpublish(request_id, "facebook")

reelspan.youtube.subtitles.add("en", request_id=request_id, file="en.srt")
reelspan.youtube.set_thumbnail("https://…/cover.jpg", user="my-brand", video_id="dQw4w9WgXcQ")
```

## Comments, DMs and AutoDM

```python
for c in reelspan.comments.all("instagram", "my-brand", request_id=request_id):
    print(c["author"]["username"], c["text"])
reelspan.comments.reply("instagram", "my-brand", comment_id, "Thank you!", request_id=request_id)
reelspan.comments.moderate("youtube", "my-brand", "hide", comment_id, post_id=video_id)

reelspan.dms.send("instagram", "my-brand", recipient_id, "Here is the link", buttons=[{"title": "Open", "url": "https://…"}])

# answer every comment that says "link" with a DM, for 15 days
monitor = reelspan.autodms.start("instagram", "my-brand", request_id=request_id, reply_message="Here you go: https://…", trigger_keywords=["link"])["monitor"]
reelspan.autodms.logs(monitor["monitor_id"])
```

## All methods

| Group | Methods |
| --- | --- |
| account | `me()`, `platforms()`, `permissions()`, `activity()`, `publishing_limits()`, `preferences.get/set` |
| `profiles` | `list`, `get`, `create`, `rename`, `delete` |
| `accounts`, `connect` | `accounts.connect/disconnect`, `connect.link/validate/oauth_start` |
| `posts` | `video`, `photos`, `text`, `document`, `status`, `wait`, `retry`, `history`, `all`, `edit`, `unpublish`, `repost` |
| `scheduled` | `list`, `get`, `edit`, `cancel`, `preview`, `thumbnail` |
| `queue` | `settings`, `set_settings`, `preview`, `next_slot`, `mark_full`, `mark_open` |
| `media`, `library` | `media.upload`; `library.list/keep/import_url/remove/file_url` |
| `analytics` | `account`, `history`, `post`, `post_by_id`, `impressions`, `best_times`, `report` |
| `comments` | `list`, `all`, `create`, `reply`, `private_reply`, `moderate`, `delete` |
| `dms` | `conversations`, `send` |
| `autodms` | `start`, `list`, `get`, `logs`, `pause`, `resume`, `stop`, `check`, `delete` |
| `youtube` | `subtitles.list/add/delete`, `set_thumbnail` |
| `facebook` | `pages`, `select_page`, `clear_page`, `page_info`, `update_page_info` |
| `linkedin` | `pages`, `select_page`, `clear_page` |
| `google_business` | `locations`, `select_location`, `clear_location` |
| `pinterest` | `boards`, `create_board`, `sections`, `create_section`, `save_pin` |
| `tiktok`, `reddit` | `tiktok.creator_info`, `reddit.posts` |
| `ai` | `write_captions`, `rewrite_captions` |
| `approvals` | `create`, `list`, `delete` |
| `webhooks` | `get`, `set`, `delete`, `test`, `deliveries`, `redeliver`, `rotate_secret`, `profile.get/set/delete` |

`request(method, path)` reaches anything else.

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
