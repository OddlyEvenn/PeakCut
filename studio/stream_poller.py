"""Stream and video poller: monitors creator channels for new streams or VODs,
deduplicates them, and enqueues jobs to generate and schedule Shorts."""
import json
import subprocess
import threading
import time
from pathlib import Path

from . import auth, config, creators, ctx, jobs, live

_lock = threading.RLock()
_SEEN_FILE = config.DATA_DIR / "seen_sources.json"


def _seen() -> set:
    if not _SEEN_FILE.exists():
        return set()
    try:
        data = json.loads(_SEEN_FILE.read_text(encoding="utf-8"))
        return set(data) if isinstance(data, list) else set()
    except (OSError, ValueError):
        return set()


def _save_seen(seen: set):
    with _lock:
        tmp = _SEEN_FILE.with_suffix(".tmp")
        tmp.write_text(json.dumps(sorted(list(seen)), indent=1), encoding="utf-8")
        tmp.replace(_SEEN_FILE)


def is_seen(video_id: str) -> bool:
    with _lock:
        return video_id in _seen()


def mark_seen(video_id: str):
    with _lock:
        s = _seen()
        s.add(video_id)
        _save_seen(s)


def fetch_recent_channel_videos(channel_url: str, limit: int = 2) -> list:
    """Fetch the latest `limit` videos or stream VODs from a channel URL using yt-dlp."""
    if not channel_url:
        return []
    url = channel_url.strip()
    if ("youtube.com/@" in url or "youtube.com/channel/" in url) and not any(x in url for x in ("/streams", "/videos")):
        url = url.rstrip("/") + "/streams"
    cmd = ["yt-dlp", "--flat-playlist", "--dump-single-json", "--playlist-end", str(limit), url]
    try:
        res = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        if res.returncode != 0:
            return []
        data = json.loads(res.stdout)
        entries = data.get("entries", [])
        out = []
        for e in entries:
            vid = e.get("id") or ""
            v_url = e.get("url") or (f"https://www.youtube.com/watch?v={vid}" if vid else "")
            title = e.get("title") or "Stream"
            is_live = (e.get("live_status") == "is_live") or bool(e.get("is_live"))
            if vid and v_url:
                out.append({"id": vid, "url": v_url, "title": title, "is_live": is_live})
        return out
    except Exception as e:
        print(f"[PeakCut] [POLLER ERROR] Failed to fetch channel {channel_url}: {e}", flush=True)
        return []


def matches_game_filter(title: str, focus: str | None = None) -> bool:
    """Check if a video title matches the targeted game filter (e.g. GTA 6)."""
    from . import games
    focus_game = (focus or config.env("PEAKCUT_FOCUS_GAME", "Grand Theft Auto VI")).strip()
    if not focus_game or focus_game.lower() in ("all", "any", "none", "*"):
        return True

    detected_game, _ = games.detect(title)
    if detected_game and (focus_game.lower() in detected_game.lower() or detected_game.lower() in focus_game.lower()):
        return True

    # High-recall keyword matching for GTA 6
    gta_keywords = ["gta 6", "gta vi", "gta6", "grand theft auto 6", "grand theft auto vi", "vice city", "lucia", "jason"]
    title_lower = title.lower()
    if any(k in focus_game.lower() for k in ("gta", "grand theft auto")):
        return any(kw in title_lower for kw in gta_keywords)

    return focus_game.lower() in title_lower


def check_creator_streams(cid: str, max_check: int = 2) -> list:
    """Check a single creator's channel for new stream VODs and enqueue jobs."""
    from . import circuit_breaker
    if not circuit_breaker.creator.is_creator_healthy(cid):
        print(f"[PeakCut] [POLLER] Creator breaker is OPEN for '{cid}' (cooldown active). Skipping.", flush=True)
        return []

    c = creators.get(cid)
    if not c:
        return []
    urls = []
    if c.get("channel"):
        urls.append(c["channel"])
    for u in c.get("channels", {}).values():
        if u and u not in urls:
            urls.append(u)

    existing_job_urls = {j.get("source", {}).get("url") for j in jobs.list_jobs()}
    new_jobs = []

    focus = c.get("game") or config.env("PEAKCUT_FOCUS_GAME", "Grand Theft Auto VI")

    for channel_url in urls:
        videos = fetch_recent_channel_videos(channel_url, limit=max_check)
        for v in videos:
            vid = v["id"]
            v_url = v["url"]
            title = v["title"]
            is_live = v.get("is_live", False)

            # Filter for GTA 6 if game focus is enabled
            if focus and not matches_game_filter(title, focus):
                print(f"[PeakCut] [POLLER] Skipping non-GTA stream: '{title}'", flush=True)
                continue

            # Active live broadcast: auto-record in real-time, do NOT queue as static download job
            if is_live:
                if not live.active_for(c["id"]):
                    try:
                        print(f"[PeakCut] [LIVE] Auto-starting real-time recording for {c['name']} (LIVE NOW): '{title}'", flush=True)
                        live.start_session(c, v_url)
                        creators.log(c["id"], f"Auto-started live recording: {title}")
                        circuit_breaker.creator.record_success(cid)
                    except Exception as e:
                        print(f"[PeakCut] [LIVE ERROR] Could not start live recording for {v_url}: {e}", flush=True)
                continue

            # Completed VOD/video: process through regular clipping job
            if is_seen(vid) or v_url in existing_job_urls:
                continue

            # Mark seen immediately so concurrent pollers won't duplicate
            mark_seen(vid)
            try:
                print(f"[PeakCut] [POLLER] Found new GTA stream VOD for {c['name']}: '{title}' ({v_url})", flush=True)
                job = jobs.create({"type": "url", "url": v_url, "name": title}, {}, creator=cid)
                creators.log(cid, f"Auto-detected GTA stream: {title}")
                new_jobs.append(job)
                circuit_breaker.creator.record_success(cid)
            except Exception as e:
                print(f"[PeakCut] [POLLER ERROR] Failed to create job for {v_url}: {e}", flush=True)
                circuit_breaker.creator.record_failure(cid, str(e))

    return new_jobs


def poll_all_creators():
    """Poll all creators across all registered users for new stream VODs."""
    from . import circuit_breaker
    if not circuit_breaker.disk.is_disk_safe():
        print("[PeakCut] [POLLER] Disk safety breaker is OPEN (< 3.5GB free). Pausing stream polling.", flush=True)
        return

    for uid in auth.user_ids():
        try:
            with ctx.as_user(uid):
                for c in creators.all_creators():
                    # Only poll if creator has autopost on or auto_watch on
                    if c.get("autopost") or c.get("auto_watch"):
                        check_creator_streams(c["id"])
        except Exception as e:
            print(f"[PeakCut] [POLLER ERROR] Error polling creators for user {uid}: {e}", flush=True)


def _loop():
    time.sleep(15)  # brief initial delay on server start
    while True:
        try:
            poll_all_creators()
        except Exception as e:
            print(f"[PeakCut] [POLLER ERROR] Unexpected error in poller loop: {e}", flush=True)
        time.sleep(900)  # poll every 15 minutes


def start():
    """Start the background stream poller daemon."""
    threading.Thread(target=_loop, daemon=True).start()
    print("[PeakCut] Stream and video poller started (polling every 15 min)", flush=True)
