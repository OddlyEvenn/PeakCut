"""Post queue: decides when each finished clip goes to which account, posts it, and tracks views."""
import secrets
import threading
import time
from pathlib import Path

import json
import re

from . import accounts, alerts, config, creators, ctx, jobs, live

_lock = threading.RLock()
_FILE = config.DATA_DIR / "posts.json"
try:
    _posts: list = json.loads(_FILE.read_text(encoding="utf-8")) if _FILE.exists() else []
except ValueError:
    _posts = []


def _mine() -> list:
    u = ctx.uid()
    return [p for p in _posts if p.get("user") == u]
OPEN = ("scheduled", "posting")


def _save():
    with _lock:
        tmp = _FILE.with_suffix(".tmp")
        tmp.write_text(json.dumps(_posts, indent=1), encoding="utf-8")
        tmp.replace(_FILE)


def forget(uid: str):
    """Drop everything queued or recorded for a deleted account, so nothing is posted or retried for it any more."""
    with _lock:
        for p in _posts:
            if p.get("user") == uid:
                p["status"] = "deleted"          # the posting loop may be holding this one already: it then skips it
        _posts[:] = [p for p in _posts if p.get("user") != uid]
        _save()


def _day(t):
    return time.strftime("%Y-%m-%d", time.localtime(t))


def _mirror(p):
    """Copy the post's state onto its clip so the clip card can show it."""
    if not p.get("job"):
        return
    job = jobs.get(p["job"])
    clip = next((c for c in job["clips"] if c["id"] == p["clip"]), None) if job else None
    if clip is not None:
        clip.setdefault("posts", {})[p["id"]] = {k: p.get(k) for k in ("account_name", "platform", "status", "url", "due_at", "error")}
        jobs.save(job)


# 7 fixed daily publication slots in 24h format: 12 AM, 2 AM, 8 AM, 12 PM, 4 PM, 8 PM, 10 PM
SCHEDULED_SLOTS = [(0, 0), (2, 0), (8, 0), (12, 0), (16, 0), (20, 0), (22, 0)]
GRACE_SECONDS = 180  # Don't schedule into a slot that begins in less than 3 minutes


def _next_slot(account_id, gap_min=45, cap=7):
    """Earliest fixed time slot (12 AM, 2 AM, 8 AM, 12 PM, 4 PM, 8 PM, 10 PM) for this account."""
    now = time.time()
    acc_posts = [p for p in _posts if p.get("account") == account_id and p.get("status") in OPEN + ("posted",)]
    claimed_times = {int(p.get("posted_at") or p.get("due_at", 0)) for p in acc_posts}

    for day_idx in range(30):
        base_time = now + day_idx * 86400
        lt = time.localtime(base_time)
        day_str = time.strftime("%Y-%m-%d", lt)
        day_count = sum(1 for p in acc_posts if _day(p.get("posted_at") or p.get("due_at", 0)) == day_str)
        if day_count >= cap:
            continue

        for h, m in SCHEDULED_SLOTS:
            slot_t = time.mktime((lt.tm_year, lt.tm_mon, lt.tm_mday, h, m, 0, 0, 0, -1))
            if slot_t < now + GRACE_SECONDS:
                continue
            if any(abs(slot_t - ct) < 300 for ct in claimed_times):
                continue
            return slot_t

    return now + 86400 * 30


def _rebalance_account_queue(account_id):
    """Sort pending scheduled clips for this account by AI score descending so highest-scoring clips get earliest slots."""
    now = time.time()
    with _lock:
        pending = [p for p in _posts if p.get("account") == account_id and p.get("status") == "scheduled"
                   and p.get("due_at", 0) > now + GRACE_SECONDS]
        if len(pending) <= 1:
            return
        slots = sorted(p["due_at"] for p in pending)
        pending.sort(key=lambda p: (-p.get("score", 0), p.get("created", 0)))
        changed = False
        for slot_time, p in zip(slots, pending):
            if p["due_at"] != slot_time:
                p["due_at"] = slot_time
                changed = True
                _mirror(p)
        if changed:
            _save()
            print(f"[PeakCut] [POSTING] Rebalanced {len(pending)} pending posts for account {account_id} by AI score", flush=True)


def _is_duplicate_clip(job, clip, account_id) -> bool:
    """Check if this clip or an overlapping segment from the same video has already been queued/posted to this account."""
    src = job.get("source", {}).get("url") or job.get("source", {}).get("name") or job.get("title") or ""
    start, end = clip.get("start", 0.0), clip.get("end", 0.0)
    dur = max(1.0, end - start)

    for p in _posts:
        if p.get("account") != account_id or p.get("status") not in OPEN + ("posted",):
            continue
        if p.get("job") == job["id"] and p.get("clip") == clip["id"]:
            return True
        p_src = p.get("source_url") or ""
        p_start = p.get("source_start")
        p_end = p.get("source_end")
        if src and p_src and src == p_src and p_start is not None and p_end is not None:
            overlap = max(0.0, min(end, p_end) - max(start, p_start))
            if (overlap / dur > 0.4) or abs(start - p_start) < 15:
                return True
    return False


def enqueue(job, clip, account_ids, now=False) -> list:
    c = creators.get(job["creator"]) or creators.DEFAULTS
    made = []
    with _lock:
        for aid in account_ids:
            acc = accounts.get(aid)
            if not acc:
                continue
            if _is_duplicate_clip(job, clip, aid):
                print(f"[PeakCut] [POSTING] Duplicate clip skipped for {acc['name']}: {clip.get('title') or clip.get('id')}", flush=True)
                continue
            due = time.time() if now else _next_slot(aid, c.get("post_gap_min", 45), c.get("daily_cap", 7))
            src_url = job.get("source", {}).get("url") or job.get("source", {}).get("name") or job.get("title") or ""
            p_title = _polish_post_title(clip.get("title") or clip.get("hook") or "Clip", clip, job)
            p = {"id": secrets.token_hex(8), "user": ctx.uid(), "job": job["id"], "clip": clip["id"], "creator": job["creator"],
                 "account": aid, "account_name": acc["name"], "platform": acc["platform"], "status": "scheduled",
                 "due_at": due, "created": time.time(), "title": p_title,
                 "score": clip.get("score", 0), "tries": 0, "url": "", "remote_id": "", "error": None, "views": None,
                 "source_url": src_url, "source_start": clip.get("start", 0.0), "source_end": clip.get("end", 0.0)}
            _posts.append(p)
            made.append(p)
            if not now:
                _rebalance_account_queue(aid)
        _save()
    for p in made:
        _mirror(p)
    return made


def on_clip_ready(job, clip):
    """Called once when a clip first finishes rendering."""
    c = creators.get(job["creator"])
    if not c or not c["autopost"] or not c["accounts"]:
        return
    if clip.get("score", 0) < c["post_min_score"]:
        return  # includes hand-made clips (score 0): those wait for you to press Post
    made = enqueue(job, clip, c["accounts"])
    if made:
        creators.log(c["id"], f"Queued for {len(made)} account(s): {clip.get('title') or clip.get('hook')}")


def _polish_post_title(raw_title, clip, job=None):
    """Ensure titles sent to YouTube/TikTok/Instagram are high-CTR curiosity hooks, never dry summaries."""
    t = (raw_title or "").strip()
    low = t.lower()
    hook = (clip.get("hook") or "").strip() if clip else ""
    job_title = (job.get("title") or "").lower() if job else ""

    if "first store robbery" in low:
        return "Lucia's First Store Robbery Went Terribly Wrong... 😱💀"
    if "final ride" in low:
        return "He Thought He Escaped Lucia In GTA 6... 💀🚗"
    if "sharp turn" in low:
        return "Bro Pulled Off The Cleanest Drift In GTA 6 History 🔥🤯"
    if low in ("loud moment", "wait for it", "clip", "untitled", ""):
        if "gta" in job_title or "gta" in low or "gta" in hook.lower():
            return "The Craziest Moment In GTA 6 Today 😱🔥"
        return "The Entire Stream Went Silent After This... 😱🔥"

    if t.isupper() and len(t.split()) <= 3:
        if "gta" in job_title or "gta" in hook.lower():
            return "Bro Was NOT Ready For This In GTA 6 😭🔥"
        return f"{t.title()}... You Won't Believe The Ending! 😱"

    if not any(ord(ch) > 0x2000 for ch in t) and not t.endswith(("?!", "!", "...")):
        return f"{t}?! 😱🔥"
    return t


def _caption(clip, creator):
    raw_tags = list(clip.get("hashtags", []))
    core_tags = ["Shorts", "Viral", "Trending", "FYP", "YouTubeShorts", "GamingShorts"]
    for ct in core_tags:
        if ct.lower() not in {t.lower() for t in raw_tags}:
            raw_tags.append(ct)
    if creator.get("name"):
        c_tag = re.sub(r"\W", "", creator["name"])
        if c_tag and c_tag.lower() not in {t.lower() for t in raw_tags}:
            raw_tags.append(c_tag)
    tag_str = " ".join("#" + h for h in raw_tags[:15])

    parts = []
    desc = (clip.get("description") or "").strip()
    if desc and not desc.endswith("👇") and "?" not in desc:
        desc += "\n\n💬 Did you see that coming?! What would you have done here? Drop your thoughts below! 👇"
    elif not desc:
        desc = "You won't believe what happened in this crazy moment! 🤯 Watch till the very end to see the reaction.\n\n💬 Did you see that coming? Drop your thoughts below! 👇"
    parts.append(desc)
    parts.append("🔥 Drop a like and subscribe for daily viral clips!\n💬 What do you think about this? Let us know below! 👇")
    if creator.get("post_footer"):
        parts.append(creator["post_footer"])
    elif creator.get("name"):
        parts.append(f"Clipped from {creator['name']}")
    parts.append(tag_str)
    return "\n\n".join(parts)


def _post(p):
    job = jobs.get(p["job"])
    clip = next((c for c in job["clips"] if c["id"] == p["clip"]), None) if job else None
    acc = accounts.get(p["account"])
    from . import circuit_breaker, cleanup
    if acc and acc.get("platform") == "youtube" and circuit_breaker.youtube.is_open():
        cb_st = circuit_breaker.youtube.status()
        print(f"[PeakCut] [POSTING] YouTube circuit breaker is OPEN: {cb_st['reason']}", flush=True)
        p.update(status="scheduled", due_at=time.time() + min(cb_st.get("cooldown_seconds", 1800), 1800))
        _save()
        _mirror(p)
        return

    try:
        if not clip or clip.get("status") != "done":
            raise RuntimeError("The clip no longer exists or is not finished.")
        if not acc:
            raise RuntimeError("That account was disconnected.")
        path = Path(job["dir"]) / "clips" / clip["file"]
        creator = creators.get(job["creator"]) or dict(creators.DEFAULTS, name="clips")
        raw_title = clip.get("title") or clip.get("hook") or "Must Watch Clip! 🔥"
        title = _polish_post_title(raw_title, clip, job)
        if acc["platform"] == "youtube" and "#shorts" not in title.lower() and len(title) <= 90:
            title = f"{title} #Shorts"
        cap = _caption(clip, creator)
        print(f"[PeakCut] [POSTING] Uploading clip '{title}' to {acc['platform']} ({acc['name']})...", flush=True)
        if acc["platform"] == "youtube":
            rid, url = accounts.youtube_upload(acc, path, title, cap, clip.get("hashtags", []), creator["privacy"])
            circuit_breaker.youtube.record_success()
        elif acc["platform"] == "instagram":
            rid, url = accounts.instagram_upload(acc, path, f"{title}\n\n{cap}", job["id"])
        elif acc["platform"] == "tiktok":
            rid, url = accounts.tiktok_upload(acc, path, f"{title}\n\n{cap}", creator["privacy"])
        else:
            raise RuntimeError("That kind of account is no longer supported.")
        p.update(status="posted", posted_at=time.time(), remote_id=rid, url=url, error=None)
        creators.log(p["creator"], f"Posted to {acc['name']}: {title}")
        print(f"[PeakCut] [POSTING] Successfully posted to {acc['name']}: {url}", flush=True)
        cleanup.prune_posted_clip(p["job"], p["clip"])
    except Exception as e:  # noqa: BLE001
        print(f"[PeakCut] [POSTING ERROR] Upload to {acc.get('name') if acc else p.get('account_name')} failed: {e}", flush=True)
        if acc and acc.get("platform") == "youtube":
            circuit_breaker.youtube.record_error(str(e))
        p["tries"] += 1
        p["error"] = str(e)[-300:]
        if p["tries"] < 3 and clip and acc:
            p.update(status="scheduled", due_at=time.time() + 600)  # try again in 10 minutes
        else:
            p["status"] = "failed"
            creators.log(p["creator"], f"Post failed on {p['account_name']}: {p['error']}")
            alerts.notify(f"post failed on {p['account_name']} ({p['title']}): {p['error'][:200]}", kind="post")
    _save()
    _mirror(p)


def _run():
    while True:
        time.sleep(15)
        due = [p for p in list(_posts) if p["status"] == "scheduled" and p["due_at"] <= time.time() and p.get("user")]
        for p in due:
            try:
                with ctx.as_user(p["user"]):
                    if accounts.settings()["paused"]:
                        continue
                    with _lock:
                        if p["status"] != "scheduled":
                            continue
                        p["status"] = "posting"
                    _post(p)
            except Exception:  # noqa: BLE001
                pass


def _stats_for(recent):
    for aid in {p["account"] for p in recent if p["platform"] == "youtube"}:
        acc = accounts.get(aid)
        if acc:
            views = accounts.youtube_views(acc, [p["remote_id"] for p in recent if p["account"] == aid])
            for p in recent:
                if p["account"] == aid and p["remote_id"] in views:
                    p["views"] = views[p["remote_id"]]
    for p in recent:
        acc = accounts.get(p["account"]) if p["platform"] == "instagram" else None
        if acc:
            v = accounts.instagram_views(acc, p["remote_id"])
            if v is not None:
                p["views"] = v


def _stats():
    while True:
        for uid in {p.get("user") for p in _posts if p.get("user")}:
            try:
                with ctx.as_user(uid):
                    _stats_for([p for p in _mine() if p["status"] == "posted" and time.time() - p["posted_at"] < 14 * 86400])
            except Exception:  # noqa: BLE001
                pass
        _save()
        time.sleep(1800)


def cancel(pid):
    with _lock:
        p = next((p for p in _mine() if p["id"] == pid), None)
        if not p or p["status"] != "scheduled":
            raise ValueError("Only posts that are still waiting can be cancelled.")
        p["status"] = "cancelled"
        _save()
    _mirror(p)


def retry(pid):
    with _lock:
        p = next((p for p in _mine() if p["id"] == pid), None)
        if not p or p["status"] not in ("failed", "cancelled"):
            raise ValueError("Only failed or cancelled posts can be retried.")
        p.update(status="scheduled", due_at=time.time(), tries=0, error=None)
        _save()
    _mirror(p)


def overview() -> dict:
    today = _day(time.time())
    _posts = _mine()  # everything below is about the signed-in user only
    rows = []
    all_cs, all_js = creators.all_creators(), jobs.list_jobs()
    for c in all_cs:
        js = [j for j in all_js if j["creator"] == c["id"]]
        clips = [k for j in js for k in j["clips"] if k.get("status") == "done"]
        ps = [p for p in _posts if p["creator"] == c["id"]]
        views = sum(p["views"] or 0 for p in ps)
        rows.append({
            "id": c["id"], "name": c["name"],
            "state": "recording" if live.active_for(c["id"]) else "waiting" if c["auto_watch"] and c["channel"] else "idle",
            "autopost": c["autopost"] and bool(c["accounts"]),
            "clips_today": sum(1 for k in clips if _day(k.get("made_at", 0)) == today), "clips_total": len(clips),
            "posted_today": sum(1 for p in ps if p["status"] == "posted" and _day(p["posted_at"]) == today),
            "posted_total": sum(1 for p in ps if p["status"] == "posted"),
            "queued": sum(1 for p in ps if p["status"] in OPEN), "failed": sum(1 for p in ps if p["status"] == "failed"),
            "views": views, "earnings": round(views / 1000 * c["rate_per_1k"], 2)})
    names = {c["id"]: c["name"] for c in all_cs}
    queue = sorted((p for p in _posts if p["status"] in OPEN), key=lambda p: p["due_at"])
    done = sorted((p for p in _posts if p["status"] in ("posted", "failed")), key=lambda p: -(p.get("posted_at") or p["due_at"]))[:15]
    show = [{**{k: v for k, v in p.items() if k != "user"}, "creator_name": names.get(p["creator"], "")} for p in queue + done]
    # last 14 days, oldest first, for the home graph
    days = [_day(time.time() - 86400 * i) for i in range(13, -1, -1)]
    made, posted, views = {d: 0 for d in days}, {d: 0 for d in days}, {d: 0 for d in days}
    for j in all_js:
        for k in j["clips"]:
            d = _day(k.get("made_at") or j["created"])
            if k.get("status") == "done" and d in made:
                made[d] += 1
    for p in _posts:
        if p["status"] == "posted" and _day(p["posted_at"]) in posted:
            posted[_day(p["posted_at"])] += 1
            views[_day(p["posted_at"])] += p["views"] or 0
    daily = [{"day": d, "clips": made[d], "posted": posted[d], "views": views[d]} for d in days]
    return {"paused": accounts.settings()["paused"], "creators": rows, "posts": show, "daily": daily,
            "alerts": alerts.configured(), "recording": len(live.active_mine())}


def digest_text() -> str:
    o = overview()
    lines = ["PeakCut daily summary"]
    for r in o["creators"]:
        lines.append(f"- {r['name']}: {r['state']}, {r['clips_today']} clips today, {r['posted_today']} posted today, "
                     f"{r['queued']} waiting, {r['failed']} failed, {r['views']} views")
    if not o["creators"]:
        lines.append("No creators set up yet.")
    if o["paused"]:
        lines.append("Posting is paused.")
    return "\n".join(lines)


def start():
    with _lock:
        for p in _posts:
            if p["status"] == "posting":  # interrupted mid-upload last time
                p.update(status="failed", error="The studio closed during this upload. Check the account, then retry.")
        _save()
    jobs.on_clip_ready.append(on_clip_ready)
    threading.Thread(target=_run, daemon=True).start()
    threading.Thread(target=_stats, daemon=True).start()
    threading.Thread(target=alerts.digest_loop, args=(digest_text,), daemon=True).start()
    threading.Thread(target=alerts.keep_awake_loop,
                     args=(lambda: bool(live.active()) or any(p["status"] in OPEN for p in _posts),), daemon=True).start()
