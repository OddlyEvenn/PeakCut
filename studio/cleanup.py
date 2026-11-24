"""Storage cleanup and garbage collection:
- Prunes rendered .mp4 files after successful YouTube/Instagram/TikTok posting.
- Prunes heavy source.mp4 video files once clips are extracted.
- Prunes intermediate .src.mp4 / .src.ts slice pieces after render.
- Prunes leftover .ts stream buffers from finished live sessions.
- Prunes rejected/low-score clips after a 24-hour grace period.
- Emergency disk purge when storage falls below safe thresholds.
"""
import shutil
import threading
import time
from pathlib import Path

from . import config, creators, ctx, jobs, posting

_lock = threading.RLock()


def _get_job(job_id: str):
    """Safely get a job object across background threads and request contexts."""
    with jobs._lock:
        if job_id in jobs._jobs:
            return jobs._jobs[job_id]
        if ctx._uid.get():
            return jobs.get(job_id)
        for u_jobs in jobs._by_user.values():
            if job_id in u_jobs:
                return u_jobs[job_id]
    return None


def prune_posted_clip(job_id: str, clip_id: str) -> bool:
    """Delete local MP4 for a clip that was successfully uploaded to all its target accounts."""
    with _lock:
        job = _get_job(job_id)
        if not job:
            return False
        clip = next((c for c in job.get("clips", []) if c["id"] == clip_id), None)
        if not clip or clip.get("file_deleted"):
            return False

        # Verify all scheduled posts for this clip have completed
        clip_posts = [p for p in posting._posts if p.get("job") == job_id and p.get("clip") == clip_id]
        if not clip_posts or not all(p.get("status") == "posted" for p in clip_posts):
            return False

        p = Path(job["dir"]) / "clips" / clip.get("file", f"{clip_id}.mp4")
        if p.exists():
            try:
                p.unlink()
                clip["file_deleted"] = True
                jobs.save(job)
                print(f"[PeakCut] [CLEANUP] Pruned local MP4 for posted clip '{clip.get('title')}' ({clip_id})", flush=True)
            except OSError as e:
                print(f"[PeakCut] [CLEANUP ERROR] Could not delete {p}: {e}", flush=True)
        else:
            clip["file_deleted"] = True
            jobs.save(job)

        # Also prune any leftover .src slice piece for this clip
        for src_f in (Path(job["dir"]) / "clips").glob(f"{clip_id}.src.*"):
            try:
                src_f.unlink(missing_ok=True)
            except OSError:
                pass

        return True


def prune_source_video(job_id: str) -> bool:
    """Delete heavy source.mp4 once all clips are extracted and rendered."""
    with _lock:
        job = _get_job(job_id)
        if not job or not job.get("clips"):
            return False
        # Only prune if all clips are done or failed
        if not all(c.get("status") in ("done", "failed") for c in job["clips"]):
            return False

        src = Path(job["dir"]) / "source.mp4"
        if src.exists():
            try:
                size_mb = round(src.stat().st_size / (1024 * 1024), 1)
                src.unlink()
                job["source_pruned"] = True
                jobs.save(job)
                print(f"[PeakCut] [CLEANUP] Pruned {size_mb} MB source video for job {job_id}", flush=True)
                return True
            except OSError as e:
                print(f"[PeakCut] [CLEANUP ERROR] Could not delete source video {src}: {e}", flush=True)
    return False


def prune_clip_source_pieces(job_id: str) -> int:
    """Delete intermediate .src.mp4 and .src.ts slice pieces after rendering."""
    count = 0
    with _lock:
        job = _get_job(job_id)
        if not job or not job.get("clips"):
            return 0
        clips_dir = Path(job["dir"]) / "clips"
        if not clips_dir.exists():
            return 0

        # If all clips are done or failed, prune all .src.* pieces
        if all(c.get("status") in ("done", "failed") for c in job["clips"]):
            for f in clips_dir.glob("*.src.*"):
                try:
                    f.unlink(missing_ok=True)
                    count += 1
                except OSError:
                    pass
    return count


def prune_finished_session_buffers() -> int:
    """Prune leftover .ts buffer segments from completed or inactive live stream recordings."""
    pruned_files = 0
    with _lock:
        from . import live
        active_ids = set(live.SESSIONS.keys())
        for j in jobs.all_jobs():
            if j.get("kind") != "live":
                continue
            jid = j.get("id")
            if jid not in active_ids or j.get("status") in ("done", "finished", "error"):
                buf = Path(j["dir"]) / "buffer"
                if buf.exists():
                    for f in list(buf.glob("*.ts")) + list(buf.glob("*.csv")):
                        try:
                            f.unlink(missing_ok=True)
                            pruned_files += 1
                        except OSError:
                            pass
    if pruned_files > 0:
        print(f"[PeakCut] [CLEANUP] Pruned {pruned_files} temporary stream buffer segments from finished sessions.", flush=True)
    return pruned_files


def prune_rejected_clips(max_age_hours: int = 24) -> int:
    """Prune rendered MP4s for clips scoring below threshold with no posts, older than max_age_hours."""
    now = time.time()
    cutoff = now - max_age_hours * 3600
    pruned_count = 0

    with _lock:
        all_j = jobs.all_jobs()
        for j in all_j:
            uid = j.get("user")
            if not uid:
                continue
            with ctx.as_user(uid):
                cid = j.get("creator")
                c_meta = creators.get(cid) if cid else None
                min_score = c_meta.get("post_min_score", 75) if c_meta else 75

                changed = False
                for c in j.get("clips", []):
                    if c.get("status") != "done" or c.get("file_deleted"):
                        continue
                    made_at = c.get("made_at") or j.get("created", 0)
                    if made_at < cutoff and c.get("score", 0) < min_score:
                        # Check if there are any scheduled/posted posts for this clip
                        has_posts = any(p.get("job") == j["id"] and p.get("clip") == c["id"] for p in posting._posts)
                        if not has_posts:
                            p = Path(j["dir"]) / "clips" / c.get("file", f"{c['id']}.mp4")
                            if p.exists():
                                try:
                                    p.unlink()
                                    c["file_deleted"] = True
                                    c["rejected"] = True
                                    changed = True
                                    pruned_count += 1
                                    print(f"[PeakCut] [CLEANUP] Pruned rejected clip {c['id']} from job {j['id']} (Score: {c.get('score')})", flush=True)
                                except OSError:
                                    pass
                            for src_f in (Path(j["dir"]) / "clips").glob(f"{c['id']}.src.*"):
                                try:
                                    src_f.unlink(missing_ok=True)
                                except OSError:
                                    pass
                if changed:
                    jobs.save(j)

    return pruned_count


def emergency_disk_cleanup():
    """Aggressively free disk space: prune all posted clips, old source videos, buffers, and stale rejected clips."""
    print("[PeakCut] [CLEANUP] Starting emergency disk space reclamation...", flush=True)
    # 1. Prune all posted clips
    for p in list(posting._posts):
        if p.get("status") == "posted" and p.get("job") and p.get("clip"):
            prune_posted_clip(p["job"], p["clip"])

    # 2. Prune source videos and slice pieces for any completed job
    for j in jobs.all_jobs():
        if j.get("status") in ("done", "finished"):
            prune_source_video(j["id"])
            prune_clip_source_pieces(j["id"])

    # 3. Prune finished live session buffers
    prune_finished_session_buffers()

    # 4. Prune rejected clips older than 2 hours
    prune_rejected_clips(max_age_hours=2)

    # 5. Clean tmp directory
    tmp_dir = config.DATA_DIR / "tmp"
    if tmp_dir.exists():
        for item in tmp_dir.glob("*"):
            try:
                if item.is_file():
                    item.unlink()
                elif item.is_dir():
                    shutil.rmtree(item, ignore_errors=True)
            except OSError:
                pass


def run_cycle():
    """Run one complete cleanup and storage reclamation cycle."""
    from . import circuit_breaker
    if not circuit_breaker.disk.is_disk_safe():
        emergency_disk_cleanup()
    else:
        # Regular background maintenance cycle
        prune_rejected_clips(max_age_hours=24)
        for j in jobs.all_jobs():
            if j.get("status") in ("done", "finished"):
                prune_source_video(j["id"])
                prune_clip_source_pieces(j["id"])
        prune_finished_session_buffers()


def _cleanup_loop():
    time.sleep(30)  # wait 30s after startup
    while True:
        try:
            run_cycle()
        except Exception as e:
            print(f"[PeakCut] [CLEANUP ERROR] Error in cleanup loop: {e}", flush=True)
        time.sleep(1800)  # run every 30 minutes


def start():
    """Start background cleanup worker."""
    threading.Thread(target=_cleanup_loop, daemon=True).start()
    print("[PeakCut] Automated storage cleanup daemon started (runs every 30m)", flush=True)
