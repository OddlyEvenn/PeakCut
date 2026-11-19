"""Multi-layer circuit breakers for PeakCut:
1. YouTube API Quota & Rate Limits (auto-resets at Pacific midnight / 1:30 PM IST)
2. AI Cascade Provider Health (bypasses failing keys for 10 min)
3. Disk Space Safety Guard (halts downloads when free space < 4 GB)
4. Creator Stream Failure Breaker (pauses broken streams for 2 hours)
"""
import re
import shutil
import threading
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

from . import config

_lock = threading.RLock()


# ------------------------------------------------------------------ 1. YouTube Circuit Breaker
class _YouTubeCircuitBreaker:
    def __init__(self):
        self.state = "CLOSED"  # CLOSED, OPEN
        self.tripped_until = 0.0
        self.reason = ""
        self.consecutive_errors = 0

    def is_open(self) -> bool:
        with _lock:
            if self.state == "OPEN":
                if time.time() >= self.tripped_until:
                    print("[PeakCut] [CIRCUIT BREAKER] YouTube breaker reset cooldown ended. Returning to CLOSED state.", flush=True)
                    self.state = "CLOSED"
                    self.reason = ""
                    self.consecutive_errors = 0
                    return False
                return True
            return False

    def record_success(self):
        with _lock:
            self.consecutive_errors = 0
            if self.state != "CLOSED":
                self.state = "CLOSED"
                self.reason = ""

    def record_error(self, error_str: str):
        with _lock:
            err = (error_str or "").lower()
            # Check for YouTube quota limit errors
            if "quotaexceeded" in err or "uploadlimitexceeded" in err or "daily limit exceeded" in err:
                self.state = "OPEN"
                # YouTube quota resets daily at midnight Pacific Time (07:00 UTC / 08:00 UTC depending on DST)
                # Next 07:30 UTC = 1:00 PM IST
                now_utc = datetime.now(timezone.utc)
                # target reset today or tomorrow at 07:30 UTC
                reset_today = now_utc.replace(hour=7, minute=30, second=0, microsecond=0)
                if now_utc >= reset_today:
                    target_reset = reset_today + timedelta(days=1)
                else:
                    target_reset = reset_today
                wait_sec = max(600, (target_reset - now_utc).total_seconds())
                self.tripped_until = time.time() + wait_sec
                self.reason = f"YouTube daily upload quota exceeded. Pausing uploads until reset at ~1:30 PM IST."
                print(f"[PeakCut] [CIRCUIT BREAKER ALERT] {self.reason} (Tripped for {round(wait_sec/3600, 1)}h)", flush=True)
                return

            self.consecutive_errors += 1
            if self.consecutive_errors >= 5:
                self.state = "OPEN"
                self.tripped_until = time.time() + 1800  # 30 min cooldown
                self.reason = f"5 consecutive upload failures. Tripping YouTube breaker for 30 minutes: {error_str[:150]}"
                print(f"[PeakCut] [CIRCUIT BREAKER ALERT] {self.reason}", flush=True)

    def status(self) -> dict:
        with _lock:
            open_ = self.is_open()
            remaining = max(0, int(self.tripped_until - time.time())) if open_ else 0
            return {"open": open_, "reason": self.reason if open_ else "Healthy", "cooldown_seconds": remaining}


# ------------------------------------------------------------------ 2. AI Provider Circuit Breaker
class _AICircuitBreaker:
    def __init__(self):
        self.providers: dict = {}  # name -> {"fails": int, "tripped_until": float, "last_err": str}

    def is_provider_healthy(self, name: str) -> bool:
        with _lock:
            info = self.providers.get(name)
            if not info:
                return True
            if info["tripped_until"] > time.time():
                return False
            # Cooldown passed, reset
            if info["fails"] >= 3:
                info["fails"] = 0
                info["tripped_until"] = 0.0
            return True

    def record_success(self, name: str):
        with _lock:
            if name in self.providers:
                self.providers[name]["fails"] = 0
                self.providers[name]["tripped_until"] = 0.0

    def record_failure(self, name: str, err: str):
        with _lock:
            p = self.providers.setdefault(name, {"fails": 0, "tripped_until": 0.0, "last_err": ""})
            p["fails"] += 1
            p["last_err"] = str(err)[:200]
            err_lower = str(err).lower()
            # If rate limited (429) or 3 consecutive errors: trip for 10 minutes
            if "429" in err_lower or "rate limit" in err_lower or p["fails"] >= 3:
                p["tripped_until"] = time.time() + 600  # 10 minutes
                print(f"[PeakCut] [CIRCUIT BREAKER] Tripped AI provider '{name}' for 10 min due to errors: {p['last_err']}", flush=True)

    def status(self) -> dict:
        with _lock:
            now = time.time()
            return {
                name: {
                    "healthy": data["tripped_until"] <= now,
                    "fails": data["fails"],
                    "cooldown_seconds": max(0, int(data["tripped_until"] - now)),
                    "last_error": data["last_err"]
                }
                for name, data in self.providers.items()
            }


# ------------------------------------------------------------------ 3. Disk Safety Circuit Breaker
class _DiskCircuitBreaker:
    MIN_SAFE_GB = 3.5

    def free_gb(self) -> float:
        try:
            return round(shutil.disk_usage(config.DATA_DIR).free / 1e9, 2)
        except OSError:
            return 0.0

    def is_disk_safe(self) -> bool:
        free = self.free_gb()
        if free < self.MIN_SAFE_GB:
            print(f"[PeakCut] [CIRCUIT BREAKER ALERT] Low disk space: only {free} GB free! Tripping disk safety breaker.", flush=True)
            return False
        return True


# ------------------------------------------------------------------ 4. Creator Stream Failure Circuit Breaker
class _CreatorCircuitBreaker:
    def __init__(self):
        self.creators: dict = {}  # cid -> {"fails": int, "tripped_until": float, "last_err": str}

    def is_creator_healthy(self, cid: str) -> bool:
        with _lock:
            info = self.creators.get(cid)
            if not info:
                return True
            if info["tripped_until"] > time.time():
                return False
            if info["fails"] >= 3:
                info["fails"] = 0
                info["tripped_until"] = 0.0
            return True

    def record_success(self, cid: str):
        with _lock:
            if cid in self.creators:
                self.creators[cid] = {"fails": 0, "tripped_until": 0.0, "last_err": ""}

    def record_failure(self, cid: str, err: str):
        with _lock:
            c = self.creators.setdefault(cid, {"fails": 0, "tripped_until": 0.0, "last_err": ""})
            c["fails"] += 1
            c["last_err"] = str(err)[:200]
            if c["fails"] >= 3:
                c["tripped_until"] = time.time() + 7200  # 2 hours cooldown
                print(f"[PeakCut] [CIRCUIT BREAKER] Creator '{cid}' has 3 consecutive stream failures. Pausing poller for 2h: {c['last_err']}", flush=True)


youtube = _YouTubeCircuitBreaker()
ai = _AICircuitBreaker()
disk = _DiskCircuitBreaker()
creator = _CreatorCircuitBreaker()
