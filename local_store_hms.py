"""
local_store_hms.py — Local JSON store for PENDING HMS sync bags + ABANDONED audit.

Three files (all in _cache/):
  - pending_hms_sync.json    : bags actively waiting for HMS sync (browser #2)
  - abandoned_hms.json       : bags that failed too many real HMS rejections
  - hms_sheet_dlq.json       : sheet write failures to retry

Each pending bag tracks `real_attempts` — only HMS rejections count toward
the abandonment threshold. Session-down / network errors do NOT count.

When real_attempts >= MAX_REAL_ATTEMPTS, bag is moved to abandoned_hms.json
and HMS_Synced cell is left blank for manual intervention.

Atomic writes (temp file + rename + fsync) prevent corruption on crashes.

Modeled after LNZT local_store.py.
"""

import json
import os
import threading
import tempfile
from datetime import datetime

import pytz

IST = pytz.timezone("Asia/Kolkata")
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CACHE_DIR = os.path.join(BASE_DIR, "_cache")

PENDING_FILE = os.path.join(CACHE_DIR, "pending_hms_sync.json")
ABANDONED_FILE = os.path.join(CACHE_DIR, "abandoned_hms.json")
DLQ_FILE = os.path.join(CACHE_DIR, "hms_sheet_dlq.json")

# How many times HMS must REALLY reject a bag before we abandon it.
# Session-down / network / browser errors do NOT count toward this.
MAX_REAL_ATTEMPTS = 6

_lock = threading.RLock()
_dlq_lock = threading.RLock()

# ── In-memory pending cache (eliminates disk reads on hot path) ──────────
# _mem_pending is the PRIMARY read source. Hydrated from disk on first access.
# All mutations update _mem_pending immediately, then flush to disk async.
_mem_pending: list = None   # lazy init on first access
_mem_pending_loaded = False
_pending_flush_pending = False
_pending_flush_timer = None
_PENDING_FLUSH_INTERVAL = 0.5   # flush at most every 500ms


# ─── Atomic file IO ─────────────────────────────────────────────────────────

def _ensure_cache_dir():
    try:
        os.makedirs(CACHE_DIR, exist_ok=True)
    except Exception:
        pass


def _atomic_write(path: str, data):
    """Write JSON atomically — temp file + rename, fsync before rename."""
    _ensure_cache_dir()
    try:
        tmp_fd, tmp_path = tempfile.mkstemp(
            prefix=f".{os.path.basename(path)}_",
            suffix=".tmp",
            dir=CACHE_DIR,
        )
        try:
            with os.fdopen(tmp_fd, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp_path, path)
        except Exception:
            try:
                os.remove(tmp_path)
            except Exception:
                pass
            raise
    except IOError as e:
        print(f"[LOCAL_STORE_HMS] Error writing {os.path.basename(path)}: {e}", flush=True)


def _load_json_list(path: str) -> list:
    try:
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                content = f.read().strip()
                if not content:
                    return []
                data = json.loads(content)
                if isinstance(data, list):
                    return data
    except (json.JSONDecodeError, IOError) as e:
        print(f"[LOCAL_STORE_HMS] Error reading {os.path.basename(path)}: {e}", flush=True)
        # Backup corrupt file
        try:
            backup = path + ".corrupt." + datetime.now(IST).strftime("%Y%m%d_%H%M%S")
            if os.path.exists(path):
                os.rename(path, backup)
                print(f"[LOCAL_STORE_HMS] Corrupt file backed up to: {backup}", flush=True)
        except Exception:
            pass
    return []


def _load_pending() -> list:
    return _load_json_list(PENDING_FILE)


def _get_mem_pending() -> list:
    """Return the in-memory pending list, hydrating from disk on first access."""
    global _mem_pending, _mem_pending_loaded
    if not _mem_pending_loaded:
        _mem_pending = _load_json_list(PENDING_FILE)
        _mem_pending_loaded = True
    return _mem_pending


def _schedule_pending_flush():
    """Schedule a deferred disk flush. Coalesces multiple writes."""
    global _pending_flush_pending, _pending_flush_timer
    if _pending_flush_pending:
        return
    _pending_flush_pending = True

    def _do_flush():
        global _pending_flush_pending
        with _lock:
            snapshot = list(_get_mem_pending())
            _pending_flush_pending = False
        _atomic_write(PENDING_FILE, snapshot)

    _pending_flush_timer = threading.Timer(_PENDING_FLUSH_INTERVAL, _do_flush)
    _pending_flush_timer.daemon = True
    _pending_flush_timer.start()


def _flush_pending_now():
    """Immediate disk flush (called on critical mutations like abandon)."""
    global _pending_flush_pending, _pending_flush_timer
    if _pending_flush_timer:
        _pending_flush_timer.cancel()
    _pending_flush_pending = False
    with _lock:
        snapshot = list(_get_mem_pending())
    _atomic_write(PENDING_FILE, snapshot)


def _load_abandoned() -> list:
    return _load_json_list(ABANDONED_FILE)


def _load_dlq() -> list:
    return _load_json_list(DLQ_FILE)


# ─── Pending API ────────────────────────────────────────────────────────────

def add_pending_bag(bag_id: str, area_barcode: str, grid: str,
                    trolley_id: str, sheet_row: int):
    """Add a bag to pending_hms_sync.json. Idempotent — duplicates skipped.

    sheet_row: 1-based row index in Live_Staging for fast write-back.
    """
    if not bag_id or not area_barcode:
        print(f"[LOCAL_STORE_HMS] WARN: empty bag_id={bag_id!r} or "
              f"area_barcode={area_barcode!r}", flush=True)
        return False

    with _lock:
        pending = _get_mem_pending()
        for rec in pending:
            if rec.get("bag_id") == bag_id and rec.get("trolley_id") == trolley_id:
                # Already queued for this same trolley — idempotent
                return False
        pending.append({
            "bag_id": bag_id,
            "area_barcode": area_barcode,
            "grid": grid,
            "trolley_id": trolley_id,
            "sheet_row": sheet_row,
            "added_at": datetime.now(IST).isoformat(),
            "real_attempts": 0,
            "last_error": None,
            "last_attempt_at": None,
        })
        _schedule_pending_flush()
        print(f"[LOCAL_STORE_HMS] ✓ Queued: {bag_id} → {area_barcode} "
              f"(grid: {grid}, trolley: {trolley_id}, total: {len(pending)})", flush=True)
        return True


def remove_synced_bag(bag_id: str, trolley_id: str = None) -> bool:
    """Remove a bag — call ONLY when HMS sync confirmed SUCCESS.

    If trolley_id given, only removes the matching record.
    Otherwise removes by bag_id alone.
    """
    if not bag_id:
        return False
    with _lock:
        pending = _get_mem_pending()
        before = len(pending)
        if trolley_id:
            new_pending = [r for r in pending
                           if not (r.get("bag_id") == bag_id and r.get("trolley_id") == trolley_id)]
        else:
            new_pending = [r for r in pending if r.get("bag_id") != bag_id]
        removed = before - len(new_pending)
        if removed > 0:
            # Update in-place so all references see the change
            global _mem_pending
            _mem_pending = new_pending
            _schedule_pending_flush()
            print(f"[LOCAL_STORE_HMS] ✓ Synced & removed: {bag_id} "
                  f"(remaining: {len(new_pending)})", flush=True)
            return True
    return False


def record_real_attempt(bag_id: str, trolley_id: str, error: str) -> int:
    """Increment real_attempts for a bag HMS actually rejected.

    ONLY call when HMS genuinely received the scan and said no
    (e.g. "not found", "invalid", explicit error keywords).
    Do NOT call for session-down / browser-crash / network errors.

    Returns new attempt count, or -1 if bag not found.
    """
    if not bag_id:
        return -1
    with _lock:
        pending = _get_mem_pending()
        new_count = -1
        for rec in pending:
            if rec.get("bag_id") == bag_id and rec.get("trolley_id") == trolley_id:
                rec["real_attempts"] = rec.get("real_attempts", 0) + 1
                rec["last_error"] = (error[:300] if error else None)
                rec["last_attempt_at"] = datetime.now(IST).isoformat()
                new_count = rec["real_attempts"]
                break
        if new_count >= 0:
            _schedule_pending_flush()
            print(f"[LOCAL_STORE_HMS] Real attempt #{new_count}/{MAX_REAL_ATTEMPTS} "
                  f"for {bag_id}. Error: {(error or 'unknown')[:80]}", flush=True)
    return new_count


def abandon_bag(bag_id: str, trolley_id: str, reason: str) -> bool:
    """Move a bag from pending → abandoned. Audit trail preserved."""
    if not bag_id:
        return False
    with _lock:
        pending = _get_mem_pending()
        target = None
        new_pending = []
        for rec in pending:
            if (rec.get("bag_id") == bag_id and rec.get("trolley_id") == trolley_id
                    and target is None):
                target = rec
            else:
                new_pending.append(rec)
        if not target:
            return False

        target["abandoned_at"] = datetime.now(IST).isoformat()
        target["abandon_reason"] = reason

        abandoned = _load_abandoned()
        abandoned.append(target)
        # Write abandoned FIRST — if pending write fails, bag still preserved
        _atomic_write(ABANDONED_FILE, abandoned)
        # Update in-memory pending immediately
        global _mem_pending
        _mem_pending = new_pending
        _flush_pending_now()  # Critical mutation → immediate flush

    print(f"[LOCAL_STORE_HMS] ☠ ABANDONED: {bag_id} after "
          f"{target.get('real_attempts', 0)} real attempts. Reason: {reason}",
          flush=True)
    return True


def get_all_pending() -> list:
    """Return all pending bags from in-memory cache (~0ms, no disk I/O)."""
    with _lock:
        return list(_get_mem_pending())


def get_pending_count() -> int:
    with _lock:
        return len(_get_mem_pending())


def get_pending_for_trolley(trolley_id: str) -> list:
    """Return all pending bags for a specific trolley."""
    if not trolley_id:
        return []
    with _lock:
        return [r for r in _get_mem_pending() if r.get("trolley_id") == trolley_id]


def get_all_abandoned() -> list:
    with _lock:
        return _load_abandoned()


def get_abandoned_count() -> int:
    with _lock:
        return len(_load_abandoned())


def is_bag_pending(bag_id: str) -> bool:
    """Check if a bag is currently in the pending queue."""
    if not bag_id:
        return False
    with _lock:
        return any(r.get("bag_id") == bag_id for r in _get_mem_pending())


# ─── DLQ for sheet writes ───────────────────────────────────────────────────

def add_to_dlq(items: list):
    """Add failed sheet writes to DLQ for retry."""
    if not items:
        return
    with _dlq_lock:
        existing = _load_dlq()
        existing.extend(items)
        _atomic_write(DLQ_FILE, existing)
    print(f"[LOCAL_STORE_HMS] DLQ +{len(items)} (total: {len(existing)})", flush=True)


def drain_dlq() -> list:
    """Pull all items from DLQ for retry. Caller must re-enqueue on failure."""
    with _dlq_lock:
        items = _load_dlq()
        if items:
            _atomic_write(DLQ_FILE, [])
    return items


def get_dlq_count() -> int:
    with _dlq_lock:
        return len(_load_dlq())


# ─── Stats / monitoring ─────────────────────────────────────────────────────

def get_stats() -> dict:
    with _lock:
        pending = list(_get_mem_pending())
    with _dlq_lock:
        dlq = _load_dlq()
    abandoned = get_abandoned_count()

    # Breakdown by trolley for visibility
    by_trolley = {}
    for rec in pending:
        tid = rec.get("trolley_id", "")
        by_trolley[tid] = by_trolley.get(tid, 0) + 1

    return {
        "pending": len(pending),
        "abandoned": abandoned,
        "dlq": len(dlq),
        "by_trolley": by_trolley,
    }


def clear_stale_pending(max_age_hours: int = 24) -> int:
    """Move bags older than max_age_hours from pending → abandoned."""
    with _lock:
        pending = _get_mem_pending()
        now = datetime.now(IST)
        kept = []
        moved = 0
        abandoned = _load_abandoned()
        for rec in pending:
            try:
                added_str = rec.get("added_at", "")
                added = datetime.fromisoformat(added_str)
                if added.tzinfo is None:
                    added = IST.localize(added)
                age_hours = (now - added).total_seconds() / 3600
                if age_hours > max_age_hours:
                    rec["abandoned_at"] = now.isoformat()
                    rec["abandon_reason"] = f"Aged out after {max_age_hours}h"
                    abandoned.append(rec)
                    moved += 1
                    continue
            except (ValueError, TypeError):
                rec["abandoned_at"] = now.isoformat()
                rec["abandon_reason"] = "Unparseable added_at"
                abandoned.append(rec)
                moved += 1
                continue
            kept.append(rec)
        if moved > 0:
            _atomic_write(ABANDONED_FILE, abandoned)
            global _mem_pending
            _mem_pending = kept
            _flush_pending_now()
    if moved:
        print(f"[LOCAL_STORE_HMS] Moved {moved} stale bag(s) to abandoned, "
              f"{len(kept)} still pending", flush=True)
    return moved