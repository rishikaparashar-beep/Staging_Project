"""
local_store_grid.py — Local pre-fetch cache for HMS grid suggestions.

PURPOSE
  Belt scan (G1/G2 floor) happens minutes before the spiral scan. During that
  window we can pre-query HMS in the background for every belted bag and
  cache its grid suggestion. By the time the bag reaches the spiral, the
  worker's HHD scan returns the grid INSTANTLY from this local cache —
  no HMS round-trip on the hot path.

  Two files (both in _cache/):
    - grid_queue.json   : ordered list of bag_ids waiting for prefetch
    - grid_cache.json   : dict { bag_id → {grid, already_staged, fetched_at,
                                           consumed_at, source} }

  The cache is intentionally a dict (not a list) for O(1) lookup.

IN-MEMORY LAYER (v2 — performance fix)
  File I/O on every lookup/store was the #1 bottleneck: ~15-25ms per call,
  blocking the asyncio event loop and causing 2-4s gaps between when a
  suggester returns a result and when the spiral scan can see it.

  Now we keep a _mem_cache dict in-memory:
    - lookup_grid() reads from _mem_cache → 0ms (dict.get)
    - store_grid() writes to _mem_cache immediately, then flushes to disk
      in a background thread (non-blocking)
    - On startup, _mem_cache is hydrated from grid_cache.json

  File writes are still atomic (temp+fsync+rename) for crash safety,
  but they no longer block the hot path.

DEDUPLICATION
  _inflight set tracks bag_ids currently being processed by a suggester
  browser. fire_prefetch_immediate and _prefetch_worker_loop check this
  set before submitting, preventing the same bag from being processed
  twice (which was wasting 50% of browser capacity).

THREAD SAFETY
  All writes go through an RLock plus atomic temp-file + rename + fsync.
  Crash-safe — a partial write leaves the previous file intact.
  _mem_cache access is guarded by _cache_lock (same lock, zero contention
  since file reads are eliminated from the hot path).

LIFECYCLE
    enqueue_for_prefetch(bag_id)   ← called from /api/conveyer-scan
        ↓
    queue.json (FIFO)
        ↓
    PrefetchWorker (in hms_sync.py asyncio loop)
        ↓
    store_grid(bag_id, grid, ...)  ← writes to _mem_cache + async file flush
        ↓
    lookup_grid(bag_id)            ← called from /api/process-scan
        → returns grid INSTANTLY from _mem_cache, else None (fallback to live HMS)
        ↓
    mark_consumed(bag_id)          ← stamps consumed_at for audit
"""

import json
import os
import tempfile
import threading
from datetime import datetime, timedelta

import pytz

IST = pytz.timezone("Asia/Kolkata")
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CACHE_DIR = os.path.join(BASE_DIR, "_cache")

QUEUE_FILE = os.path.join(CACHE_DIR, "grid_queue.json")
CACHE_FILE = os.path.join(CACHE_DIR, "grid_cache.json")

# Cache entries older than this are purged from grid_cache.json on next janitor
# sweep. Keep it generous (24 h) so we don't lose an entry while a bag is
# travelling down the spiral.
CACHE_TTL_HOURS = 24

# Consumed entries older than this are REPLACED on next enqueue_for_prefetch.
# This prevents stale entries from prior shifts blocking fresh prefetches.
CONSUMED_STALE_MINUTES = 30

_queue_lock = threading.RLock()
_cache_lock = threading.RLock()

# Event: signaled when new items arrive in the queue so the prefetch worker
# wakes instantly instead of polling on a sleep loop.
_queue_ready = threading.Event()

# ── In-memory cache + inflight tracking ───────────────────────────────────
# _mem_cache is the PRIMARY read source. It is hydrated from disk on first
# access and kept in sync with every store/mark/remove call.
_mem_cache: dict = None  # lazy init on first access
_mem_cache_loaded = False

# Bag IDs currently being processed by a suggester browser.
# Prevents double-submit from fire_prefetch_immediate + _prefetch_worker_loop.
_inflight: set = set()
_inflight_lock = threading.Lock()

# Background flush: batches disk writes so we don't fsync on every store_grid.
_flush_pending = False
_flush_timer: threading.Timer = None
_FLUSH_INTERVAL_SEC = 0.5  # flush at most every 500ms


# ── Atomic file IO ────────────────────────────────────────────────────────

def _ensure_cache_dir():
    try:
        os.makedirs(CACHE_DIR, exist_ok=True)
    except Exception:
        pass


def _atomic_write(path: str, data):
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
        print(f"[LOCAL_STORE_GRID] write {os.path.basename(path)}: {e}",
              flush=True)


def _load_json(path: str, default):
    try:
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                content = f.read().strip()
                if not content:
                    return default
                data = json.loads(content)
                if isinstance(data, type(default)):
                    return data
    except (json.JSONDecodeError, IOError) as e:
        print(f"[LOCAL_STORE_GRID] read {os.path.basename(path)}: {e}",
              flush=True)
        # Backup corrupt file so it doesn't trip us up next read
        try:
            backup = path + ".corrupt." + datetime.now(IST).strftime("%Y%m%d_%H%M%S")
            if os.path.exists(path):
                os.rename(path, backup)
        except Exception:
            pass
    return default


def _load_queue() -> list:
    return _load_json(QUEUE_FILE, [])


def _load_cache_from_disk() -> dict:
    return _load_json(CACHE_FILE, {})


def _get_mem_cache() -> dict:
    """Return the in-memory cache, hydrating from disk on first access."""
    global _mem_cache, _mem_cache_loaded
    if not _mem_cache_loaded:
        _mem_cache = _load_cache_from_disk()
        _mem_cache_loaded = True
    return _mem_cache


def _schedule_flush():
    """Schedule a deferred disk flush. Coalesces multiple writes into one."""
    global _flush_pending, _flush_timer
    if _flush_pending:
        return  # Already scheduled
    _flush_pending = True

    def _do_flush():
        global _flush_pending
        with _cache_lock:
            cache_snapshot = dict(_get_mem_cache())
            _flush_pending = False
        # Write outside the lock to avoid blocking lookups
        _atomic_write(CACHE_FILE, cache_snapshot)

    _flush_timer = threading.Timer(_FLUSH_INTERVAL_SEC, _do_flush)
    _flush_timer.daemon = True
    _flush_timer.start()


def _flush_now():
    """Immediate disk flush (called from purge_stale, etc.)."""
    global _flush_pending, _flush_timer
    if _flush_timer:
        _flush_timer.cancel()
    _flush_pending = False
    with _cache_lock:
        cache_snapshot = dict(_get_mem_cache())
    _atomic_write(CACHE_FILE, cache_snapshot)


# ── Inflight tracking (deduplication) ─────────────────────────────────────

def mark_inflight(bag_id: str) -> bool:
    """Mark a bag as inflight. Returns False if already inflight (skip it)."""
    bag_id = str(bag_id).strip().upper()
    with _inflight_lock:
        if bag_id in _inflight:
            return False
        _inflight.add(bag_id)
        return True


def clear_inflight(bag_id: str):
    """Remove a bag from inflight set (after result received)."""
    bag_id = str(bag_id).strip().upper()
    with _inflight_lock:
        _inflight.discard(bag_id)


def is_inflight(bag_id: str) -> bool:
    """Check if bag is currently being processed."""
    bag_id = str(bag_id).strip().upper()
    with _inflight_lock:
        return bag_id in _inflight


# ── Queue API (FIFO) ──────────────────────────────────────────────────────

def enqueue_for_prefetch(bag_id: str) -> bool:
    """Append bag_id to the prefetch queue.

    Skips ONLY if already in cache with a FRESH unconsumed entry.
    If the cached entry was consumed (stale from a prior spiral scan pass)
    or is older than CONSUMED_STALE_MINUTES, we DELETE it and re-enqueue
    so the prefetch worker fetches fresh data.

    Returns True if newly enqueued, False otherwise.
    Signals the prefetch worker to wake up immediately.
    """
    if not bag_id:
        return False
    bag_id = str(bag_id).strip().upper()
    if not bag_id:
        return False

    # Short-circuit: check in-memory cache (instant — no file I/O)
    with _cache_lock:
        cache = _get_mem_cache()
        existing = cache.get(bag_id)
        if existing:
            # If entry was already consumed → it's from a PRIOR scan pass.
            # Delete it so fresh data can be fetched.
            if existing.get("consumed_at"):
                del cache[bag_id]
                _schedule_flush()
            else:
                # Unconsumed but check age — entries older than stale threshold
                # might be from a prior session/shift.
                try:
                    fetched = datetime.fromisoformat(existing.get("fetched_at", ""))
                    if fetched.tzinfo is None:
                        fetched = IST.localize(fetched)
                    age = datetime.now(IST) - fetched
                    if age.total_seconds() < CONSUMED_STALE_MINUTES * 60:
                        return False  # Fresh and unconsumed → skip
                except (ValueError, TypeError):
                    pass
                # Stale unconsumed entry → delete and re-fetch
                del cache[bag_id]
                _schedule_flush()

    with _queue_lock:
        queue = _load_queue()
        if bag_id in queue:
            return False
        queue.append(bag_id)
        _atomic_write(QUEUE_FILE, queue)
    # Wake up the prefetch worker IMMEDIATELY — no polling delay.
    _queue_ready.set()
    return True


def pop_next_for_prefetch() -> str:
    """FIFO dequeue. Returns "" if queue empty."""
    with _queue_lock:
        queue = _load_queue()
        if not queue:
            return ""
        bag_id = queue.pop(0)
        _atomic_write(QUEUE_FILE, queue)
        return bag_id


def pop_batch_for_prefetch(max_items: int = 4) -> list:
    """FIFO dequeue up to max_items at once for parallel prefetch.

    Returns list of bag_ids (may be empty).
    """
    with _queue_lock:
        queue = _load_queue()
        if not queue:
            return []
        batch = queue[:max_items]
        remaining = queue[max_items:]
        _atomic_write(QUEUE_FILE, remaining)
        return batch


def wait_for_queue(timeout: float = 1.0) -> bool:
    """Block until queue has items or timeout. Returns True if signaled."""
    result = _queue_ready.wait(timeout=timeout)
    _queue_ready.clear()  # Reset for next wait
    return result


def queue_size() -> int:
    with _queue_lock:
        return len(_load_queue())


def requeue_for_prefetch(bag_id: str):
    """Put a bag back at the FRONT of the queue (for retry)."""
    if not bag_id:
        return
    bag_id = str(bag_id).strip().upper()
    with _queue_lock:
        queue = _load_queue()
        if bag_id in queue:
            return
        queue.insert(0, bag_id)
        _atomic_write(QUEUE_FILE, queue)
    _queue_ready.set()  # Wake worker for retry too


# ── Cache API ─────────────────────────────────────────────────────────────

def store_grid(bag_id: str, grid: str, already_staged: bool = False,
               source: str = "prefetch") -> bool:
    """Write a (bag_id → grid) mapping to the cache.

    source: 'prefetch' (background worker), 'on_demand' (spiral fallback),
            'immediate_prefetch' (fire_prefetch_immediate).
    Returns True on success.

    FAST PATH: writes to _mem_cache immediately (0ms), then schedules
    an async disk flush (500ms coalesced). The spiral scan sees the data
    instantly via lookup_grid().
    """
    if not bag_id or not grid:
        return False
    bag_id = str(bag_id).strip().upper()
    grid = str(grid).strip().upper()
    with _cache_lock:
        cache = _get_mem_cache()
        cache[bag_id] = {
            "grid": grid,
            "already_staged": bool(already_staged),
            "fetched_at": datetime.now(IST).isoformat(),
            "consumed_at": None,
            "source": source,
        }
        _schedule_flush()
    print(f"[LOCAL_STORE_GRID] ✓ Cached: {bag_id} → {grid} "
          f"(source: {source}, staged: {already_staged})", flush=True)
    return True


def lookup_grid(bag_id: str) -> dict:
    """Return cached entry for bag_id, or None.

    FAST PATH: reads from _mem_cache (dict.get → ~0ms).
    No file I/O on the hot path.

    Does NOT mark consumed (caller does that via mark_consumed).
    Entry shape: {grid, already_staged, fetched_at, consumed_at, source}
    """
    if not bag_id:
        return None
    bag_id = str(bag_id).strip().upper()
    with _cache_lock:
        cache = _get_mem_cache()
        return cache.get(bag_id)


def mark_consumed(bag_id: str) -> bool:
    """Stamp consumed_at on the cache entry — audit only.

    The entry is kept (not deleted) so we can see the prefetch hit history.
    """
    if not bag_id:
        return False
    bag_id = str(bag_id).strip().upper()
    with _cache_lock:
        cache = _get_mem_cache()
        if bag_id not in cache:
            return False
        cache[bag_id]["consumed_at"] = datetime.now(IST).isoformat()
        _schedule_flush()
    return True


def remove_from_cache(bag_id: str) -> bool:
    """Delete the entry outright (rare — for explicit invalidation)."""
    if not bag_id:
        return False
    bag_id = str(bag_id).strip().upper()
    with _cache_lock:
        cache = _get_mem_cache()
        if bag_id not in cache:
            return False
        del cache[bag_id]
        _schedule_flush()
    return True


def purge_stale(max_age_hours: int = CACHE_TTL_HOURS) -> int:
    """Drop cache entries older than max_age_hours. Returns count removed."""
    cutoff = datetime.now(IST) - timedelta(hours=max_age_hours)
    removed = 0
    with _cache_lock:
        cache = _get_mem_cache()
        kept = {}
        for bag_id, entry in cache.items():
            try:
                fetched = datetime.fromisoformat(entry.get("fetched_at", ""))
                if fetched.tzinfo is None:
                    fetched = IST.localize(fetched)
                if fetched >= cutoff:
                    kept[bag_id] = entry
                else:
                    removed += 1
            except (ValueError, TypeError):
                # Unparseable timestamp → keep, will retry next sweep
                kept[bag_id] = entry
        if removed:
            # Replace in-memory cache with cleaned version
            _mem_cache.clear()
            _mem_cache.update(kept)
    if removed:
        _flush_now()
        print(f"[LOCAL_STORE_GRID] Purged {removed} stale cache entry(ies)",
              flush=True)
    return removed


# ── Stats ─────────────────────────────────────────────────────────────────

def get_stats() -> dict:
    """Snapshot for the dashboard / debugging."""
    with _queue_lock:
        q = len(_load_queue())
    with _cache_lock:
        cache = _get_mem_cache()
        cached = len(cache)
        consumed = sum(1 for e in cache.values() if e.get("consumed_at"))
        unconsumed = cached - consumed
    with _inflight_lock:
        inflight = len(_inflight)
    return {
        "queue_size": q,
        "cached_total": cached,
        "cached_unconsumed": unconsumed,
        "cached_consumed": consumed,
        "inflight": inflight,
    }