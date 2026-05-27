"""
hms_sync.py — Triple-browser async Playwright HMS automation.

PATCHED VERSION — addresses recurring cache-miss + Suggester-A wedge,
PLUS the EPATHJR-4304457-1834 false-positive-stale bug in Committer.

WHAT CHANGED FROM PRIOR VERSION
================================
1. SuggesterBrowser now tracks consecutive_failures. Routing skips a
   browser once it hits DEGRADED_THRESHOLD (3) failures in a row.
   Forces is_ready=False at STUCK_THRESHOLD (5) so health watcher
   recovers it.
2. Tie-break in both _do_immediate_prefetch and get_grid_suggestion
   now alternates via self._next_route. Previously qa <= qb always
   picked A when both queues were empty (the common case), so a
   wedged A kept soaking up traffic while B sat idle.
3. _read_suggestion now closes+reopens the PAGE (in the same context)
   when Cancel put refuses to clear, instead of doing an inline
   page.reload() that sometimes preserves the broken Angular state.
4. _health_watcher runs every 10s instead of 30s and uses a shorter
   throttle interval (10s vs 30s) for browsers that are degraded but
   not fully dead. Recovery happens within seconds of detection.
5. record_failure / record_success are called on every read so the
   counter stays accurate.

6. [NEW — fixes EPATHJR-4304457-1834 type silent drops]
   _commit_one now does an ACTIVE pre-clear of page state before typing
   the bag ID. Previously STEP 0 only passively waited for stale
   "put confirmed" to disappear (up to 1.5s) and proceeded anyway.
   That left pre_has_confirm=True in many cases, which then triggered
   the "Stale confirmation" rejection branch after a genuinely
   successful commit — bag gets stuck in pending forever (soft
   cooldown, real_attempts stays 0, never abandoned, never synced).

   The new pre-clear:
     a) If "put to" or "put confirmed" / "successfully put" is visible,
        click Cancel put aggressively (5-layer fallback like Suggester).
     b) Clear the input field.
     c) Verify the page is clean (NO "put to", NO "put confirmed") via
        a tight 1.5s polling loop. If still dirty, click Cancel again.
     d) If after 2s of effort the page is still dirty, RECREATE the
        page (same trick as Suggester when Cancel wedges).

   This guarantees pre_has_confirm == False at the moment we submit,
   so the post-submit logic can trust that any "put confirmed" it sees
   is a FRESH confirmation. The complicated three-branch stale logic
   is preserved as a safety net but should now almost never fire.

   IMPORTANT: This change is ONLY in the Committer (post-trolley-put
   HMS sync). The Suggester browsers (conveyer-scan grid prefetch +
   cancel put) are UNTOUCHED — their workflow doesn't involve
   "put confirmed" detection.

ARCHITECTURE (unchanged — 3 Chromium browser contexts, all on Inbound staging put page)

  Browser #1A "Spiral Suggester A"
    - Reads green "Put to X" suggestion or "already staged" message
    - Clicks Cancel put after every read (never commits)

  Browser #1B "Spiral Suggester B"
    - Identical to #1A
    - Routing picks shorter queue; ties alternate

  Browser #2 "HMS Sync Committer" (LOCAL-FILE approach)
    - Reads pending_hms_sync.json (populated at Grid Put time)
    - Workflow: pre-clear page -> scan bag_id -> fill scan_barcode -> wait confirmed
"""

import asyncio
import json
import os
import re
import threading
import time
import logging
from datetime import datetime
from queue import Queue, Empty
from typing import Optional, Dict, List

import pytz
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger("hms_sync")
logger.setLevel(logging.INFO)
if not logger.handlers:
    _h = logging.StreamHandler()
    _h.setFormatter(logging.Formatter(
        "%(asctime)s [%(name)s] %(levelname)s: %(message)s",
        datefmt="%H:%M:%S",
    ))
    logger.addHandler(_h)


IST = pytz.timezone("Asia/Kolkata")


def _ts():
    return datetime.now(IST).strftime("%H:%M:%S.%f")[:-3]


# Configuration
HMS_URL = os.getenv("HMS_URL", "http://10.24.0.157")
HMS_FACILITY = os.getenv("HMS_FACILITY", "Motherhub_SAI_4")
HMS_HEADLESS = os.getenv("HMS_HEADLESS", "false").lower() == "true"
HMS_BROWSER_PATH = os.getenv("HMS_BROWSER_PATH", "").strip()

SUGGEST_TIMEOUT_MS = int(os.getenv("HMS_SUGGEST_TIMEOUT_MS", "2500"))
COMMIT_TIMEOUT_MS = int(os.getenv("HMS_COMMIT_TIMEOUT_MS", "3000"))

SHEET_BATCH_SIZE = 25
SHEET_BATCH_WAIT_MS = 800
MIN_API_INTERVAL = 1.0

_cred_raw = os.getenv("HMS_CREDENTIALS", "")
HMS_CRED_SLOTS = []
if _cred_raw:
    for pair in _cred_raw.split("|"):
        parts = pair.strip().split(",")
        if len(parts) == 2:
            HMS_CRED_SLOTS.append((parts[0].strip(), parts[1].strip()))

if not HMS_CRED_SLOTS:
    _u = os.getenv("HMS_USERNAME", "")
    _p = os.getenv("HMS_PASSWORD", "")
    if _u and _p:
        HMS_CRED_SLOTS.append((_u, _p))

SLOT_HOURS = 6
SLOT_START_HOUR = 6


def _get_current_slot() -> int:
    if not HMS_CRED_SLOTS:
        return 0
    hour = datetime.now(IST).hour
    shifted = (hour - SLOT_START_HOUR) % 24
    return (shifted // SLOT_HOURS) % len(HMS_CRED_SLOTS)


def _is_network_error(exc: Exception) -> bool:
    """True if exception looks like a network/timeout problem (NOT a credential issue)."""
    msg = str(exc).lower()
    network_markers = (
        "timeout", "timed out", "net::err_", "page.goto", "navigation",
        "connection refused", "connection reset", "econnreset", "econnrefused",
        "name not resolved", "dns", "unreachable",
    )
    return any(m in msg for m in network_markers)


# Live_Staging column indices (0-based) - 18-col layout
# Area_Put + Area_Put_TS inserted at index 9/10. Everything that was
# at index 9+ shifts +2.
COL_CONVEYER_ID       = 0
COL_CONVEYER_TS       = 1
COL_CNV_BAG_SCAN_TS   = 2
COL_SPIRAL_BAG_SCAN_TS = 3
COL_BAG_ID            = 4
COL_CASPER_ID         = 5
COL_GRID              = 6
COL_TROLLEY_ID        = 7
COL_GRID_BARCODE      = 8
COL_AREA_PUT          = 9    # NEW
COL_AREA_PUT_TS       = 10   # NEW
COL_TROLLEY_PUT       = 11   # was 9
COL_TROLLEY_PUT_TS    = 12   # was 10
COL_GRID_PUT          = 13   # was 11
COL_GRID_PUT_TS       = 14   # was 12
COL_HMS_SYNCED        = 15   # was 13
COL_HMS_SYNCED_TS     = 16   # was 14


class SuggestRequest:
    """A single request for grid suggestion from spiral. Awaitable result."""
    __slots__ = ("bag_id", "future", "queued_at")

    def __init__(self, bag_id: str, future: asyncio.Future):
        self.bag_id = bag_id
        self.future = future
        self.queued_at = time.time()


class HMSBrowser:
    """One Playwright browser context permanently parked on Put Item page."""

    def __init__(self, name: str):
        self.name = name
        self.context = None
        self.page = None
        self.is_ready = False
        self.is_initializing = False
        self.error = None
        self.current_slot = _get_current_slot()
        if HMS_CRED_SLOTS:
            self.user, self.password = HMS_CRED_SLOTS[self.current_slot]
        else:
            self.user, self.password = "", ""
        self.failed_slots = set()
        self.login_count = 0
        self.last_recovery = 0.0
        self.recovery_count = 0

    async def initialize(self, browser):
        self.is_initializing = True
        self.error = None
        try:
            self.context = await browser.new_context(
                viewport={"width": 1280, "height": 800},
                ignore_https_errors=True,
                user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                            "AppleWebKit/537.36 (KHTML, like Gecko) "
                            "Chrome/125.0.0.0 Safari/537.36"),
            )
            self.page = await self.context.new_page()
            self.page.set_default_timeout(15000)

            async def _dismiss(d):
                try:
                    await d.dismiss()
                except Exception:
                    pass
            self.page.on("dialog", lambda d: asyncio.ensure_future(_dismiss(d)))

            await self._do_full_login()
            self.is_ready = True
            self.is_initializing = False
            logger.info(f"[{self.name}] Ready (slot={self.current_slot}, user={self.user})")
        except Exception as e:
            self.error = str(e)
            self.is_initializing = False
            self.is_ready = False
            logger.error(f"[{self.name}] Init failed: {e}")
            if _is_network_error(e) and "INVALID_CREDENTIAL" not in str(e):
                logger.info(f"[{self.name}] Network error - will retry same slot via health watcher.")
                return
            await self._try_failover(browser)

    async def _try_failover(self, browser):
        self.failed_slots.add(self.current_slot)
        for i in range(len(HMS_CRED_SLOTS)):
            if i in self.failed_slots:
                continue
            user, pwd = HMS_CRED_SLOTS[i]
            logger.warning(f"[{self.name}] Failover -> slot {i} ({user})")
            self.current_slot = i
            self.user, self.password = user, pwd
            try:
                if self.context:
                    try:
                        await self.context.close()
                    except Exception:
                        pass
                self.context = await browser.new_context(
                    viewport={"width": 1280, "height": 800},
                    ignore_https_errors=True,
                )
                self.page = await self.context.new_page()
                self.page.set_default_timeout(15000)

                async def _dismiss(d):
                    try:
                        await d.dismiss()
                    except Exception:
                        pass
                self.page.on("dialog", lambda d: asyncio.ensure_future(_dismiss(d)))
                await self._do_full_login()
                self.is_ready = True
                self.error = None
                logger.info(f"[{self.name}] Failover SUCCESS on slot {i} ({user})")
                return
            except Exception as e:
                if _is_network_error(e) and "INVALID_CREDENTIAL" not in str(e):
                    logger.warning(f"[{self.name}] Failover slot {i} network error - aborting loop.")
                    return
                self.failed_slots.add(i)
                logger.error(f"[{self.name}] Failover slot {i} also failed: {e}")
        logger.error(f"[{self.name}] All credential slots exhausted!")
        self.error = "All credentials failed"
        self.is_ready = False

    async def _do_full_login(self):
        page = self.page
        goto_ok = False
        goto_err = None
        for attempt in range(3):
            try:
                await page.goto(HMS_URL, wait_until="domcontentloaded", timeout=30000)
                goto_ok = True
                break
            except Exception as e:
                goto_err = e
                logger.warning(f"[{self.name}] page.goto attempt {attempt + 1}/3 failed: {e}")
                if attempt < 2:
                    await asyncio.sleep(2 * (attempt + 1))

        if not goto_ok:
            raise Exception(f"page.goto timeout after 3 retries: {goto_err}")

        await asyncio.sleep(1.5)
        content = await page.content()

        if "Put Item" in content or "Scan item" in content:
            logger.info(f"[{self.name}] Already on Put Item")
            return

        if "username" in content.lower() or "login" in page.url.lower() or \
           await page.locator("input[type='password']").count() > 0:
            self.login_count += 1
            logger.info(f"[{self.name}] Login attempt #{self.login_count} as {self.user}")

            await page.wait_for_selector("input[type='text'], input[name='username']", timeout=15000)
            try:
                un_loc = page.locator("input[name='username']")
                if await un_loc.count() > 0:
                    await un_loc.fill(self.user)
                else:
                    await page.locator("input[type='text']").first.fill(self.user)
            except Exception:
                await page.locator("input[type='text']").first.fill(self.user)
            await page.locator("input[type='password']").fill(self.password)

            try:
                await page.locator("button:has-text('Submit')").first.click(timeout=5000)
            except Exception:
                await page.locator("button[type='submit']").first.click(timeout=5000)

            try:
                await page.wait_for_url("**/10.24.0.157**", timeout=20000)
            except Exception:
                await asyncio.sleep(3)

            await asyncio.sleep(2)

            try:
                bt = (await page.text_content("body") or "").lower()
                if "invalid" in bt or "incorrect" in bt or "bad credentials" in bt:
                    raise Exception(f"INVALID_CREDENTIAL: {self.user}")
            except Exception as inner_e:
                if "INVALID_CREDENTIAL" in str(inner_e):
                    raise

        await self._select_facility()
        await asyncio.sleep(1.5)
        await self._navigate_to_put_page()
        await asyncio.sleep(1.5)

        content = await page.content()
        if not ("Scan item" in content or "Put Item" in content or "Scan Item" in content):
            raise Exception("Failed to reach Put Item page after login")

        logger.info(f"[{self.name}] Parked on Put Item")

    async def _select_facility(self):
        page = self.page
        try:
            sel = page.locator("select")
            if await sel.count() > 0:
                for _ in range(15):
                    options = await sel.locator("option").all()
                    real = [o for o in options
                            if (await o.text_content() or "").strip().lower()
                                not in ("", "select facility", "select")]
                    if real:
                        break
                    await asyncio.sleep(0.5)

                options = await sel.locator("option").all()
                matched = False
                for opt in options:
                    txt = (await opt.text_content() or "")
                    if HMS_FACILITY in txt:
                        await sel.select_option(label=txt)
                        logger.info(f"[{self.name}] Facility: {txt}")
                        matched = True
                        break
                if not matched:
                    for opt in options:
                        txt = (await opt.text_content() or "")
                        if HMS_FACILITY.lower() in txt.lower():
                            await sel.select_option(label=txt)
                            logger.info(f"[{self.name}] Facility (partial): {txt}")
                            matched = True
                            break

                await asyncio.sleep(0.4)

                for label in ["Submit", "Go", "Select", "OK", "Proceed", "Enter"]:
                    try:
                        b = page.locator(f"button:has-text('{label}')").first
                        if await b.count() > 0:
                            await b.click(timeout=3000)
                            logger.info(f"[{self.name}] Facility confirmed via '{label}'")
                            await asyncio.sleep(2)
                            return
                    except Exception:
                        pass

                try:
                    btns = page.locator("button")
                    cnt = await btns.count()
                    for i in range(cnt):
                        b = btns.nth(i)
                        txt = (await b.inner_text() or "").strip()
                        if txt and txt.lower() not in ("logout", "cancel", "close"):
                            await b.click(timeout=3000)
                            logger.info(f"[{self.name}] Facility confirmed via '{txt}'")
                            await asyncio.sleep(2)
                            return
                except Exception:
                    pass
                return
        except Exception as e:
            logger.warning(f"[{self.name}] Facility selection failed: {e}")
            raise

    async def _navigate_to_put_page(self):
        page = self.page
        try:
            clicked = await page.evaluate("""() => {
                var links = document.querySelectorAll('a, button, div[routerlink], mat-card, mat-list-item');
                var puts = [];
                for (var i = 0; i < links.length; i++) {
                    var own = (links[i].innerText || '').trim();
                    if (own === 'Put' || own === '+ Put') puts.push(links[i]);
                }
                if (puts.length > 0) {
                    puts[puts.length - 1].click();
                    return true;
                }
                return false;
            }""")
            if clicked:
                await asyncio.sleep(3)
                c = await page.content()
                if "Put Item" in c or "Scan item" in c or "Scan Item" in c:
                    return
        except Exception:
            pass

        for route in ["/operation#/home1", "/operation#/put", "/operation#/outbound/put", "/#/home1"]:
            try:
                base = HMS_URL.rstrip("/")
                await page.goto(base + route, wait_until="domcontentloaded", timeout=15000)
                await asyncio.sleep(2)
                c = await page.content()
                if "Put Item" in c or "Scan item" in c or "Scan Item" in c:
                    return
            except Exception:
                pass

        try:
            opened = await page.evaluate("""() => {
                var candidates = [];
                document.querySelectorAll('mat-icon').forEach(function (el) {
                    var t = (el.innerText || '').trim().toLowerCase();
                    if (t === 'menu') candidates.push(el);
                });
                document.querySelectorAll('button, [role="button"], a').forEach(function (el) {
                    var aria = (el.getAttribute('aria-label') || '').toLowerCase();
                    var title = (el.getAttribute('title') || '').toLowerCase();
                    if (aria.indexOf('menu') !== -1 || title.indexOf('menu') !== -1) {
                        candidates.push(el);
                    }
                });
                if (candidates.length === 0) return false;
                candidates[0].click();
                return true;
            }""")
            if opened:
                await asyncio.sleep(1)
                await page.evaluate("""() => {
                    var all = document.querySelectorAll('*');
                    for (var i = 0; i < all.length; i++) {
                        var own = (all[i].innerText || '').trim();
                        if (own === 'Outbound staging') { all[i].click(); return; }
                    }
                }""")
                await asyncio.sleep(1)
                await page.evaluate("""() => {
                    var all = document.querySelectorAll('*');
                    for (var i = 0; i < all.length; i++) {
                        var own = (all[i].innerText || '').trim();
                        if (own === '+ Put' || own === 'Put') { all[i].click(); return; }
                    }
                }""")
                await asyncio.sleep(2)
        except Exception as e:
            logger.debug(f"[{self.name}] Hamburger nav skipped: {e}")

    async def _on_put_page(self) -> bool:
        try:
            url = (self.page.url or "").lower()
            if "operation" not in url:
                return False
            cnt = await self.page.locator("input[type='text']").count()
            return cnt > 0
        except Exception:
            return False

    async def ensure_ready(self, browser) -> bool:
        if await self._on_put_page():
            return True
        logger.warning(f"[{self.name}] Not on Put Item - recovering")
        try:
            await self._navigate_to_put_page()
            await asyncio.sleep(1)
            if await self._on_put_page():
                return True
        except Exception:
            pass
        try:
            await self.page.reload(wait_until="domcontentloaded", timeout=15000)
            await asyncio.sleep(2)
            content = await self.page.content()
            if "username" in content.lower() or await self.page.locator("input[type='password']").count() > 0:
                logger.warning(f"[{self.name}] Session expired - re-logging in")
                await self._do_full_login()
                return True
            if await self._on_put_page():
                return True
            await self._select_facility()
            await asyncio.sleep(1)
            await self._navigate_to_put_page()
            return await self._on_put_page()
        except Exception as e:
            logger.error(f"[{self.name}] ensure_ready failed: {e}")
            self.is_ready = False
            return False

    async def close(self):
        if self.context:
            try:
                await self.context.close()
            except Exception:
                pass
        self.context = None
        self.page = None
        self.is_ready = False


# ===========================================================================
# SuggesterBrowser - with health tracking [UNCHANGED FROM PRIOR PATCH]
# ===========================================================================

class SuggesterBrowser(HMSBrowser):
    """Browser #1A or #1B - reads grid suggestions and clicks Cancel put.

    HEALTH TRACKING
    ---------------
    Each browser tracks consecutive_failures: count of back-to-back timeouts
    or "Cancel put didn't clear" errors. Reset on first successful read.

    DEGRADED_THRESHOLD (3): is_degraded() returns True -> routing skips this
      browser when a healthy alternative exists.
    STUCK_THRESHOLD (5): force is_ready=False -> health watcher recovers it.

    [IMPORTANT] This class is INTENTIONALLY UNCHANGED in this patch.
    The conveyer-scan flow (cancel put after reading grid) is a totally
    different process from the post-trolley-put HMS commit flow, and it
    does NOT involve "put confirmed" detection. Leaving it alone.
    """

    DEGRADED_THRESHOLD = 3
    STUCK_THRESHOLD    = 5

    def __init__(self, name: str):
        super().__init__(name)
        self.queue: asyncio.Queue = asyncio.Queue()
        self.processed = 0
        self.errors = 0
        self.last_busy_at = 0.0
        self.consecutive_failures = 0
        self.last_failure_at = 0.0

    def is_degraded(self) -> bool:
        """Routing skips degraded browsers when a healthy alternative exists."""
        return self.consecutive_failures >= self.DEGRADED_THRESHOLD

    def record_failure(self, reason: str = ""):
        """Increment consecutive failures; force-degrade if STUCK_THRESHOLD."""
        self.consecutive_failures += 1
        self.last_failure_at = time.time()
        logger.warning(f"[{self.name}] failure #{self.consecutive_failures} "
                       f"({reason})")
        if self.consecutive_failures >= self.STUCK_THRESHOLD:
            logger.error(f"[{self.name}] STUCK after "
                         f"{self.consecutive_failures} consecutive failures "
                         f"- flagging is_ready=False")
            self.is_ready = False

    def record_success(self):
        """Reset failure counter on any clean read."""
        if self.consecutive_failures > 0:
            logger.info(f"[{self.name}] Recovered after "
                        f"{self.consecutive_failures} failure(s)")
        self.consecutive_failures = 0

    async def run_loop(self, browser):
        while True:
            try:
                req: SuggestRequest = await self.queue.get()
            except Exception:
                await asyncio.sleep(0.1)
                continue
            self.last_busy_at = time.time()
            try:
                if not self.is_ready:
                    if not req.future.done():
                        req.future.set_result({
                            "ok": False,
                            "reason": f"{self.name} not ready: {self.error or 'unknown'}",
                            "grid": "",
                            "already_staged": False,
                        })
                    continue

                ok = await self.ensure_ready(browser)
                if not ok:
                    if not req.future.done():
                        req.future.set_result({
                            "ok": False,
                            "reason": "Not on Put Item page",
                            "grid": "",
                            "already_staged": False,
                        })
                    continue

                result = await self._read_suggestion(req.bag_id)
                if not req.future.done():
                    req.future.set_result(result)
                self.processed += 1
            except Exception as e:
                logger.error(f"[{self.name}] suggest error for {req.bag_id}: {e}")
                self.errors += 1
                self.record_failure(f"loop crash: {e}")
                if not req.future.done():
                    req.future.set_result({
                        "ok": False,
                        "reason": f"Internal error: {e}",
                        "grid": "",
                        "already_staged": False,
                    })

    async def _read_suggestion(self, bag_id: str) -> dict:
        """Type bag_id, wait for green box, extract grid, click Cancel put.

        [UNCHANGED FROM PRIOR PATCH] On stuck Cancel put, closes+reopens
        the PAGE instead of inline reload.

        This is the CONVEYER scan flow — it has nothing to do with
        "put confirmed". Do NOT touch this for the EPATHJR fix.
        """
        page = self.page
        t0 = time.time()
        bag_id = str(bag_id).strip().upper()

        REJECT_KEYWORDS = (
            "incorrect ba", "incorrect barcode", "not found", "not allowed",
            "not expected", "does not belong", "wrong barcode",
            "invalid barcode", "invalid item",
        )
        TERMINAL_KEYWORDS = ("put to", "already staged") + REJECT_KEYWORDS

        try:
            inp = page.locator("input[type='text']").first
            try:
                await inp.fill("", timeout=2000)
            except Exception:
                pass
            await inp.fill(bag_id, timeout=2000)
            await inp.press("Enter")

            text = ""
            deadline = time.time() + (SUGGEST_TIMEOUT_MS / 1000.0)
            while time.time() < deadline:
                try:
                    text = await page.evaluate(
                        "() => (document.body && document.body.innerText || '').toLowerCase()"
                    )
                except Exception:
                    text = ""
                if any(kw in text for kw in TERMINAL_KEYWORDS):
                    break
                await asyncio.sleep(0.05)

            elapsed_ms = int((time.time() - t0) * 1000)

            # CASE 1: Already staged
            if "already staged" in text:
                m = re.search(r"in grid:\s*([^\s\n]+)", text)
                grid = m.group(1).upper() if m else ""
                try:
                    await page.locator("input[type='text']").first.fill("", timeout=1000)
                except Exception:
                    pass
                if grid:
                    try:
                        from local_store_grid import store_grid as _store_grid_fast
                        _store_grid_fast(
                            bag_id, grid,
                            already_staged=True,
                            source=f"{self.name}_read",
                        )
                    except Exception as _store_e:
                        logger.error(f"[{self.name}] direct-store failed "
                                     f"for {bag_id}: {_store_e}")
                self.record_success()
                logger.info(f"[{self.name}] {bag_id} -> already_staged in {grid} ({elapsed_ms}ms)")
                return {
                    "ok": True, "grid": grid,
                    "already_staged": True, "reason": "",
                }

            # CASE 2: Green "Put to X" suggestion
            if "put to" in text:
                try:
                    raw = await page.evaluate(
                        "() => (document.body && document.body.innerText || '')"
                    )
                except Exception:
                    raw = ""
                m = re.search(r"[Pp]ut to\s+([^\s\n]+)", raw)
                grid = m.group(1).strip() if m else ""

                if grid:
                    try:
                        from local_store_grid import store_grid as _store_grid_fast
                        _store_grid_fast(
                            bag_id, grid,
                            already_staged=False,
                            source=f"{self.name}_read",
                        )
                    except Exception as _store_e:
                        logger.error(f"[{self.name}] direct-store failed "
                                     f"for {bag_id}: {_store_e}")

                # Click Cancel put - 5-layer fallback
                clicked = False
                try:
                    clicked = await page.evaluate("""() => {
                        var btns = document.querySelectorAll('button');
                        for (var i = 0; i < btns.length; i++) {
                            var t = (btns[i].innerText || '').trim().toLowerCase();
                            if (t === 'cancel put' || t === 'cancel') {
                                btns[i].click();
                                return true;
                            }
                        }
                        return false;
                    }""")
                except Exception:
                    pass

                if not clicked:
                    try:
                        clicked = await page.evaluate("""() => {
                            var matBtns = document.querySelectorAll(
                                'button[mat-button], button[mat-raised-button], button[mat-flat-button], ' +
                                'button[mat-stroked-button], button.mat-button, button.mat-raised-button, ' +
                                'button.mat-mdc-button, button.mat-mdc-raised-button, ' +
                                'a[mat-button], a[mat-raised-button]'
                            );
                            for (var i = 0; i < matBtns.length; i++) {
                                var t = (matBtns[i].innerText || matBtns[i].textContent || '').trim().toLowerCase();
                                if (t.indexOf('cancel') !== -1) {
                                    matBtns[i].click();
                                    return true;
                                }
                            }
                            var spans = document.querySelectorAll('button span, button .mat-button-wrapper, button .mdc-button__label');
                            for (var j = 0; j < spans.length; j++) {
                                var st = (spans[j].innerText || spans[j].textContent || '').trim().toLowerCase();
                                if (st === 'cancel put' || st === 'cancel') {
                                    var btn = spans[j].closest('button');
                                    if (btn) { btn.click(); return true; }
                                }
                            }
                            return false;
                        }""")
                    except Exception:
                        pass

                if not clicked:
                    try:
                        await page.locator("button:has-text('Cancel put')").first.click(timeout=1500)
                        clicked = True
                    except Exception:
                        pass

                if not clicked:
                    try:
                        await page.locator("button:has-text('Cancel')").first.click(timeout=1000)
                        clicked = True
                    except Exception:
                        pass

                if not clicked:
                    try:
                        await page.locator("input[type='text']").first.fill("", timeout=1000)
                        await asyncio.sleep(0.2)
                    except Exception:
                        pass

                # VERIFY cancel landed
                verify_deadline = time.time() + 3.0
                page_ok = False
                retry_click_done = False
                while time.time() < verify_deadline:
                    try:
                        verify_text = await page.evaluate(
                            "() => (document.body && document.body.innerText || '').toLowerCase()"
                        )
                    except Exception:
                        verify_text = ""
                    if "put to" not in verify_text:
                        page_ok = True
                        break
                    if not retry_click_done and (time.time() > verify_deadline - 1.5):
                        retry_click_done = True
                        try:
                            await page.evaluate("""() => {
                                var all = document.querySelectorAll('button, a, [role="button"]');
                                for (var i = 0; i < all.length; i++) {
                                    var t = (all[i].innerText || all[i].textContent || '').trim().toLowerCase();
                                    if (t.indexOf('cancel') !== -1) {
                                        all[i].click();
                                        return;
                                    }
                                }
                            }""")
                        except Exception:
                            pass
                    await asyncio.sleep(0.05)

                if not page_ok:
                    logger.warning(f"[{self.name}] Cancel put didn't clear - "
                                   f"recreating page (full DOM reset)")
                    self.record_failure("cancel stuck")
                    try:
                        old_page = self.page
                        new_page = await self.context.new_page()
                        new_page.set_default_timeout(15000)

                        async def _dismiss(d):
                            try:
                                await d.dismiss()
                            except Exception:
                                pass
                        new_page.on("dialog",
                                    lambda d: asyncio.ensure_future(_dismiss(d)))

                        self.page = new_page
                        try:
                            await old_page.close()
                        except Exception:
                            pass

                        await self.page.goto(HMS_URL,
                                             wait_until="domcontentloaded",
                                             timeout=15000)
                        await asyncio.sleep(1.5)
                        content = await self.page.content()
                        if "Scan item" not in content and "Put Item" not in content:
                            if "username" in content.lower() or \
                               await self.page.locator("input[type='password']").count() > 0:
                                await self._do_full_login()
                            else:
                                await self._select_facility()
                                await asyncio.sleep(1)
                                await self._navigate_to_put_page()
                    except Exception as nav_e:
                        logger.error(f"[{self.name}] Page recreate failed: {nav_e}")
                        self.is_ready = False

                self.record_success()
                logger.info(f"[{self.name}] {bag_id} -> {grid} ({elapsed_ms}ms)")
                return {
                    "ok": True, "grid": grid,
                    "already_staged": False, "reason": "",
                }

            # CASE 3: Real HMS rejection
            for kw in REJECT_KEYWORDS:
                if kw in text:
                    try:
                        await page.locator("input[type='text']").first.fill("", timeout=1000)
                    except Exception:
                        pass
                    snippet = text[:120].replace("\n", " ").strip()
                    self.record_success()
                    logger.info(f"[{self.name}] {bag_id} -> REJECT '{kw}' ({elapsed_ms}ms): {snippet}")
                    return {
                        "ok": False, "grid": "",
                        "already_staged": False,
                        "reason": f"HMS rejected: {kw}",
                    }

            # CASE 4: True timeout
            logger.warning(f"[{self.name}] {bag_id} -> no suggestion ({elapsed_ms}ms): {text[:100]}")
            self.record_failure(f"timeout reading {bag_id}")
            try:
                await page.locator("input[type='text']").first.fill("", timeout=1000)
            except Exception:
                pass
            return {
                "ok": False, "grid": "",
                "already_staged": False,
                "reason": "No suggestion from HMS (timeout)",
            }
        except Exception as e:
            elapsed_ms = int((time.time() - t0) * 1000)
            logger.error(f"[{self.name}] _read_suggestion crashed at {elapsed_ms}ms: {e}")
            self.record_failure(f"crash: {e}")
            self.is_ready = False
            return {
                "ok": False, "grid": "",
                "already_staged": False,
                "reason": f"Browser error: {e}",
            }

    def queue_size(self) -> int:
        return self.queue.qsize()


# ===========================================================================
# CommitterBrowser - [PATCHED] active pre-clear before every commit
# ===========================================================================

class CommitterBrowser(HMSBrowser):
    """Browser #2 - commits Grid_Put bags to HMS.

    [PATCHED — EPATHJR FIX]
    -----------------------
    Before typing any bag ID, _commit_one now does an ACTIVE pre-clear:
      1. Detect any stale "put to X" or "put confirmed" state.
      2. Click Cancel put aggressively if present.
      3. Clear the input field.
      4. Verify the page is CLEAN (NO "put to", NO "put confirmed").
      5. If verification fails after 2s, RECREATE the page.

    This guarantees pre_has_confirm == False at submit time, so the
    post-submit logic can trust any "put confirmed" it sees is FRESH.
    Adds ~100ms in the clean case, ~500ms-1.5s when actually stale.
    """

    SOFT_COOLDOWN_SEC = 10

    # How long we'll spend trying to make the page clean before giving up
    # and recreating it. Keep this tight — we want speed.
    PRECLEAR_MAX_MS = 2000

    def __init__(self, name: str = "Committer"):
        super().__init__(name)
        self.synced_count = 0
        self.failed_count = 0
        self._soft_cooldowns: Dict[str, float] = {}

    async def run_loop(self, browser, on_synced_cb, on_failed_cb):
        from local_store_hms import (
            get_all_pending, record_real_attempt, abandon_bag,
            MAX_REAL_ATTEMPTS,
        )

        while True:
            try:
                if not self.is_ready:
                    await asyncio.sleep(2)
                    continue

                pending = get_all_pending()
                if not pending:
                    await asyncio.sleep(1.0)
                    continue

                ok = await self.ensure_ready(browser)
                if not ok:
                    await asyncio.sleep(2)
                    continue

                now = time.time()
                processed = 0

                for rec in pending:
                    if processed >= 5:
                        break

                    bag_id       = rec.get("bag_id", "")
                    area_barcode = rec.get("area_barcode", "")
                    trolley_id   = rec.get("trolley_id", "")
                    sheet_row    = rec.get("sheet_row", 0)
                    real_attempts = rec.get("real_attempts", 0)

                    if not bag_id or not area_barcode:
                        continue

                    cooldown_until = self._soft_cooldowns.get(bag_id, 0)
                    if now < cooldown_until:
                        continue

                    if real_attempts >= MAX_REAL_ATTEMPTS:
                        abandon_bag(bag_id, trolley_id,
                                    f"Exceeded {MAX_REAL_ATTEMPTS} real HMS rejections")
                        on_failed_cb(bag_id, trolley_id, sheet_row,
                                     f"Abandoned after {MAX_REAL_ATTEMPTS} rejections",
                                     True)
                        processed += 1
                        continue

                    try:
                        result = await self._commit_one(bag_id, area_barcode)
                    except Exception as e:
                        logger.error(f"[{self.name}] commit crashed for {bag_id}: {e}")
                        result = {"ok": False, "reason": f"crash: {e}",
                                  "real_rejection": False}

                    if result["ok"]:
                        self.synced_count += 1
                        self._soft_cooldowns.pop(bag_id, None)
                        on_synced_cb(bag_id, trolley_id, sheet_row,
                                     result.get("reason", ""))
                    else:
                        self.failed_count += 1
                        if result.get("real_rejection"):
                            new_count = record_real_attempt(
                                bag_id, trolley_id,
                                result.get("reason", ""))
                            if new_count >= MAX_REAL_ATTEMPTS:
                                abandon_bag(bag_id, trolley_id,
                                            result.get("reason", ""))
                                on_failed_cb(bag_id, trolley_id, sheet_row,
                                             result.get("reason", ""), True)
                            else:
                                remaining = MAX_REAL_ATTEMPTS - new_count
                                logger.warning(
                                    f"[{self.name}] {bag_id} real attempt "
                                    f"{new_count}/{MAX_REAL_ATTEMPTS}: "
                                    f"{result.get('reason','')} "
                                    f"({remaining} left)")
                        else:
                            self._soft_cooldowns[bag_id] = now + self.SOFT_COOLDOWN_SEC
                            logger.warning(
                                f"[{self.name}] {bag_id} soft fail: "
                                f"{result.get('reason','')} "
                                f"(retry in {self.SOFT_COOLDOWN_SEC}s)")

                    processed += 1

                if len(self._soft_cooldowns) > 200:
                    cutoff = now - 300
                    self._soft_cooldowns = {
                        k: v for k, v in self._soft_cooldowns.items()
                        if v > cutoff
                    }

                if not processed:
                    await asyncio.sleep(1.0)

            except Exception as e:
                logger.error(f"[{self.name}] run_loop error: {e}")
                await asyncio.sleep(2)

    # ────────────────────────────────────────────────────────────
    # NEW: Active pre-clear helper
    # ────────────────────────────────────────────────────────────
    async def _click_any_cancel(self) -> bool:
        """Aggressively try to click any 'Cancel' / 'Cancel put' button.
        Same 5-layer fallback as Suggester. Returns True if a click landed.
        """
        page = self.page

        # Layer 1: plain button text match
        try:
            clicked = await page.evaluate("""() => {
                var btns = document.querySelectorAll('button');
                for (var i = 0; i < btns.length; i++) {
                    var t = (btns[i].innerText || '').trim().toLowerCase();
                    if (t === 'cancel put' || t === 'cancel') {
                        btns[i].click();
                        return true;
                    }
                }
                return false;
            }""")
            if clicked:
                return True
        except Exception:
            pass

        # Layer 2: Material buttons + span labels
        try:
            clicked = await page.evaluate("""() => {
                var matBtns = document.querySelectorAll(
                    'button[mat-button], button[mat-raised-button], button[mat-flat-button], ' +
                    'button[mat-stroked-button], button.mat-button, button.mat-raised-button, ' +
                    'button.mat-mdc-button, button.mat-mdc-raised-button, ' +
                    'a[mat-button], a[mat-raised-button]'
                );
                for (var i = 0; i < matBtns.length; i++) {
                    var t = (matBtns[i].innerText || matBtns[i].textContent || '').trim().toLowerCase();
                    if (t.indexOf('cancel') !== -1) {
                        matBtns[i].click();
                        return true;
                    }
                }
                var spans = document.querySelectorAll('button span, button .mat-button-wrapper, button .mdc-button__label');
                for (var j = 0; j < spans.length; j++) {
                    var st = (spans[j].innerText || spans[j].textContent || '').trim().toLowerCase();
                    if (st === 'cancel put' || st === 'cancel') {
                        var btn = spans[j].closest('button');
                        if (btn) { btn.click(); return true; }
                    }
                }
                return false;
            }""")
            if clicked:
                return True
        except Exception:
            pass

        # Layer 3-4: Playwright locator fallbacks
        try:
            await page.locator("button:has-text('Cancel put')").first.click(timeout=1000)
            return True
        except Exception:
            pass
        try:
            await page.locator("button:has-text('Cancel')").first.click(timeout=800)
            return True
        except Exception:
            pass
        return False

    async def _preclear_page(self, bag_id: str) -> bool:
        """Make the page CLEAN before typing a new bag ID.

        Clean = NO 'put to', NO 'put confirmed', NO 'successfully put'
        visible in body text. Input field empty.

        Returns True if the page is clean. Returns False if it can't be
        made clean even after recreating the page (caller should fail
        this commit and let health watcher recover).

        Fast path: if the page is already clean, returns in ~50-100ms
        (single page.evaluate + one fill("")).

        Slow path: actively clicks Cancel, polls until clean, retries
        once, then recreates the page. Hard ceiling: PRECLEAR_MAX_MS.
        """
        page = self.page
        DIRTY_KEYWORDS = ("put to", "put confirmed", "successfully put")

        # Quick check first
        try:
            text = await page.evaluate(
                "() => (document.body && document.body.innerText || '').toLowerCase()"
            )
        except Exception:
            text = ""

        if not any(kw in text for kw in DIRTY_KEYWORDS):
            # Already clean — just make sure input is empty
            try:
                await page.locator("input[type='text']").first.fill("", timeout=1000)
            except Exception:
                pass
            return True

        # Page is dirty — we need to clean it
        logger.info(f"[{self.name}] {bag_id}: pre-clearing stale state "
                    f"({[kw for kw in DIRTY_KEYWORDS if kw in text]})")

        deadline = time.time() + (self.PRECLEAR_MAX_MS / 1000.0)
        cancel_attempts = 0

        while time.time() < deadline:
            # If "put to" is present, we need to Cancel put
            if "put to" in text:
                cancel_attempts += 1
                await self._click_any_cancel()
                await asyncio.sleep(0.15)
            else:
                # No "put to" but "put confirmed" / "successfully put" is
                # there as a stale toast. Try to dismiss by clearing the
                # input and waiting a moment for the toast to auto-fade.
                try:
                    await page.locator("input[type='text']").first.fill("", timeout=800)
                except Exception:
                    pass
                await asyncio.sleep(0.2)

            # Re-check
            try:
                text = await page.evaluate(
                    "() => (document.body && document.body.innerText || '').toLowerCase()"
                )
            except Exception:
                text = ""

            if not any(kw in text for kw in DIRTY_KEYWORDS):
                # Clean! Make sure input is empty and return success
                try:
                    await page.locator("input[type='text']").first.fill("", timeout=1000)
                except Exception:
                    pass
                logger.info(f"[{self.name}] {bag_id}: pre-clear OK "
                            f"({cancel_attempts} cancel click(s))")
                return True

        # Hit the deadline — page won't clean up. Recreate it.
        logger.warning(f"[{self.name}] {bag_id}: pre-clear timeout, "
                       f"recreating page")
        try:
            old_page = self.page
            new_page = await self.context.new_page()
            new_page.set_default_timeout(15000)

            async def _dismiss(d):
                try:
                    await d.dismiss()
                except Exception:
                    pass
            new_page.on("dialog",
                        lambda d: asyncio.ensure_future(_dismiss(d)))

            self.page = new_page
            try:
                await old_page.close()
            except Exception:
                pass

            await self.page.goto(HMS_URL,
                                 wait_until="domcontentloaded",
                                 timeout=15000)
            await asyncio.sleep(1.5)
            content = await self.page.content()
            if "Scan item" not in content and "Put Item" not in content:
                if "username" in content.lower() or \
                   await self.page.locator("input[type='password']").count() > 0:
                    await self._do_full_login()
                else:
                    await self._select_facility()
                    await asyncio.sleep(1)
                    await self._navigate_to_put_page()

            # Verify the new page is clean
            try:
                text = await self.page.evaluate(
                    "() => (document.body && document.body.innerText || '').toLowerCase()"
                )
            except Exception:
                text = ""
            if not any(kw in text for kw in DIRTY_KEYWORDS):
                logger.info(f"[{self.name}] {bag_id}: page recreated, clean")
                return True
            else:
                logger.error(f"[{self.name}] {bag_id}: page recreated but "
                             f"still dirty?! flagging not ready")
                self.is_ready = False
                return False
        except Exception as nav_e:
            logger.error(f"[{self.name}] {bag_id}: page recreate failed: {nav_e}")
            self.is_ready = False
            return False

    async def _commit_one(self, bag_id: str, area_barcode: str) -> dict:
        page = self.page
        t0 = time.time()
        bag_id = str(bag_id).strip().upper()

        STEP1_REJECT_KEYWORDS = (
            "incorrect ba", "incorrect barcode", "not found",
            "not allowed", "not expected", "does not belong",
            "wrong barcode", "invalid barcode", "invalid item",
        )
        STEP1_TERMINAL = ("put to", "already staged") + STEP1_REJECT_KEYWORDS

        try:
            # ═══════════════════════════════════════════════════════════
            # STEP 0 [PATCHED]: ACTIVE pre-clear of page state
            # ═══════════════════════════════════════════════════════════
            # Guarantees pre_has_confirm == False before we submit, so
            # the post-submit branch logic can trust any "put confirmed"
            # it sees is FRESH (not leftover from previous bag).
            #
            # Replaces the old passive STEP 0 which only waited up to
            # 1.5s for stale state to disappear on its own (it usually
            # didn't), then proceeded anyway with pre_has_confirm=True,
            # which then misfired the "Stale confirmation" branch and
            # silently dropped genuine successes.
            preclear_ok = await self._preclear_page(bag_id)
            if not preclear_ok:
                return {"ok": False,
                        "reason": "Could not clear stale page state",
                        "real_rejection": False}
            # page is now clean — re-fetch self.page in case it was recreated
            page = self.page
            # ═══════════════════════════════════════════════════════════

            # STEP 1: Submit the bag ID
            inp = page.locator("input[type='text']").first
            try:
                await inp.fill("", timeout=2000)
            except Exception:
                pass
            await inp.fill(bag_id, timeout=2000)
            await inp.press("Enter")

            text = ""
            deadline = time.time() + (SUGGEST_TIMEOUT_MS / 1000.0)
            while time.time() < deadline:
                try:
                    text = await page.evaluate(
                        "() => (document.body && document.body.innerText || '').toLowerCase()"
                    )
                except Exception:
                    text = ""
                if any(kw in text for kw in STEP1_TERMINAL):
                    break
                await asyncio.sleep(0.05)

            if "already staged" in text:
                try:
                    await page.locator("input[type='text']").first.fill("", timeout=1000)
                except Exception:
                    pass
                elapsed = int((time.time() - t0) * 1000)
                logger.info(f"[Committer] {bag_id} ALREADY_STAGED ({elapsed}ms)")
                return {"ok": True, "reason": "already_staged", "real_rejection": False}

            for kw in STEP1_REJECT_KEYWORDS:
                if kw in text:
                    elapsed = int((time.time() - t0) * 1000)
                    snippet = text[:120].replace("\n", " ").strip()
                    logger.warning(f"[Committer] {bag_id} -> REJECT '{kw}' ({elapsed}ms): {snippet}")
                    try:
                        await page.locator("input[type='text']").first.fill("", timeout=1000)
                    except Exception:
                        pass
                    return {"ok": False, "reason": f"HMS rejected: {kw}",
                            "real_rejection": True}

            if "put to" not in text:
                try:
                    await page.locator("input[type='text']").first.press("Enter")
                except Exception:
                    pass
                await asyncio.sleep(0.5)
                try:
                    text = await page.evaluate(
                        "() => (document.body && document.body.innerText || '').toLowerCase()"
                    )
                except Exception:
                    text = ""
                if "already staged" in text:
                    try:
                        await page.locator("input[type='text']").first.fill("", timeout=1000)
                    except Exception:
                        pass
                    return {"ok": True, "reason": "already_staged", "real_rejection": False}
                for kw in STEP1_REJECT_KEYWORDS:
                    if kw in text:
                        try:
                            await page.locator("input[type='text']").first.fill("", timeout=1000)
                        except Exception:
                            pass
                        return {"ok": False, "reason": f"HMS rejected: {kw}",
                                "real_rejection": True}
                if "put to" not in text:
                    err_msg = text[:120].replace("\n", " ").strip()
                    try:
                        await page.locator("input[type='text']").first.fill("", timeout=1000)
                    except Exception:
                        pass
                    return {"ok": False, "reason": f"No suggestion: {err_msg}",
                            "real_rejection": False}

            # STEP 2: Fill the area barcode into the scan input.
            #
            # HMS SEQUENTIAL WORKFLOW: The HMS page shows inputs ONE AT A TIME.
            # After step 1 (bag ID entered, "Put to X" shown), the page
            # transitions and presents a SINGLE input for the area barcode.

            # Wait for page to stabilize after step 1 "Put to" appeared
            await asyncio.sleep(1.0)

            try:
                bc = None
                bc_deadline = time.time() + 5.0
                while time.time() < bc_deadline:
                    all_inputs = page.locator("input[type='text']")
                    input_count = await all_inputs.count()
                    if input_count >= 2:
                        bc = all_inputs.nth(1)
                        logger.info(
                            f"[Committer] {bag_id}: found {input_count} inputs, "
                            f"using index 1 for barcode")
                        break
                    elif input_count == 1:
                        check_text = ""
                        try:
                            check_text = await page.evaluate(
                                "() => (document.body && document.body.innerText || '').toLowerCase()"
                            )
                        except Exception:
                            check_text = ""
                        if "put to" in check_text:
                            bc = all_inputs.first
                            logger.info(
                                f"[Committer] {bag_id}: sequential mode — "
                                f"1 input visible with 'Put to' shown, "
                                f"using it for barcode")
                            break
                    await asyncio.sleep(0.3)

                if bc is None:
                    logger.error(
                        f"[Committer] {bag_id}: ABORTING — could not find "
                        f"barcode input within 5s. Will retry.")
                    try:
                        await page.locator("input[type='text']").first.fill("", timeout=1000)
                    except Exception:
                        pass
                    return {"ok": False,
                            "reason": "No barcode input found after step 1",
                            "real_rejection": False}

                await bc.fill("", timeout=2000)
                await bc.fill(area_barcode, timeout=2000)
            except Exception as e:
                logger.error(
                    f"[Committer] {bag_id}: barcode input failed: {e}")
                return {"ok": False, "reason": f"barcode input: {e}",
                        "real_rejection": False}

            # ═══════════════════════════════════════════════════════════
            # POST-SUBMIT CONFIRMATION CHECK
            # ═══════════════════════════════════════════════════════════
            # Because STEP 0 above guarantees the page was CLEAN of any
            # "put confirmed" / "successfully put" text right before we
            # typed the bag ID (and the page transitioned through the
            # "put to" intermediate state), pre_has_confirm is now
            # reliably False.
            #
            # That said, we still capture the text right before submit
            # and keep the three-branch stale-detection logic as a
            # safety net (in case some other UI element with the word
            # "confirmed" appears between the pre-clear and submit).
            # In normal operation pre_has_confirm should ALWAYS be False
            # here.
            pre_submit_text = ""
            try:
                pre_submit_text = await page.evaluate(
                    "() => (document.body && document.body.innerText || '').toLowerCase()"
                )
            except Exception:
                pre_submit_text = ""

            CONFIRM_KEYWORDS = ("put confirmed", "successfully put")
            FAIL_KEYWORDS = ("incorrect barcode", "wrong barcode",
                             "invalid barcode", "not found",
                             "not expected", "does not belong")

            pre_has_confirm = any(kw in pre_submit_text for kw in CONFIRM_KEYWORDS)
            if pre_has_confirm:
                # This should now be RARE thanks to STEP 0. Log it so we
                # can monitor whether the pre-clear is working.
                logger.warning(
                    f"[Committer] {bag_id}: UNEXPECTED pre-submit "
                    f"confirmation text — pre-clear may have raced. "
                    f"Will require text change to accept.")

            try:
                await bc.press("Enter")
                logger.info(
                    f"[Committer] {bag_id}: barcode '{area_barcode}' submitted")
            except Exception as e:
                logger.error(
                    f"[Committer] {bag_id}: barcode Enter failed: {e}")
                return {"ok": False, "reason": f"barcode enter: {e}",
                        "real_rejection": False}

            text = ""
            deadline = time.time() + (COMMIT_TIMEOUT_MS / 1000.0)
            while time.time() < deadline:
                try:
                    text = await page.evaluate(
                        "() => (document.body && document.body.innerText || '').toLowerCase()"
                    )
                except Exception:
                    text = ""
                has_confirm_now = any(kw in text for kw in CONFIRM_KEYWORDS)
                if has_confirm_now and not pre_has_confirm:
                    break
                if has_confirm_now and pre_has_confirm:
                    if "put to" not in text:
                        break
                if any(kw in text for kw in FAIL_KEYWORDS):
                    break
                if ("scan item" in text and "put to" not in text
                        and not has_confirm_now):
                    break
                await asyncio.sleep(0.05)

            elapsed_ms = int((time.time() - t0) * 1000)
            page_snippet = text[:200].replace("\n", " ").strip()
            logger.info(
                f"[Committer] {bag_id} step2 result ({elapsed_ms}ms): "
                f"{page_snippet}")

            has_confirm_final = any(kw in text for kw in CONFIRM_KEYWORDS)

            # SUCCESS PATH — now simplified because STEP 0 makes
            # pre_has_confirm reliably False.
            if has_confirm_final:
                if not pre_has_confirm:
                    # Clean case (the normal one now)
                    logger.info(f"[Committer] {bag_id} -> CONFIRMED ({elapsed_ms}ms)")
                    return {"ok": True, "reason": "confirmed", "real_rejection": False}
                elif "put to" not in text:
                    # Stale-but-transitioned: page went through confirm
                    # cycle even though we already had stale text. This
                    # is still a genuine success.
                    logger.info(
                        f"[Committer] {bag_id} -> CONFIRMED (post-stale-cleared, "
                        f"{elapsed_ms}ms)")
                    return {"ok": True, "reason": "confirmed", "real_rejection": False}
                else:
                    # Genuinely ambiguous: stale confirm text + page
                    # still shows "put to". With STEP 0 in place this
                    # should be VERY rare. Don't mark as synced — but
                    # also don't penalize the bag (real_rejection=False
                    # so it just retries after cooldown).
                    logger.error(
                        f"[Committer] {bag_id} -> STALE 'put confirmed' "
                        f"with 'put to' still present — NOT marking synced "
                        f"({elapsed_ms}ms). This indicates STEP 0 pre-clear "
                        f"raced with another UI element.")
                    try:
                        await self._click_any_cancel()
                    except Exception:
                        pass
                    return {"ok": False,
                            "reason": "Stale confirmation text (not a real confirm)",
                            "real_rejection": False}

            # Page reset to scan WITHOUT explicit confirmation — NOT a success.
            if ("scan item" in text and "put to" not in text and
                    "error" not in text and "incorrect" not in text):
                logger.warning(
                    f"[Committer] {bag_id} -> page reset to scan WITHOUT "
                    f"confirmation text — NOT marking as synced ({elapsed_ms}ms)")
                return {"ok": False,
                        "reason": "Page reset without confirmation (ambiguous)",
                        "real_rejection": False}

            if "put to" in text:
                logger.warning(f"[Committer] {bag_id} -> barcode rejected ({elapsed_ms}ms)")
                try:
                    await self._click_any_cancel()
                except Exception:
                    pass
                verify_end = time.time() + 2.0
                while time.time() < verify_end:
                    try:
                        vt = await page.evaluate(
                            "() => (document.body && document.body.innerText || '').toLowerCase()"
                        )
                    except Exception:
                        vt = ""
                    if "put to" not in vt:
                        break
                    await asyncio.sleep(0.05)
                return {"ok": False, "reason": "Area barcode not accepted",
                        "real_rejection": True}

            logger.warning(f"[Committer] {bag_id} -> ambiguous ({elapsed_ms}ms): {text[:80]}")
            return {"ok": False, "reason": f"ambiguous: {text[:80]}",
                    "real_rejection": False}

        except Exception as e:
            elapsed_ms = int((time.time() - t0) * 1000)
            logger.error(f"[Committer] commit crashed for {bag_id} at {elapsed_ms}ms: {e}")
            self.is_ready = False
            return {"ok": False, "reason": f"crash: {e}", "real_rejection": False}


# ===========================================================================
# HMSManager - [UNCHANGED FROM PRIOR PATCH]
# ===========================================================================

class HMSManager:

    def __init__(self):
        self._playwright = None
        self._browser = None
        self._loop = None
        self._thread = None
        self._started = False

        self.suggester_a: Optional[SuggesterBrowser] = None
        self.suggester_b: Optional[SuggesterBrowser] = None
        self.committer: Optional[CommitterBrowser] = None

        self._next_route = "a"

        self._pending_sync_writes: List[dict] = []
        self._writes_lock = threading.Lock()
        self._writer_thread = None
        self._writer_running = False

    def _pick_suggester(self) -> Optional["SuggesterBrowser"]:
        """Pick the best suggester for a new request."""
        sa = self.suggester_a
        sb = self.suggester_b
        sa_healthy = bool(sa and sa.is_ready and not sa.is_degraded())
        sb_healthy = bool(sb and sb.is_ready and not sb.is_degraded())

        if sa_healthy and sb_healthy:
            qa, qb = sa.queue_size(), sb.queue_size()
            if qa < qb:
                return sa
            if qb < qa:
                return sb
            if self._next_route == "a":
                self._next_route = "b"
                return sa
            else:
                self._next_route = "a"
                return sb

        if sa_healthy:
            return sa
        if sb_healthy:
            return sb

        sa_ready = bool(sa and sa.is_ready)
        sb_ready = bool(sb and sb.is_ready)
        if sa_ready and sb_ready:
            if sa.consecutive_failures <= sb.consecutive_failures:
                return sa
            return sb

        if sa_ready:
            return sa
        if sb_ready:
            return sb

        return None

    def start(self):
        if self._started:
            return
        if not HMS_CRED_SLOTS:
            logger.warning("HMS_CREDENTIALS not set - HMS sync DISABLED")
            return
        self._started = True
        self._thread = threading.Thread(
            target=self._run_loop, name="hms-asyncio", daemon=True)
        self._thread.start()

        self._writer_running = True
        self._writer_thread = threading.Thread(
            target=self._sheet_writer_loop, name="hms-sheet-writer", daemon=True)
        self._writer_thread.start()

        logger.info("[HMSManager] Started (asyncio + sheet writer threads, 3 browsers)")

    def fire_prefetch_immediate(self, bag_id: str):
        if not self._loop or not self._started:
            return
        bag_id = str(bag_id).strip().upper()
        if not bag_id:
            return
        try:
            asyncio.run_coroutine_threadsafe(
                self._do_immediate_prefetch(bag_id), self._loop
            )
        except Exception as e:
            logger.debug(f"[Prefetch-Immediate] submit failed for {bag_id}: {e}")

    async def _do_immediate_prefetch(self, bag_id: str):
        from local_store_grid import (
            lookup_grid, store_grid, mark_inflight, clear_inflight,
        )

        cached = lookup_grid(bag_id)
        if cached and cached.get("grid") and not cached.get("consumed_at"):
            return

        if not mark_inflight(bag_id):
            logger.debug(f"[Prefetch-Immediate] {bag_id} already inflight - skip")
            return

        try:
            chosen = self._pick_suggester()
            if not chosen:
                logger.debug(f"[Prefetch-Immediate] no browser for {bag_id} - queue-only")
                return

            if chosen.queue_size() > 6:
                logger.debug(f"[Prefetch-Immediate] {chosen.name} backlogged - skip {bag_id}")
                return

            fut = self._loop.create_future()
            chosen.queue.put_nowait(SuggestRequest(bag_id, fut))

            try:
                result = await asyncio.wait_for(fut, timeout=10.0)
            except asyncio.TimeoutError:
                logger.warning(f"[Prefetch-Immediate] {bag_id} timed out on {chosen.name}")
                return
            except Exception as e:
                logger.warning(f"[Prefetch-Immediate] {bag_id} error: {e}")
                return

            if result.get("ok") and result.get("grid"):
                grid = result["grid"].strip().upper()
                store_grid(
                    bag_id, grid,
                    already_staged=result.get("already_staged", False),
                    source="immediate_prefetch",
                )
                logger.info(f"[Prefetch-Immediate] OK {bag_id} -> {grid} "
                            f"(browser: {chosen.name})")
            else:
                reason = result.get("reason", "unknown")
                logger.info(f"[Prefetch-Immediate] {bag_id} -> no grid: {reason}")
        finally:
            clear_inflight(bag_id)

    def get_grid_suggestion(self, bag_id: str, timeout: float = 4.0) -> dict:
        if not self._loop or not self._started:
            return {"ok": False, "grid": "", "already_staged": False,
                    "reason": "HMS not started"}

        chosen = self._pick_suggester()
        if not chosen:
            return {"ok": False, "grid": "", "already_staged": False,
                    "reason": "No suggester browser available"}

        future_holder = []

        def _submit():
            fut = self._loop.create_future()
            future_holder.append(fut)
            chosen.queue.put_nowait(SuggestRequest(bag_id, fut))

        try:
            asyncio.run_coroutine_threadsafe(
                self._submit_async(_submit), self._loop).result(timeout=1.0)
        except Exception as e:
            return {"ok": False, "grid": "", "already_staged": False,
                    "reason": f"submit error: {e}"}

        if not future_holder:
            return {"ok": False, "grid": "", "already_staged": False,
                    "reason": "submit lost"}

        try:
            result = asyncio.run_coroutine_threadsafe(
                asyncio.wait_for(self._await_future(future_holder[0]),
                                 timeout=timeout),
                self._loop,
            ).result(timeout=timeout + 1.0)
            return result
        except asyncio.TimeoutError:
            return {"ok": False, "grid": "", "already_staged": False,
                    "reason": f"Suggester timed out (>{timeout}s)"}
        except Exception as e:
            return {"ok": False, "grid": "", "already_staged": False,
                    "reason": f"Suggester error: {e}"}

    async def _submit_async(self, fn):
        fn()

    async def _await_future(self, fut):
        return await fut

    def get_status(self) -> dict:
        from local_store_hms import get_stats
        try:
            from local_store_grid import get_stats as get_grid_stats
            grid_stats = get_grid_stats()
        except Exception:
            grid_stats = {}
        c = self.committer
        out = {
            "started": self._started,
            "suggester_a": {
                "ready": self.suggester_a.is_ready if self.suggester_a else False,
                "degraded": self.suggester_a.is_degraded() if self.suggester_a else False,
                "consecutive_failures": (self.suggester_a.consecutive_failures
                                         if self.suggester_a else 0),
                "queue": self.suggester_a.queue_size() if self.suggester_a else 0,
                "user": self.suggester_a.user if self.suggester_a else "",
                "processed": self.suggester_a.processed if self.suggester_a else 0,
                "errors": self.suggester_a.errors if self.suggester_a else 0,
            },
            "suggester_b": {
                "ready": self.suggester_b.is_ready if self.suggester_b else False,
                "degraded": self.suggester_b.is_degraded() if self.suggester_b else False,
                "consecutive_failures": (self.suggester_b.consecutive_failures
                                         if self.suggester_b else 0),
                "queue": self.suggester_b.queue_size() if self.suggester_b else 0,
                "user": self.suggester_b.user if self.suggester_b else "",
                "processed": self.suggester_b.processed if self.suggester_b else 0,
                "errors": self.suggester_b.errors if self.suggester_b else 0,
            },
            "committer": {
                "ready": c.is_ready if c else False,
                "user": c.user if c else "",
                "synced": c.synced_count if c else 0,
                "failed": c.failed_count if c else 0,
            },
            "queue_stats": get_stats(),
            "grid_cache": grid_stats,
        }
        return out

    @property
    def is_ready(self) -> bool:
        return (self._started and
                self.suggester_a and self.suggester_a.is_ready and
                self.suggester_b and self.suggester_b.is_ready)

    def _run_loop(self):
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        try:
            self._loop.run_until_complete(self._async_main())
        except Exception as e:
            logger.error(f"[HMSManager] asyncio loop crashed: {e}")

    async def _async_main(self):
        from playwright.async_api import async_playwright
        self._playwright = await async_playwright().start()

        launch_args = {
            "headless": HMS_HEADLESS,
            "args": [
                "--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu",
                "--ignore-certificate-errors",
                "--disable-blink-features=AutomationControlled",
            ],
        }
        if HMS_BROWSER_PATH:
            launch_args["executable_path"] = HMS_BROWSER_PATH
        else:
            launch_args["channel"] = "chrome"

        self._browser = await self._playwright.chromium.launch(**launch_args)
        logger.info(f"[HMSManager] Chromium launched (headless={HMS_HEADLESS})")

        self.suggester_a = SuggesterBrowser("Suggester-A")
        self.suggester_b = SuggesterBrowser("Suggester-B")
        self.committer = CommitterBrowser("Committer")

        logger.info("[HMSManager] Stage 1: bringing up Suggester-A (warms HMS)")
        try:
            await self.suggester_a.initialize(self._browser)
        except Exception as e:
            logger.error(f"[HMSManager] Suggester-A init crashed: {e}")

        await asyncio.sleep(2)

        logger.info("[HMSManager] Stage 2: bringing up Suggester-B + Committer")
        await asyncio.gather(
            self.suggester_b.initialize(self._browser),
            self.committer.initialize(self._browser),
            return_exceptions=True,
        )

        logger.info(f"[HMSManager] Browser status: "
                    f"SugA={self.suggester_a.is_ready}, "
                    f"SugB={self.suggester_b.is_ready}, "
                    f"Comm={self.committer.is_ready}")

        tasks = [
            asyncio.create_task(self.suggester_a.run_loop(self._browser)),
            asyncio.create_task(self.suggester_b.run_loop(self._browser)),
            asyncio.create_task(self.committer.run_loop(
                self._browser, self._on_synced, self._on_failed)),
            asyncio.create_task(self._slot_rotation_watcher()),
            asyncio.create_task(self._health_watcher()),
            asyncio.create_task(self._prefetch_worker_loop()),
            asyncio.create_task(self._cache_janitor_loop()),
        ]

        await asyncio.gather(*tasks, return_exceptions=True)

    async def _prefetch_worker_loop(self):
        from local_store_grid import (
            pop_batch_for_prefetch, store_grid, requeue_for_prefetch,
            queue_size, lookup_grid, wait_for_queue,
            is_inflight, mark_inflight, clear_inflight,
        )
        consecutive_fail = 0
        _retry_counts = {}
        MAX_PREFETCH_RETRIES = 5
        while True:
            try:
                sa = self.suggester_a
                sb = self.suggester_b
                if not ((sa and sa.is_ready) or (sb and sb.is_ready)):
                    await asyncio.sleep(2)
                    continue

                await asyncio.get_event_loop().run_in_executor(
                    None, wait_for_queue, 1.0)

                batch = pop_batch_for_prefetch(max_items=4)
                if not batch:
                    continue

                to_fetch = []
                for bag_id in batch:
                    cached = lookup_grid(bag_id)
                    if cached and cached.get("grid"):
                        logger.debug(f"[Prefetch] {bag_id} already cached - skip")
                        _retry_counts.pop(bag_id, None)
                        continue
                    if is_inflight(bag_id):
                        logger.debug(f"[Prefetch] {bag_id} already inflight - skip")
                        continue
                    to_fetch.append(bag_id)

                if not to_fetch:
                    continue

                available = []
                if sa and sa.is_ready and not sa.is_degraded():
                    available.append(sa)
                if sb and sb.is_ready and not sb.is_degraded():
                    available.append(sb)
                if not available:
                    if sa and sa.is_ready:
                        available.append(sa)
                    if sb and sb.is_ready:
                        available.append(sb)

                if not available:
                    for bag_id in to_fetch:
                        retries = _retry_counts.get(bag_id, 0)
                        if retries < MAX_PREFETCH_RETRIES:
                            requeue_for_prefetch(bag_id)
                            _retry_counts[bag_id] = retries + 1
                        else:
                            logger.info(f"[Prefetch] {bag_id} dropped after "
                                        f"{MAX_PREFETCH_RETRIES} retries (no browser)")
                            _retry_counts.pop(bag_id, None)
                    await asyncio.sleep(1)
                    continue

                total_backlog = sum(b.queue_size() for b in available)
                if total_backlog > 8:
                    for bag_id in to_fetch:
                        retries = _retry_counts.get(bag_id, 0)
                        if retries < MAX_PREFETCH_RETRIES:
                            requeue_for_prefetch(bag_id)
                            _retry_counts[bag_id] = retries + 1
                        else:
                            _retry_counts.pop(bag_id, None)
                    await asyncio.sleep(0.3)
                    continue

                async def _prefetch_one(bag_id, browser):
                    if not mark_inflight(bag_id):
                        return bag_id, {"ok": False, "reason": "already inflight"}
                    try:
                        fut = self._loop.create_future()
                        browser.queue.put_nowait(SuggestRequest(bag_id, fut))
                        try:
                            result = await asyncio.wait_for(fut, timeout=8.0)
                        except asyncio.TimeoutError:
                            return bag_id, {"ok": False, "reason": "timeout"}
                        return bag_id, result
                    finally:
                        clear_inflight(bag_id)

                tasks = []
                for idx, bag_id in enumerate(to_fetch):
                    browser = available[idx % len(available)]
                    tasks.append(_prefetch_one(bag_id, browser))

                results = await asyncio.gather(*tasks, return_exceptions=True)

                for r in results:
                    if isinstance(r, Exception):
                        consecutive_fail += 1
                        continue
                    bag_id, result = r
                    if result.get("ok") and result.get("grid"):
                        store_grid(
                            bag_id, result["grid"],
                            already_staged=result.get("already_staged", False),
                            source="prefetch",
                        )
                        consecutive_fail = 0
                        _retry_counts.pop(bag_id, None)
                    else:
                        reason = result.get("reason", "unknown")
                        soft = any(k in reason.lower() for k in [
                            "timeout", "not ready", "submit", "browser",
                            "no suggester", "session",
                        ])
                        if soft:
                            retries = _retry_counts.get(bag_id, 0)
                            if retries < MAX_PREFETCH_RETRIES:
                                requeue_for_prefetch(bag_id)
                                _retry_counts[bag_id] = retries + 1
                            else:
                                logger.info(f"[Prefetch] {bag_id} dropped after "
                                            f"{MAX_PREFETCH_RETRIES} soft retries: {reason}")
                                _retry_counts.pop(bag_id, None)
                            consecutive_fail += 1
                        else:
                            logger.info(f"[Prefetch] {bag_id} -> real reject: {reason}")
                            consecutive_fail = 0
                            _retry_counts.pop(bag_id, None)

                qs = queue_size()
                if qs > 0:
                    logger.info(f"[Prefetch] {qs} bag(s) still queued")

                if consecutive_fail >= 8:
                    await asyncio.sleep(5)
                    consecutive_fail = 0

                if len(_retry_counts) > 200:
                    _retry_counts.clear()

            except Exception as e:
                logger.error(f"[Prefetch] loop error: {e}")
                await asyncio.sleep(2)

    async def _cache_janitor_loop(self):
        from local_store_grid import purge_stale, CACHE_TTL_HOURS
        while True:
            try:
                await asyncio.sleep(3600)
                purge_stale(CACHE_TTL_HOURS)
            except Exception as e:
                logger.error(f"[CacheJanitor] {e}")

    async def _slot_rotation_watcher(self):
        last_slot = _get_current_slot()
        while True:
            await asyncio.sleep(60)
            try:
                cur = _get_current_slot()
                if cur != last_slot:
                    logger.info(f"[HMSManager] Credential slot rotated: {last_slot} -> {cur}")
                    last_slot = cur
                    for b in [self.suggester_a, self.suggester_b,
                              self.committer]:
                        if b:
                            b.failed_slots.clear()
                            b.current_slot = cur
                            if HMS_CRED_SLOTS:
                                b.user, b.password = HMS_CRED_SLOTS[cur]
                            try:
                                await b.close()
                                await b.initialize(self._browser)
                            except Exception as e:
                                logger.error(f"[HMSManager] Rotation re-init for {b.name}: {e}")
            except Exception as e:
                logger.error(f"[HMSManager] Slot watcher: {e}")

    async def _health_watcher(self):
        while True:
            await asyncio.sleep(10)
            try:
                for b in [self.suggester_a, self.suggester_b,
                          self.committer]:
                    if b is None:
                        continue
                    is_suggester = isinstance(b, SuggesterBrowser)
                    needs_recovery = (not b.is_ready) or \
                                     (is_suggester and b.is_degraded())
                    if not needs_recovery:
                        continue
                    if b.is_initializing:
                        continue
                    now = time.time()
                    min_interval = 10 if b.is_ready else 30
                    if now - b.last_recovery < min_interval:
                        continue
                    if b.recovery_count >= 50:
                        continue
                    b.last_recovery = now
                    b.recovery_count += 1
                    state = "degraded" if b.is_ready else "dead"
                    fails = (b.consecutive_failures
                             if is_suggester else 0)
                    logger.info(f"[HMSManager] Recovering {b.name} "
                                f"({state}, attempt {b.recovery_count}, "
                                f"consec_fails={fails})")
                    try:
                        await b.close()
                    except Exception:
                        pass
                    try:
                        if len(b.failed_slots) >= len(HMS_CRED_SLOTS):
                            b.failed_slots.clear()
                        b.current_slot = _get_current_slot()
                        if HMS_CRED_SLOTS:
                            b.user, b.password = HMS_CRED_SLOTS[b.current_slot]
                        await b.initialize(self._browser)
                        if is_suggester:
                            b.consecutive_failures = 0
                    except Exception as e:
                        logger.error(f"[HMSManager] Recovery failed for {b.name}: {e}")
            except Exception as e:
                logger.error(f"[HMSManager] Health watcher: {e}")

    def _on_synced(self, bag_id: str, trolley_id: str, sheet_row: int,
                   reason: str = ""):
        from sheets import ts as _ts_now, get_cache
        now = _ts_now()
        is_already_staged = (reason == "already_staged")
        status = "Already_Staged" if is_already_staged else "Done"

        try:
            cache = get_cache()
            if sheet_row and sheet_row > 0:
                cache.update_cell("Live_Staging", sheet_row, COL_HMS_SYNCED + 1, status)
                cache.update_cell("Live_Staging", sheet_row, COL_HMS_SYNCED_TS + 1, now)
        except Exception as e:
            logger.error(f"[HMSManager] cache update for {bag_id} failed: {e}")

        try:
            from local_store_hms import remove_synced_bag
            remove_synced_bag(bag_id, trolley_id)
        except Exception:
            pass

        with self._writes_lock:
            self._pending_sync_writes.append({
                "bag_id": bag_id,
                "trolley_id": trolley_id,
                "sheet_row": sheet_row,
                "ts": now,
                "already_staged": is_already_staged,
            })

    def _on_failed(self, bag_id: str, trolley_id: str, sheet_row: int,
                   reason: str, real_rejection: bool):
        logger.warning(f"[HMSManager] HMS sync FAILED for {bag_id}: {reason} "
                       f"(real={real_rejection})")

    def _sheet_writer_loop(self):
        from local_store_hms import drain_dlq, add_to_dlq
        from sheets import get_cache, get_sheet, _col_letter, ts as _ts_now

        while self._writer_running:
            try:
                batch = []
                deadline = time.time() + (SHEET_BATCH_WAIT_MS / 1000.0)
                with self._writes_lock:
                    if self._pending_sync_writes:
                        batch.extend(self._pending_sync_writes[:SHEET_BATCH_SIZE])
                        del self._pending_sync_writes[:len(batch)]
                if not batch:
                    dlq_items = drain_dlq()
                    if dlq_items:
                        batch.extend(dlq_items[:SHEET_BATCH_SIZE])
                if not batch:
                    time.sleep(0.5)
                    continue

                while time.time() < deadline and len(batch) < SHEET_BATCH_SIZE:
                    with self._writes_lock:
                        if self._pending_sync_writes:
                            take = min(
                                SHEET_BATCH_SIZE - len(batch),
                                len(self._pending_sync_writes),
                            )
                            batch.extend(self._pending_sync_writes[:take])
                            del self._pending_sync_writes[:take]
                            continue
                    time.sleep(0.05)

                ok = self._flush_synced_batch(batch)
                if not ok:
                    add_to_dlq(batch)
                else:
                    self._maybe_release_trolleys(batch)

            except Exception as e:
                logger.error(f"[SheetWriter] Loop error: {e}")
                time.sleep(2)

    def _flush_synced_batch(self, batch: List[dict]) -> bool:
        if not batch:
            return True
        try:
            from sheets import get_sheet, _col_letter, get_cache
            ws = get_sheet("Live_Staging")
            cache = get_cache()

            col_synced = _col_letter(COL_HMS_SYNCED + 1)
            col_ts = _col_letter(COL_HMS_SYNCED_TS + 1)
            payload = []
            max_row = 0
            for item in batch:
                row_num = item["sheet_row"]
                if not row_num or row_num <= 0:
                    continue
                max_row = max(max_row, row_num)
                status = "Already_Staged" if item.get("already_staged") else "Done"
                payload.append({
                    "range": f"{col_synced}{row_num}:{col_ts}{row_num}",
                    "values": [[status, item["ts"]]],
                })

            if not payload:
                return True

            if max_row > ws.row_count:
                ws.add_rows(max_row - ws.row_count + 10)

            for attempt in range(5):
                try:
                    ws.batch_update(payload, value_input_option="RAW")
                    logger.info(f"[SheetWriter] Pushed {len(payload)} HMS_Synced row(s)")
                    try:
                        with cache._lock:
                            if "Live_Staging" in cache._dirty_rows:
                                for item in batch:
                                    rn = item["sheet_row"]
                                    if rn and rn > 0:
                                        cache._dirty_rows["Live_Staging"].discard(rn - 1)
                    except Exception:
                        pass
                    return True
                except Exception as e:
                    err = str(e)
                    if "429" in err or "RESOURCE_EXHAUSTED" in err or "quota" in err.lower():
                        wait = min(2 ** attempt, 30)
                        logger.warning(f"[SheetWriter] Rate limited, wait {wait}s")
                        time.sleep(wait)
                        continue
                    wait = min(2 ** attempt, 10)
                    logger.warning(f"[SheetWriter] attempt {attempt+1}/5 failed: {e}, wait {wait}s")
                    time.sleep(wait)
            logger.error(f"[SheetWriter] Batch failed after 5 attempts -> DLQ")
            return False
        except Exception as e:
            logger.error(f"[SheetWriter] Flush crashed: {e}")
            return False

    def _maybe_release_trolleys(self, batch: List[dict]):
        from sheets import get_cache, ts as _ts_now

        trolleys_to_check = set(item.get("trolley_id", "") for item in batch
                                if item.get("trolley_id"))
        if not trolleys_to_check:
            return

        cache = get_cache()
        now = _ts_now()
        l_data = cache.get_all_values("Live_Staging")

        for trolley_id in trolleys_to_check:
            has_unfinished = False
            bag_count = 0
            for i in range(1, len(l_data)):
                row = l_data[i]
                row_trolley = str(row[COL_TROLLEY_ID]).strip() if len(row) > COL_TROLLEY_ID else ""
                row_trolley_put = str(row[COL_TROLLEY_PUT]).strip() if len(row) > COL_TROLLEY_PUT else ""
                row_grid_put = str(row[COL_GRID_PUT]).strip() if len(row) > COL_GRID_PUT else ""
                row_hms_synced = str(row[COL_HMS_SYNCED]).strip() if len(row) > COL_HMS_SYNCED else ""

                if row_trolley == trolley_id and row_trolley_put == "Done":
                    bag_count += 1
                    if row_grid_put != "Done" or not row_hms_synced:
                        has_unfinished = True
                        break

            if has_unfinished or bag_count == 0:
                continue

            try:
                t_data = cache.get_all_values("Trolley_Registry")
                for j in range(1, len(t_data)):
                    row = t_data[j]
                    if str(row[0]).strip() == str(trolley_id):
                        row_num = j + 1
                        cur_status = str(row[2]).strip() if len(row) > 2 else ""
                        if cur_status == "Active":
                            cache.update_cell("Trolley_Registry", row_num, 2, "")
                            cache.update_cell("Trolley_Registry", row_num, 3, "Available")
                            cache.update_cell("Trolley_Registry", row_num, 4, now)
                            logger.info(f"[SheetWriter] Trolley {trolley_id} RELEASED "
                                        f"(all {bag_count} bags HMS-synced + grid-put done)")
                        break
            except Exception as e:
                logger.error(f"[SheetWriter] Release trolley {trolley_id} failed: {e}")


# Singleton

_manager: Optional[HMSManager] = None


def get_hms_manager() -> HMSManager:
    global _manager
    if _manager is None:
        _manager = HMSManager()
    return _manager


def start_hms_sync() -> HMSManager:
    m = get_hms_manager()
    m.start()
    return m


def get_hms_sync() -> HMSManager:
    return get_hms_manager()