"""
sheets.py — Google Sheets helper for Staging Hub Flask app
Uses gspread + OAuth (Desktop / Installed-app) to read/write the Google Sheet.

PERFORMANCE: In-memory cache layer loads reference sheets + live data into RAM.
Reads are instant (~0ms). Writes go to RAM immediately + Google Sheets in a
background thread so workers never wait for the API.

──────────────────────────────────────────────────────────────────────────
GRIDWISE PATCH (2026-05-19)
──────────────────────────────────────────────────────────────────────────
  • lookup_destination() now reads from Gridwise_Data (sheet ID stored in
    GRIDWISE_SPREADSHEET_ID env var, tab "Total_IB") instead of
    Stagging_Mapping.
  • Gridwise_Data column layout (0-based, Total_IB tab):
        C (2)  = Bag Closing PN
        D (3)  = Bag Closing in M4
        E (4)  = Bag Closing in PP
        F (5)  = Bag Closing for Priority
        G (6)  = Bag closing for Regular
        L (11) = General Bag Tag
        J (9)  = Grid   ← destination
        K (10) = Air Grid (stored but not used for routing currently)
  • All six tag columns (C-G, L) are indexed → O(1) prefix/exact lookup,
    identical to the old Stagging_Mapping behaviour.
  • Stagging_Mapping sheet + lookup_destination_legacy() kept as a fallback
    in case Gridwise_Data is empty or unavailable. Remove when stable.
  • Cache initialisation loads "Gridwise_Data_Total_IB" as a virtual sheet
    name so the refresh loop treats it like any other reference sheet.

──────────────────────────────────────────────────────────────────────────
HARDENING PATCH (2026-05-13b)
──────────────────────────────────────────────────────────────────────────
  (see prior version for full notes — unchanged here)
"""

import gspread
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from datetime import datetime
import pytz
import os
import json
import tempfile
import time
import threading
import logging
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger("sheets")

# --- AUTH & CONNECTION ---

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

SPREADSHEET_ID = os.getenv("SPREADSHEET_ID", "")
CREDENTIALS_FILE = os.getenv("GOOGLE_CREDENTIALS_FILE", "credentials.json")
TOKEN_FILE = os.getenv("TOKEN_FILE", "token.json")

# ── Gridwise_Data spreadsheet (separate Google Sheet) ──────────────────────
GRIDWISE_SPREADSHEET_ID = os.getenv(
    "GRIDWISE_SPREADSHEET_ID", "13RrhnlnFjoRBI3YaYJsLewo_4GauLdZoHuxcqjZEUsI"
)
GRIDWISE_TAB = "Total_IB"

_client = None
_creds = None
_spreadsheet = None              # cached gspread.Spreadsheet (main)
_gridwise_spreadsheet = None     # cached gspread.Spreadsheet (Gridwise_Data)
_worksheet_cache = {}            # name -> gspread.Worksheet  (main sheet)
_worksheet_cache_lock = threading.Lock()


def get_credentials():
    """Return valid OAuth2 credentials, running the Desktop flow if needed."""
    global _creds

    if _creds and _creds.valid:
        return _creds

    creds = None

    if os.path.exists(TOKEN_FILE):
        creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                CREDENTIALS_FILE, SCOPES
            )
            creds = flow.run_local_server(port=0)

        with open(TOKEN_FILE, "w") as f:
            f.write(creds.to_json())
        print(f"  ✅ Google auth token saved to {TOKEN_FILE}")

    _creds = creds
    return _creds


def get_client():
    global _client
    if _client is not None:
        return _client
    _client = gspread.authorize(get_credentials())
    return _client


def get_drive_service():
    return build("drive", "v3", credentials=get_credentials())


def get_spreadsheet():
    global _spreadsheet
    if _spreadsheet is not None:
        return _spreadsheet
    _spreadsheet = get_client().open_by_key(SPREADSHEET_ID)
    return _spreadsheet


def get_gridwise_spreadsheet():
    """Return the cached gspread.Spreadsheet for Gridwise_Data."""
    global _gridwise_spreadsheet
    if _gridwise_spreadsheet is not None:
        return _gridwise_spreadsheet
    _gridwise_spreadsheet = get_client().open_by_key(GRIDWISE_SPREADSHEET_ID)
    return _gridwise_spreadsheet


def get_sheet(name):
    """Get a worksheet by name from the MAIN spreadsheet. Cached."""
    with _worksheet_cache_lock:
        ws = _worksheet_cache.get(name)
        if ws is not None:
            return ws
        ws = get_spreadsheet().worksheet(name)
        _worksheet_cache[name] = ws
        return ws


def get_gridwise_sheet():
    """Return the Total_IB worksheet from Gridwise_Data. NOT cached in
    _worksheet_cache (different spreadsheet)."""
    return get_gridwise_spreadsheet().worksheet(GRIDWISE_TAB)


def reset_connection():
    """Clear all cached connection state."""
    global _client, _spreadsheet, _gridwise_spreadsheet
    with _worksheet_cache_lock:
        _worksheet_cache.clear()
    _spreadsheet = None
    _gridwise_spreadsheet = None
    _client = None


# --- TIMESTAMP ---

IST = pytz.timezone("Asia/Kolkata")


def ts():
    """Return current IST timestamp in ISO 8601 format with timezone offset."""
    now = datetime.now(IST)
    return (now.strftime("%Y-%m-%dT%H:%M:%S.") +
            f"{now.microsecond // 1000:03d}" +
            now.strftime("%z")[:3] + ":" + now.strftime("%z")[3:])


# --- LOOKUP HELPERS ---


def is_trolley(code):
    return bool(code) and str(code).startswith("TRL-")


def is_grid(code):
    return bool(code) and str(code).startswith("GRD-")


def is_conveyer(code):
    if not code:
        return False
    s = str(code).upper()
    return s.startswith("CNV-") or s.startswith("G1_BL") or s.startswith("G2_BL")


# ══════════════════════════════════════════════════════════════
#  GRIDWISE_DATA lookup (PRIMARY — replaces Stagging_Mapping)
# ══════════════════════════════════════════════════════════════
#
# Total_IB column layout (0-based):
#   0  = COC
#   1  = Bag Closing in PN       (col B in sheet)
#   2  = Bag Closing in M4       (col C — labelled "n PN" in header row)
#   3  = Bag Closing in M4       (col D)
#   4  = Bag Closing in PP       (col E)
#   5  = Bag Closing for Priority(col F)
#   6  = Bag closing for Regular (col G)
#   7  = Cluster                 (col H)
#   8  = FWD                     (col I)
#   9  = Grid                    (col J)  ← DESTINATION
#  10  = Air Grid                (col K)
#  11  = General Bag Tag         (col L)
#
# NOTE: the sheet header row has merged/offset columns; we verified the
# index mapping from image 1 (Gridwise_Data) vs image 2 (Stagging_Mapping).
# Tag columns indexed: B(1), C(2), D(3), E(4), F(5), G(6), L(11).
# Grid is col J (index 9).

_gridwise_index: dict = {}
_gridwise_index_version: int = 0

# Column indices inside Total_IB that contain bag-closing tags
_GRIDWISE_TAG_COLS = (1, 2, 3, 4, 5, 6, 11)  # B, C, D, E, F, G, L
_GRIDWISE_GRID_COL = 9    # J
_GRIDWISE_AIRGRID_COL = 10  # K
_GRIDWISE_COC_COL = 0     # A
_GRIDWISE_FWD_COL = 8     # I


def _build_gridwise_index(gridwise_data: list):
    """Build O(1) lookup dict from Total_IB rows.

    Keys are the tag values (uppercased, stripped).
    Value is a dict with keys matching what lookup_destination() used to
    return so callers need zero changes: {coc, forwardMH, grid, airGrid}.
    A tag of '-' or empty is skipped.
    """
    global _gridwise_index, _gridwise_index_version
    idx: dict = {}
    skipped = 0
    for i in range(1, len(gridwise_data)):  # skip header
        row = gridwise_data[i]
        grid = str(row[_GRIDWISE_GRID_COL]).strip() if len(row) > _GRIDWISE_GRID_COL else ""
        if not grid or grid == "-":
            skipped += 1
            continue
        entry = {
            "coc":       str(row[_GRIDWISE_COC_COL]).strip() if len(row) > _GRIDWISE_COC_COL else "",
            "forwardMH": str(row[_GRIDWISE_FWD_COL]).strip() if len(row) > _GRIDWISE_FWD_COL else "",
            "grid":      grid,
            "airGrid":   str(row[_GRIDWISE_AIRGRID_COL]).strip() if len(row) > _GRIDWISE_AIRGRID_COL else "",
        }
        for col_idx in _GRIDWISE_TAG_COLS:
            if len(row) > col_idx:
                tag = str(row[col_idx]).strip().upper()
                if tag and tag != "-":
                    idx[tag] = entry
    _gridwise_index = idx
    _gridwise_index_version = len(gridwise_data)
    logger.info(
        f"Built Gridwise index: {len(idx)} tag keys from "
        f"{len(gridwise_data) - 1} data rows ({skipped} rows skipped — no grid)"
    )


def lookup_destination(bag_id, gridwise_data: list):
    """O(1) lookup against Gridwise_Data Total_IB.

    Tries 7-char prefix first (matching the old Stagging_Mapping behaviour),
    then the full bag_id (for exact-match tags like 'EBAGSIL').

    Returns dict {coc, forwardMH, grid, airGrid} or None.
    """
    global _gridwise_index, _gridwise_index_version
    if not bag_id:
        return None

    if _gridwise_index_version != len(gridwise_data):
        _build_gridwise_index(gridwise_data)

    prefix7 = str(bag_id)[:7].upper()
    result = _gridwise_index.get(prefix7)
    if result:
        return result

    full_id = str(bag_id).strip().upper()
    return _gridwise_index.get(full_id)


# ── Legacy Stagging_Mapping lookup (kept as fallback) ──────────────────────
_mapping_index: dict = {}
_mapping_index_version: int = 0


def _build_mapping_index(mapping_data):
    """
    Build a dict keyed by every prefix (PN, M4, Regular) for O(1) lookups.
    Column layout: COC(0), Bag Closing PN(1), Bag Closing M4(2),
                   Regular(3), Forward MH(4), Grid(5)
    """
    global _mapping_index, _mapping_index_version
    idx = {}
    for i in range(1, len(mapping_data)):
        row = mapping_data[i]
        entry = {
            "coc": row[0] if len(row) > 0 else "",
            "forwardMH": row[4] if len(row) > 4 else "",
            "grid": row[5] if len(row) > 5 else "",
        }
        pn = str(row[1]).strip().upper() if len(row) > 1 else ""
        if pn and pn != "-":
            idx[pn] = entry
        m4 = str(row[2]).strip().upper() if len(row) > 2 else ""
        if m4 and m4 != "-":
            idx[m4] = entry
        reg = str(row[3]).strip().upper() if len(row) > 3 else ""
        if reg and reg != "-":
            idx[reg] = entry
    _mapping_index = idx
    _mapping_index_version = len(mapping_data)
    logger.info(f"Built Stagging_Mapping index (legacy): {len(idx)} keys from "
                f"{len(mapping_data)-1} rows")


def lookup_destination_legacy(bag_id, mapping_data):
    """O(1) prefix lookup against Stagging_Mapping (legacy fallback)."""
    global _mapping_index, _mapping_index_version
    if not bag_id:
        return None

    if _mapping_index_version != len(mapping_data):
        _build_mapping_index(mapping_data)

    prefix7 = str(bag_id)[:7].upper()
    result = _mapping_index.get(prefix7)
    if result:
        return result

    full_id = str(bag_id).strip().upper()
    return _mapping_index.get(full_id)


def lookup_area(barcode, area_data):
    if not barcode:
        return None
    barcode_str = str(barcode).strip()

    for i in range(1, len(area_data)):
        row = area_data[i]
        if str(row[0]).strip() == barcode_str:
            return {
                "areaName": str(row[1]).strip() if len(row) > 1 else "",
                "grid": str(row[2]).strip() if len(row) > 2 else "",
            }
    return None


# ══════════════════════════════════════════════════════════════
#  IN-MEMORY DATA CACHE
# ══════════════════════════════════════════════════════════════

CACHE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "_cache")

# Virtual sheet name used inside the cache for Gridwise Total_IB data.
_GRIDWISE_CACHE_KEY = "Gridwise_Total_IB"


def _is_quota_error(exc) -> bool:
    s = str(exc)
    return ("429" in s) or ("RESOURCE_EXHAUSTED" in s) or ("Quota exceeded" in s)


APPEND_TOMBSTONE_TTL = 30.0  # seconds


class DataCache:
    """
    Keeps Google Sheets data in RAM for instant reads.
    - Reference sheets (Gridwise_Total_IB, Stagging_Mapping, Area_Registry)
      refresh every 5 min.
    - Live sheets (Live_Staging, Trolley_Registry) are AUTHORITATIVE in RAM.
    - Local JSON files in _cache/ for crash recovery.
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._data = {}
        self._dirty_rows = {}
        self._append_queue = {}
        self._initialized = False
        self._bg_thread = None
        self._running = False

        self._quota_backoff_ticks = 0
        self._append_tombstones = {}

        os.makedirs(CACHE_DIR, exist_ok=True)

    # ── Init ───────────────────────────────────────────────

    def initialize(self):
        if self._initialized:
            return
        logger.info("📦 Loading all sheets into memory cache…")
        start = time.time()

        # Main spreadsheet sheets
        sheets_to_load = [
            "Stagging_Mapping", "Area_Registry",
            "Live_Staging", "Trolley_Registry",
        ]

        for name in sheets_to_load:
            try:
                data = get_sheet(name).get_all_values()
                self._data[name] = data
                self._save_json(name, data, force=True)
                logger.info(f"  ✅ {name}: {len(data)} rows loaded")
            except Exception as e:
                logger.error(f"  ❌ {name}: {e}")
                cached = self._load_json(name)
                if cached:
                    self._data[name] = cached
                    logger.info(f"  🔄 {name}: loaded {len(cached)} rows "
                                f"from local cache")
                else:
                    self._data[name] = []

        # Gridwise_Data (separate spreadsheet)
        try:
            gw_data = get_gridwise_sheet().get_all_values()
            self._data[_GRIDWISE_CACHE_KEY] = gw_data
            self._save_json(_GRIDWISE_CACHE_KEY, gw_data, force=True)
            logger.info(f"  ✅ Gridwise_Data/Total_IB: {len(gw_data)} rows loaded")
        except Exception as e:
            logger.error(f"  ❌ Gridwise_Data/Total_IB: {e}")
            cached = self._load_json(_GRIDWISE_CACHE_KEY)
            if cached:
                self._data[_GRIDWISE_CACHE_KEY] = cached
                logger.info(f"  🔄 Gridwise_Data/Total_IB: loaded {len(cached)} rows "
                            f"from local cache")
            else:
                self._data[_GRIDWISE_CACHE_KEY] = []

        elapsed = time.time() - start
        logger.info(f"📦 Cache loaded in {elapsed:.1f}s")
        self._initialized = True

        self._running = True
        self._bg_thread = threading.Thread(
            target=self._bg_sync_loop, name="cache-sync", daemon=True)
        self._bg_thread.start()

    # ── Tombstone helpers ──────────────────────────────────

    def _add_tombstone(self, sheet_name, bag_id):
        if not bag_id:
            return
        key = str(bag_id).strip()
        if not key:
            return
        bucket = self._append_tombstones.setdefault(sheet_name, {})
        bucket[key] = time.time()
        if len(bucket) > 5000:
            cutoff = time.time() - APPEND_TOMBSTONE_TTL
            for k in list(bucket.keys()):
                if bucket[k] < cutoff:
                    del bucket[k]

    def _fresh_tombstones(self, sheet_name):
        bucket = self._append_tombstones.get(sheet_name, {})
        cutoff = time.time() - APPEND_TOMBSTONE_TTL
        return {k for k, ts_val in bucket.items() if ts_val >= cutoff}

    # ── Read ────────────────────────────────────────────────

    def get_all_values(self, sheet_name):
        with self._lock:
            return [row[:] for row in self._data.get(sheet_name, [])]

    # ── Write: update cell ──────────────────────────────────

    def update_cell(self, sheet_name, row_idx, col_idx, value):
        with self._lock:
            data = self._data.get(sheet_name, [])
            r = row_idx - 1
            c = col_idx - 1
            if r < len(data):
                while len(data[r]) <= c:
                    data[r].append("")
                data[r][c] = value
                if sheet_name not in self._dirty_rows:
                    self._dirty_rows[sheet_name] = set()
                self._dirty_rows[sheet_name].add(r)
            self._save_json(sheet_name, data)

    # ── Write: append row ───────────────────────────────────

    def append_row(self, sheet_name, row):
        with self._lock:
            data = self._data.get(sheet_name, [])
            data.append(row[:])
            new_idx = len(data) - 1
            self._data[sheet_name] = data
            if sheet_name not in self._append_queue:
                self._append_queue[sheet_name] = []
            self._append_queue[sheet_name].append(new_idx)
            if sheet_name == "Live_Staging" and len(row) > 4:
                self._add_tombstone(sheet_name, row[4])
            self._save_json(sheet_name, data)

    def append_row_if_unique(self, sheet_name, col_idx, value, row):
        with self._lock:
            data = self._data.get(sheet_name, [])
            for i in range(len(data) - 1, 0, -1):
                if (len(data[i]) > col_idx and
                        str(data[i][col_idx]) == str(value)):
                    return False, i
            data.append(row[:])
            new_idx = len(data) - 1
            self._data[sheet_name] = data
            if sheet_name not in self._append_queue:
                self._append_queue[sheet_name] = []
            self._append_queue[sheet_name].append(new_idx)
            if sheet_name == "Live_Staging" and len(row) > 4:
                self._add_tombstone(sheet_name, row[4])
            self._save_json(sheet_name, data)
            return True, new_idx

    # ── Write: clear data rows ─────────────────────────────

    def clear_data_rows(self, sheet_name):
        with self._lock:
            data = self._data.get(sheet_name, [])
            if data:
                self._data[sheet_name] = [data[0]]
            else:
                self._data[sheet_name] = []
            self._append_tombstones.pop(sheet_name, None)
            self._save_json(sheet_name, self._data[sheet_name], force=True)

    # ── Background sync ────────────────────────────────────

    def _bg_sync_loop(self):
        ref_refresh_counter = 0
        trolley_reconcile_counter = 0
        _consecutive_net_errors = 0

        while self._running:
            time.sleep(2)

            if self._quota_backoff_ticks > 0:
                self._quota_backoff_ticks -= 1
                continue

            try:
                self._push_dirty()
                _consecutive_net_errors = 0
            except Exception as e:
                err_str = str(e)
                if _is_quota_error(e):
                    self._quota_backoff_ticks = max(
                        self._quota_backoff_ticks, 5)
                    logger.warning(
                        f"Quota hit in _push_dirty; backing off "
                        f"{self._quota_backoff_ticks * 2}s")
                elif ("10013" in err_str or "10054" in err_str or
                      "NewConnectionError" in err_str):
                    _consecutive_net_errors += 1
                    if (_consecutive_net_errors <= 3 or
                            _consecutive_net_errors % 30 == 0):
                        logger.warning(
                            f"Network unavailable "
                            f"(attempt {_consecutive_net_errors}), "
                            f"will retry: {e}")
                    time.sleep(min(_consecutive_net_errors * 5, 60))
                    continue
                else:
                    logger.error(f"Cache sync error: {e}")

            trolley_reconcile_counter += 1
            if trolley_reconcile_counter >= 30:
                trolley_reconcile_counter = 0
                try:
                    self._reconcile_trolleys_from_ram()
                except Exception as e:
                    logger.error(f"Trolley reconcile error: {e}")

            ref_refresh_counter += 1
            if ref_refresh_counter >= 150:
                ref_refresh_counter = 0
                try:
                    self._refresh_reference_sheets()
                except Exception as e:
                    if _is_quota_error(e):
                        self._quota_backoff_ticks = max(
                            self._quota_backoff_ticks, 15)

    def _push_dirty(self):
        """Push all pending writes to Google Sheets (main spreadsheet only)."""
        with self._lock:
            append_work = dict(self._append_queue)
            self._append_queue = {}
            dirty_work = dict(self._dirty_rows)
            self._dirty_rows = {}

        all_work = {}
        for sheet_name in set(list(append_work.keys()) +
                              list(dirty_work.keys())):
            # Gridwise is read-only — never push to it
            if sheet_name == _GRIDWISE_CACHE_KEY:
                continue
            indices = set()
            if sheet_name in append_work:
                indices.update(append_work[sheet_name])
            if sheet_name in dirty_work:
                indices.update(dirty_work[sheet_name])
            if indices:
                all_work[sheet_name] = indices

        for sheet_name, row_indices in all_work.items():
            if not row_indices:
                continue
            try:
                ws = get_sheet(sheet_name)
                data = self.get_all_values(sheet_name)

                max_row_needed = max(row_indices) + 1
                if max_row_needed > ws.row_count:
                    ws.add_rows(max_row_needed - ws.row_count + 10)

                batch = []
                for r in sorted(row_indices):
                    if r < len(data):
                        row_data = data[r]
                        row_num = r + 1
                        end_col = (_col_letter(len(row_data))
                                   if row_data else 'A')
                        batch.append({
                            'range': f'A{row_num}:{end_col}{row_num}',
                            'values': [row_data]
                        })
                if batch:
                    ws.batch_update(batch, value_input_option='RAW')
                    logger.debug(f"Synced {len(row_indices)} rows to "
                                 f"{sheet_name}")
            except Exception as e:
                if _is_quota_error(e):
                    self._quota_backoff_ticks = max(
                        self._quota_backoff_ticks, 5)
                    logger.warning(
                        f"Quota hit syncing {sheet_name}; re-queuing "
                        f"{len(row_indices)} row(s), backing off "
                        f"{self._quota_backoff_ticks * 2}s")
                else:
                    logger.error(f"Failed to sync {sheet_name}: {e}")
                with self._lock:
                    if sheet_name not in self._dirty_rows:
                        self._dirty_rows[sheet_name] = set()
                    self._dirty_rows[sheet_name].update(row_indices)

    def _reconcile_trolleys_from_ram(self):
        """Release Active trolleys whose bags ALL have Grid_Put == 'Done'.
        Works purely against in-RAM Live_Staging — no Sheets read.
        """
        _BAG_ID      = 4
        _TROLLEY_ID  = 7
        _TROLLEY_PUT = 11
        _GRID_PUT    = 13

        live_data = self.get_all_values("Live_Staging")
        if not live_data:
            return

        trolley_bag_counts = {}
        for i in range(1, len(live_data)):
            row = live_data[i]
            bag_id = (str(row[_BAG_ID]).strip()
                      if len(row) > _BAG_ID else "")
            trolley_id = (str(row[_TROLLEY_ID]).strip()
                          if len(row) > _TROLLEY_ID else "")
            trolley_put = (str(row[_TROLLEY_PUT]).strip()
                           if len(row) > _TROLLEY_PUT else "")
            grid_put = (str(row[_GRID_PUT]).strip()
                        if len(row) > _GRID_PUT else "")

            if not bag_id or not trolley_id or trolley_put != "Done":
                continue

            if trolley_id not in trolley_bag_counts:
                trolley_bag_counts[trolley_id] = {"total": 0, "grid_done": 0}
            trolley_bag_counts[trolley_id]["total"] += 1
            if grid_put == "Done":
                trolley_bag_counts[trolley_id]["grid_done"] += 1

        t_data = self.get_all_values("Trolley_Registry")
        now_str = ts()
        changed = False
        for j in range(1, len(t_data)):
            row = t_data[j]
            trolley_id = str(row[0]).strip() if len(row) > 0 else ""
            location = str(row[1]).strip() if len(row) > 1 else ""
            status = str(row[2]).strip() if len(row) > 2 else ""

            if status != "Active" or not location:
                continue

            counts = trolley_bag_counts.get(trolley_id)
            if not counts:
                continue

            if counts["total"] > 0 and counts["grid_done"] == counts["total"]:
                row_num = j + 1
                self.update_cell("Trolley_Registry", row_num, 2, "")
                self.update_cell("Trolley_Registry", row_num, 3, "Available")
                self.update_cell("Trolley_Registry", row_num, 4, now_str)
                logger.info(f"🔓 Released trolley {trolley_id} from "
                            f"[{location}] — all {counts['total']} bag(s) "
                            f"grid-put done")
                changed = True

        if changed:
            logger.info("Trolley reconciliation complete")

    def reload_live_from_sheets(self):
        """OPT-IN: reload Live_Staging from Google Sheets.
        NEVER call on a timer — admin endpoint only.
        """
        with self._lock:
            has_pending = (
                bool(self._append_queue.get("Live_Staging")) or
                bool(self._dirty_rows.get("Live_Staging"))
            )
        if has_pending:
            return {"ok": False, "reason": "pending writes — try again later"}

        try:
            live_data = get_sheet("Live_Staging").get_all_values()
        except Exception as e:
            return {"ok": False, "reason": f"sheets read failed: {e}"}

        if not live_data:
            return {"ok": False, "reason": "sheet returned empty"}

        fresh = self._fresh_tombstones("Live_Staging")
        if fresh:
            bag_ids_in_reload = set()
            for i in range(1, len(live_data)):
                r = live_data[i]
                if len(r) > 4 and str(r[4]).strip():
                    bag_ids_in_reload.add(str(r[4]).strip())
            missing = fresh - bag_ids_in_reload
            if missing:
                return {
                    "ok": False,
                    "reason": (f"stale read — {len(missing)} freshly-scanned "
                               f"bag(s) missing from reload"),
                    "missing_sample": list(missing)[:5],
                }

        with self._lock:
            has_pending = (
                bool(self._append_queue.get("Live_Staging")) or
                bool(self._dirty_rows.get("Live_Staging"))
            )
            if has_pending:
                return {"ok": False,
                        "reason": "writes arrived mid-reload — try again"}
            self._data["Live_Staging"] = live_data

        self._save_json("Live_Staging", live_data, force=True)
        return {"ok": True, "rows": len(live_data)}

    def _refresh_reference_sheets(self):
        """Reload Gridwise_Data/Total_IB, Stagging_Mapping, and Area_Registry."""
        # Gridwise_Data (separate spreadsheet — read-only reference)
        try:
            gw_data = get_gridwise_sheet().get_all_values()
            with self._lock:
                self._data[_GRIDWISE_CACHE_KEY] = gw_data
            self._save_json(_GRIDWISE_CACHE_KEY, gw_data, force=True)
            logger.debug(f"Refreshed Gridwise_Data/Total_IB: {len(gw_data)} rows")
        except Exception as e:
            if _is_quota_error(e):
                self._quota_backoff_ticks = max(self._quota_backoff_ticks, 15)
                logger.warning(
                    f"Quota hit refreshing Gridwise_Data; backing off "
                    f"{self._quota_backoff_ticks * 2}s, skipping rest")
                return
            logger.error(f"Failed to refresh Gridwise_Data/Total_IB: {e}")

        # Legacy reference sheets (kept for fallback)
        for name in ["Stagging_Mapping", "Area_Registry"]:
            try:
                data = get_sheet(name).get_all_values()
                with self._lock:
                    self._data[name] = data
                self._save_json(name, data, force=True)
                logger.debug(f"Refreshed {name}: {len(data)} rows")
            except Exception as e:
                if _is_quota_error(e):
                    self._quota_backoff_ticks = max(
                        self._quota_backoff_ticks, 15)
                    logger.warning(
                        f"Quota hit refreshing {name}; backing off "
                        f"{self._quota_backoff_ticks * 2}s, skipping rest")
                    return
                logger.error(f"Failed to refresh {name}: {e}")

    def force_sync_now(self):
        logger.info("⚡ Force-syncing all pending writes…")
        self._push_dirty()

    def full_reload(self):
        """Used by daily_backup after clearing data."""
        logger.info("🔄 Full cache reload from Google Sheets…")
        # Gridwise_Data
        try:
            gw_data = get_gridwise_sheet().get_all_values()
            with self._lock:
                self._data[_GRIDWISE_CACHE_KEY] = gw_data
                self._append_tombstones.pop(_GRIDWISE_CACHE_KEY, None)
            self._save_json(_GRIDWISE_CACHE_KEY, gw_data, force=True)
            logger.info(f"  ✅ Gridwise_Data/Total_IB: {len(gw_data)} rows")
        except Exception as e:
            logger.error(f"  ❌ Gridwise_Data/Total_IB: {e}")

        for name in ["Live_Staging", "Trolley_Registry",
                     "Stagging_Mapping", "Area_Registry"]:
            try:
                data = get_sheet(name).get_all_values()
                with self._lock:
                    self._data[name] = data
                    self._append_tombstones.pop(name, None)
                self._save_json(name, data, force=True)
                logger.info(f"  ✅ {name}: {len(data)} rows")
            except Exception as e:
                logger.error(f"  ❌ {name}: {e}")

    # ── Local JSON backup ─────────────────────────────────

    def _save_json(self, sheet_name, data, force=False):
        """Save sheet data to local JSON atomically.

        SHRINK-GUARD: rejects writes that would shrink the file unless
        force=True is passed.
        """
        if data is None:
            return
        try:
            path = os.path.join(CACHE_DIR, f"{sheet_name}.json")

            if not force and os.path.exists(path):
                try:
                    on_disk_size = os.path.getsize(path)
                    if not data and on_disk_size > 4:
                        return
                    if on_disk_size > 4 and len(data) < self._row_count_on_disk(path):
                        logger.warning(
                            f"_save_json({sheet_name}): refusing to shrink "
                            f"{self._row_count_on_disk(path)} → {len(data)} "
                            f"rows (use force=True for authoritative writes)")
                        return
                except OSError:
                    pass

            tmp_fd, tmp_path = tempfile.mkstemp(
                prefix=f".{sheet_name}_",
                suffix=".tmp",
                dir=CACHE_DIR,
            )
            try:
                with os.fdopen(tmp_fd, "w", encoding="utf-8") as f:
                    json.dump(data, f, ensure_ascii=False)
                    f.flush()
                    os.fsync(f.fileno())
                os.replace(tmp_path, path)
            except Exception:
                try:
                    os.remove(tmp_path)
                except Exception:
                    pass
                raise
        except Exception as e:
            logger.debug(f"_save_json({sheet_name}): {e}")

    def _row_count_on_disk(self, path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                content = f.read().strip()
            if not content or content == "[]":
                return 0
            parsed = json.loads(content)
            return len(parsed) if isinstance(parsed, list) else 0
        except Exception:
            return 0

    def _load_json(self, sheet_name):
        try:
            path = os.path.join(CACHE_DIR, f"{sheet_name}.json")
            if os.path.exists(path):
                with open(path, "r", encoding="utf-8") as f:
                    content = f.read().strip()
                    if not content:
                        return None
                    return json.loads(content)
        except json.JSONDecodeError as e:
            logger.error(f"_load_json({sheet_name}) corrupt: {e}")
            try:
                path = os.path.join(CACHE_DIR, f"{sheet_name}.json")
                backup = (path + ".corrupt." +
                          datetime.now(IST).strftime("%Y%m%d_%H%M%S"))
                os.rename(path, backup)
                logger.info(f"  Backed up corrupt JSON to "
                            f"{os.path.basename(backup)}")
            except Exception:
                pass
        except Exception:
            pass
        return None

    def stop(self):
        self._running = False
        try:
            self._push_dirty()
        except Exception:
            pass


def _col_letter(col_num):
    """1→A, 2→B, 27→AA"""
    result = ""
    while col_num > 0:
        col_num, remainder = divmod(col_num - 1, 26)
        result = chr(65 + remainder) + result
    return result

# ── Singleton ──────────────────────────────────────────────

_cache = None


def get_cache():
    global _cache
    if _cache is None:
        _cache = DataCache()
    return _cache