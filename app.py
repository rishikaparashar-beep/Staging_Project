"""
app.py — Flask server for Staging Hub.

UPDATED COLUMN LAYOUT (18 columns):
  A  Conveyer_ID            (0)
  B  Conveyer_Timestamp     (1)
  C  Cnv_Bag_Scan_TS        (2)
  D  Spiral_Bag_Scan_TS     (3)
  E  Bag_ID                 (4)
  F  Casper_ID              (5)
  G  Grid                   (6)
  H  Trolley_ID             (7)
  I  Grid_Barcode           (8)
  J  Area_Put               (9)   ← NEW
  K  Area_Put_Timestamp     (10)  ← NEW
  L  Trolley_Put            (11)
  M  Trolley_Put_Timestamp  (12)
  N  Grid_Put               (13)
  O  Grid_Put_Timestamp     (14)
  P  HMS_Synced             (15)
  Q  HMS_Synced_TS          (16)
  R  (reserved)             (17)

WHAT CHANGED FROM PRIOR VERSION
  • Two new columns J/K inserted: Area_Put + Area_Put_TS.
  • Everything from old index 9 onward shifts +2.
  • Bag Scan (Spiral) page: after bag scan, worker now scans an AREA barcode
    (not a trolley). The area is validated against the bag's assigned grid;
    on success we write Area_Put="Done" + Area_Put_TS. On mismatch we return
    WRONG_AREA with the correct grid name so the worker can retry.
  • NEW Trolley Scan page: worker scans first bag, then the trolley once
    (locks the trolley to the first bag's grid), then keeps scanning bags.
    Each subsequent bag must belong to the locked grid or we return
    WRONG_GRID. Each bag scan writes Trolley_Put + Trolley_Put_TS.
  • handle_trolley_after_bag REPLACED by handle_area_put_after_bag.
  • New endpoint /api/trolley-scan + handler handle_trolley_scan_page.
  • Dashboard counts Area_Put done/pending across all rows.
  • _row_to_bag_dict exposes areaPut + areaPutTs.
  • api_filtered_bags supports 'area-done' and 'area-pending' filters.

GRIDWISE PATCH (2026-05-19)
  • TIER 2 lookup in handle_bag_scan_at_spiral now uses Gridwise_Data
    (Total_IB tab, sheet ID in GRIDWISE_SPREADSHEET_ID env var) instead of
    Stagging_Mapping. lookup_destination() signature unchanged — callers
    pass gridwise_data instead of mapping_data.
"""

from flask import Flask, render_template, request, jsonify
from sheets import (
    get_sheet,
    lookup_area,
    lookup_destination,
    lookup_destination_legacy,
    is_trolley,
    is_conveyer,
    ts,
    get_cache,
    _GRIDWISE_CACHE_KEY,
)
import re
import time as _time

app = Flask(__name__)


# ─── Live_Staging column layout (0-based, 18 columns) ─────────────
COL_CONVEYER_ID        = 0   # A
COL_CONVEYER_TS        = 1   # B
COL_CNV_BAG_SCAN_TS    = 2   # C
COL_SPIRAL_BAG_SCAN_TS = 3   # D
COL_BAG_ID             = 4   # E
COL_CASPER_ID          = 5   # F
COL_GRID               = 6   # G
COL_TROLLEY_ID         = 7   # H
COL_GRID_BARCODE       = 8   # I
COL_AREA_PUT           = 9   # J   ← NEW
COL_AREA_PUT_TS        = 10  # K   ← NEW
COL_TROLLEY_PUT        = 11  # L   (was 9)
COL_TROLLEY_PUT_TS     = 12  # M   (was 10)
COL_GRID_PUT           = 13  # N   (was 11)
COL_GRID_PUT_TS        = 14  # O   (was 12)
COL_HMS_SYNCED         = 15  # P   (was 13)
COL_HMS_SYNCED_TS      = 16  # Q   (was 14)
TOTAL_COLS             = 18  # A..R (R reserved)


# ── Barcode sanitizer ────────────────────────────────────────
def sanitize_barcode(raw):
    if not raw:
        return ""
    cleaned = re.sub(r'[\x00-\x1f\x7f-\x9f]', '', str(raw))
    cleaned = cleaned.strip()
    return cleaned


def _empty_row():
    """Return a list of TOTAL_COLS empty strings for a fresh Live_Staging row."""
    return [""] * TOTAL_COLS


# ============================================================
#  PAGE ROUTES
# ============================================================

@app.route("/")
def index():
    return render_template("index.html")


@app.route("/dashboard")
def dashboard():
    return render_template("dashboard.html")


@app.route("/generate-barcodes")
def generate_barcodes():
    return render_template("barcode_generator.html")


# ============================================================
#  API: PROCESS SCAN  (Bag Scan Spiral page)
#  Flow: bag scan first → area barcode scan second.
#  No more trolley logic on this page.
# ============================================================

@app.route("/api/process-scan", methods=["POST"])
def api_process_scan():
    data = request.get_json(force=True)
    casper_id = sanitize_barcode(data.get("Casper_ID", ""))
    barcode = sanitize_barcode(data.get("barcode", ""))
    context = {
        "lastBagId": sanitize_barcode(data.get("lastBagId", "")),
        "lastGrid": data.get("lastGrid", ""),
    }
    if not barcode:
        return jsonify({"action": "ERROR", "message": "No barcode received."})
    try:
        if context["lastBagId"]:
            return jsonify(handle_area_put_after_bag(
                context["lastBagId"], barcode, casper_id))
        return jsonify(handle_bag_scan_at_spiral(barcode, casper_id))
    except Exception as e:
        return jsonify({"action": "ERROR", "message": f"Server error: {e}"})


# ============================================================
#  API: CONVEYER SCAN (G1/G2 floor) — unchanged
# ============================================================

@app.route("/api/conveyer-scan", methods=["POST"])
def api_conveyer_scan():
    data = request.get_json(force=True)
    conveyer_barcode = sanitize_barcode(data.get("conveyer_barcode", ""))
    bag_id = sanitize_barcode(data.get("bag_id", ""))
    casper_id = sanitize_barcode(data.get("Casper_ID", ""))
    if not conveyer_barcode or not bag_id:
        return jsonify({"action": "ERROR",
                        "message": "Both conveyer barcode and bag ID are required."})
    if not is_conveyer(conveyer_barcode):
        return jsonify({"action": "ERROR",
                        "message": "❌ Invalid conveyer barcode. Must start with CNV-, G1_BL, or G2_BL."})
    try:
        return jsonify(handle_conveyer_scan(conveyer_barcode, bag_id, casper_id))
    except Exception as e:
        return jsonify({"action": "ERROR", "message": f"Server error: {e}"})


# ============================================================
#  API: TROLLEY SCAN PAGE (NEW)
# ============================================================

@app.route("/api/trolley-scan", methods=["POST"])
def api_trolley_scan():
    data = request.get_json(force=True)
    casper_id = sanitize_barcode(data.get("Casper_ID", ""))
    bag_id = sanitize_barcode(data.get("bagId", ""))
    trolley_id = sanitize_barcode(data.get("trolleyId", ""))
    locked_grid = sanitize_barcode(data.get("lockedGrid", "")).upper()
    try:
        return jsonify(handle_trolley_scan_page(
            bag_id, trolley_id, locked_grid, casper_id))
    except Exception as e:
        return jsonify({"action": "ERROR", "message": f"Server error: {e}"})


# ============================================================
#  API: TROLLEY GRID LOOKUP
# ============================================================

@app.route("/api/trolley-grid", methods=["POST"])
def api_trolley_grid():
    data = request.get_json(force=True)
    trolley_id = sanitize_barcode(data.get("trolleyId", ""))
    if not trolley_id:
        return jsonify({"grid": ""})
    try:
        cache = get_cache()
        t_data = cache.get_all_values("Trolley_Registry")
        for j in range(1, len(t_data)):
            row = t_data[j]
            if str(row[0]).strip() == trolley_id and str(row[2]).strip() == "Active":
                return jsonify({"grid": str(row[1]).strip()})
        return jsonify({"grid": ""})
    except Exception:
        return jsonify({"grid": ""})


# ============================================================
#  API: GRID PUT — unchanged
# ============================================================

@app.route("/api/grid-put", methods=["POST"])
def api_grid_put():
    data = request.get_json(force=True)
    barcode = sanitize_barcode(data.get("barcode", ""))
    casper_id = sanitize_barcode(data.get("Casper_ID", ""))
    trolley_id = sanitize_barcode(data.get("trolleyId", ""))
    try:
        cache = get_cache()
        area_data = cache.get_all_values("Area_Registry")
        grid_obj = lookup_area(barcode, area_data)
        if not grid_obj or not grid_obj.get("grid"):
            return jsonify({"action": "ERROR", "message": "❌ Unknown grid barcode."})
        return jsonify(handle_grid_scan(
            grid_obj["grid"], barcode, casper_id, trolley_id))
    except Exception as e:
        return jsonify({"action": "ERROR", "message": f"Server error: {e}"})


# ============================================================
#  API: DASHBOARD DATA
# ============================================================

@app.route("/api/dashboard-data", methods=["GET"])
def api_dashboard_data():
    try:
        cache = get_cache()
        l_data = cache.get_all_values("Live_Staging")
        t_data = cache.get_all_values("Trolley_Registry")

        trolley_map = {}
        for j in range(1, len(t_data)):
            loc = str(t_data[j][1]).strip() if len(t_data[j]) > 1 else ""
            trl_st = str(t_data[j][2]) if len(t_data[j]) > 2 else ""
            if loc and (loc not in trolley_map or trl_st == "Active"):
                trolley_map[loc] = {"id": str(t_data[j][0]), "status": trl_st}

        grid_stats = {}
        cnv_only = 0
        spiral_scan_total = 0

        for i in range(1, len(l_data)):
            row = l_data[i]
            grid = str(row[COL_GRID]).strip() if len(row) > COL_GRID else ""
            bag_id = str(row[COL_BAG_ID]).strip() if len(row) > COL_BAG_ID else ""
            cnv_ts = (str(row[COL_CNV_BAG_SCAN_TS]).strip()
                      if len(row) > COL_CNV_BAG_SCAN_TS else "")
            spiral_ts = (str(row[COL_SPIRAL_BAG_SCAN_TS]).strip()
                         if len(row) > COL_SPIRAL_BAG_SCAN_TS else "")

            if cnv_ts and not spiral_ts and bag_id:
                cnv_only += 1

            if spiral_ts and bag_id:
                spiral_scan_total += 1

            if not grid or not bag_id:
                continue
            area_put = (row[COL_AREA_PUT]
                        if len(row) > COL_AREA_PUT else "")
            trolley_put = (row[COL_TROLLEY_PUT]
                           if len(row) > COL_TROLLEY_PUT else "")
            grid_put = (row[COL_GRID_PUT]
                        if len(row) > COL_GRID_PUT else "")

            if grid not in grid_stats:
                grid_stats[grid] = {"total": 0, "scanned": 0,
                                    "areaPut": 0,
                                    "trolleyPut": 0, "gridDone": 0}

            grid_stats[grid]["total"] += 1
            if not trolley_put and not grid_put:
                grid_stats[grid]["scanned"] += 1
            if area_put == "Done":
                grid_stats[grid]["areaPut"] += 1
            if trolley_put == "Done":
                grid_stats[grid]["trolleyPut"] += 1
            if grid_put == "Done":
                grid_stats[grid]["gridDone"] += 1

        all_areas = []
        try:
            a_data = cache.get_all_values("Area_Registry")
            for k in range(1, len(a_data)):
                row = a_data[k]
                if len(row) > 2 and row[2]:
                    all_areas.append({
                        "barcode": str(row[0]),
                        "areaName": str(row[1]),
                        "gridName": str(row[2]),
                        "areaType": str(row[3]) if len(row) > 3 else "",
                    })
        except Exception:
            pass

        for grid in grid_stats:
            if grid in trolley_map:
                grid_stats[grid]["trolleyId"] = trolley_map[grid]["id"]
                grid_stats[grid]["trolleyStatus"] = trolley_map[grid]["status"]
            else:
                grid_stats[grid]["trolleyId"] = ""
                grid_stats[grid]["trolleyStatus"] = ""

        return jsonify({
            "areas": grid_stats,
            "allAreas": all_areas,
            "conveyerOnly": cnv_only,
            "spiralScanTotal": spiral_scan_total,
            "timestamp": ts(),
        })
    except Exception as e:
        return jsonify({"areas": {}, "allAreas": [], "conveyerOnly": 0,
                        "spiralScanTotal": 0, "timestamp": ts(), "error": str(e)})


# ============================================================
#  API: GRID DETAIL
# ============================================================

@app.route("/api/grid-detail", methods=["GET"])
def api_grid_detail():
    grid_name = request.args.get("grid", "").strip()
    if not grid_name:
        return jsonify({"bags": [], "error": "No grid name provided."})
    try:
        cache = get_cache()
        l_data = cache.get_all_values("Live_Staging")
        bags = []
        for i in range(1, len(l_data)):
            row = l_data[i]
            row_grid = str(row[COL_GRID]).strip() if len(row) > COL_GRID else ""
            row_bag = str(row[COL_BAG_ID]).strip() if len(row) > COL_BAG_ID else ""
            if row_grid != grid_name or not row_bag:
                continue
            bags.append(_row_to_bag_dict(row))
        return jsonify({"bags": bags, "grid": grid_name, "count": len(bags)})
    except Exception as e:
        return jsonify({"bags": [], "error": str(e)})


@app.route("/api/filtered-bags", methods=["GET"])
def api_filtered_bags():
    filter_type = request.args.get("filter", "total").strip()
    if not filter_type:
        return jsonify({"bags": [], "filter": filter_type, "count": 0,
                        "error": "No filter provided."})
    try:
        cache = get_cache()
        l_data = cache.get_all_values("Live_Staging")
        bags = []
        for i in range(1, len(l_data)):
            row = l_data[i]
            bag_id = str(row[COL_BAG_ID]).strip() if len(row) > COL_BAG_ID else ""
            if not bag_id:
                continue
            area_put = (str(row[COL_AREA_PUT]).strip()
                        if len(row) > COL_AREA_PUT else "")
            trolley_put = (str(row[COL_TROLLEY_PUT]).strip()
                           if len(row) > COL_TROLLEY_PUT else "")
            grid_put = (str(row[COL_GRID_PUT]).strip()
                        if len(row) > COL_GRID_PUT else "")

            include = False
            if filter_type == "total":
                include = True
            elif filter_type == "area-done":
                include = area_put == "Done"
            elif filter_type == "area-pending":
                include = area_put != "Done"
            elif filter_type == "trolley-done":
                include = trolley_put == "Done"
            elif filter_type == "trolley-pending":
                include = trolley_put != "Done"
            elif filter_type == "grid-done":
                include = grid_put == "Done"
            elif filter_type == "grid-pending":
                include = grid_put != "Done"
            elif filter_type == "spiral-scan":
                spiral_ts = (str(row[COL_SPIRAL_BAG_SCAN_TS]).strip()
                             if len(row) > COL_SPIRAL_BAG_SCAN_TS else "")
                include = bool(spiral_ts)

            if include:
                bags.append(_row_to_bag_dict(row))
        return jsonify({"bags": bags, "filter": filter_type, "count": len(bags)})
    except Exception as e:
        return jsonify({"bags": [], "filter": filter_type,
                        "count": 0, "error": str(e)})


@app.route("/api/belt-detail", methods=["GET"])
def api_belt_detail():
    try:
        cache = get_cache()
        l_data = cache.get_all_values("Live_Staging")
        bags = []
        for i in range(1, len(l_data)):
            row = l_data[i]
            cnv_ts = (str(row[COL_CNV_BAG_SCAN_TS]).strip()
                      if len(row) > COL_CNV_BAG_SCAN_TS else "")
            spiral_ts = (str(row[COL_SPIRAL_BAG_SCAN_TS]).strip()
                         if len(row) > COL_SPIRAL_BAG_SCAN_TS else "")
            bag_id = str(row[COL_BAG_ID]).strip() if len(row) > COL_BAG_ID else ""
            if cnv_ts and not spiral_ts and bag_id:
                bags.append(_row_to_bag_dict(row))
        return jsonify({"bags": bags, "count": len(bags)})
    except Exception as e:
        return jsonify({"bags": [], "count": 0, "error": str(e)})


# ============================================================
#  API: HMS STATUS / STATS
# ============================================================

@app.route("/api/hms-stats", methods=["GET"])
def api_hms_stats():
    try:
        from hms_sync import get_hms_manager
        m = get_hms_manager()
        return jsonify(m.get_status())
    except Exception as e:
        return jsonify({"error": str(e)})


@app.route("/api/hms-pending", methods=["GET"])
def api_hms_pending():
    try:
        from local_store_hms import get_all_pending, get_all_abandoned
        return jsonify({
            "pending": get_all_pending(),
            "abandoned": get_all_abandoned(),
        })
    except Exception as e:
        return jsonify({"error": str(e)})


# ============================================================
#  API: FIX FORMATTING / MANUAL BACKUP
# ============================================================

@app.route("/api/fix-formatting", methods=["POST"])
def api_fix_formatting():
    try:
        live_sheet = get_sheet("Live_Staging")
        row_count = live_sheet.row_count
        format_to = max(row_count, 500)
        if row_count < format_to:
            live_sheet.resize(rows=format_to)
        live_sheet.format(f"A2:R{format_to}", {
            "backgroundColor": {"red": 1, "green": 1, "blue": 1},
            "textFormat": {"bold": False},
        })
        return jsonify({"action": "OK",
                        "message": f"✅ Reset rows 2-{format_to} to white"})
    except Exception as e:
        return jsonify({"action": "ERROR", "message": f"Failed: {e}"})


@app.route("/api/manual-backup", methods=["POST"])
def api_manual_backup():
    try:
        from daily_backup import run_backup
        run_backup(force=True)
        return jsonify({"action": "OK",
                        "message": "✅ Backup done. Live_Staging + Trolley_Registry cleared."})
    except Exception as e:
        return jsonify({"action": "ERROR", "message": f"Backup failed: {e}"})


# ============================================================
#  API: ADMIN — RELOAD LIVE_STAGING FROM SHEETS
# ============================================================

@app.route("/api/reload-live-from-sheets", methods=["POST"])
def api_reload_live_from_sheets():
    try:
        cache = get_cache()
        result = cache.reload_live_from_sheets()
        if result.get("ok"):
            return jsonify({
                "action": "OK",
                "message": f"✅ Reloaded {result['rows']} rows from sheet",
            })
        return jsonify({
            "action": "ERROR",
            "message": f"❌ Reload rejected: {result.get('reason')}",
            "details": result,
        })
    except Exception as e:
        return jsonify({"action": "ERROR", "message": f"Server error: {e}"})


# ============================================================
#  INTERNAL LOGIC
# ============================================================

def _row_to_bag_dict(row):
    def safe(idx):
        return str(row[idx]) if len(row) > idx else ""
    return {
        "conveyerId":      safe(COL_CONVEYER_ID),
        "conveyerTs":      safe(COL_CONVEYER_TS),
        "cnvBagScanTs":    safe(COL_CNV_BAG_SCAN_TS),
        "spiralBagScanTs": safe(COL_SPIRAL_BAG_SCAN_TS),
        "bagId":           safe(COL_BAG_ID),
        "casperId":        safe(COL_CASPER_ID),
        "grid":            safe(COL_GRID),
        "trolleyId":       safe(COL_TROLLEY_ID),
        "gridBarcode":     safe(COL_GRID_BARCODE),
        "areaPut":         safe(COL_AREA_PUT),
        "areaPutTs":       safe(COL_AREA_PUT_TS),
        "trolleyPut":      safe(COL_TROLLEY_PUT),
        "trolleyPutTs":    safe(COL_TROLLEY_PUT_TS),
        "gridPut":         safe(COL_GRID_PUT),
        "gridPutTs":       safe(COL_GRID_PUT_TS),
        "hmsSynced":       safe(COL_HMS_SYNCED),
        "hmsSyncedTs":     safe(COL_HMS_SYNCED_TS),
    }


def handle_conveyer_scan(conveyer_barcode, bag_id, casper_id):
    """Worker on G1/G2 floor places bag on belt. Unchanged."""
    _t0 = _time.time()
    from local_store_grid import enqueue_for_prefetch
    from hms_sync import get_hms_manager

    cache = get_cache()
    now = ts()
    data = cache.get_all_values("Live_Staging")

    for i in range(len(data) - 1, 0, -1):
        row = data[i]
        if (len(row) > COL_BAG_ID and
                str(row[COL_BAG_ID]).strip() == str(bag_id).strip()):
            row_num = i + 1
            grid_put = (str(row[COL_GRID_PUT]).strip()
                        if len(row) > COL_GRID_PUT else "")
            cache.update_cell("Live_Staging", row_num,
                              COL_CONVEYER_ID + 1, conveyer_barcode)
            cache.update_cell("Live_Staging", row_num,
                              COL_CONVEYER_TS + 1, now)
            cache.update_cell("Live_Staging", row_num,
                              COL_CNV_BAG_SCAN_TS + 1, now)
            if casper_id:
                cache.update_cell("Live_Staging", row_num,
                                  COL_CASPER_ID + 1, casper_id)
            try:
                enqueue_for_prefetch(bag_id)
            except Exception as e:
                print(f"[CONVEYER] prefetch enqueue failed for {bag_id}: {e}",
                      flush=True)
            try:
                get_hms_manager().fire_prefetch_immediate(bag_id)
            except Exception as e:
                print(f"[CONVEYER] immediate prefetch failed for {bag_id}: {e}",
                      flush=True)
            _elapsed = int((_time.time() - _t0) * 1000)
            if grid_put == "Done":
                return {
                    "action": "ALREADY_STAGED",
                    "bagId": bag_id,
                    "conveyerId": conveyer_barcode,
                    "elapsed_ms": _elapsed,
                    "message": f"⚠️ Bag already staged previously — belt scan logged anyway. ({_elapsed}ms)",
                }
            return {
                "action": "CONVEYER_LOGGED",
                "bagId": bag_id,
                "conveyerId": conveyer_barcode,
                "elapsed_ms": _elapsed,
                "message": f"✅ Bag on {conveyer_barcode} ({_elapsed}ms)",
            }

    new_row = _empty_row()
    new_row[COL_CONVEYER_ID]      = conveyer_barcode
    new_row[COL_CONVEYER_TS]      = now
    new_row[COL_CNV_BAG_SCAN_TS]  = now
    new_row[COL_BAG_ID]           = bag_id
    new_row[COL_CASPER_ID]        = casper_id
    cache.append_row_if_unique(
        "Live_Staging", COL_BAG_ID, bag_id, new_row)
    try:
        enqueue_for_prefetch(bag_id)
    except Exception as e:
        print(f"[CONVEYER] prefetch enqueue failed for {bag_id}: {e}", flush=True)
    try:
        get_hms_manager().fire_prefetch_immediate(bag_id)
    except Exception as e:
        print(f"[CONVEYER] immediate prefetch failed for {bag_id}: {e}", flush=True)
    _elapsed = int((_time.time() - _t0) * 1000)
    return {
        "action": "CONVEYER_LOGGED",
        "bagId": bag_id,
        "conveyerId": conveyer_barcode,
        "elapsed_ms": _elapsed,
        "message": f"✅ Bag on {conveyer_barcode} ({_elapsed}ms)",
    }


def handle_bag_scan_at_spiral(bag_id, casper_id):
    """Worker scans bag at bottom of spiral.

    Lookup order (post-conveyor scan):
      TIER 1 — Sheet (Gridwise_Data → Stagging_Mapping fallback)
      TIER 2 — Pre-fetched cache
      TIER 3 — Live HMS browser (2 attempts)
    """
    _t0 = _time.time()
    from hms_sync import get_hms_manager
    from local_store_grid import lookup_grid, store_grid, mark_consumed

    cache = get_cache()
    now = ts()
    data = cache.get_all_values("Live_Staging")

    existing_row_num = -1
    existing_row = None
    for i in range(len(data) - 1, 0, -1):
        row = data[i]
        if (len(row) > COL_BAG_ID and
                str(row[COL_BAG_ID]).strip() == str(bag_id).strip()):
            existing_row_num = i + 1
            existing_row = row
            break

    locally_staged = False
    if existing_row is not None:
        grid_put = (str(existing_row[COL_GRID_PUT]).strip()
                    if len(existing_row) > COL_GRID_PUT else "")
        if grid_put == "Done":
            locally_staged = True

    grid = ""
    already_staged_in_hms = False
    cache_hit = False
    grid_source = ""

    # ── TIER 1: Sheet lookup (Gridwise_Data / Stagging_Mapping) ──
    try:
        gridwise_data = cache.get_all_values(_GRIDWISE_CACHE_KEY)
        mapped = lookup_destination(bag_id, gridwise_data)
    except Exception as e:
        print(f"[SPIRAL] ⚠ Gridwise_Data lookup error for "
              f"{bag_id}: {e}", flush=True)
        mapped = None

    if not mapped or not mapped.get("grid"):
        try:
            mapping_data = cache.get_all_values("Stagging_Mapping")
            mapped = lookup_destination_legacy(bag_id, mapping_data)
            if mapped and mapped.get("grid"):
                print(f"[SPIRAL] ℹ️ Gridwise miss — fell back to "
                      f"Stagging_Mapping for {bag_id}", flush=True)
        except Exception as e:
            print(f"[SPIRAL] ⚠ Stagging_Mapping fallback error for "
                  f"{bag_id}: {e}", flush=True)
            mapped = None

    if mapped and mapped.get("grid"):
        grid = str(mapped["grid"]).strip().upper()
        already_staged_in_hms = False
        grid_source = "sheet"
        _tier1_ms = int((_time.time() - _t0) * 1000)
        print(f"[SPIRAL] ⚡ TIER 1 (sheet) HIT for {bag_id} → {grid} "
              f"(prefix: {str(bag_id)[:7].upper()}, {_tier1_ms}ms)", flush=True)

        try:
            store_grid(bag_id, grid,
                       already_staged=False,
                       source="sheet_lookup")
            mark_consumed(bag_id)
        except Exception as e:
            print(f"[SPIRAL] sheet cache-write failed for "
                  f"{bag_id}: {e}", flush=True)

        try:
            from local_store_grid import (
                _queue_lock, _load_queue, _atomic_write, QUEUE_FILE,
            )
            with _queue_lock:
                queue = _load_queue()
                bag_upper = str(bag_id).strip().upper()
                if bag_upper in queue:
                    queue.remove(bag_upper)
                    _atomic_write(QUEUE_FILE, queue)
        except Exception:
            pass

    # ── TIER 2: pre-fetched cache lookup ─────────────────────
    if not grid:
        cached = lookup_grid(bag_id)
        if cached and cached.get("grid"):
            grid = cached["grid"].strip().upper()
            already_staged_in_hms = bool(cached.get("already_staged"))
            cache_hit = True
            grid_source = "cache"
            try:
                mark_consumed(bag_id)
            except Exception:
                pass
            _tier2_ms = int((_time.time() - _t0) * 1000)
            print(f"[SPIRAL] ⚡ TIER 2 (cache) HIT for {bag_id} → {grid} "
                  f"(staged: {already_staged_in_hms}, "
                  f"source: {cached.get('source')}, {_tier2_ms}ms)", flush=True)

    # ── TIER 3: live HMS browser query (2 attempts) ──────────
    if not grid:
        print(f"[SPIRAL] ⌛ TIER 3 (live HMS) for {bag_id} — "
              f"sheet + cache both missed (likely a seal ID)", flush=True)

        try:
            from local_store_grid import (
                _queue_lock, _load_queue, _atomic_write, QUEUE_FILE,
            )
            with _queue_lock:
                queue = _load_queue()
                bag_upper = str(bag_id).strip().upper()
                if bag_upper in queue:
                    queue.remove(bag_upper)
                    _atomic_write(QUEUE_FILE, queue)
        except Exception:
            pass

        hms = get_hms_manager()
        suggestion = None
        for _attempt in range(1, 3):
            suggestion = hms.get_grid_suggestion(bag_id, timeout=2.0)
            if suggestion.get("ok") and suggestion.get("grid", "").strip():
                print(f"[SPIRAL] ✓ HMS attempt {_attempt}/2 succeeded for "
                      f"{bag_id}", flush=True)
                break
            print(f"[SPIRAL] ⚠ HMS attempt {_attempt}/2 failed for {bag_id}: "
                  f"{suggestion.get('reason', 'no grid')}", flush=True)

        if not suggestion or not suggestion.get("ok"):
            return {
                "action": "ERROR",
                "message": (f"❌ HMS suggestion failed after 2 attempts: "
                            f"{suggestion.get('reason', 'unknown') if suggestion else 'unknown'}"),
            }

        grid = suggestion.get("grid", "").strip()
        already_staged_in_hms = suggestion.get("already_staged", False)
        grid_source = "hms"

        if grid:
            try:
                store_grid(bag_id, grid, already_staged_in_hms,
                           source="on_demand")
                mark_consumed(bag_id)
            except Exception:
                pass

    already_staged = already_staged_in_hms or locally_staged

    if not grid:
        if locally_staged and existing_row is not None:
            grid = (str(existing_row[COL_GRID]).strip()
                    if len(existing_row) > COL_GRID else "")
        if not grid:
            return {
                "action": "ERROR",
                "message": "❌ HMS returned empty grid",
            }

    if existing_row_num > 0:
        row_num = existing_row_num
        cache.update_cell("Live_Staging", row_num,
                          COL_SPIRAL_BAG_SCAN_TS + 1, now)
        cache.update_cell("Live_Staging", row_num, COL_GRID + 1, grid)
        if casper_id:
            cache.update_cell("Live_Staging", row_num,
                              COL_CASPER_ID + 1, casper_id)
        if already_staged:
            for col in (COL_TROLLEY_ID, COL_GRID_BARCODE,
                        COL_AREA_PUT, COL_AREA_PUT_TS,
                        COL_TROLLEY_PUT, COL_TROLLEY_PUT_TS,
                        COL_GRID_PUT, COL_GRID_PUT_TS,
                        COL_HMS_SYNCED, COL_HMS_SYNCED_TS):
                cache.update_cell("Live_Staging", row_num, col + 1, "")
    else:
        new_row = _empty_row()
        new_row[COL_SPIRAL_BAG_SCAN_TS] = now
        new_row[COL_BAG_ID]             = bag_id
        new_row[COL_CASPER_ID]          = casper_id
        new_row[COL_GRID]               = grid
        cache.append_row_if_unique(
            "Live_Staging", COL_BAG_ID, bag_id, new_row)

    _elapsed = int((_time.time() - _t0) * 1000)
    return {
        "action": "SUGGESTION",
        "bagId": bag_id,
        "grid": grid,
        "alreadyStaged": already_staged,
        "cacheHit": cache_hit,
        "gridSource": grid_source,
        "elapsed_ms": _elapsed,
        "message": (
            f"⚠️ Already staged — re-verify Area Put in [{grid}] ({_elapsed}ms)"
            if already_staged
            else f"🎯 Target: {grid} ({_elapsed}ms)"
        ),
    }


def handle_area_put_after_bag(bag_id, area_barcode, casper_id):
    """Step 2 on Bag Scan (Spiral) page: worker scans the AREA barcode."""
    _t0 = _time.time()
    if not bag_id:
        return {"action": "ERROR", "message": "Please scan a bag barcode first."}

    cache = get_cache()

    area_data = cache.get_all_values("Area_Registry")
    area_obj = lookup_area(area_barcode, area_data)
    if not area_obj or not area_obj.get("grid"):
        return {"action": "ERROR", "message": "❌ Unknown area barcode."}
    scanned_grid = area_obj["grid"].strip().upper()

    data = cache.get_all_values("Live_Staging")
    bag_row_index = -1
    bag_grid = ""
    for i in range(len(data) - 1, 0, -1):
        row = data[i]
        if (len(row) > COL_BAG_ID and
                str(row[COL_BAG_ID]).strip() == str(bag_id).strip() and
                (len(row) <= COL_AREA_PUT or not row[COL_AREA_PUT])):
            bag_row_index = i + 1
            bag_grid = (str(row[COL_GRID]).strip().upper()
                        if len(row) > COL_GRID else "")
            break

    if bag_row_index == -1:
        return {"action": "ERROR",
                "message": "Bag not found or area already scanned."}
    if not bag_grid:
        return {"action": "ERROR",
                "message": "Bag has no grid assigned (HMS suggestion missing)."}

    if scanned_grid != bag_grid:
        return {
            "action": "WRONG_AREA",
            "message": (f"❌ Wrong area! This bag belongs to [{bag_grid}], "
                        f"not [{scanned_grid}]. Scan the {bag_grid} area barcode."),
            "correctGrid": bag_grid,
            "scannedGrid": scanned_grid,
            "bagId": bag_id,
        }

    now = ts()
    cache.update_cell("Live_Staging", bag_row_index,
                      COL_AREA_PUT + 1, "Done")
    cache.update_cell("Live_Staging", bag_row_index,
                      COL_AREA_PUT_TS + 1, now)
    cache.update_cell("Live_Staging", bag_row_index,
                      COL_GRID_BARCODE + 1, area_barcode)

    _elapsed = int((_time.time() - _t0) * 1000)
    return {
        "action": "AREA_PUT_DONE",
        "bagId": bag_id,
        "grid": bag_grid,
        "areaBarcode": area_barcode,
        "elapsed_ms": _elapsed,
        "message": f"✅ Area Put → [{bag_grid}] ({_elapsed}ms)",
    }


def handle_trolley_scan_page(bag_id, trolley_id, locked_grid, casper_id):
    """Trolley Scan page handler. Unchanged."""
    _t0 = _time.time()
    if not bag_id:
        return {"action": "ERROR", "message": "Bag barcode required."}
    if not trolley_id:
        return {"action": "ERROR", "message": "Trolley barcode required."}

    cache = get_cache()
    data = cache.get_all_values("Live_Staging")

    bag_row_index = -1
    bag_grid = ""
    for i in range(len(data) - 1, 0, -1):
        row = data[i]
        if (len(row) > COL_BAG_ID and
                str(row[COL_BAG_ID]).strip() == str(bag_id).strip() and
                (len(row) <= COL_TROLLEY_PUT or not row[COL_TROLLEY_PUT])):
            bag_row_index = i + 1
            bag_grid = (str(row[COL_GRID]).strip().upper()
                        if len(row) > COL_GRID else "")
            break

    if bag_row_index == -1:
        return {"action": "ERROR",
                "message": "Bag not found or already on a trolley."}
    if not bag_grid:
        return {"action": "ERROR",
                "message": "Bag has no grid assigned."}

    if locked_grid and bag_grid != locked_grid:
        return {
            "action": "WRONG_GRID",
            "message": (f"❌ Wrong bag! Trolley [{trolley_id}] is locked to "
                        f"[{locked_grid}], but this bag belongs to [{bag_grid}]. "
                        f"Put it on a different trolley."),
            "correctGrid": bag_grid,
            "lockedGrid": locked_grid,
            "bagId": bag_id,
        }

    now = ts()
    t_data = cache.get_all_values("Trolley_Registry")
    trolley_row_index = -1
    for j in range(1, len(t_data)):
        row = t_data[j]
        if str(row[0]) == str(trolley_id):
            trolley_row_index = j + 1
            if row[2] == "Active" and str(row[1]).strip() != bag_grid:
                return {
                    "action": "ERROR",
                    "message": (f"❌ Trolley {trolley_id} is Active at "
                                f"[{row[1]}], not [{bag_grid}]!"),
                }
            break

    if trolley_row_index > -1:
        cache.update_cell("Trolley_Registry", trolley_row_index, 2, bag_grid)
        cache.update_cell("Trolley_Registry", trolley_row_index, 3, "Active")
        cache.update_cell("Trolley_Registry", trolley_row_index, 4, now)
    else:
        cache.append_row("Trolley_Registry",
                         [trolley_id, bag_grid, "Active", now])

    cache.update_cell("Live_Staging", bag_row_index,
                      COL_TROLLEY_ID + 1, trolley_id)
    cache.update_cell("Live_Staging", bag_row_index,
                      COL_TROLLEY_PUT + 1, "Done")
    cache.update_cell("Live_Staging", bag_row_index,
                      COL_TROLLEY_PUT_TS + 1, now)

    _elapsed = int((_time.time() - _t0) * 1000)
    return {
        "action": "TROLLEY_PUT_DONE",
        "trolleyId": trolley_id,
        "grid": bag_grid,
        "bagId": bag_id,
        "lockedGrid": bag_grid,
        "elapsed_ms": _elapsed,
        "message": f"✅ Bag → Trolley [{trolley_id}] at [{bag_grid}] ({_elapsed}ms)",
    }


def handle_grid_scan(grid_name, grid_barcode, casper_id, scanned_trolley_id):
    """Grid Put: mark bags Grid_Put=Done AND queue them for HMS sync. Unchanged."""
    _t0 = _time.time()
    from local_store_hms import add_pending_bag

    cache = get_cache()
    t_data = cache.get_all_values("Trolley_Registry")
    target_trolley_id = ""
    trolley_row_index = -1
    for j in range(1, len(t_data)):
        row = t_data[j]
        if str(row[1]).strip() == grid_name and row[2] == "Active":
            target_trolley_id = str(row[0])
            trolley_row_index = j + 1
            break
    if not target_trolley_id:
        return {"action": "ERROR",
                "message": f"❌ No Active trolley found at Grid [{grid_name}]."}
    if scanned_trolley_id and scanned_trolley_id != target_trolley_id:
        return {"action": "ERROR",
                "message": f"❌ Trolley mismatch! Scanned [{scanned_trolley_id}] "
                           f"but Grid [{grid_name}] has [{target_trolley_id}]."}

    l_data = cache.get_all_values("Live_Staging")
    count = 0
    queued = 0
    now = ts()

    for i in range(1, len(l_data)):
        row = l_data[i]
        if (len(row) > COL_GRID_PUT and
                str(row[COL_TROLLEY_ID]) == target_trolley_id and
                row[COL_TROLLEY_PUT] == "Done" and
                not row[COL_GRID_PUT]):
            row_num = i + 1
            bag_id = str(row[COL_BAG_ID]).strip() if len(row) > COL_BAG_ID else ""
            cache.update_cell("Live_Staging", row_num,
                              COL_GRID_BARCODE + 1, grid_barcode)
            cache.update_cell("Live_Staging", row_num,
                              COL_GRID_PUT + 1, "Done")
            cache.update_cell("Live_Staging", row_num,
                              COL_GRID_PUT_TS + 1, now)
            count += 1

            already_synced = (str(row[COL_HMS_SYNCED]).strip() == "Done"
                              if len(row) > COL_HMS_SYNCED else False)
            if not already_synced and bag_id:
                ok = add_pending_bag(
                    bag_id=bag_id,
                    area_barcode=grid_barcode,
                    grid=grid_name,
                    trolley_id=target_trolley_id,
                    sheet_row=row_num,
                )
                if ok:
                    queued += 1

    if count == 0:
        return {"action": "ERROR",
                "message": f"Trolley [{target_trolley_id}] is empty or bags already Grid Put."}

    _elapsed = int((_time.time() - _t0) * 1000)
    return {
        "action": "GRID_DONE",
        "count": count,
        "queued": queued,
        "gridName": grid_name,
        "trolleyId": target_trolley_id,
        "elapsed_ms": _elapsed,
        "message": f"✅ Grid Put: {count} bag(s), {queued} queued for HMS sync. ({_elapsed}ms)",
    }


# ============================================================
#  RUN SERVER
# ============================================================

if __name__ == "__main__":
    import os
    import socket
    import logging
    import sys

    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    )

    def _get_local_ip():
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.settimeout(2)
            s.connect(("8.8.8.8", 80))
            ip = s.getsockname()[0]
            s.close()
            return ip
        except Exception:
            pass
        try:
            hostname = socket.gethostname()
            ip = socket.gethostbyname(hostname)
            if ip and not ip.startswith("127."):
                return ip
        except Exception:
            pass
        return "127.0.0.1"

    LOCAL_IP = _get_local_ip()
    HOSTNAME = socket.gethostname()

    debug_mode = ("--debug" in sys.argv or
                  os.environ.get("STAGING_DEBUG", "").lower() == "true")
    production_mode = not debug_mode

    print("\n" + "=" * 55)
    print("  🏭 STAGING HUB — Flask Server")
    print("=" * 55)
    print(f"  Host          : {HOSTNAME}")
    print(f"  Mode          : {'DEBUG (auto-reload)' if debug_mode else 'PRODUCTION'}")
    print(f"  Worker Portal : http://10.244.3.157:5000")
    print(f"  Dashboard     : http://10.244.3.157:5000/dashboard")
    print(f"  QR Generator  : http://10.244.3.157:5000/generate-barcodes")
    print("=" * 55)

    should_init = (production_mode or
                   os.environ.get("WERKZEUG_RUN_MAIN") == "true")
    if should_init:
        print("  Cache         : Loading all sheets into RAM…")
        cache = get_cache()
        cache.initialize()
        print("  Cache         : ✅ READY (instant scans enabled)")

        try:
            import threading as _th
            def _fix_formatting_bg():
                import time as _t
                _t.sleep(8)
                try:
                    live_sheet = get_sheet("Live_Staging")
                    row_count = live_sheet.row_count
                    format_to = max(row_count, 500)
                    if row_count < format_to:
                        live_sheet.resize(rows=format_to)
                    live_sheet.format(f"A2:R{format_to}", {
                        "backgroundColor": {"red": 1, "green": 1, "blue": 1},
                        "textFormat": {"bold": False},
                    })
                    print("  Formatting    : ✅ rows reset to white")
                except Exception as e:
                    print(f"  Formatting    : ⚠️ {e}")
            _th.Thread(target=_fix_formatting_bg, name="fix-formatting", daemon=True).start()
        except Exception as e:
            print(f"  Formatting    : FAILED — {e}")

        try:
            from hms_sync import start_hms_sync
            start_hms_sync()
            print("  HMS Sync      : ENABLED (3 browsers: SugA + SugB + Committer)")
        except Exception as e:
            print(f"  HMS Sync      : FAILED — {e}")

        try:
            from daily_backup import check_and_run_backup, start_backup_scheduler
            import threading
            start_backup_scheduler()
            print("  Backup Sched  : ✅ Daily backup scheduler started")
            def _startup_backup():
                import time as _t
                _t.sleep(5)
                check_and_run_backup()
            threading.Thread(target=_startup_backup,
                             name="startup-backup", daemon=True).start()
            print("  Missed Backup : auto-detect ENABLED")
        except Exception as e:
            print(f"  Daily Backup  : FAILED — {e}")
    else:
        print("  HMS Sync      : waiting for reloader …")
    print("=" * 55 + "\n")

    app.run(host="0.0.0.0", port=5000,
            debug=debug_mode, use_reloader=debug_mode)