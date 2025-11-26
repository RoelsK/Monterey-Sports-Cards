import os
import sys
from datetime import datetime
import tkinter as tk
from tkinter import scrolledtext, filedialog, messagebox, simpledialog
import csv
import threading
import time

# ----------------------------------------------------
# FORCE-ADD ROOT & PRICING DIRECTORY TO PYTHON PATH
# ----------------------------------------------------
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.abspath(os.path.join(SCRIPT_DIR, ".."))

if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)
    
PRICING_DIR = os.path.join(ROOT_DIR, "pricing")
if PRICING_DIR not in sys.path:
    sys.path.insert(0, PRICING_DIR)

# ----------------------------------------------------
# IMPORT FROM pricing_engine (NO debug_capture_from_title!)
# ----------------------------------------------------
from pricing_engine import (
    get_item_details,
    _build_dynamic_query,
    _build_active_fallback_queries,
    _fetch_active_items_browse_for_query,
    _fetch_active_items_finding_for_query,
    _extract_card_signature_from_title,
    _extract_serial_fragment,
    _titles_match_strict,
    safe_hybrid_filter,
    ACTIVE_LIMIT,
)
print(">>> Loaded pricing_engine from:", os.path.join(PRICING_DIR, "pricing_engine.py"))

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, ROOT)

# -------------------------------------------------------------------
# IMPORT YOUR REAL PRICING ENGINE + HELPERS
# -------------------------------------------------------------------
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.abspath(os.path.join(SCRIPT_DIR, ".."))
sys.path.append(ROOT_DIR)
from pricing.pricing_engine import (
    get_item_details,
    _build_dynamic_query,
    _build_active_fallback_queries,
    _fetch_active_items_browse_for_query,
    _fetch_active_items_finding_for_query,
    _extract_card_signature_from_title,
    _extract_serial_fragment,
    _titles_match_strict,
    safe_hybrid_filter,
    ACTIVE_LIMIT,  # use your real limit
)

# -------------------------------------------------------------------
# GLOBALS FOR EXPORT
# -------------------------------------------------------------------
last_debug_data = None  # will hold the most recent debug run
show_reasons_var = None  # BooleanVar set in GUI

# -------------------------------------------------------------------
# CORE DEBUG LOGIC (WORKS FROM TITLE, USED BY ITEM-ID FLOW)
# -------------------------------------------------------------------
def debug_capture_from_title(title: str):
    """ IDENTICAL PIPELINE TO search_active() BUT WITH DEBUG OUTPUT.
    Includes:
    • Browse dynamic queries
    • Browse fallback queries
    • Finding dynamic queries
    • Full dedupe
    • Strict-layer info
    """
    raw_title = (title or "").strip()

    # Build structured queries
    dynamic_query = _build_dynamic_query(title)
    browse_queries = []
    if dynamic_query:
        browse_queries.append(dynamic_query)
    browse_queries.extend(_build_active_fallback_queries(title))
    if raw_title:
        browse_queries.append(raw_title)

    finding_queries = []
    if dynamic_query:
        finding_queries.append(dynamic_query)
    if raw_title:
        finding_queries.append(raw_title)

    merged_items = []
    seen = set()

    # -------------------
    # BROWSE QUERIES
    # -------------------
    for q in browse_queries:
        if not q:
            continue
        try:
            items = _fetch_active_items_browse_for_query(q, ACTIVE_LIMIT)
        except Exception:
            items = []
        for it in items:
            key = (it["title"].lower().strip(), it["total"])
            if key not in seen:
                seen.add(key)
                merged_items.append(it)

    # -------------------
    # FINDING QUERIES
    # -------------------
    for q in finding_queries:
        if not q:
            continue
        print(f"[FINDING QUERY] {q}")
        try:
            items = _fetch_active_items_finding_for_query(q, ACTIVE_LIMIT)
        except Exception:
            items = []
        for it in items:
            key = (it["title"].lower().strip(), it["total"])
            if key not in seen:
                seen.add(key)
                merged_items.append(it)

    # -------------------
    # STRICT FILTER
    # -------------------
    subject_sig = _extract_card_signature_from_title(title)
    subject_serial = _extract_serial_fragment(title)
    filtered_items = []
    removed_items = []

    for it in merged_items:
        comp_title = it["title"] or ""
        price = it["total"]
        reason = None
        comp_serial = _extract_serial_fragment(comp_title)

        if subject_serial and comp_serial and comp_serial != subject_serial:
            reason = (
                f"Serial mismatch: subject '{subject_serial}' vs comp '{comp_serial}'"
            )
        else:
            # STRICT MATCH v7 — correct signature call
            comp_sig = _extract_card_signature_from_title(comp_title)
            if subject_sig is not None and not _titles_match_strict(subject_sig, comp_sig):
                reason = "Failed _titles_match_strict (v7 signature mismatch)"
            # HYBRID FILTER
            elif price >= 10 and not safe_hybrid_filter(title, comp_title, price):
                reason = "Rejected by safe_hybrid_filter"

        if reason:
            removed_items.append(
                {"title": comp_title, "total": price, "reason": reason}
            )
        else:
            filtered_items.append(it)

    filtered_items.sort(key=lambda x: x["total"])
    active_totals = [it["total"] for it in filtered_items]
    return merged_items, filtered_items, removed_items, active_totals

def _title(title: str):
    """ IDENTICAL PIPELINE TO search_active() BUT WITH DEBUG OUTPUT.
    Includes:
    • Browse dynamic queries
    • Browse fallback queries
    • Finding dynamic queries
    • Full dedupe
    • Strict-layer info
    """
    raw_title = (title or "").strip()

    # -------------------------
    # Build structured queries
    # -------------------------
    dynamic_query = _build_dynamic_query(title)
    browse_queries = []
    if dynamic_query:
        browse_queries.append(dynamic_query)
    # Browse fallback queries
    browse_queries.extend(_build_active_fallback_queries(title))
    # Always include raw title (last resort)
    if raw_title:
        browse_queries.append(raw_title)

    # Finding queries (simpler)
    finding_queries = []
    if dynamic_query:
        finding_queries.append(dynamic_query)
    if raw_title:
        finding_queries.append(raw_title)

    merged_items = []
    seen = set()

    # =====================================================
    # 🔵 BROWSE QUERIES (HIGH ACCURACY — RETURNS GOOD EMOTION CARDS)
    # =====================================================
    for q in browse_queries:
        if not q:
            continue
        try:
            items = _fetch_active_items_browse_for_query(q, ACTIVE_LIMIT)
        except Exception:
            items = []
        for it in items:
            key = (it["title"].lower().strip(), it["total"])
            if key not in seen:
                seen.add(key)
                merged_items.append(it)

    # =====================================================
    # 🟠 FINDING QUERIES (low accuracy, but useful fallback)
    # =====================================================
    for q in finding_queries:
        if not q:
            continue
        # Log the query for GUI output
        print(f"[FINDING QUERY] {q}")
        try:
            items = _fetch_active_items_finding_for_query(q, ACTIVE_LIMIT)
        except Exception:
            items = []
        for it in items:
            key = (it["title"].lower().strip(), it["total"])
            if key not in seen:
                seen.add(key)
                merged_items.append(it)

    # =====================================================
    # STRICT FILTERING LAYER (PATCHED FOR v7 STRICT MATCHER)
    # =====================================================
    subject_sig = _extract_card_signature_from_title(title)
    subject_serial = _extract_serial_fragment(title)
    filtered_items = []
    removed_items = []

    for it in merged_items:
        comp_title = it["title"] or ""
        price = it["total"]
        reason = None

        # SERIAL MATCH
        comp_serial = _extract_serial_fragment(comp_title)
        if subject_serial and comp_serial and comp_serial != subject_serial:
            reason = (
                f"Serial mismatch: subject '{subject_serial}' vs comp '{comp_serial}'"
            )
        else:
            # STRICT MATCH v7 — correct signature call
            comp_sig = _extract_card_signature_from_title(comp_title)
            if subject_sig is not None and not _titles_match_strict(subject_sig, comp_sig):
                reason = "Failed _titles_match_strict (v7 signature mismatch)"
            # HYBRID FILTER
            elif price >= 10 and not safe_hybrid_filter(title, comp_title, price):
                reason = "Rejected by safe_hybrid_filter"

        if reason:
            removed_items.append(
                {"title": comp_title, "total": price, "reason": reason}
            )
        else:
            filtered_items.append(it)

    # =====================================================
    # FINAL NUMERIC LIST
    # =====================================================
    filtered_items.sort(key=lambda x: x["total"])
    active_totals = [it["total"] for it in filtered_items]
    return merged_items, filtered_items, removed_items, active_totals

# -------------------------------------------------------------------
# MEDIAN HELPER FOR A2 (LOWEST K MEDIAN)
# -------------------------------------------------------------------
def compute_a2_median(active_totals, k_max=5):
    act_values = sorted([float(v) for v in active_totals])
    
    if not act_values:
        return [], [], None
    
    k = min(k_max, len(act_values))
    lowest_k = act_values[:k]
    
    if not lowest_k:
        return act_values, lowest_k, None
    
    n = len(lowest_k)
    mid = n // 2
    
    if n % 2 == 1:
        median_active = round(lowest_k[mid], 2)
    else:
        median_active = round((lowest_k[mid - 1] + lowest_k[mid]) / 2, 2)
    
    return act_values, lowest_k, median_active

# -------------------------------------------------------------------
# GUI CALLBACK: RUN DEBUG FOR A GIVEN ITEM ID
# -------------------------------------------------------------------
def run_debug_for_item(item_id, output_box, learn_callback=None):
    global last_debug_data
    # CLEAR output box instead of undefined "textbox"
    output_box.delete("1.0", tk.END)
    item_id = item_id.strip()
    if not item_id:
        output_box.insert(tk.END, "❌ Please enter an Item ID.\n")
        return
    if not item_id.isdigit():
        output_box.insert(tk.END, f"❌ Invalid Item ID: '{item_id}'. Must be numeric.\n")
        return
    # 1) Fetch REAL TITLE + PRICE
    try:
        title, current_price = get_item_details(item_id)
    except Exception as e:
        output_box.insert(tk.END, f"❌ Error calling get_item_details: {e}\n")
        return
    output_box.insert(tk.END, f"🟦 ITEM ID: {item_id}\n")
    output_box.insert(tk.END, f"TITLE FROM EBAY: {title}\n")
    output_box.insert(tk.END, f"CURRENT PRICE: {current_price}\n\n")
    # 2) Capture active comps
    try:
        merged_items, filtered_items, removed_items, active_totals = debug_capture_from_title(title)
    except Exception as e:
        output_box.insert(tk.END, f"❌ Error while capturing active comps: {e}\n")
        return
    # 3) A2 median
    act_values, lowest_k, median_active = compute_a2_median(active_totals, k_max=5)
    
    # ------------------------- RAW MERGED -------------------------
    output_box.insert(tk.END, "═════════════════════════════════════════════════════\n")
    output_box.insert(tk.END, "RAW MERGED ACTIVE RESULTS (Browse + Finding)\n")
    output_box.insert(tk.END, "═════════════════════════════════════════════════════\n")
    if not merged_items:
        output_box.insert(tk.END, "No active results found.\n\n")
    else:
        for it in merged_items:
            output_box.insert(tk.END, f"[RAW] {it['title']} → ${it['total']}\n")
    
    # ------------------------- FILTERED -------------------------
    output_box.insert(tk.END, "\n═════════════════════════════════════════════════════\n")
    output_box.insert(tk.END, "FILTERED ACTIVE RESULTS (Strict matching layer)\n")
    output_box.insert(tk.END, "═════════════════════════════════════════════════════\n")
    if not filtered_items:
        output_box.insert(tk.END, "No comps survived strict filters.\n\n")
    else:
        for it in filtered_items:
            output_box.insert(tk.END, f"[FILTERED] {it['title']} → ${it['total']}\n")
    
    # ------------------------- REMOVED (reasons) -------------------------
    if show_reasons_var.get():
        output_box.insert(tk.END, "\n═════════════════════════════════════════════════════\n")
        output_box.insert(tk.END, "REMOVED COMPS — Strict Filter Reasons\n")
        output_box.insert(tk.END, "═════════════════════════════════════════════════════\n")
        if not removed_items:
            output_box.insert(tk.END, "No comps were removed by the strict layer.\n\n")
        else:
            for it in removed_items:
                output_box.insert(
                    tk.END,
                    f"[REMOVED] {it['title']} → ${it['total']}\n"
                    f" REASON: {it['reason']}\n"
                )
    
    # ------------------------- FINAL active_totals -------------------------
    output_box.insert(tk.END, "\n═════════════════════════════════════════════════════\n")
    output_box.insert(tk.END, "FINAL active_totals list (what A2 uses as input)\n")
    output_box.insert(tk.END, "═════════════════════════════════════════════════════\n")
    output_box.insert(tk.END, f"{active_totals}\n\n")
    
    # ------------------------- A2 median -------------------------
    output_box.insert(tk.END, "═════════════════════════════════════════════════════\n")
    output_box.insert(tk.END, "A2 Active Median Computation (lowest K = 5)\n")
    output_box.insert(tk.END, "═════════════════════════════════════════════════════\n")
    output_box.insert(tk.END, f"act_values = {act_values}\n")
    output_box.insert(tk.END, f"lowest_k = {lowest_k}\n")
    output_box.insert(tk.END, f"median_act = {median_active}\n\n")
    
    # ------------------------- SAVE -------------------------
    last_debug_data = {
        "timestamp": datetime.now().isoformat(timespec="seconds"),
        "item_id": item_id,
        "title": title,
        "current_price": current_price,
        "merged_items": merged_items,
        "filtered_items": filtered_items,
        "removed_items": removed_items,
        "active_totals": active_totals,
        "act_values": act_values,
        "lowest_k": lowest_k,
        "median_active": median_active,
    }