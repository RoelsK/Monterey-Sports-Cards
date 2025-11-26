#!/usr/bin/env python3
"""
test_active_comps_gui.py

Tkinter GUI to debug active comps for a single item.

Features:
- Runs from ItemID (via pricing_engine.get_item_details)
- Uses your live pricing_engine strict pipeline:
  * _build_dynamic_query
  * _build_active_fallback_queries
  * _fetch_active_items_browse_for_query
  * _fetch_active_items_finding_for_query
  * _extract_card_signature_from_title
  * _extract_serial_fragment
  * _titles_match_strict
  * safe_hybrid_filter
- Shows:
  * RAW merged actives (Browse + Finding)
  * STRICT filtered results
  * REMOVED comps with reasons
  * FINAL active_totals list
  * A2 active-median (lowest K = 5)
"""

import os
import sys
from datetime import datetime
import tkinter as tk
from tkinter import scrolledtext, filedialog, messagebox, simpledialog
import csv
import threading
import time
from typing import List, Dict, Any, Optional, Tuple

# ----------------------------------------------------
# FORCE-ADD ROOT & PRICING DIRECTORY TO PYTHON PATH
# ----------------------------------------------------
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.abspath(os.path.join(SCRIPT_DIR, "."))

if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

PRICING_DIR = os.path.join(ROOT_DIR, "pricing")
if PRICING_DIR not in sys.path:
    sys.path.insert(0, PRICING_DIR)

# ----------------------------------------------------
# IMPORT FROM pricing_engine
# (uses module name "pricing_engine", NOT "pricing.pricing_engine")
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

# ----------------------------------------------------
# GLOBALS FOR EXPORT / LEARN
# ----------------------------------------------------
last_debug_data: Optional[Dict[str, Any]] = None
show_reasons_var: Optional[tk.BooleanVar] = None


# ----------------------------------------------------
# A2 ACTIVE MEDIAN (lowest-K)
# ----------------------------------------------------
def compute_a2_median(active_totals: List[float], k_max: int = 5) -> Tuple[List[float], List[float], Optional[float]]:
    """
    A2-Price-Safe active median:
    - Sort all active prices
    - Take lowest K (up to k_max)
    - Median of that slice
    Returns: (all_sorted_values, lowest_k_slice, median_or_None)
    """
    act_values = sorted([float(v) for v in active_totals]) if active_totals else []
    if not act_values:
        return [], [], None

    k = min(k_max, len(act_values))
    lowest_k = act_values[:k]
    if not lowest_k:
        return act_values, [], None

    n = len(lowest_k)
    mid = n // 2
    if n % 2 == 1:
        median_active = round(lowest_k[mid], 2)
    else:
        median_active = round((lowest_k[mid - 1] + lowest_k[mid]) / 2, 2)

    return act_values, lowest_k, median_active


# ----------------------------------------------------
# CORE DEBUG CAPTURE FROM TITLE
# ----------------------------------------------------
def debug_capture_from_title(title: str) -> Dict[str, Any]:
    """
    IDENTICAL PIPELINE TO search_active() BUT WITH DEBUG OUTPUT.

    Returns a dict:
    {
      "raw_title": ...,
      "dynamic_query": ...,
      "browse_queries": [...],
      "finding_queries": [...],
      "raw_items": [...],
      "filtered_items": [...],
      "removed_items": [...],
      "active_totals": [...],
      "act_values": [...],
      "lowest_k": [...],
      "median_active": float or None,
    }
    """
    raw_title = (title or "").strip()

    # -------------------------
    # Build structured queries
    # -------------------------
    dynamic_query = _build_dynamic_query(raw_title)
    browse_queries: List[str] = []
    if dynamic_query:
        browse_queries.append(dynamic_query)

    # Browse fallback queries
    browse_queries.extend(_build_active_fallback_queries(raw_title))

    # Always include raw title as last resort
    if raw_title:
        browse_queries.append(raw_title)

    # Finding queries (simpler)
    finding_queries: List[str] = []
    if dynamic_query:
        finding_queries.append(dynamic_query)
    if raw_title:
        finding_queries.append(raw_title)

    # Dedupe while preserving order
    def _dedupe(seq: List[str]) -> List[str]:
        seen_local = set()
        out = []
        for q in seq:
            q_norm = (q or "").strip()
            if not q_norm:
                continue
            if q_norm.lower() in seen_local:
                continue
            seen_local.add(q_norm.lower())
            out.append(q_norm)
        return out

    browse_queries = _dedupe(browse_queries)
    finding_queries = _dedupe(finding_queries)

    merged_items: List[Dict[str, Any]] = []
    seen_keys = set()

    # ==========================================
    # 🔵 BROWSE QUERIES (high accuracy)
    # ==========================================
    for q in browse_queries:
        if not q:
            continue
        try:
            items = _fetch_active_items_browse_for_query(q, ACTIVE_LIMIT)
        except Exception:
            items = []

        for it in items:
            key = ((it.get("title") or "").lower().strip(), float(it.get("total") or 0))
            if key in seen_keys:
                continue
            seen_keys.add(key)
            merged_items.append({"title": it.get("title"), "total": float(it.get("total"))})

    # ==========================================
    # 🟠 FINDING QUERIES (fallback)
    # ==========================================
    for q in finding_queries:
        if not q:
            continue
        try:
            items = _fetch_active_items_finding_for_query(q, ACTIVE_LIMIT)
        except Exception:
            items = []

        for it in items:
            key = ((it.get("title") or "").lower().strip(), float(it.get("total") or 0))
            if key in seen_keys:
                continue
            seen_keys.add(key)
            merged_items.append({"title": it.get("title"), "total": float(it.get("total"))})

    # ==========================================
    # STRICT FILTERING LAYER (v7/v8 strict matcher)
    # Mirrors the patched block in pricing_engine.py
    # ==========================================
    subject_sig = _extract_card_signature_from_title(raw_title)
    subject_serial = _extract_serial_fragment(raw_title)

    filtered_items: List[Dict[str, Any]] = []
    removed_items: List[Dict[str, Any]] = []

    for it in merged_items:
        comp_title = it.get("title") or ""
        price = float(it.get("total") or 0.0)
        reason = None

        # SERIAL MATCH
        comp_serial = _extract_serial_fragment(comp_title)
        if subject_serial and comp_serial and comp_serial != subject_serial:
            reason = f"Serial mismatch: subject '{subject_serial}' vs comp '{comp_serial}'"
        else:
            # STRICT MATCH v7 — correct signature call
            comp_sig = _extract_card_signature_from_title(comp_title)
            if subject_sig is not None and not _titles_match_strict(subject_sig, comp_sig):
                reason = "Failed _titles_match_strict (v7 signature mismatch)"
            # HYBRID FILTER (premium guardrail)
            elif price >= 10 and not safe_hybrid_filter(raw_title, comp_title, price):
                reason = "Rejected by safe_hybrid_filter"

        if reason:
            removed_items.append(
                {
                    "title": comp_title,
                    "total": price,
                    "reason": reason,
                }
            )
        else:
            filtered_items.append({"title": comp_title, "total": price})

    active_totals = [float(it["total"]) for it in filtered_items]
    act_values, lowest_k, median_active = compute_a2_median(active_totals, k_max=5)

    return {
        "raw_title": raw_title,
        "dynamic_query": dynamic_query,
        "browse_queries": browse_queries,
        "finding_queries": finding_queries,
        "raw_items": merged_items,
        "filtered_items": filtered_items,
        "removed_items": removed_items,
        "active_totals": active_totals,
        "act_values": act_values,
        "lowest_k": lowest_k,
        "median_active": median_active,
    }


# ----------------------------------------------------
# RUN DEBUG FROM ITEM ID (calls pricing_engine.get_item_details)
# ----------------------------------------------------
def run_debug_for_item(item_id: str, output_box: scrolledtext.ScrolledText, learn_callback=None) -> None:
    global last_debug_data

    output_box.delete("1.0", tk.END)

    try:
        details = get_item_details(item_id)
    except Exception as e:
        messagebox.showerror("Error", f"Failed to fetch item details: {e}")
        return

    title = details.get("title") or ""
    current_price = details.get("price")
    sku = details.get("sku") or ""
    item_url = details.get("viewItemURL") or ""

    # Header
    header = (
        f"🟦 ITEM ID: {item_id}\n"
        f"TITLE FROM EBAY: {title}\n"
        f"CURRENT PRICE: {current_price}\n"
        f"SKU: {sku}\n"
        f"URL: {item_url}\n\n"
    )
    output_box.insert(tk.END, header)

    try:
        debug_data = debug_capture_from_title(title)
        last_debug_data = debug_data
    except Exception as e:
        output_box.insert(tk.END, f"❌ Error while capturing active comps: {e}\n")
        output_box.see(tk.END)
        return

    # ---- RAW MERGED RESULTS ----
    output_box.insert(
        tk.END,
        "══════════════════════════════════════════════\n"
        "RAW MERGED ACTIVE RESULTS (Browse + Finding)\n"
        "══════════════════════════════════════════════\n",
    )

    raw_items = debug_data["raw_items"]
    if not raw_items:
        output_box.insert(tk.END, "No active results found.\n\n")
    else:
        for it in raw_items:
            output_box.insert(
                tk.END,
                f"[RAW] {it['title']} → ${it['total']}\n",
            )
        output_box.insert(tk.END, "\n")

    # ---- FILTERED RESULTS ----
    output_box.insert(
        tk.END,
        "══════════════════════════════════════════════\n"
        "FILTERED ACTIVE RESULTS (Strict matching layer)\n"
        "══════════════════════════════════════════════\n",
    )

    filtered_items = debug_data["filtered_items"]
    if not filtered_items:
        output_box.insert(tk.END, "No comps survived strict filters.\n\n")
    else:
        for it in filtered_items:
            output_box.insert(
                tk.END,
                f"[FILTERED] {it['title']} → ${it['total']}\n",
            )
        output_box.insert(tk.END, "\n")

    # ---- REMOVED COMPS ----
    output_box.insert(
        tk.END,
        "══════════════════════════════════════════════\n"
        "REMOVED COMPS — Strict Filter Reasons\n"
        "══════════════════════════════════════════════\n",
    )

    removed_items = debug_data["removed_items"]
    if not removed_items:
        output_box.insert(tk.END, "No comps were removed by the strict layer.\n\n")
    else:
        for it in removed_items:
            output_box.insert(
                tk.END,
                f"[REMOVED] {it['title']} → ${it['total']}\n"
                f"   REASON: {it['reason']}\n",
            )
        output_box.insert(tk.END, "\n")

    # ---- FINAL ACTIVE TOTALS / A2 MEDIAN ----
    output_box.insert(
        tk.END,
        "══════════════════════════════════════════════\n"
        "FINAL active_totals list (what A2 uses as input)\n"
        "══════════════════════════════════════════════\n",
    )

    active_totals = debug_data["active_totals"]
    output_box.insert(tk.END, f"{active_totals}\n\n")

    output_box.insert(
        tk.END,
        "══════════════════════════════════════════════\n"
        "A2 Active Median Computation (lowest K = 5)\n"
        "══════════════════════════════════════════════\n",
    )
    output_box.insert(
        tk.END,
        f"act_values = {debug_data['act_values']}\n"
        f"lowest_k = {debug_data['lowest_k']}\n"
        f"median_act = {debug_data['median_active']}\n\n",
    )

    output_box.insert(tk.END, "..................................\n")
    output_box.see(tk.END)

    # Optional learning hook
    if learn_callback is not None:
        try:
            learn_callback(debug_data)
        except Exception:
            pass


# ----------------------------------------------------
# EXPORT TO CSV
# ----------------------------------------------------
def export_results_to_csv():
    global last_debug_data
    if not last_debug_data:
        messagebox.showinfo("Export", "No debug data to export yet.")
        return

    save_path = filedialog.asksaveasfilename(
        defaultextension=".csv",
        filetypes=[("CSV Files", "*.csv")],
        title="Save filtered comps to CSV",
    )
    if not save_path:
        return

    rows = last_debug_data["filtered_items"]
    fieldnames = ["title", "total"]

    try:
        with open(save_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for r in rows:
                writer.writerow({"title": r["title"], "total": r["total"]})
        messagebox.showinfo("Export", f"Exported {len(rows)} rows to:\n{save_path}")
    except Exception as e:
        messagebox.showerror("Export Error", f"Failed to export CSV:\n{e}")


# ----------------------------------------------------
# TKINTER GUI
# ----------------------------------------------------
def run_gui():
    global show_reasons_var

    root = tk.Tk()
    root.title("MSC – Active Comps Strict Debugger")

    # Top frame (ItemID input + button)
    top_frame = tk.Frame(root)
    top_frame.pack(side=tk.TOP, fill=tk.X, padx=8, pady=4)

    tk.Label(top_frame, text="Item ID:").pack(side=tk.LEFT)
    item_entry = tk.Entry(top_frame, width=20)
    item_entry.pack(side=tk.LEFT, padx=4)

    def _run_thread():
        item_id = item_entry.get().strip()
        if not item_id:
            messagebox.showwarning("Input", "Please enter an Item ID.")
            return

        def worker():
            run_debug_for_item(item_id, output_box)

        t = threading.Thread(target=worker, daemon=True)
        t.start()

    tk.Button(top_frame, text="Run Debug", command=_run_thread).pack(side=tk.LEFT, padx=6)
    tk.Button(top_frame, text="Export CSV", command=export_results_to_csv).pack(side=tk.LEFT, padx=6)

    # Center: output
    output_box = scrolledtext.ScrolledText(root, width=120, height=40)
    output_box.pack(side=tk.TOP, fill=tk.BOTH, expand=True, padx=8, pady=4)

    # Status bar
    status_var = tk.StringVar(value="Ready.")
    status_label = tk.Label(root, textvariable=status_var, anchor="w")
    status_label.pack(side=tk.BOTTOM, fill=tk.X)

    root.mainloop()


if __name__ == "__main__":
    run_gui()