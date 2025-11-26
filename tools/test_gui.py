import os
import sys
from datetime import datetime
import tkinter as tk
from tkinter import scrolledtext, filedialog, messagebox, simpledialog
import csv
import threading
import time

# ----------------------------------------------------
# FORCE-ADD ROOT TO PYTHON PATH
# ----------------------------------------------------
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.abspath(os.path.join(SCRIPT_DIR, ".."))

if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

# ----------------------------------------------------
# IMPORT YOUR REAL PRICING ENGINE ONCE
# ----------------------------------------------------
import pricing.pricing_engine as pe

# Show exactly which file is loaded
print(">>> Loaded pricing_engine from:", pe.__file__)

# GUI should be verbose; LIVE will keep DEBUG_MODE = False
pe.DEBUG_MODE = True

# -------------------------------------------------------------------
# GLOBALS FOR EXPORT
# -------------------------------------------------------------------

last_debug_data = None  # will hold the most recent debug run
show_reasons_var = None  # BooleanVar set in GUI


# -------------------------------------------------------------------
# CORE DEBUG LOGIC (WORKS FROM TITLE, USED BY ITEM-ID FLOW)
# -------------------------------------------------------------------
def debug_capture_from_title(title: str):
    """
    IDENTICAL PIPELINE TO search_active() BUT WITH DEBUG OUTPUT.
    Includes:
      • Browse dynamic queries
      • Browse fallback queries
      • Finding dynamic queries
      • Full dedupe
      • Strict-layer info
    """
    raw_title = (title or "").strip()

    # Build structured queries
    dynamic_query = pe._build_dynamic_query(title)
    browse_queries = []
    if dynamic_query:
        browse_queries.append(dynamic_query)

    browse_queries.extend(pe._build_active_fallback_queries(title))

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
            items = pe._fetch_active_items_browse_for_query(q, pe.ACTIVE_LIMIT)
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
            items = pe._fetch_active_items_finding_for_query(q, pe.ACTIVE_LIMIT)
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
    subject_sig = pe._extract_card_signature_from_title(title)
    subject_serial = pe._extract_serial_fragment(title)

    filtered_items = []
    removed_items = []

    for it in merged_items:
        comp_title = it["title"] or ""
        price = it["total"]
        reason = None

        comp_serial = pe._extract_serial_fragment(comp_title)
        if subject_serial and comp_serial and comp_serial != subject_serial:
            reason = (
                f"Serial mismatch: subject '{subject_serial}' vs comp '{comp_serial}'"
            )
        else:
            comp_sig = pe._extract_card_signature_from_title(comp_title)

            if subject_sig is not None and not pe._titles_match_strict(subject_sig, comp_sig):
                reason = "Failed _titles_match_strict (v7 signature mismatch)"

            elif price >= 10 and not pe.safe_hybrid_filter(title, comp_title, price):
                reason = "Rejected by safe_hybrid_filter"

        if reason:
            removed_items.append({
                "title": comp_title,
                "total": price,
                "reason": reason
            })
        else:
            filtered_items.append(it)

    filtered_items.sort(key=lambda x: x["total"])
    active_totals = [it["total"] for it in filtered_items]

    return merged_items, filtered_items, removed_items, active_totals


def _title(title: str):
    """
    IDENTICAL PIPELINE TO search_active() BUT WITH DEBUG OUTPUT.
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
    dynamic_query = pe._build_dynamic_query(title)
    browse_queries = []
    if dynamic_query:
        browse_queries.append(dynamic_query)

    # Browse fallback queries
    browse_queries.extend(pe._build_active_fallback_queries(title))

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
    # 🔵 BROWSE QUERIES
    # =====================================================
    for q in browse_queries:
        if not q:
            continue
        try:
            items = pe._fetch_active_items_browse_for_query(q, pe.ACTIVE_LIMIT)
        except Exception:
            items = []

        for it in items:
            key = (it["title"].lower().strip(), it["total"])
            if key not in seen:
                seen.add(key)
                merged_items.append(it)

    # =====================================================
    # 🟠 FINDING QUERIES
    # =====================================================
    for q in finding_queries:
        if not q:
            continue

        print(f"[FINDING QUERY] {q}")

        try:
            items = pe._fetch_active_items_finding_for_query(q, pe.ACTIVE_LIMIT)
        except Exception:
            items = []

        for it in items:
            key = (it["title"].lower().strip(), it["total"])
            if key not in seen:
                seen.add(key)
                merged_items.append(it)

    # =====================================================
    # STRICT FILTERING LAYER
    # =====================================================
    subject_sig = pe._extract_card_signature_from_title(title)
    subject_serial = pe._extract_serial_fragment(title)

    filtered_items = []
    removed_items = []

    for it in merged_items:
        comp_title = it["title"] or ""
        price = it["total"]
        reason = None

        # SERIAL MATCH
        comp_serial = pe._extract_serial_fragment(comp_title)
        if subject_serial and comp_serial and comp_serial != subject_serial:
            reason = (
                f"Serial mismatch: subject '{subject_serial}' vs comp '{comp_serial}'"
            )
        else:
            # STRICT MATCH v7 — correct signature call
            comp_sig = pe._extract_card_signature_from_title(comp_title)
            if subject_sig is not None and not pe._titles_match_strict(subject_sig, comp_sig):
                reason = "Failed _titles_match_strict (v7 signature mismatch)"

            # HYBRID FILTER
            elif price >= 10 and not pe.safe_hybrid_filter(title, comp_title, price):
                reason = "Rejected by safe_hybrid_filter"

        if reason:
            removed_items.append({
                "title": comp_title,
                "total": price,
                "reason": reason
            })
        else:
            filtered_items.append(it)

    # =====================================================
    # FINAL NUMERIC LIST
    # =====================================================
    filtered_items.sort(key=lambda x: x["total"])
    active_totals = [it["total"] for it in filtered_items]

    return merged_items, filtered_items, removed_items, active_totals


# Here is the function that was missing before to ensure that it is running properly
def run_debug_for_item(item_id, output_box):
    try:
        output_box.delete("1.0", tk.END)
        # Call the actual debug function
        merged_items, filtered_items, removed_items, active_totals = debug_capture_from_title(item_id)
        # Process the results
        output_box.insert(tk.END, "Merged Items:\n")
        for item in merged_items:
            output_box.insert(tk.END, f"{item['title']} - ${item['total']}\n")
        output_box.insert(tk.END, "\nFiltered Items:\n")
        for item in filtered_items:
            output_box.insert(tk.END, f"{item['title']} - ${item['total']}\n")
    except Exception as e:
        output_box.insert(tk.END, f"Error: {e}\n")

# GUI initialization
def main():
    window = tk.Tk()
    window.title("Active Comp Debugger")
    window.geometry("950x720")

    # Create a Label
    label = tk.Label(window, text="Enter eBay Item ID:", font=("Arial", 14))
    label.pack(pady=5)

    # Create an Entry widget
    entry = tk.Entry(window, font=("Arial", 14), width=30)
    entry.pack(pady=10)

    # Create a Button to trigger the debug function
    button = tk.Button(window, text="Run Debug", font=("Arial", 14), command=lambda: run_debug_for_item(entry.get(), output_box))
    button.pack(pady=5)

    # Create a scrolled text output box
    output_box = scrolledtext.ScrolledText(window, width=120, height=20, font=("Consolas", 12))
    output_box.pack(pady=10)

    # Run the GUI loop
    window.mainloop()

# Add the entry point
if __name__ == "__main__":
    main()