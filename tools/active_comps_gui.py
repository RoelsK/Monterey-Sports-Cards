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
    """
    IDENTICAL PIPELINE TO search_active() BUT WITH DEBUG OUTPUT.

    Returns:
        merged_items   – all unique active comps (Browse + Finding)
        filtered_items – comps that survived strict filters
        removed_items  – comps rejected, with reasons
        active_totals  – numeric list of prices used by A2
    """

    # -------------------------
    # Normalize SUBJECT title (dict → string safe)
    # -------------------------
    if isinstance(title, dict):
        subj_title = title.get("value") or title.get("text") or ""
    else:
        subj_title = str(title or "")

    raw_title = subj_title.strip()

    # -------------------------
    # Build structured queries
    # -------------------------
    dynamic_query = _build_dynamic_query(subj_title)
    browse_queries = []
    if dynamic_query:
        browse_queries.append(dynamic_query)

    # Browse fallback queries
    browse_queries.extend(_build_active_fallback_queries(subj_title))

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
    seen_keys = set()

    # =====================================================
    # 🔵 BROWSE QUERIES
    # =====================================================
    for q in browse_queries:
        if not q:
            continue
        try:
            items = _fetch_active_items_browse_for_query(q, ACTIVE_LIMIT)
        except Exception:
            items = []

        for it in items:
            # Normalize comp title (dict → string)
            raw_comp = it.get("title")
            if isinstance(raw_comp, dict):
                comp_title = raw_comp.get("value") or raw_comp.get("text") or ""
            else:
                comp_title = str(raw_comp or "")

            key = (comp_title.lower().strip(), it.get("total"))

            if key in seen_keys:
                continue
            seen_keys.add(key)

            # Overwrite title so downstream is always a string
            it["title"] = comp_title
            merged_items.append(it)

    # =====================================================
    # 🟠 FINDING QUERIES
    # =====================================================
    for q in finding_queries:
        if not q:
            continue

        # Log to GUI / console
        print(f"[FINDING QUERY] {q}")

        try:
            items = _fetch_active_items_finding_for_query(q, ACTIVE_LIMIT)
        except Exception:
            items = []

        for it in items:
            raw_comp = it.get("title")
            if isinstance(raw_comp, dict):
                comp_title = raw_comp.get("value") or raw_comp.get("text") or ""
            else:
                comp_title = str(raw_comp or "")

            key = (comp_title.lower().strip(), it.get("total"))

            if key in seen_keys:
                continue
            seen_keys.add(key)

            it["title"] = comp_title
            merged_items.append(it)

    # If nothing survived, short-circuit
    if not merged_items:
        return [], [], [], []

    # =====================================================
    # STRICT FILTERING (MATCHES search_active)
    # =====================================================
    subject_sig = _extract_card_signature_from_title(subj_title)
    subject_serial = _extract_serial_fragment(subj_title)

    filtered_items = []
    removed_items = []

    for it in merged_items:
        comp_title = it["title"] or ""
        price = it["total"]
        reason = None

        # SERIAL HANDLING: only enforce when subject itself is serial-numbered
        comp_serial = _extract_serial_fragment(comp_title)
        if subject_serial and comp_serial and comp_serial != subject_serial:
            reason = (
                f"Serial mismatch: subject '{subject_serial}' vs comp '{comp_serial}'"
            )
        else:
            # STRICT MATCH v7 — NOTE: _titles_match_strict expects (subject_sig, comp_title, comp_price)
            if subject_sig is not None and not _titles_match_strict(subject_sig, comp_title, price):
                reason = "Failed _titles_match_strict (v7 signature mismatch)"

            # HYBRID FILTER – only for higher price comps
            elif price >= 10:
                if not safe_hybrid_filter(subj_title, comp_title, price):
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

    if active_totals:
        pass
    
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

# -------------------------------------------------------------------
# EXPORT LOGIC (CSV or TXT)
# -------------------------------------------------------------------
def export_results():
    global last_debug_data
    if not last_debug_data:
        messagebox.showinfo("Export Results", "No debug results to export yet.")
        return

    # Ask format CSV / TXT
    choice = messagebox.askquestion(
        "Export Format", "Export as CSV?\n\nYes = CSV\nNo = TXT"
    )
    if choice == "yes":
        fmt = "csv"
    else:
        fmt = "txt"

    # Ask file path
    default_name = f"msc_active_debug_{last_debug_data['item_id']}_{fmt}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    if fmt == "csv":
        file_path = filedialog.asksaveasfilename(
            title="Save CSV Debug File",
            defaultextension=".csv",
            initialfile=default_name,
            filetypes=[("CSV files", "*.csv"), ("All files", "*.*")]
        )
    else:
        file_path = filedialog.asksaveasfilename(
            title="Save TXT Debug File",
            defaultextension=".txt",
            initialfile=default_name,
            filetypes=[("Text files", "*.txt"), ("All files", "*.*")]
        )

    if not file_path:
        return  # user cancelled

    try:
        if fmt == "csv":
            _export_csv(file_path, last_debug_data)
        else:
            _export_txt(file_path, last_debug_data)
        messagebox.showinfo("Export Results", f"Exported debug data to:\n{file_path}")
    except Exception as e:
        messagebox.showerror("Export Error", f"Failed to export: {e}")

# -------------------------------------------------------------------
# on_run() — CLICK HANDLER
# -------------------------------------------------------------------
def on_run(item_id, output_box):
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

# Additional export logic methods (_export_csv, _export_txt)
#...
# GUI SETUP & mainloop
# GUI SETUP & mainloop
def main():
    global show_reasons_var
    window = tk.Tk()
    window.title("MSC — Active Comp Debugger")
    window.geometry("950x720")

    # ------------------------------ # TOP INPUT FRAME # ------------------------------
    top_frame = tk.Frame(window)
    top_frame.pack(pady=10)  # Packing the frame to the window

    label = tk.Label(top_frame, text="Enter eBay Item ID to inspect:", font=("Arial", 14))
    label.grid(row=0, column=0, padx=5, pady=5, sticky="w")

    entry = tk.Entry(top_frame, font=("Arial", 14), width=25)
    entry.grid(row=0, column=1, padx=5, pady=5)

    # Checkbox: show strict filter reasons
    show_reasons_var = tk.BooleanVar(value=True)
    reasons_cb = tk.Checkbutton(
        top_frame, text="Show strict filter reasons", variable=show_reasons_var, font=("Arial", 11)
    )
    reasons_cb.grid(row=1, column=0, columnspan=2, pady=5, sticky="w")

    # ------------------------------ # BUTTON FRAME # ------------------------------
    # BUTTON FRAME
    btn_frame = tk.Frame(window)
    btn_frame.pack(pady=5)  # Ensure it's packed properly

    # Run Button (fixed)
    run_button = tk.Button(
        btn_frame,
        text="Run Active Comp Debug",
        font=("Arial", 13),
        command=lambda: on_run(entry.get(), output_box)
    )
    run_button.grid(row=0, column=0, padx=10)  # Use grid() to avoid conflict with pack()

    # Export Button
    export_button = tk.Button(
        btn_frame,
        text="Export Results...",
        font=("Arial", 13),
        command=export_results
    )
    export_button.grid(row=0, column=1, padx=10)

    
    
    
    
    #btn_frame = tk.Frame(window)
    #btn_frame.pack(pady=5)

    #run_button = tk.Button(btn_frame, text="Run Active Comp Debug", font=("Arial", 13),
    #command=lambda: on_run(entry.get(), output_box))

    #export_button = tk.Button(btn_frame, text="Export Results…", font=("Arial", 13), command=export_results)
    #export_button.grid(row=0, column=1, padx=10)

    # ------------------------------ # OUTPUT TEXT BOX # ------------------------------
    output_box = scrolledtext.ScrolledText(window, width=110, height=30, font=("Consolas", 10))
    output_box.pack(pady=10)

    # ------------------------------ # SCROLLED TEXT BOX FOR TOKEN-RULE LEARNING OUTPUT # ------------------------------
    learn_box = scrolledtext.ScrolledText(window, width=110, height=10, font=("Consolas", 10), bg="#111111", fg="#00ff66")
    learn_box.pack(pady=5)

    # ------------- Start the Tkinter Event Loop -------------
    window.mainloop()  # Starts Tkinter event loop

if __name__ == "__main__":
    main()