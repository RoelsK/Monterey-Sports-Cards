import os
import sys
import tkinter as tk
import threading
import csv
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple
from tkinter import scrolledtext, filedialog, messagebox, simpledialog

# -----------------------------------------------------
# FIX sys.path FIRST — absolutely must be BEFORE imports
# -----------------------------------------------------
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.abspath(os.path.join(SCRIPT_DIR, ".."))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

PRICING_DIR = os.path.join(ROOT_DIR, "pricing")
if PRICING_DIR not in sys.path:
    sys.path.insert(0, PRICING_DIR)

# -----------------------------------------------------
# NOW import pricing_engine
# -----------------------------------------------------
from pricing_engine import (
    debug_capture_from_title,
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
# RUN DEBUG FROM ITEM ID (calls pricing_engine.get_item_details)
# ----------------------------------------------------
def run_debug_for_item(item_id: str, output_box: scrolledtext.ScrolledText, learn_callback=None) -> None:
    global last_debug_data

    output_box.delete("1.0", tk.END)

    # -------------------------------------------------------------
    # FETCH ITEM DETAILS (which may return dict or tuple)
    # -------------------------------------------------------------
    try:
        details = get_item_details(item_id)
    except Exception as e:
        messagebox.showerror("Error", f"Failed to fetch item details: {e}")
        return

    # -------------------------------------------------------------
    # FIX: Normalize tuple-returning get_item_details() to a dict
    # -------------------------------------------------------------
    if isinstance(details, tuple):
        # Pattern 1: (dict, status or metadata)
        if len(details) >= 1 and isinstance(details[0], dict):
            details = details[0]
        else:
            # Pattern 2: unknown tuple → fail-safe fallback
            details = {
                "title": "",
                "price": None,
                "sku": "",
                "viewItemURL": "",
            }

    # -------------------------------------------------------------
    # Extract fields (safe now, because details is guaranteed a dict)
    # -------------------------------------------------------------
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

    # -------------------------------------------------------------
    # STRICT PIPELINE DEBUGGER
    # -------------------------------------------------------------
    import traceback

    try:
        debug_data = debug_capture_from_title(title)
        last_debug_data = debug_data
    except Exception as e:
        tb = traceback.format_exc()
        output_box.insert(tk.END, "❌ FULL DEBUG TRACEBACK:\n")
        output_box.insert(tk.END, tb + "\n")
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