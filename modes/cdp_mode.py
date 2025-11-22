# cdp_mode.py — CDP Script (SYNCED TO pricing_engine v28c1)
# - STRICT MODE header handling
# - SAFE-MODE injects prices directly into original CSV (no results file)
# - FULL API MODE delegates pricing to update_store_prices_v26 (Browse + Finding + A2-Price-Safe)
# - ONLY modifies *StartPrice (never touches BuyItNowPrice)
# - No fallback to BuyItNowPrice

import os
import time
import glob
import re
from typing import Any

import pandas as pd

from helpers_v10 import load_config
from pricing import pricing_engine as repricer

CSV_FOLDER = r"C:\Users\Undertow\Desktop\eBayAPI\CSV"

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(BASE_DIR, "config_v24.json")
config_v9 = load_config(CONFIG_PATH)

PRICE_FLOOR = float(getattr(repricer, "PRICE_FLOOR", 1.50))
SLEEP_BETWEEN_CALLS_SEC = float(getattr(repricer, "SLEEP_BETWEEN_CALLS_SEC", 0.40))

PRICING_MODE = "FULL"  # "SAFE" or "FULL" (set at runtime in main())


def _clean_str(s: Any) -> str:
    return "" if s is None else str(s).strip()


def _read_price(v: Any):
    s = _clean_str(v)
    if not s:
        return None
    try:
        return float(s)
    except Exception:
        return None


def _get_col(row, *names, default: str = "") -> str:
    for n in names:
        if n in row and pd.notna(row[n]):
            return str(row[n]).strip()
    return default


def _price_card_safe_mode(gen_title: str, fallback_title: str, your_price):
    """
    Offline Safe-Mode pricing:
      - Simple tiered logic (numbered/auto/patch, parallels, rookies/stars, inserts, base)
      - DOES NOT call any APIs
      - Returns a safe placeholder price high enough to protect value
    """
    t = (gen_title or fallback_title or "").upper()

    is_numbered = bool(re.search(r"\b\d{1,3}/\d{1,4}\b", t)) or "#/" in t
    is_auto = any(x in t for x in [" AUTO", " AUTOGRAPH", " SIG ", " SIGNATURE"])
    is_patch = any(x in t for x in ["PATCH", "JERSEY", "RELIC", "MEM "])
    is_rookie = (" ROOKIE" in t or " RC" in t)
    is_parallel = any(x in t for x in [
        "REFRACTOR", "PRIZM", "PRISM", "HOLO", "MOJO", "MOSAIC", "CHROME", "OPTIC", "SELECT"
    ])
    is_insert = "INSERT" in t
    is_star = any(x in t for x in [
        "TROUT", "JETER", "GRIFFEY", "BONDS", "KOBE", "JORDAN", "LEBRON", "CURRY",
        "BRADY", "MAHOMES"
    ])

    price = 2.49
    if is_numbered or is_auto or is_patch:
        price = 9.99
    elif is_parallel:
        price = 5.99
    elif is_rookie or is_star:
        price = 3.99
    elif is_insert:
        price = 3.99

    if your_price:
        try:
            yp = float(your_price)
            if yp > price and yp <= price * 3:
                price = yp
        except Exception:
            pass

    if price < PRICE_FLOOR:
        price = PRICE_FLOOR

    if hasattr(repricer, "_human_round"):
        price = repricer._human_round(price)

    return price


def _price_card_full_api(gen_title: str, fallback_title: str, your_price):
    """
    FULL API MODE:
      - Build a clean title
      - Use repricer v26's merged Browse + Finding engines
      - Apply A2-Price-Safe strict engine via get_price_strict()
    """
    title = (gen_title or fallback_title or "").strip()
    if not title:
        # No usable title; fall back to your_price or floor
        base = your_price if your_price is not None else PRICE_FLOOR
        if hasattr(repricer, "_human_round"):
            base = repricer._human_round(base)
        return base

    # Use repricer v26 search helpers
    try:
        active_limit = int(getattr(repricer, "ACTIVE_LIMIT", 15))
    except Exception:
        active_limit = 15

    try:
        sold_limit = int(getattr(repricer, "SOLD_LIMIT", 5))
    except Exception:
        sold_limit = 5

    try:
        active_totals, act_source, supply_count = repricer.search_active(
            title,
            limit=active_limit,
            active_cache=None,
        )
    except Exception as e:
        print(f"   [CDP] Active search error for '{title}': {e}")
        active_totals, act_source, supply_count = [], "CDP-active-error", 0

    try:
        sold_totals, sold_source, new_cache_rows = repricer.search_sold(
            title,
            limit=sold_limit,
            cache_df=None,
        )
    except Exception as e:
        print(f"   [CDP] Sold search error for '{title}': {e}")
        sold_totals, sold_source, new_cache_rows = [], "CDP-sold-error", []

    try:
        median_sold, median_active, suggested, note = repricer.get_price_strict(
            active_totals,
            sold_totals,
            current_price=your_price,
        )
    except Exception as e:
        print(f"   [CDP] get_price_strict error for '{title}': {e}")
        median_sold, median_active, suggested, note = None, None, None, ""

    # Fallback if repricer cannot suggest a price
    if suggested is None:
        if your_price is not None:
            suggested = your_price
        else:
            suggested = PRICE_FLOOR

    try:
        suggested_f = float(suggested)
    except Exception:
        suggested_f = PRICE_FLOOR

    # Final human rounding using repricer's helper if available
    if hasattr(repricer, "_human_round"):
        suggested_f = repricer._human_round(suggested_f)

    if suggested_f < PRICE_FLOOR:
        suggested_f = PRICE_FLOOR

    return suggested_f


def _price(gen_title: str, fallback_title: str, your_price):
    if PRICING_MODE == "SAFE":
        return _price_card_safe_mode(gen_title, fallback_title, your_price)
    return _price_card_full_api(gen_title, fallback_title, your_price)


def read_cdp_file(path: str):
    """
    STRICT MODE CSV reader:
      - If first line contains CDP/eBay metadata ('Info' and 'Template='), skip it (header=1)
      - Require *Title or Title
      - REQUIRE *StartPrice (we only ever write to *StartPrice)
      - NEVER use or fall back to BuyItNowPrice
    """
    with open(path, 'r', encoding='utf-8', errors='ignore') as f:
        first_line = f.readline()

    if "Info" in first_line and "Template=" in first_line:
        df = pd.read_csv(path, header=1)
    else:
        df = pd.read_csv(path)

    cols = df.columns

    if "*Title" not in cols and "Title" not in cols:
        raise ValueError("STRICT MODE: Missing Title / *Title column in header.")

    if "*StartPrice" not in cols:
        raise ValueError("STRICT MODE: Missing *StartPrice column — file invalid for CDP pricing.")

    title_col = "*Title" if "*Title" in cols else "Title"
    price_col = "*StartPrice"

    # Ensure pricing column is numeric to avoid dtype warnings
    df[price_col] = pd.to_numeric(df[price_col], errors='coerce')

    return df, title_col, price_col


def process_file(path: str):
    df, tcol, pcol = read_cdp_file(path)
    base = os.path.basename(path)
    print(f"Processing: {base}")

    for idx, row in df.iterrows():
        title = str(row.get(tcol, "")).strip()
        if not title:
            continue

        your_price = _read_price(row.get(pcol))

        player = _get_col(row, "*C:Player/Athlete", "Player")
        year = _get_col(row, "C:Year Manufactured", "Year")
        set_name = _get_col(row, "*C:Set", "Set")
        subset = _get_col(row, "C:Insert Set", "Subset", "C:Card Name")
        cardno = _get_col(row, "*C:Card Number", "CardNumber", "Card")
        attribute = _get_col(row, "*C:Parallel/Variety", "Attribute")
        variation = _get_col(row, "Variation")
        team = _get_col(row, "*C:Team", "Team")

        parts = []
        for v in (year, set_name, player, subset):
            if v:
                parts.append(v)
        if cardno:
            parts.append(f"#{cardno}")
        for v in (team, attribute, variation):
            if v:
                parts.append(v)

        gen_title = " ".join(parts).strip()

        new_price = _price(gen_title, title, your_price)

        # Write back ONLY into *StartPrice
        df.at[idx, pcol] = float(new_price)

        # Respect global pacing between cards for API safety
        if PRICING_MODE == "FULL" and SLEEP_BETWEEN_CALLS_SEC > 0:
            time.sleep(SLEEP_BETWEEN_CALLS_SEC)

    df.to_csv(path, index=False)
    print(f"✔ Injected pricing directly into {base}\n")


def main():
    global PRICING_MODE

    banner_version = "v28c1"
    print(f"Monterey Sports Cards – CDP Pricing {banner_version} (STRICT MODE, synced to repricer v26)\n")
    print("Choose Pricing Mode:")
    print("  1) SAFE-MODE (no eBay API calls)")
    print("  2) FULL API MODE (Browse + Finding via update_store_prices_v26)")
    choice = input("Enter 1 or 2 [default = 2]: ").strip()
    if choice == "1":
        PRICING_MODE = "SAFE"
        print("\n➡ Running in SAFE-MODE (offline tiered pricing, NO API calls).\n")
    else:
        PRICING_MODE = "FULL"
        print("\n➡ Running in FULL API MODE (live comps via update_store_prices_v26 v26).\n")

    print(f"📂 Scanning folder: {CSV_FOLDER}")

    csv_files = sorted(glob.glob(os.path.join(CSV_FOLDER, "*.csv")))
    if not csv_files:
        print("⚠ No CSV files found.\n")
        return

    print(f"📦 Found {len(csv_files)} CSV(s) to process.\n")

    for i, filepath in enumerate(csv_files, 1):
        print(f"➡️ ({i}/{len(csv_files)}) {os.path.basename(filepath)}")
        process_file(filepath)
        # Small delay between files (separate from per-row pacing)
        if SLEEP_BETWEEN_CALLS_SEC > 0:
            time.sleep(SLEEP_BETWEEN_CALLS_SEC)

    print("🎯 All done.\n")


if __name__ == "__main__":
    main()
