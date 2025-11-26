# cdp_mode.py — CDP Script (SYNCED TO pricing_engine v28)
# - STRICT MODE header handling
# - SAFE-MODE injects prices directly into original CSV (no results file)
# - FULL API MODE delegates pricing to repricer v28 strict engine
# - ONLY modifies *StartPrice
# - No fallback to BuyItNowPrice

import os
import time
import glob
import re
from typing import Any

import pandas as pd

from helpers_v10 import load_config
from pricing import pricing_engine as repricer  # v28 engine

RESET  = "\033[0m"
BOLD   = "\033[1m"
DIM    = "\033[2m"

RED    = "\033[31m"
GREEN  = "\033[32m"
YELLOW = "\033[33m"
BLUE   = "\033[34m"
MAGENTA= "\033[35m"
CYAN   = "\033[36m"
WHITE  = "\033[37m"


def clear_screen():
    os.system("cls" if os.name == "nt" else "clear")

CSV_FOLDER = r"C:\Users\Undertow\Desktop\eBayAPI\CSV"

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(BASE_DIR, "config_v24.json")
config_v24 = load_config(CONFIG_PATH)

# Hard floor in case repricer is unavailable
PRICE_FLOOR = float(getattr(repricer, "PRICE_FLOOR", 1.50))

# CDP should NOT depend on repricer’s internal pacing
DEFAULT_SLEEP = 0.40
SLEEP_BETWEEN_CALLS_SEC = DEFAULT_SLEEP

# Pricing mode resets on every run
PRICING_MODE = "FULL"

# Cached star database for Safe Mode
STAR_NAMES = [
    "TROUT","JETER","GRIFFEY","BONDS","KOBE","JORDAN","LEBRON","CURRY","BRADY","MAHOMES",
    "OTANI","OHTANI","WEMBY","WEMBANYAMA","LUKA","DONCIC","ACUNA","ELLY","DE LA CRUZ","SKENES",
]


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


# ---------------- SAFE MODE ---------------- #

def _price_card_safe_mode(gen_title: str, fallback_title: str, your_price):
    """
    Offline Safe-Mode pricing:
      - Simple tiered logic (numbered/auto/patch/parallel etc.)
      - NO API usage, no repricer calls
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
    is_star = any(x in t for x in STAR_NAMES)

    # Simple price tiers
    price = 2.49
    if is_numbered or is_auto or is_patch:
        price = 9.99
    elif is_parallel:
        price = 5.99
    elif is_rookie or is_star:
        price = 3.99
    elif is_insert:
        price = 3.99

    # Keep user's current price if reasonable
    if your_price:
        try:
            yp = float(your_price)
            if yp > price and yp <= price * 3:
                price = yp
        except Exception:
            pass

    # Floor protect
    price = max(price, PRICE_FLOOR)

    if hasattr(repricer, "_human_round"):
        price = repricer._human_round(price)

    return price


# ---------------- FULL API MODE ---------------- #

def _price_card_full_api(gen_title: str, fallback_title: str, your_price):
    """
    FULL API MODE:
      - Clean title string
      - Calls repricer v28 strict logic
      - Uses repricer active cache to reduce API calls
    """
    title = (gen_title or fallback_title or "").strip()

    if not title:
        # fallback if no title exists at all
        base = your_price if your_price is not None else PRICE_FLOOR
        if hasattr(repricer, "_human_round"):
            base = repricer._human_round(base)
        return base

    # Retrieve repricer global cache (reuse active_cache)
    active_cache = getattr(repricer, "ACTIVE_CACHE", None)

    try:
        active_limit = int(getattr(repricer, "ACTIVE_LIMIT", 15))
    except Exception:
        active_limit = 15

    try:
        sold_limit = int(getattr(repricer, "SOLD_LIMIT", 5))
    except Exception:
        sold_limit = 5

    # ACTIVE
    try:
        active_totals, act_source, supply_count = repricer.search_active(
            title,
            limit=active_limit,
            active_cache=active_cache,
        )
    except Exception as e:
        print(f"   [CDP] Active search error for '{title}': {e}")
        active_totals, act_source, supply_count = [], "CDP-active-error", 0

    # SOLD
    try:
        sold_totals, sold_source, new_cache_rows = repricer.search_sold(
            title,
            limit=sold_limit,
            cache_df=None,
        )
    except Exception as e:
        print(f"   [CDP] Sold search error for '{title}': {e}")
        sold_totals, sold_source, new_cache_rows = [], "CDP-sold-error", []

    # STRICT MODE pricing using v28
    try:
        median_sold, median_active, suggested, note = repricer.get_price_strict(
            active_totals,
            sold_totals,
            current_price=your_price,
        )
    except Exception as e:
        print(f"   [CDP] get_price_strict error for '{title}': {e}")
        suggested = None

    # Fallback if repricer suggests nothing
    if suggested is None:
        suggested = your_price if your_price is not None else PRICE_FLOOR

    try:
        suggested_f = float(suggested)
    except Exception:
        suggested_f = PRICE_FLOOR

    if hasattr(repricer, "_human_round"):
        suggested_f = repricer._human_round(suggested_f)

    return max(suggested_f, PRICE_FLOOR)


# Unified dispatcher
def _price(gen_title: str, fallback_title: str, your_price):
    if PRICING_MODE == "SAFE":
        return _price_card_safe_mode(gen_title, fallback_title, your_price)
    return _price_card_full_api(gen_title, fallback_title, your_price)


# ---------------- CSV READING ---------------- #

def read_cdp_file(path: str):
    """
    Strict CDP CSV reader — safer version
    """
    with open(path, 'r', encoding='utf-8', errors='ignore') as f:
        first_line = f.readline()

    try:
        if "Info" in first_line and "Template=" in first_line:
            df = pd.read_csv(path, header=1)
        else:
            df = pd.read_csv(path)
    except Exception as e:
        raise ValueError(f"Error reading CSV ({path}): {e}")

    cols = df.columns

    # Required columns
    if "*Title" not in cols and "Title" not in cols:
        raise ValueError("STRICT MODE: Missing Title / *Title column in header.")

    if "*StartPrice" not in cols:
        raise ValueError("STRICT MODE: Missing *StartPrice column — file invalid for CDP pricing.")

    title_col = "*Title" if "*Title" in cols else "Title"
    price_col = "*StartPrice"

    # Ensure pricing column is numeric
    df[price_col] = pd.to_numeric(df[price_col], errors='coerce')

    return df, title_col, price_col


# ---------------- PROCESSING ---------------- #

def process_file(path: str):
    df, tcol, pcol = read_cdp_file(path)
    base = os.path.basename(path)
    print(f"Processing: {base}")

    total_rows = len(df)

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

        df.at[idx, pcol] = float(new_price)

        # Full API pacing
        if PRICING_MODE == "FULL" and SLEEP_BETWEEN_CALLS_SEC > 0:
            time.sleep(SLEEP_BETWEEN_CALLS_SEC)

    df.to_csv(path, index=False)
    print(f"✔ Injected pricing directly into {base}\n")


# ---------------- MENU ---------------- #

def main():
    global PRICING_MODE
    global SLEEP_BETWEEN_CALLS_SEC

    # Reset each run
    PRICING_MODE = "FULL"
    SLEEP_BETWEEN_CALLS_SEC = DEFAULT_SLEEP

    banner_version = "v28c1"
    clear_screen()

    print("\n")
    print(f"{BOLD}{CYAN}MONTEREY SPORTS CARDS {RESET}{BOLD}{WHITE}–{RESET} {BOLD}{CYAN}CDP PRICING PANEL{RESET}")
    print(f"{BLUE}{'=' * 41}{RESET}")
    print("\n" * 0)
    print(f"{CYAN}{BOLD}Choose Pricing Mode:{RESET}")
    print(f"{BLUE}1){RESET} {BOLD}SAFE-MODE{RESET} {WHITE}(no eBay API calls){RESET}")
    print(f"{BLUE}2){RESET} {BOLD}FULL API MODE{RESET}")

    choice = input(f"{CYAN}Enter choice [default 2]:{RESET} ").strip()


    if choice == "1":
        PRICING_MODE = "SAFE"
        print(f"\n➡ Running in {BOLD}{GREEN}SAFE-MODE{RESET} (offline tiered pricing, NO API calls)...\n")
    else:
        PRICING_MODE = "FULL"
        print("\n➡ Running in FULL API MODE (live comps via repricer v28).\n")

    print(f"📂 Scanning folder: {CSV_FOLDER}")

    csv_files = sorted(glob.glob(os.path.join(CSV_FOLDER, "*.csv")))

    # -----------------------------
    # NEW: Detect missing CSV files
    # -----------------------------
    if not csv_files:
        print(f"\n{BOLD}{RED}⚠ NO CSV FILES FOUND in your CDP folder.{RESET}")
        print(f"   Location checked:\n   {CSV_FOLDER}")
        print(f"\n{BOLD}Please add one or more CSV files to this folder.{RESET}")
        input("\nPress Enter to return to the Control Panel...")
        return  # clean exit back to main menu

    print(f"📦 Found {len(csv_files)} CSV(s) to process.\n")

    for i, filepath in enumerate(csv_files, 1):
        print(f"➡️ ({i}/{len(csv_files)}) {os.path.basename(filepath)}")
        process_file(filepath)

        if SLEEP_BETWEEN_CALLS_SEC > 0:
            time.sleep(SLEEP_BETWEEN_CALLS_SEC)

    print("🎯 All done.\n")
    print("\nPress Enter to return to the Control Panel...")
    try:
        input()
        input()  # <-- Flushes leftover newline from previous input
    except:
        pass
    return


if __name__ == "__main__":
    main()