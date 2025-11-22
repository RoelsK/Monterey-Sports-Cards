import os
import sys
import time
import math
import json
import pandas as pd
import requests
from dotenv import load_dotenv
from typing import List, Optional, Tuple, Dict
from datetime import datetime, timedelta, timezone
import re
import glob
import hashlib
import csv as csv_mod  # for robust CSV quoting if needed

VERSION = "v28"

# ===============================
# API USAGE METERS (GLOBAL)
# ===============================
BROWSE_CALLS = 0
REVISE_CALLS = 0
REVISE_SAVED = 0

RATE_LIMITS = {
    "browse": {"limit": None, "used": None, "remaining": None, "reset": None},
    "finding": {"limit": None, "used": None, "remaining": None, "reset": None},
    "trading": {"limit": None, "used": None, "remaining": None, "reset": None},
    "other": {"limit": None, "used": None, "remaining": None, "reset": None},
}

from helpers_v10 import (
    load_config,
    load_active_cache,
    save_active_cache,
    maybe_use_active_cache,
    update_active_cache,
    safe_hybrid_filter,
    adjust_price_with_enhancements,
)

# ================= CONFIG =================
DRY_RUN = True  # default to TEST mode (no live updates)
SANDBOX_MODE = False  # when True, run in sandbox (no autosave, no cache writes, no revise calls)
PRICE_FLOOR = 1.50
PERCENT_THRESHOLD = 10          # % change threshold for updates
BATCH_SIZE = 1000
COMPETITIVE_UNDERCUT = 1.00     # cap vs medians (1.00 = equal, <1.0 = undercut)

# === PATH CONFIGURATION (Monterey Sports Cards) ==========================
# BASE_DIR = project root, one level above the /pricing package.
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Folder for manual ID inputs (item_ids.txt, etc.)
IDS_FOLDER = os.path.join(BASE_DIR, "ids")

# Logs folder
REPORT_FOLDER = os.path.join(BASE_DIR, "logs")

# Core data folders
CACHE_FOLDER = os.path.join(BASE_DIR, "cache")
AUTOSAVE_FOLDER = os.path.join(BASE_DIR, "autosave")
RESULTS_FOLDER = os.path.join(BASE_DIR, "results")

# Config + cache files
CONFIG_PATH = os.path.join(BASE_DIR, "config_v24.json")
ACTIVE_CACHE_PATH = os.path.join(CACHE_FOLDER, "active_cache.json")

# Paths for full-store ID caching and resume support
FULL_STORE_IDS_PATH = os.path.join(CACHE_FOLDER, "full_store_ids.json")
# (Optional) position file if you ever switch away from autosave-based resume
FULL_STORE_POSITION_PATH = os.path.join(CACHE_FOLDER, "full_store_position.txt")



COMPARE_MODE = "HYBRID"
MARKETPLACE_ID = "EBAY_US"
EBAY_BROWSE_SEARCH = "https://api.ebay.com/buy/browse/v1/item_summary/search"
EBAY_FINDING_API = "https://svcs.ebay.com/services/search/FindingService/v1"
EBAY_UPDATE_URL = "https://api.ebay.com/sell/inventory/v1/offer/"

ACTIVE_LIMIT = 15
SOLD_LIMIT = 5
LOWEST_ACTIVE_AVG_K = 5
MIN_ACTIVE_SAMPLES = 3
MIN_SOLD_SAMPLES = 1
SOLD_LOOKBACK_DAYS = 45

ACTIVE_TIMEOUT = 20
SOLD_TIMEOUT = 20
SLEEP_BETWEEN_CALLS_SEC = 0.40

RATE_LIMIT_FALLBACK_SLEEP_SEC = 1800
MAX_SINGLE_COOLDOWN_SEC = 3600

# Load V24 config
config_v24 = load_config(CONFIG_PATH)
ACTIVE_CACHE_TTL_MIN = int(config_v24.get("active_cache_ttl_minutes", 720))

# ================= LOAD TOKEN =================
load_dotenv()
OAUTH_TOKEN = os.getenv("EBAY_OAUTH_TOKEN", "").strip()
APP_ID = os.getenv("EBAY_APP_ID", "").strip()

if not OAUTH_TOKEN.startswith("v^"):
    print("Warning: Invalid or missing token in .env")
if not APP_ID:
    print("Warning: Missing EBAY_APP_ID in .env")


# ================= UTILITIES =================
def _headers(for_update: bool = False) -> dict:
    base = {
        "Authorization": f"Bearer {OAUTH_TOKEN}",
        "Content-Type": "application/json",
    }
    if not for_update:
        base["X-EBAY-C-MARKETPLACE-ID"] = MARKETPLACE_ID
    return base


def _safe_float(v, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return default


def _is_graded(title: str) -> bool:
    if not title:
        return False
    title = title.upper()
    return any(term in title for term in ["PSA", "BGS", "SGC", "CGC", "CSG", "GMA", "BCCG"])


# Dynamic exclusion terms (to avoid high-end/graded comps when title isn't graded)
EXCLUDE_TERMS = ["auto", "autograph", "signature", "graded", "psa", "bgs", "sgc", "serial"]


def _extract_serial_fragment(title: Optional[str]) -> Optional[str]:
    if not title:
        return None
    # #158/199, #009/100, etc.
    m = re.search(r"#\s*([0-9A-Za-z]+/[0-9A-Za-z]+)", title)
    if m:
        return m.group(1).strip().lower()
    # 158/199, 01/100, etc.
    m2 = re.search(r"\b([0-9]{1,3}/[0-9]{1,4})\b", title)
    if m2:
        return m2.group(1).strip().lower()
    return None


def _build_dynamic_query(title: str) -> str:
    if not title:
        return ""

    raw = title.strip()
    tokens = raw.split()

    # Year
    year_idx = None
    for i, tok in enumerate(tokens):
        if tok.isdigit() and len(tok) == 4:
            y = int(tok)
            if 1950 <= y <= 2035:
                year_idx = i
                break

    # Card number marker
    hash_idx = None
    for i, tok in enumerate(tokens):
        tlow = tok.lower()
        if tlow.startswith("#") or tlow in ("no.", "no"):
            hash_idx = i
            break

    card_num = None
    cut_idx = None
    if hash_idx is not None:
        tok = tokens[hash_idx]
        tlow = tok.lower()
        if tok.startswith("#") and len(tok) > 1:
            card_num = tok[1:]
            cut_idx = hash_idx + 1
        elif tlow in ("no.", "no") and hash_idx + 1 < len(tokens):
            next_tok = tokens[hash_idx + 1]
            if re.match(r"^[0-9]+[a-zA-Z]*$", next_tok):
                card_num = next_tok
                cut_idx = hash_idx + 2
        else:
            cut_idx = hash_idx + 1
    else:
        cut_idx = len(tokens)

    if year_idx is not None:
        player_tokens = tokens[:year_idx]
        mid_tokens = tokens[year_idx:cut_idx]
        rest_tokens = tokens[cut_idx:]
        year_str = tokens[year_idx]
    else:
        player_tokens = tokens[:2]
        mid_tokens = tokens[2:cut_idx]
        rest_tokens = tokens[cut_idx:]
        year_str = ""

    player = " ".join(player_tokens).strip()
    mid_str = " ".join(mid_tokens).strip()

    safe_parallels = {
        "refractor", "xfractor", "x-fractor",
        "prizm", "prism", "mosaic",
        "silver", "gold", "orange", "red", "blue", "green", "pink", "purple", "black", "white",
        "atomic", "holo", "holoview",
        "die-cut", "diecut",
        "laser", "mojo", "velocity",
        "optic", "chrome", "finest",
        "steel", "acetate", "shimmer", "wave", "checkerboard",
        "invincible", "gems", "masterpiece", "showdown", "showcase", "touchdown", "kings",
        "air", "command", "raid", "united", "stand",
    }

    parallels = []
    for tok in mid_tokens:
        low = tok.lower().strip(",./-")
        if low in safe_parallels:
            parallels.append(tok)

    for tok in rest_tokens:
        low = tok.lower().strip(",./-")
        if low in safe_parallels:
            parallels.append(tok)

    seen = set()
    uniq_parallels = []
    for p in parallels:
        pl = p.lower()
        if pl not in seen:
            seen.add(pl)
            uniq_parallels.append(p)

    parts = []
    if player:
        parts.append(player)
    if year_str:
        parts.append(year_str)
    if mid_str:
        parts.append(mid_str)
    if uniq_parallels:
        parts.append(" ".join(uniq_parallels))
    if card_num:
        parts.append(f"#{card_num}")

    base_query = " ".join(parts).strip()
    if not base_query:
        base_query = raw

    lower_title = raw.lower()

    exclude_terms = set(EXCLUDE_TERMS + [
        "cgc", "csg", "gma", "bccg",
        "lot", "lots", "set", "sets", "break", "box", "case",
        "jersey", "patch", "team", "sealed", "factory", "complete", "pack"
    ])

    excludes = []
    for term in exclude_terms:
        if term not in lower_title:
            excludes.append(f"-{term}")

    query = f"{base_query} {' '.join(excludes)}".strip()
    return query


def _extract_total_price(item: dict) -> Optional[float]:
    if "price" not in item:
        return None
    price = _safe_float(item["price"].get("value", 0))

    if price < 0.99:
        return None

    ship = 0.0
    if "shippingOptions" in item:
        costs = [
            _safe_float(opt.get("shippingCost", {}).get("value", 0))
            for opt in item.get("shippingOptions", [])
        ]
        if costs:
            ship = min(costs)

    total = price + ship

    if total < PRICE_FLOOR:
        return None
    if total > 100.0:
        return None

    return round(total, 2)


def _extract_total_price_finding(item: dict) -> Optional[float]:
    price_obj = (item.get("sellingStatus") or {}).get("currentPrice") or {}
    price_val = price_obj.get("__value__") or price_obj.get("value")
    price = _safe_float(price_val, 0.0)
    if price < 0.99:
        return None

    ship = 0.0
    ship_info = item.get("shippingInfo") or {}
    ship_cost = ship_info.get("shippingServiceCost") or {}
    ship_val = ship_cost.get("__value__") or ship_cost.get("value")
    if ship_val is not None:
        ship = _safe_float(ship_val, 0.0)

    total = price + ship
    if total < PRICE_FLOOR:
        return None
    if total > 100.0:
        return None
    return round(total, 2)


def _request(
    method: str,
    url: str,
    *,
    headers=None,
    params=None,
    data=None,
    timeout: int = 20,
    label: str = "API"
):
    try:
        r = requests.request(method=method, url=url, headers=headers, params=params, data=data, timeout=timeout)
    except Exception as e:
        print(f"   [{label}] Exception {e.__class__.__name__}: {e}")
        return None, None
    if _handle_rate_limit_and_token(r, label):
        try:
            r = requests.request(method=method, url=url, headers=headers, params=params, data=data, timeout=timeout)
            _handle_rate_limit_and_token(r, label)
        except Exception as e:
            print(f"   [{label}] Exception after cooldown: {e}")
            return None, None
    return r, r.headers if r else (None, None)


def api_meter_browse():
    global BROWSE_CALLS
    BROWSE_CALLS += 1


def api_meter_revise():
    global REVISE_CALLS
    REVISE_CALLS += 1


def api_meter_revise_saved():
    global REVISE_SAVED
    REVISE_SAVED += 1



def print_rate_limit_snapshot():
    """Pretty-print the current in-memory rate-limit snapshot for all APIs."""
    def _fmt(key: str, label: str):
        info = RATE_LIMITS.get(key) or {}
        limit = info.get("limit")
        used = info.get("used")
        remaining = info.get("remaining")
        reset = info.get("reset")
        if limit is None and used is None and remaining is None and reset is None:
            print(f"{label}: no live data yet (no headers seen this run).")
            return
        print(
            f"{label}: limit={limit if limit is not None else '-'} | "
            f"used={used if used is not None else '-'} | "
            f"remaining={remaining if remaining is not None else '-'} | "
            f"reset={reset or '-'}"
        )

    print("\n──────── LIVE API RATE LIMIT SNAPSHOT ────────")
    _fmt("browse", "Browse")
    _fmt("finding", "Finding")
    _fmt("trading", "Trading/Revise")
    print("──────────────────────────────────────────────\n")


def refresh_rate_limits_live():
    """Make one cheap call to each API family to populate RATE_LIMITS."""
    # Browse – simple 1-result search.
    try:
        _request(
            "GET",
            EBAY_BROWSE_SEARCH,
            headers=_headers(),
            params={"q": "test", "limit": "1"},
            timeout=5,
            label="Browse/MeterCheck",
        )
    except SystemExit:
        raise
    except Exception as e:
        print(f"   [Browse/MeterCheck] Error while checking meter: {e}")

    # Finding – 1-result keyword search.
    try:
        params = {
            "OPERATION-NAME": "findItemsByKeywords",
            "SERVICE-VERSION": "1.13.0",
            "SECURITY-APPNAME": APP_ID,
            "RESPONSE-DATA-FORMAT": "JSON",
            "REST-PAYLOAD": "",
            "keywords": "test",
            "paginationInput.entriesPerPage": "1",
        }
        _request(
            "GET",
            EBAY_FINDING_API,
            headers=None,
            params=params,
            timeout=5,
            label="Finding/MeterCheck",
        )
    except SystemExit:
        raise
    except Exception as e:
        print(f"   [Finding/MeterCheck] Error while checking meter: {e}")

    # Trading – lightweight GetMyeBaySelling page 1 to read headers.
    try:
        url = "https://api.ebay.com/ws/api.dll"
        headers = {
            "X-EBAY-API-CALL-NAME": "GetMyeBaySelling",
            "X-EBAY-API-SITEID": "0",
            "X-EBAY-API-COMPATIBILITY-LEVEL": "967",
            "Content-Type": "text/xml",
            "Authorization": f"Bearer {OAUTH_TOKEN}",
        }
        body = f"""<?xml version=\"1.0\" encoding=\"utf-8\"?>
        <GetMyeBaySellingRequest xmlns=\"urn:ebay:apis:eBLBaseComponents\">
            <RequesterCredentials>
                <eBayAuthToken>{OAUTH_TOKEN}</eBayAuthToken>
            </RequesterCredentials>
            <ActiveList>
                <Include>true</Include>
                <Pagination>
                    <EntriesPerPage>1</EntriesPerPage>
                    <PageNumber>1</PageNumber>
                </Pagination>
            </ActiveList>
        </GetMyeBaySellingRequest>"""
        _request(
            "POST",
            url,
            headers=headers,
            data=body.encode("utf-8"),
            timeout=10,
            label="Trading/MeterCheck",
        )
    except SystemExit:
        raise
    except Exception as e:
        print(f"   [Trading/MeterCheck] Error while checking meter: {e}")


def check_meters_only():
    """Entry point for `--meters` CLI mode: refresh live meters and print snapshot."""
    print("Fetching live API rate-limit headers (Browse, Finding, Trading)…")
    refresh_rate_limits_live()
    print_rate_limit_snapshot()
def _parse_reset_header(reset_str: Optional[str]) -> Optional[float]:
    if not reset_str:
        return None
    try:
        dt = datetime.strptime(reset_str, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        now = datetime.now(timezone.utc)
        seconds = (dt - now).total_seconds()
        return max(0, seconds)
    except Exception:
        return None


def _log_quota_headers(r: requests.Response, label: str):
    """Capture and print eBay rate-limit headers per API family (Browse/Finding/Trading)."""
    if not r:
        return
    hdrs = r.headers or {}

    # eBay has two different header schemes; support both.
    remaining = hdrs.get("X-EBAY-C-REMAINING-REQUESTS") or hdrs.get("X-EBAY-C-API-CALL-REMAINING")
    reset_at = hdrs.get("X-EBAY-C-RESET-TIME")
    limit = hdrs.get("X-EBAY-C-API-CALL-LIMIT")
    used = hdrs.get("X-EBAY-C-API-CALL-USED")

    if remaining or reset_at:
        print(f"   [{label}] Remaining: {remaining or '-'} | Resets at (UTC): {reset_at or '-'}")

    # Map label → logical API bucket
    api_key = "other"
    lbl = (label or "").lower()
    if "browse" in lbl:
        api_key = "browse"
    elif "finding" in lbl:
        api_key = "finding"
    elif "trading" in lbl or "revise" in lbl or "inventory" in lbl:
        api_key = "trading"

    rl = RATE_LIMITS.get(api_key)
    def _to_int(x):
        try:
            return int(x)
        except Exception:
            return None

    if rl is not None:
        if limit is not None:
            rl["limit"] = _to_int(limit)
        if used is not None:
            rl["used"] = _to_int(used)
        if remaining is not None:
            rl["remaining"] = _to_int(remaining)
        if reset_at:
            rl["reset"] = reset_at

    # Preserve legacy globals for existing summary logic.
    globals()["X-EBAY-C-REMAINING-REQUESTS"] = remaining
    globals()["X-EBAY-C-RESET-TIME"] = reset_at


def _token_expired(r: requests.Response) -> bool:
    if r.status_code in (401, 403):
        txt = (r.text or "").lower()
        return ("invalid access token" in txt) or ("expired" in txt) or ("auth token is invalid" in txt)
    return False


def _handle_rate_limit_and_token(r: requests.Response, label: str) -> bool:
    _log_quota_headers(r, label)
    if _token_expired(r):
        print("eBay OAuth token expired/invalid. Refresh token and rerun.")
        raise SystemExit(1)
    if r.status_code == 429:
        reset_header = r.headers.get("X-EBAY-C-RESET-TIME")
        reset_in = _parse_reset_header(reset_header)
        if reset_in is not None:
            reset_dt = datetime.now(timezone.utc) + timedelta(seconds=reset_in)
            reset_str = reset_dt.strftime("%Y-%m-%d %H:%M:%S")
            print(f"Rate limit hit (429). Window resets at {reset_str}. Sleeping {int(reset_in)}s...")
            time.sleep(min(max(60, reset_in), MAX_SINGLE_COOLDOWN_SEC))
        else:
            now = datetime.now(timezone.utc)
            next_hour = (now + timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
            eta = (next_hour - now).total_seconds()
            eta_str = next_hour.strftime("%Y-%m-%d %H:%M:%S")
            print(f"No reset header. Sleeping until {eta_str} (~{int(eta)}s)...")
            time.sleep(min(eta, RATE_LIMIT_FALLBACK_SLEEP_SEC))
        return True
    return False


def get_custom_label(item_id: str) -> Optional[str]:
    url = "https://api.ebay.com/ws/api.dll"
    headers = {
        "X-EBAY-API-CALL-NAME": "GetItem",
        "X-EBAY-API-SITEID": "0",
        "X-EBAY-API-COMPATIBILITY-LEVEL": "967",
        "Content-Type": "text/xml",
        "Authorization": f"Bearer {OAUTH_TOKEN}",
    }
    body = f"""<?xml version="1.0" encoding="utf-8"?>
    <GetItemRequest xmlns="urn:ebay:apis:eBLBaseComponents">
        <RequesterCredentials>
            <eBayAuthToken>{OAUTH_TOKEN}</eBayAuthToken>
        </RequesterCredentials>
        <ItemID>{item_id}</ItemID>
        <DetailLevel>ReturnAll</DetailLevel>
    </GetItemRequest>"""
    r, hdrs = _request("POST", url, headers=headers, data=body.encode("utf-8"), timeout=25, label="Trading/GetItem")
    api_meter_browse()
    if not r or r.status_code != 200:
        return None
    m = re.search(r"<SKU>(.*?)</SKU>", r.text)
    if m:
        return m.group(1).strip()
    m2 = re.search(r"<CustomLabel>(.*?)</CustomLabel>", r.text)
    return m2.group(1).strip() if m2 else None


# ================= AUTO-FETCH ALL ACTIVE ITEM IDS (OPTION A – OVERRIDE MODE) =================


def _load_full_store_ids_from_cache() -> List[str]:
    """
    Load the cached full-store ItemID list from FULL_STORE_IDS_PATH, if present.
    Returns a list of ItemID strings, or [] if not available/invalid.
    """
    if not os.path.exists(FULL_STORE_IDS_PATH):
        return []
    try:
        with open(FULL_STORE_IDS_PATH, "r", encoding="utf-8") as f:
            raw = json.load(f)
        if isinstance(raw, list):
            ids = [str(x).strip() for x in raw if str(x).strip()]
            if ids:
                print(f"📁 Loaded {len(ids)} cached full-store ItemIDs from {os.path.basename(FULL_STORE_IDS_PATH)}")
                return ids
    except Exception as e:
        print(f"[ID-CACHE] Failed to load cached full-store IDs ({e}). Will rebuild from eBay.")
    return []


def _save_full_store_ids_to_cache(ids: List[str]) -> None:
    """
    Persist the full-store ItemID list so future runs can avoid a fresh full-store fetch.
    """
    try:
        os.makedirs(os.path.dirname(FULL_STORE_IDS_PATH), exist_ok=True)
        # De-duplicate while preserving order
        seen = set()
        ordered_ids: List[str] = []
        for iid in ids:
            s = str(iid).strip()
            if s and s not in seen:
                seen.add(s)
                ordered_ids.append(s)
        with open(FULL_STORE_IDS_PATH, "w", encoding="utf-8") as f:
            json.dump(ordered_ids, f, indent=2)
        print(f"💾 Cached {len(ordered_ids)} full-store ItemIDs to {os.path.basename(FULL_STORE_IDS_PATH)}")
    except Exception as e:
        print(f"[ID-CACHE] Could not save full-store IDs ({e}).")


def load_or_fetch_full_store_ids(force_refresh: bool = False) -> Tuple[List[str], str]:
    """
    Core helper for full-store mode.

    Priority:
      1) If not force_refresh and a cached JSON exists, use it.
      2) Otherwise, call fetch_all_active_item_ids() once, then cache the result.

    Returns (ids, source_label).
    """
    if not force_refresh:
        cached = _load_full_store_ids_from_cache()
        if cached:
            return cached, "cached full-store JSON"

    print("⚙️  No usable cached full-store IDs, fetching from eBay via GetMyeBaySelling ...")
    fresh_ids = fetch_all_active_item_ids()
    if fresh_ids:
        _save_full_store_ids_to_cache(fresh_ids)
        return fresh_ids, "fresh full-store GetMyeBaySelling scan"

    return [], "empty full-store result from GetMyeBaySelling"


def fetch_all_active_item_ids(max_items: int = 50000) -> List[str]:
    """
    Use eBay Trading API GetMyeBaySelling to fetch all active ItemIDs.
    This is called ONLY when no batch item_ids.txt is present (full-store mode).
    """
    url = "https://api.ebay.com/ws/api.dll"
    headers = {
        "X-EBAY-API-CALL-NAME": "GetMyeBaySelling",
        "X-EBAY-API-SITEID": "0",
        "X-EBAY-API-COMPATIBILITY-LEVEL": "967",
        "Content-Type": "text/xml",
        "Authorization": f"Bearer {OAUTH_TOKEN}",
    }

    all_ids: List[str] = []
    page = 1
    entries_per_page = 200

    print("🔍 Fetching all active item IDs from eBay (GetMyeBaySelling)...")

    while len(all_ids) < max_items:
        body = f"""<?xml version="1.0" encoding="utf-8"?>
        <GetMyeBaySellingRequest xmlns="urn:ebay:apis:eBLBaseComponents">
            <RequesterCredentials>
                <eBayAuthToken>{OAUTH_TOKEN}</eBayAuthToken>
            </RequesterCredentials>
            <ActiveList>
                <Include>true</Include>
                <Pagination>
                    <EntriesPerPage>{entries_per_page}</EntriesPerPage>
                    <PageNumber>{page}</PageNumber>
                </Pagination>
            </ActiveList>
        </GetMyeBaySellingRequest>"""

        r, hdrs = _request(
            "POST",
            url,
            headers=headers,
            data=body.encode("utf-8"),
            timeout=30,
            label=f"Trading/GetMyeBaySelling p{page}"
        )
        api_meter_browse()

        if not r or r.status_code != 200:
            print(f"   GetMyeBaySelling page {page} failed with status {r.status_code if r else 'N/A'}. Stopping.")
            break

        text = r.text or ""
        page_ids = re.findall(r"<ItemID>(\d+)</ItemID>", text)
        if not page_ids:
            print(f"   No ItemIDs found on page {page}. Stopping pagination.")
            break

        for iid in page_ids:
            if iid not in all_ids:
                all_ids.append(iid)

        print(f"   Page {page}: fetched {len(page_ids)} IDs (total so far: {len(all_ids)})")

        # If fewer than a full page returned, we're done.
        if len(page_ids) < entries_per_page:
            break

        page += 1

    print(f"✅ Finished fetching active IDs. Total unique active ItemIDs: {len(all_ids)}")
    return all_ids


# ================= STRICT CARD SIGNATURE HELPERS (Option A) =================

def _extract_card_signature_from_title(title: str) -> Optional[Dict]:
    if not title:
        return None

    raw = title.strip()
    tokens = raw.split()

    year_idx = None
    year_val: Optional[int] = None
    for i, tok in enumerate(tokens):
        if tok.isdigit() and len(tok) == 4:
            y = int(tok)
            if 1950 <= y <= 2035:
                year_idx = i
                year_val = y
                break

    text = " ".join(tokens)
    card_num_val: Optional[int] = None

    m_hash = re.search(r"(?:#|No\.?\s*)([A-Za-z0-9]+)", text, re.IGNORECASE)
    if m_hash:
        raw_num = m_hash.group(1)
        if "/" not in raw_num:
            m_digits = re.match(r"(\d+)", raw_num)
            if m_digits:
                try:
                    card_num_val = int(m_digits.group(1))
                except ValueError:
                    card_num_val = None

    if card_num_val is None:
        for tok in reversed(tokens[-4:]):
            if tok.isdigit():
                try:
                    val = int(tok)
                except ValueError:
                    continue
                if not (1950 <= val <= 2035):
                    card_num_val = val
                    break

    if year_idx is not None:
        player_tokens = tokens[:year_idx]
    else:
        player_tokens = tokens[:2]

    family_tokens: List[str] = []
    if year_idx is not None:
        for tok in tokens[year_idx + 1:]:
            lower = tok.lower()
            if lower.startswith("#") or lower in ("no.", "no"):
                break
            family_tokens.append(tok)

    safe_parallels = {
        "refractor", "xfractor", "x-fractor",
        "prizm", "prism", "mosaic",
        "silver", "gold", "orange", "red", "blue", "green", "pink", "purple", "black", "white",
        "atomic", "holo", "holoview",
        "die-cut", "diecut",
        "laser", "mojo", "velocity",
        "optic", "chrome", "finest",
        "steel", "acetate", "shimmer", "wave", "checkerboard",
    }
    parallels: List[str] = []
    for tok in tokens:
        low = tok.lower().strip(",./-")
        if low in safe_parallels:
            parallels.append(low)

    norm_player = [t.lower().strip(",./-") for t in player_tokens if t.isalpha()]

    skip_family = {"trading", "cards", "card", "mlb", "nba", "nfl", "nhl", "baseball", "football", "basketball"}
    norm_family = [t.lower().strip(",./-") for t in family_tokens if t.lower().strip(",./-") not in skip_family]

    # Universal parallel engine classification: split colors vs patterns.
    color_terms = {
        "green", "silver", "gold", "red", "blue", "purple", "pink", "orange", "black", "white",
        "teal", "aqua", "yellow", "bronze", "copper", "maroon", "brown", "lime", "emerald"
    }
    pattern_terms = {
        "reactive", "wave", "shimmer", "disco", "fast", "break", "no", "huddle", "cracked", "ice",
        "hyper", "laser", "mojo", "velocity", "camo", "checkerboard", "atomic", "sparkle",
        "swirl", "choice", "fluorescent", "holo", "refractor", "xfractor", "x-fractor"
    }
    color_parallels: List[str] = []
    pattern_parallels: List[str] = []
    for tok in tokens:
        low = tok.lower().strip(",./-")
        if low in color_terms and low not in color_parallels:
            color_parallels.append(low)
        if low in pattern_terms and low not in pattern_parallels:
            pattern_parallels.append(low)

    return {
        "year": year_val,
        "card_num": card_num_val,
        "player_tokens": norm_player,
        "family_tokens": norm_family,
        "parallels": sorted(set(parallels)),
        "color_parallels": sorted(color_parallels),
        "pattern_parallels": sorted(pattern_parallels),
        "raw_title": raw,
    }


def _compute_signature_hash(title: str, sku: Optional[str]) -> str:
    """Create a stable hash for duplicate detection based on title signature + SKU."""
    sig = _extract_card_signature_from_title(title or "")
    parts = []
    if sig:
        parts.append(str(sig.get("year") or ""))
        parts.append(str(sig.get("card_num") or ""))
        parts.extend(sig.get("player_tokens") or [])
        parts.extend(sig.get("family_tokens") or [])
        parts.extend(sig.get("parallels") or [])
    if sku:
        parts.append(str(sku))
    base = "|".join(parts)
    return hashlib.sha256(base.encode("utf-8")).hexdigest()


def _titles_match_strict(subject: Dict, comp_title: str, price: Optional[float] = None) -> bool:
    if not comp_title or not subject:
        return False

    comp_sig = _extract_card_signature_from_title(comp_title)
    if not comp_sig:
        return False

    low_value = (price is not None and price < 10.0)

    sy = subject.get("year")
    cy = comp_sig.get("year")
    if sy is not None and cy is not None and sy != cy:
        return False

    sn = subject.get("card_num")
    cn = comp_sig.get("card_num")
    if sn is not None and cn is not None and sn != cn:
        return False

    comp_lower = comp_sig.get("raw_title", "").lower()
    for tok in subject.get("player_tokens", []):
        if tok and tok not in comp_lower:
            return False

    subj_family = subject.get("family_tokens", [])
    if subj_family and not low_value:
        fam_tokens = [tok for tok in subj_family if tok]
        # Require at least one family token (e.g. Mosaic, Prizm, Optic, Chrome) to appear,
        # instead of all of them. This prevents over-strict rejection when sellers omit
        # parts of the brand line (like 'Panini') in the title.
        if fam_tokens and not any(tok in comp_lower for tok in fam_tokens):
            return False

    subj_family = subject.get("family_tokens", [])
    comp_family = comp_sig.get("family_tokens", [])

    subj_par = set(subject.get("parallels", []))
    comp_par = set(comp_sig.get("parallels", []))

    # Normalize basic synonyms for parallels
    def _norm_par_set(par_set):
        mapped = set()
        for p in par_set:
            p_low = p.lower()
            if p_low == "prism":
                p_low = "prizm"
            mapped.add(p_low)
        return mapped

    subj_par = _norm_par_set(subj_par)
    comp_par = _norm_par_set(comp_par)

    # Treat brand-level tokens (mosaic, prizm, optic, chrome, finest, select) as family, not parallel
    brandish = {"mosaic", "prizm", "prism", "optic", "chrome", "finest", "select"}

    def _strip_brandish(par_set, family_tokens):
        fam = {t.lower() for t in family_tokens or []}
        cleaned = set()
        for p in par_set:
            if p in brandish and p in fam:
                continue
            cleaned.add(p)
        return cleaned

    subj_par = _strip_brandish(subj_par, subj_family)
    comp_par = _strip_brandish(comp_par, comp_family)

    # Universal parallel engine:
    # - Color terms (green, blue, silver, gold, etc.)
    # - Pattern terms (reactive, wave, shimmer, disco, cracked, swirl, choice, etc.)
    color_terms = {
        "green", "silver", "gold", "red", "blue", "purple", "pink", "orange", "black", "white",
        "teal", "aqua", "yellow", "bronze", "copper", "maroon", "brown", "lime", "emerald"
    }
    pattern_terms = {
        "reactive", "wave", "shimmer", "disco", "fast", "break", "no", "huddle", "cracked", "ice",
        "hyper", "laser", "mojo", "velocity", "camo", "checkerboard", "atomic", "sparkle",
        "swirl", "choice", "fluorescent", "holo", "refractor", "xfractor", "x-fractor"
    }

    def _split_color_pattern(par_set):
        colors, patterns, others = set(), set(), set()
        for p in par_set:
            pl = p.lower()
            if pl in color_terms:
                colors.add(pl)
            elif pl in pattern_terms:
                patterns.add(pl)
            else:
                others.add(pl)
        return colors, patterns, others

    subj_colors, subj_patterns, subj_other = _split_color_pattern(subj_par)
    comp_colors, comp_patterns, comp_other = _split_color_pattern(comp_par)

    # If the subject declares a color, require the comp to match that color set.
    if subj_colors and comp_colors and subj_colors != comp_colors:
        return False

    if not low_value:
        # For higher-value cards, keep strict pattern/parallel rules.
        # If the subject has no pattern (base color only), reject comps that introduce a pattern layer.
        if not subj_patterns and comp_patterns:
            return False

        # If the subject has explicit pattern(s), require them to be present in the comp.
        if subj_patterns and not subj_patterns.issubset(comp_patterns):
            return False

        # Finally, for any remaining parallel tokens, keep the old subset rule as a safety net.
        if subj_par:
            if not subj_par.issubset(comp_par):
                return False

    return True


def _fetch_prices_for_query(
    query: str,
    base_title: Optional[str] = None,
    sold: bool = False,
    limit: int = 20
) -> List[float]:
    if not query:
        return []

    subject_sig = _extract_card_signature_from_title(base_title) if base_title else None
    subject_serial = _extract_serial_fragment(base_title) if base_title else None

    filter_parts = [
        "buyingOptions:FIXED_PRICE",
        "priceType:FIXED",
    ]
    if sold:
        filter_parts.append("soldItemsOnly:true")

    filter_str = ",".join(filter_parts)

    params = {
        "q": query,
        "limit": str(limit),
        "filter": filter_str,
        "fieldgroups": "EXTENDED",
    }

    r, hdrs = _request(
        "GET",
        EBAY_BROWSE_SEARCH,
        headers=_headers(),
        params=params,
        timeout=SOLD_TIMEOUT if sold else ACTIVE_TIMEOUT,
        label="Browse/Sold" if sold else "Browse/Active",
    )

    api_meter_browse()

    if not r or r.status_code != 200:
        return []

    items = r.json().get("itemSummaries", [])
    totals: List[float] = []

    bad_condition_terms = [
        "poor", "fair", "filler", "filler card", "crease", "creased",
        "damage", "damaged", "bent", "writing", "pen", "marker",
        "tape", "miscut", "off-center", "oc", "kid card"
    ]

    lot_like_terms = [
        " lot", "lot of", "lots", "complete set", "factory set", "team set", "set of",
        "sealed box", "hobby box", "blaster box", "mega box", "hanger box", "value box",
        "cello box", "rack pack", "value pack", "fat pack", "jumbo box",
        "case break", "player break", "team break", "group break", "box break",
        "box", "case",
    ]

    for it in items:
        title_it = (it.get("title") or "")
        lower_title = title_it.lower()

        if _is_graded(title_it):
            continue

        opts = (it.get("buyingOptions") or [])
        if "FIXED_PRICE" not in opts:
            continue
        if "AUCTION" in opts:
            continue

        group_type = it.get("itemGroupType")
        if group_type:
            continue
        web_url = (it.get("itemWebUrl") or "").lower()
        if "variation" in web_url:
            continue
        if "auction" in web_url or "bid=" in web_url or "bids=" in web_url:
            continue

        if any(term in lower_title for term in lot_like_terms):
            continue

        if re.search(r"\b\d+\s*(card|cards)\b", lower_title):
            continue
        if re.search(r"\bx\d{1,3}\b", lower_title):
            continue

        if any(term in lower_title for term in bad_condition_terms):
            continue

        # Compute price early so we can scale filters by price level.
        price = _extract_total_price(it)
        if not price:
            continue

        # Serial handling: only enforce matching when the subject itself is serial-numbered.
        comp_serial = _extract_serial_fragment(title_it)
        if subject_serial and comp_serial and comp_serial != subject_serial:
            continue

        # Title/parallel matching (relaxed for low-value cards)
        if subject_sig is not None:
            if not _titles_match_strict(subject_sig, title_it, price):
                continue

        # Sold-specific filters
        if sold:
            # Exclude solds > $100
            if price > 100:
                continue
            # Exclude special parallel mismatches for high-value cards only
            special_terms = [
                "swirl", "case hit", "honeycomb", "genesis", "fluorescent",
                "choice", "disco", "no huddle", "fast break", "reactive", "sparkle"
            ]
            base_lower = (base_title or "").lower()
            for st in special_terms:
                if st in lower_title and st not in base_lower:
                    # Only enforce this strictly for higher-value sales
                    if price >= 10:
                        continue

        # === SAFE-HYBRID FILTER (scaled by price) ===
        user_title = base_title or title_it
        if price >= 10:
            if not safe_hybrid_filter(user_title, title_it, price):
                continue

        totals.append(price)

    return sorted(totals)


def _fetch_prices(title: str, sold: bool = False, limit: int = 20) -> List[float]:
    query = _build_dynamic_query(title)
    totals = _fetch_prices_for_query(query, base_title=title, sold=sold, limit=limit)
    if totals:
        return totals

    totals = _fetch_prices_for_query(title, base_title=title, sold=sold, limit=limit)
    return totals


# =============== HUMAN ROUNDING =================

def _human_round(value: float) -> float:
    if value is None:
        return value
    value = float(value)

    endings = [0.00, 0.05, 0.09,
               0.10, 0.15, 0.19,
               0.20, 0.25, 0.29,
               0.30, 0.35, 0.39,
               0.40, 0.45, 0.49,
               0.50, 0.55, 0.59,
               0.60, 0.65, 0.69,
               0.70, 0.75, 0.79,
               0.80, 0.85, 0.89,
               0.90, 0.95, 0.99]

    whole = math.floor(value)
    cents = value - whole
    best = min(endings, key=lambda e: abs(e - cents))
    rounded = round(whole + best, 2)
    return rounded


# ================= SEARCH HELPERS (ACTIVE / SOLD) =================

def _build_active_fallback_queries(title: str) -> List[str]:
    if not title:
        return []

    raw = title.strip()
    tokens = raw.split()

    year_idx = None
    year_val = ""
    for i, tok in enumerate(tokens):
        if tok.isdigit() and len(tok) == 4:
            year_num = int(tok)
            if 1950 <= year_num <= 2035:
                year_idx = i
                year_val = tok
                break

    card_num_base = None
    card_num_serial = None

    text_joined = " ".join(tokens)

    m_serial = re.search(r"#([A-Za-z0-9]+/[A-Za-z0-9]+)", text_joined)
    if m_serial:
        card_num_serial = m_serial.group(1)

    m_base = re.search(r"#([A-Za-z0-9]+)", text_joined)
    if m_base:
        card_num_base = m_base.group(1)

    if card_num_base is None:
        m_no = re.search(r"[Nn][Oo]\.?\s*([A-Za-z0-9]+)", text_joined)
        if m_no:
            card_num_base = m_no.group(1)

    if year_idx is not None:
        player_tokens = tokens[:year_idx]
    else:
        player_tokens = tokens[:2]
    player = " ".join(player_tokens).strip()

    family_tokens = []
    if year_idx is not None:
        for tok in tokens[year_idx + 1:]:
            if tok.startswith("#") or tok.lower() in ("no.", "no"):
                break
            family_tokens.append(tok)
    family = " ".join(family_tokens).strip()

    is_vintage = False
    if year_val.isdigit():
        try:
            y = int(year_val)
            if 1950 <= y <= 1989:
                is_vintage = True
        except Exception:
            pass

    lower_title = raw.lower()
    exclude_terms = set(EXCLUDE_TERMS + [
        "cgc", "csg", "gma", "bccg",
        "lot", "lots", "set", "sets", "break", "box", "case",
        "jersey", "patch", "team", "sealed", "factory", "complete", "pack"
    ])
    excludes = [f"-{term}" for term in exclude_terms if term not in lower_title]
    excl_suffix = " " + " ".join(excludes) if excludes else ""

    strong_subset_keywords = [
        "invincible", "gems", "gems of the diamond", "showcase",
        "certified", "red", "season's best", "season", "best",
        "air", "command", "raid", "touchdown", "kings",
        "masterpiece", "showdown", "united", "we", "stand",
        "promo", "promos", "showdown", "air raid", "air command",
    ]
    lower_family = family.lower()
    has_strong_subset = any(k in lower_family for k in strong_subset_keywords)

    queries: List[str] = []

    if player and year_val and family and card_num_base:
        queries.append(f"{player} {year_val} {family} #{card_num_base}{excl_suffix}".strip())

    if player and year_val and card_num_base and not is_vintage and not has_strong_subset:
        queries.append(f"{player} {year_val} #{card_num_base}{excl_suffix}".strip())

    if player and year_val and family and card_num_serial:
        queries.append(f"{player} {year_val} {family} #{card_num_serial}{excl_suffix}".strip())

    if player and year_val and card_num_serial and not is_vintage and not has_strong_subset:
        queries.append(f"{player} {year_val} #{card_num_serial}{excl_suffix}".strip())

    return queries


def search_active(
    title: str,
    limit: int = ACTIVE_LIMIT,
    active_cache: Optional[Dict] = None,
) -> Tuple[List[float], str, int]:
    supply_count = 0

    if active_cache is not None:
        cached, from_cache = maybe_use_active_cache(title, active_cache, ACTIVE_CACHE_TTL_MIN)
        if from_cache and cached:
            supply_count = len(cached)
            return cached, "ActiveCache (Merged)", supply_count

    raw_title = (title or "").strip()
    dynamic_query = _build_dynamic_query(title)

    browse_queries: List[str] = []
    if dynamic_query:
        browse_queries.append(dynamic_query)
    browse_queries.extend(_build_active_fallback_queries(title))
    if raw_title:
        browse_queries.append(raw_title)

    finding_queries: List[str] = []
    if dynamic_query:
        finding_queries.append(dynamic_query)
    if raw_title:
        finding_queries.append(raw_title)

    merged_items: List[Dict] = []
    seen_keys = set()
    any_browse = False
    any_finding = False

    for q in browse_queries:
        if not q:
            continue
        items = _fetch_active_items_browse_for_query(q, limit)
        if items:
            any_browse = True
        for it in items:
            key = (it["title"].lower().strip(), it["total"])
            if key in seen_keys:
                continue
            seen_keys.add(key)
            merged_items.append(it)

    for q in finding_queries:
        if not q:
            continue
        items = _fetch_active_items_finding_for_query(q, limit)
        if items:
            any_finding = True
        for it in items:
            key = (it["title"].lower().strip(), it["total"])
            if key in seen_keys:
                continue
            seen_keys.add(key)
            merged_items.append(it)

    if not merged_items:
        return [], "No actives", supply_count

    subject_sig = _extract_card_signature_from_title(title)
    subject_serial = _extract_serial_fragment(title)

    filtered_items: List[Dict] = []
    for it in merged_items:
        comp_title = it["title"] or ""
        price = it["total"]

        # Serial handling: only enforce when the subject itself is serial-numbered.
        comp_serial = _extract_serial_fragment(comp_title)
        if subject_serial and comp_serial and comp_serial != subject_serial:
            continue

        if subject_sig is not None:
            if not _titles_match_strict(subject_sig, comp_title, price):
                continue

        # Scale safe_hybrid_filter by price level: relax for low-value cards.
        if price >= 10:
            if not safe_hybrid_filter(title, comp_title, price):
                continue

        filtered_items.append(it)

    if not filtered_items:
        return [], "No actives", supply_count

    filtered_items.sort(key=lambda x: x["total"])
    active_totals = [it["total"] for it in filtered_items]
    supply_count = len(active_totals)

    source_bits = []
    if any_browse:
        source_bits.append("Browse")
    if any_finding:
        source_bits.append("Finding")
    act_source = " + ".join(source_bits) + " (Merged)" if source_bits else "No actives"

    if active_cache is not None and active_totals:
        update_active_cache(title, active_totals, active_cache)

    return active_totals, act_source, supply_count


def _fetch_active_items_browse_for_query(query: str, limit: int = ACTIVE_LIMIT) -> List[Dict]:
    if not query:
        return []

    filter_parts = [
        "buyingOptions:FIXED_PRICE",
        "priceType:FIXED",
    ]
    filter_str = ",".join(filter_parts)

    params = {
        "q": query,
        "limit": str(limit),
        "filter": filter_str,
        "fieldgroups": "EXTENDED",
    }

    r, hdrs = _request(
        "GET",
        EBAY_BROWSE_SEARCH,
        headers=_headers(),
        params=params,
        timeout=ACTIVE_TIMEOUT,
        label="Browse/ActiveMerged",
    )
    api_meter_browse()

    if not r or r.status_code != 200:
        return []

    items = r.json().get("itemSummaries", [])
    results: List[Dict] = []

    bad_condition_terms = [
        "poor", "fair", "filler", "filler card", "crease", "creased",
        "damage", "damaged", "bent", "writing", "pen", "marker",
        "tape", "miscut", "off-center", "oc", "kid card"
    ]

    lot_like_terms = [
        " lot", "lot of", "lots", "complete set", "factory set", "team set", "set of",
        "sealed box", "hobby box", "blaster box", "mega box", "hanger box", "value box",
        "cello box", "rack pack", "value pack", "fat pack", "jumbo box",
        "case break", "player break", "team break", "group break", "box break",
        "box", "case",
    ]

    for it in items:
        title_it = it.get("title") or ""
        lower_title = title_it.lower()

        if _is_graded(title_it):
            continue

        opts = it.get("buyingOptions") or []
        if "FIXED_PRICE" not in opts:
            continue
        if "AUCTION" in opts:
            continue

        group_type = it.get("itemGroupType")
        if group_type:
            continue

        web_url = (it.get("itemWebUrl") or "").lower()
        if "variation" in web_url:
            continue
        if "auction" in web_url or "bid=" in web_url or "bids=" in web_url:
            continue

        if any(term in lower_title for term in lot_like_terms):
            continue
        if re.search(r"\b\d+\s*(card|cards)\b", lower_title):
            continue
        if re.search(r"\bx\d{1,3}\b", lower_title):
            continue

        if any(term in lower_title for term in bad_condition_terms):
            continue

        price = _extract_total_price(it)
        if not price:
            continue

        results.append({"title": title_it, "total": price})

    return results


def _fetch_active_items_finding_for_query(query: str, limit: int = ACTIVE_LIMIT) -> List[Dict]:
    if not query:
        return []

    params = {
        "OPERATION-NAME": "findItemsByKeywords",
        "SERVICE-VERSION": "1.13.0",
        "SECURITY-APPNAME": APP_ID,
        "RESPONSE-DATA-FORMAT": "JSON",
        "REST-PAYLOAD": "",
        "keywords": query,
        "paginationInput.entriesPerPage": str(limit),
    }

    r, hdrs = _request(
        "GET",
        EBAY_FINDING_API,
        headers=None,
        params=params,
        timeout=ACTIVE_TIMEOUT,
        label="Finding/ActiveMerged",
    )
    api_meter_browse()

    if not r or r.status_code != 200:
        return []

    try:
        data = r.json()
        root_list = data.get("findItemsByKeywordsResponse") or []
        if not root_list:
            return []
        root = root_list[0]
        sr_list = root.get("searchResult") or []
        if not sr_list:
            return []
        items = sr_list[0].get("item") or []
    except Exception:
        return []

    results: List[Dict] = []

    bad_condition_terms = [
        "poor", "fair", "filler", "filler card", "crease", "creased",
        "damage", "damaged", "bent", "writing", "pen", "marker",
        "tape", "miscut", "off-center", "oc", "kid card"
    ]

    lot_like_terms = [
        " lot", "lot of", "lots", "complete set", "factory set", "team set", "set of",
        "sealed box", "hobby box", "blaster box", "mega box", "hanger box", "value box",
        "cello box", "rack pack", "value pack", "fat pack", "jumbo box",
        "case break", "player break", "team break", "group break", "box break",
        "box", "case",
    ]

    for it in items:
        title_it = it.get("title") or ""
        lower_title = title_it.lower()

        if _is_graded(title_it):
            continue

        listing_info = it.get("listingInfo") or {}
        listing_type = (listing_info.get("listingType") or "").upper()
        if listing_type not in ("FIXEDPRICE", "STOREINVENTORY"):
            continue

        if any(term in lower_title for term in lot_like_terms):
            continue
        if re.search(r"\b\d+\s*(card|cards)\b", lower_title):
            continue
        if re.search(r"\bx\d{1,3}\b", lower_title):
            continue

        if any(term in lower_title for term in bad_condition_terms):
            continue

        price = _extract_total_price_finding(it)
        if not price:
            continue

        results.append({"title": title_it, "total": price})

    return results


def search_sold(
    title: str,
    limit: int = SOLD_LIMIT,
    cache_df: Optional[pd.DataFrame] = None
) -> Tuple[List[float], str, List[Dict]]:
    totals = _fetch_prices(title, sold=True, limit=limit)
    if totals:
        return totals, "Browse BIN", []
    return [], "No solds", []


# ================= STRICT PRICING ENGINE (A2-Price-Safe) =================

def _median(values: List[float]) -> Optional[float]:
    vals = sorted([v for v in values if v is not None])
    if not vals:
        return None
    n = len(vals)
    mid = n // 2
    return round(vals[mid] if n % 2 else (vals[mid - 1] + vals[mid]) / 2, 2)


def get_price_strict(
    active_totals: List[float],
    sold_totals: List[float],
    current_price: Optional[float] = None
) -> Tuple[Optional[float], Optional[float], Optional[float], str]:
    """
    A2-Price-Safe strict pricing engine.

    A2 rules:
      - Use merged actives as primary source, SOLD as secondary.
      - Take lowest up to K (=5) active comps, then median of those K.
      - If <K actives, still median of however many (1–4).
      - SOLD median used only when we have no usable actives.
      - If median_sold >> median_active and few sold samples, drop sold.

    Price-Safe layer:
      - Human rounding.
      - Enforce PRICE_FLOOR.
      - If current_price is known:
          * Max 40% drop in a single pass.
          * For higher-priced items (>= 4.99), don't crash below 3.49.
    """
    note_parts: List[str] = []

    # ---------- Normalize ACTIVE ----------
    act_values: List[float] = []
    for v in active_totals or []:
        try:
            fv = float(v)
            if fv > 0:
                act_values.append(fv)
        except Exception:
            continue

    median_active: Optional[float] = None
    active_n = 0
    if act_values:
        act_values = sorted(act_values)
        # Lowest K strategy
        k = min(LOWEST_ACTIVE_AVG_K, len(act_values))
        lowest_k = act_values[:k]
        active_n = len(lowest_k)
        median_active = _median(lowest_k)
        note_parts.append(f"Active(A2,k={active_n})")

    # ---------- Normalize SOLD ----------
    sold_values: List[float] = []
    for v in sold_totals or []:
        try:
            fv = float(v)
            if fv > 0:
                sold_values.append(fv)
        except Exception:
            continue

    median_sold: Optional[float] = None
    sold_n = len(sold_values)
    if sold_values:
        sold_values = sorted(sold_values)
        median_sold = _median(sold_values)
        note_parts.append(f"Sold(A2,n={sold_n})")

    # ---------- Sold vs Active mismatch guard ----------
    if median_sold is not None and median_active is not None:
        try:
            if sold_n <= 3 and median_sold > 2 * median_active:
                # Discard suspiciously high sold median when few sold samples
                median_sold = None
                note_parts.append("Sold dropped (>> actives w/low N)")
        except Exception:
            pass

    # ---------- Primary selection ----------
    suggested: Optional[float] = None
    if median_active is not None:
        suggested = median_active
        primary_source = "Active median A2"
    elif median_sold is not None:
        suggested = median_sold
        primary_source = "Sold median A2"
    else:
        return None, None, None, "No data"

    note_parts.append(primary_source)

    # ---------- Human rounding + floor ----------
    suggested = _human_round(suggested)

    if suggested < PRICE_FLOOR:
        suggested = PRICE_FLOOR
        note_parts.append("Floor clamp")

    # ---------- Price-Safe layer (relative to current price) ----------
    if current_price is not None:
        try:
            cp = float(current_price)
        except Exception:
            cp = None
        if cp is not None and cp > 0:
            HIGH_PRICE_THRESHOLD = 4.99
            HIGH_PRICE_FLOOR = 3.49
            MAX_DROP_PERCENT = 40

            # Max 40% drop vs current price
            min_allowed_by_percent = round(cp * (1 - MAX_DROP_PERCENT / 100), 2)
            if suggested < min_allowed_by_percent:
                suggested = min_allowed_by_percent
                note_parts.append("Drop capped 40% vs current")

            # For higher-priced items, don't crash below HIGH_PRICE_FLOOR
            if cp >= HIGH_PRICE_THRESHOLD and suggested < HIGH_PRICE_FLOOR:
                suggested = HIGH_PRICE_FLOOR
                note_parts.append("High-price floor clamp")

            if suggested < PRICE_FLOOR:
                suggested = PRICE_FLOOR
                if "Floor clamp" not in note_parts:
                    note_parts.append("Floor clamp")

    final_note = " | ".join(note_parts) if note_parts else ""
    return median_sold, median_active, round(suggested, 2), final_note


def summarize_prices(active_totals, sold_totals, current_price=None):
    """
    Backwards-compatible wrapper that now delegates to get_price_strict()
    using the A2-Price-Safe engine.
    """
    return get_price_strict(active_totals, sold_totals, current_price=current_price)


def update_ebay_price(item_id: str, new_price: float) -> Tuple[bool, str]:
    url = "https://api.ebay.com/ws/api.dll"
    headers = {
        "X-EBAY-API-CALL-NAME": "ReviseFixedPriceItem",
        "X-EBAY-API-SITEID": "0",
        "X-EBAY-API-COMPATIBILITY-LEVEL": "967",
        "Content-Type": "text/xml",
        "Authorization": f"Bearer {OAUTH_TOKEN}",
    }
    body = f"""<?xml version="1.0" encoding="utf-8"?>
    <ReviseFixedPriceItemRequest xmlns="urn:ebay:apis:eBLBaseComponents">
        <RequesterCredentials>
            <eBayAuthToken>{OAUTH_TOKEN}</eBayAuthToken>
        </RequesterCredentials>
        <Item>
            <ItemID>{item_id}</ItemID>
            <StartPrice>{round(new_price, 2)}</StartPrice>
        </Item>
    </ReviseFixedPriceItemRequest>"""
    r, hdrs = _request("POST", url, headers=headers, data=body.encode("utf-8"), timeout=25, label="Trading/Revise")
    api_meter_revise()
    if not r:
        return False, "API error"
    if r.status_code == 200 and "<Ack>Success</Ack>" in r.text:
        return True, f"Updated successfully to ${new_price}"
    elif "<Ack>Warning</Ack>" in r.text:
        return True, "Updated successfully (business policy warning only)"
    elif "Duplicate Listing policy" in r.text:
        return False, "Duplicate Listing policy -- will log for manual review"
    else:
        return False, f"API {r.status_code}: {r.text[:300]}"


# ================= AUTOSAVE HELPERS =================
def _latest_autosave_path(folder: str, base_name: str) -> Optional[str]:
    pattern = os.path.join(folder, base_name.replace(".csv", "*.csv"))
    files = glob.glob(pattern)
    if not files:
        return None
    files.sort(key=os.path.getmtime, reverse=True)
    latest = files[0]
    print(f"Resuming from latest autosave: {os.path.basename(latest)}")
    return latest



def _autosave_write_atomic(df: pd.DataFrame, folder: str, base_name: str):
    """
    Robust autosave helper for long runs.

    Rules:
      - Always write to a single, version-independent autosave file.
      - Never allow the autosave to "shrink" (fewer rows than an existing autosave).
      - Use atomic write via *.tmp replacement.
    """
    os.makedirs(folder, exist_ok=True)

    base_path = os.path.join(folder, base_name)
    tmp_path = base_path + ".tmp"

    new_len = len(df) if df is not None else 0

    # --- Do not overwrite if existing autosave has MORE rows ---
    try:
        if os.path.exists(base_path):
            try:
                existing_df = pd.read_csv(base_path, dtype={"item_id": str})
                old_len = len(existing_df)
            except Exception:
                old_len = None

            if old_len is not None and old_len > new_len:
                print(f"Autosave not written: existing autosave has {old_len} rows vs {new_len} new rows.")
                return
    except Exception:
        pass  # even if this fails, continue to safe atomic write below

    # --- Primary atomic write ---
    try:
        df = df.copy()
        df["item_id"] = df["item_id"].astype(str)

        df.to_csv(tmp_path, index=False)
        os.replace(tmp_path, base_path)
        return

    except PermissionError:
        # Base autosave is locked (Excel open?) → write a timestamped fallback file
        if os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
            except Exception:
                pass

        ts = int(time.time())
        ts_path = base_path.replace(".csv", f"_{ts}.csv")

        try:
            df = df.copy()
            df["item_id"] = df["item_id"].astype(str)

            df.to_csv(ts_path, index=False)
            print(f"Autosave locked; wrote fallback: {os.path.basename(ts_path)}")
            return
        except Exception as e:
            print(f"Autosave fallback failed: {e}")
            return

    except Exception as e:
        print(f"Autosave failed: {e}")
        return

# ================= ITEM HANDLERS =================
def read_item_ids() -> List[str]:
    txt_path = os.path.join(IDS_FOLDER, "item_ids.txt")
    if os.path.exists(txt_path):
        with open(txt_path, "r", encoding="utf-8") as f:
            return [x.strip() for x in f.readlines() if x.strip()]
    print("No batch item_ids.txt found in IDs folder.")
    return []


def get_item_details(item_id: str):
    browse_id = item_id if not item_id.isdigit() else f"v1|{item_id}|0"
    title = f"Item {item_id}"
    current_price = None
    r, hdrs = _request(
        "GET",
        f"https://api.ebay.com/buy/browse/v1/item/{browse_id}",
        headers=_headers(),
        timeout=15,
        label="Browse/Item",
    )
    api_meter_browse()

    if r and r.status_code == 200:
        try:
            data = r.json()
            title = data.get("title", title)
            if "price" in data:
                current_price = _safe_float(data["price"].get("value"))
        except Exception:
            pass
    return title, current_price


def process_item(
    item_id: str,
    cache_df: pd.DataFrame,
    active_cache: Dict,
    dup_logged: set,
    dup_log_file: str
) -> Tuple[Dict, List[Dict]]:
    try:
        title, current_price = get_item_details(item_id)
        sold_totals, sold_source, new_cache_rows = search_sold(title, limit=SOLD_LIMIT, cache_df=cache_df)
        time.sleep(SLEEP_BETWEEN_CALLS_SEC)
        active_totals, act_source, supply_count = search_active(title, limit=ACTIVE_LIMIT, active_cache=active_cache)

        median_sold, median_active, suggested, note = get_price_strict(
            active_totals,
            sold_totals,
            current_price=current_price
        )

        combined_note = note or ""

        

        print(
            f"-> {title} | Source: {sold_source} | Current: {current_price or '-'} | "
            f"Sold(med): {median_sold or '-'} | Active(med): {median_active or '-'} | Suggest: {suggested or '-'}"
        )

        update_status = ""
        if not DRY_RUN and suggested and current_price:
            diff_abs = abs(suggested - current_price)
            diff_pct = diff_abs / current_price if current_price > 0 else 0.0

            if diff_abs >= 0.30 or diff_pct >= (PERCENT_THRESHOLD / 100):
                success, update_status = update_ebay_price(item_id, suggested)
                print(f"   {update_status}")
                if "Duplicate Listing policy" in update_status and item_id not in dup_logged:
                    dup_logged.add(item_id)
                    sku = get_custom_label(item_id)
                    if sku:
                        print(f"   ↳ SKU found: {sku}")
                    else:
                        print("   ↳ No SKU found for this item.")
                    dup_entry = {
                        "item_id": f"'{item_id}",
                        "CustomLabel": sku or "",
                        "Title": title,
                        "CurrentPrice": current_price,
                        "SuggestedPrice": suggested,
                        "MedianSold": median_sold or "",
                        "ActiveAvg": median_active or "",
                        "Note": f"{sold_source} | {act_source}",
                        "Detected": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    }
                    try:
                        pd.DataFrame([dup_entry]).to_csv(
                            dup_log_file,
                            mode="a",
                            index=False,
                            header=not os.path.exists(dup_log_file),
                            quoting=csv_mod.QUOTE_MINIMAL,
                        )
                        print(f"   Logged duplicate to {dup_log_file}")
                    except PermissionError:
                        print("   Could not write to duplicates log (file open?).")
            else:
                api_meter_revise_saved()
                print(f"   Change below threshold (Δ${diff_abs:.2f}, {diff_pct*100:.1f}%). No update sent.")

        elif DRY_RUN:
            print("   Dry run - no live update sent.")

        row = {
            "item_id": item_id,
            "Title": title,
            "CurrentPrice": current_price or "",
            "MedianSold": median_sold or "",
            "ActiveAvg": median_active or "",
            "SuggestedPrice": suggested or "",
            "SupplyCount": supply_count,
            "Note": f"{sold_source} | {act_source} | {combined_note}".strip(" |"),
            "UpdateStatus": update_status,
        }
        return row, new_cache_rows
    except Exception as e:
        print(f"   ERROR while processing {item_id}: {e}")
        return {"item_id": item_id, "Error": str(e)}, []



def main():
    global DRY_RUN, SANDBOX_MODE

    # CLI args (for meters / resume, etc.)
    args = sys.argv[1:]

    # Fast meters-only mode
    if args and args[0] in ("--meters", "-m", "--meter", "--limits"):
        check_meters_only()
        return

    # Optional: manual resume index from CLI (takes precedence over autosave-based inference)
    resume_from_index = None
    if "--resume" in args or "-r" in args:
        for idx, a in enumerate(args):
            if a in ("--resume", "-r") and idx + 1 < len(args):
                try:
                    resume_from_index = int(args[idx + 1])
                    print(f"🔁 Manual resume offset enabled from CLI: starting at item index {resume_from_index}")
                except ValueError:
                    print("Invalid value for --resume; ignoring.")
                break

    # Optional: allow a manual full-store ID refresh that rebuilds the cached JSON.
    force_refresh_ids = any(
        a in ("--refresh-ids", "--refresh_ids", "--refresh-full-store-ids", "--refresh_all_ids")
        for a in args
    )

    print(f"Monterey Sports Cards – Price Optimizer {VERSION} (Merged Active + A2-Price-Safe Engine)")

    # ---------- MENU UI ----------
    has_batch_file = os.path.exists(os.path.join(IDS_FOLDER, "item_ids.txt"))
    autosave_base = "msc_autosave_temp.csv"

    while True:
        # Recompute autosave row count each time in case files changed
        autosave_rows = _get_autosave_progress(AUTOSAVE_FOLDER, autosave_base)

        mode_label = "LIVE MODE" if not DRY_RUN else "TEST MODE"
        sandbox_status = "🟢 ON" if SANDBOX_MODE else "🟤 OFF"

        print(f"============ MONTEREY SPORTS CARDS ({mode_label}) =============")
        print(f"   [ Sandbox: {sandbox_status} ]")
        print("------------------------------------------------------------")
        print("Choose a mode:")
        print("  0) Toggle Live Updates (LIVE/TEST)")
        print("  1) Full-Store Scan (all active listings)")
        if autosave_rows > 0:
            print(f"  2) Resume from Autosave (~{autosave_rows} items already processed)")
        else:
            print("  2) Resume from Autosave (no valid autosave found)")
        print("  3) Newly Listed Items Only (runs items created within last N days)")
        print("  4) Custom SKU Only (exact match)")
        if has_batch_file:
            print("  5) Batch File Mode (item_ids.txt present)")
        else:
            print("  5) Batch File Mode (no item_ids.txt found)")
        print("  6) Resume From Manual Index (Advanced)")
        print("  S) Sandbox Test Mode (no autosave, no updates)")
        print("  7) Show API Limit Snapshot Only")
        print("  8) Reset Autosave + Full-Store Scan")
        print("  9) Exit")
        print("=================================================")

        # Default selection logic:
        # - If batch file exists, default to 5
        # - Else default to 1 (full-store)
        default_choice = "5" if has_batch_file else "1"
        choice = input(f"Enter choice [default {default_choice}]: ").strip().upper() or default_choice

        valid_choices = {"0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "S"}
        while choice not in valid_choices:
            choice = input("Invalid choice. Enter 0–9 or S: ").strip().upper()

        # Toggle live vs test (DRY_RUN) and re-show menu
        if choice == "0":
            prev_mode_live = not DRY_RUN
            DRY_RUN = not DRY_RUN
            mode_label = "LIVE MODE" if not DRY_RUN else "TEST MODE"
            print(f"🔄 Live Updates Toggled: Now running in {mode_label}.")

            # If we just switched into LIVE, force Sandbox OFF
            if (not prev_mode_live) and (not DRY_RUN) and SANDBOX_MODE:
                SANDBOX_MODE = False
                print("Sandbox mode is only available in TEST mode. Turning Sandbox OFF.")
            continue

        # Toggle sandbox mode (no autosave, no cache writes, no revise calls)
        if choice == "S":
            if not DRY_RUN:
                print("Sandbox mode is only available in TEST mode. Please switch to TEST mode first.")
            else:
                SANDBOX_MODE = not SANDBOX_MODE
                on_off = "ON" if SANDBOX_MODE else "OFF"
                print(f"🧪 Sandbox Mode is now {on_off}.")
            continue

        # Show live meter snapshot and exit
        if choice == "7":
            check_meters_only()
            return

        # Reset autosave then fall through to a full-store scan
        if choice == "8":
            base_path = os.path.join(AUTOSAVE_FOLDER, autosave_base)
            if os.path.exists(base_path):
                try:
                    os.remove(base_path)
                    print(f"🧹 Reset autosave file: {base_path}")
                except Exception as e:
                    print(f"Could not delete autosave file: {e}")
            else:
                print("No autosave file found to reset. Starting fresh full-store scan.")
            # After reset, behave like Full-Store Scan
            choice = "1"

        # Exit without running pricing engine
        if choice == "9":
            print("Exiting without running pricing engine.")
            return

        # Any other valid choice (1,2,3,4,5,6) breaks out to run that mode
        break

    # Helper to load IDs based on chosen mode
    ids: List[str] = []
    ids_source = ""

    # If manual index resume (mode 6), prompt for starting index (1-based)
    if choice == "6":
        while True:
            idx_str = input("Enter starting index (1-based) to resume from (e.g., 500): ").strip()
            try:
                idx_val = int(idx_str)
                if idx_val <= 0:
                    print("Please enter a positive integer for index.")
                    continue
                resume_from_index = idx_val
                print(f"Manual resume selected. Will start from index {resume_from_index}.")
                break
            except ValueError:
                print("Invalid number. Please enter a positive integer.")

    # Mode 1, 2 & 6: Full-store base
    if choice in ("1", "2", "6"):
        # If full-store JSON exists and not forcing refresh, use it (or rebuild if forced)
        cached_ids, src_label = load_or_fetch_full_store_ids(force_refresh=force_refresh_ids)
        ids = cached_ids
        ids_source = src_label

        if not ids:
            print("No active items found or failed to fetch active IDs. Exiting.")
            return

        if choice == "2":
            if autosave_rows <= 0:
                print("No valid autosave found; cannot resume. Consider running Full-Store Scan instead.")
                return
            else:
                print(f"Resume mode selected. Autosave indicates ~{autosave_rows} rows already processed.")

    # Mode 3: Newly listed items only (based on StartTime)
    elif choice == "3":
        print("\nHow many days back for newly listed items? (e.g., 3)")
        print("You can type:")
        print("  1  → only listings created today")
        print("  2  → listings from the last 48 hours")
        print("  3  → listings from the last 72 hours")
        print("  7  → listings from the last week")
        print("  30 → last month’s new listings")
        print("  90 → if you want a quarterly new-listing scan")
        while True:
            days_str = input("Enter the number of days to scan: ").strip()
            try:
                days_back = int(days_str)
                if days_back <= 0:
                    print("Please enter a positive integer for days.")
                    continue
                break
            except ValueError:
                print("Invalid number. Please enter an integer.")
        ids, ids_source = fetch_recent_item_ids(days_back)
        if not ids:
            print(f"No active items found that were started in the last {days_back} days. Exiting.")
            return
        print(f"Loaded {len(ids)} newly listed items from last {days_back} days.")

    # Mode 4: Custom SKU only
    elif choice == "4":
      while True:
        print("\nExample SKU format: 041425-MJ")
        custom_sku = input("Enter the exact Custom SKU to process (or press Enter to return to menu): ").strip()

        # Allow escape back to menu
        if not custom_sku:
            print("Returning to main menu...")
            break

        # Fetch exact-match SKU results
        ids, ids_source = fetch_item_ids_by_custom_sku(custom_sku)

        # If no match, retry option 4
        if not ids:
            print(f"❌ No active items found with Custom SKU/Label = '{custom_sku}'. Try another SKU.\n")
            continue

        # Valid SKU found
        print(f"Loaded {len(ids)} items with Custom SKU/Label = '{custom_sku}'.")
        # ← Run your normal processing logic here
        # For example:
        # process_ids(ids)
        break  # Leave option 4 after processing

    # Mode 5: Batch file mode
    elif choice == "5":
        ids = read_item_ids()
        ids_source = "batch file item_ids.txt"
        if not ids:
            print("No batch item_ids.txt found or file was empty. Exiting.")
            return

    # ---------- Shared setup after IDs are chosen ----------
    if not SANDBOX_MODE:
        os.makedirs(CACHE_FOLDER, exist_ok=True)
        active_cache = load_active_cache(ACTIVE_CACHE_PATH, ACTIVE_CACHE_TTL_MIN)
    else:
        active_cache = None

    # If we have numeric IDs, we can reorder by quantity using Inventory API like before
    numeric_ids = []
    for raw in ids:
        raw_clean = str(raw).strip().replace("v1|", "").replace("|0", "")
        try:
            numeric_ids.append(int(raw_clean))
        except Exception:
            continue

    if numeric_ids:
        chunk_size = BATCH_SIZE
        full_df_list = []

        for start in range(0, len(numeric_ids), chunk_size):
            chunk = numeric_ids[start:start + chunk_size]
            id_str = ",".join(str(x) for x in chunk)

            r, hdrs = _request(
                "GET",
                "https://api.ebay.com/sell/inventory/v1/inventory_item",
                headers=_headers(for_update=False),
                params={"sku": id_str},
                timeout=25,
                label="Inventory/Batch"
            )
            api_meter_browse()

            if r and r.status_code == 200:
                items = r.json().get("inventoryItems", [])
                for itm in items:
                    sku = str(itm.get("sku", ""))
                    avail = itm.get("availability", {})
                    ship_to = avail.get("shipToLocationAvailability", {})
                    qty = ship_to.get("quantity", 0)

                    full_df_list.append({
                        "item_id": sku,
                        "qty": qty
                    })

        if full_df_list:
            df = pd.DataFrame(full_df_list)
            df = df.sort_values("qty", ascending=False)
            ids = df["item_id"].astype(str).tolist()

    print(f"Reordered batch: {len(ids)} items (in-stock first, OOS last)")
    print(f"Loaded {len(ids)} cards from {ids_source}")

    os.makedirs(REPORT_FOLDER, exist_ok=True)
    os.makedirs(AUTOSAVE_FOLDER, exist_ok=True)
    os.makedirs(RESULTS_FOLDER, exist_ok=True)

    cache_df = None

    # Autosave recovery by processed IDs
    if not SANDBOX_MODE:
        processed_ids = set()
        latest_autosave = _latest_autosave_path(AUTOSAVE_FOLDER, autosave_base)
        if latest_autosave and os.path.exists(latest_autosave):
            try:
                df_prev = pd.read_csv(latest_autosave, dtype={"item_id": str})
                processed_ids = set(df_prev["item_id"].astype(str).tolist())
                if processed_ids:
                    print(f"Resuming - {len(processed_ids)} items previously processed; will skip them.")
            except Exception as e:
                print(f"Could not read autosave file: {e}")
    else:
        processed_ids = set()

    run_stamp = datetime.now().astimezone().strftime("%Y-%m-%d")
    dup_log_file = os.path.join(REPORT_FOLDER, f"duplicates_found_{run_stamp}.csv")
    dup_logged = set()

    if not os.path.exists(dup_log_file):
        with open(dup_log_file, "w", encoding="utf-8", newline="") as f:
            f.write("item_id,CustomLabel,Title,CurrentPrice,SuggestedPrice,MedianSold,ActiveAvg,Note,Detected\n")
        print(f"🗂️  Created new duplicates log for today: {os.path.basename(dup_log_file)}")
    else:
        print(f"📎  Appending to existing duplicates log: {os.path.basename(dup_log_file)}")

    results = []
    last_remaining = None
    last_reset = None

    try:
        for i, item_id in enumerate(ids, start=1):
            # CLI-based resume offset (index-based)
            if not SANDBOX_MODE and resume_from_index is not None and i < resume_from_index:
                print(f"[{i}/{len(ids)}] Skipping (resume_offset index) item {item_id}")
                continue

            # Autosave-based resume (by item_id)
            if not SANDBOX_MODE and str(item_id) in processed_ids:
                print(f"[{i}/{len(ids)}] Skipping already processed item {item_id}")
                continue

            print(f"[{i}/{len(ids)}] Checking item {item_id} ...")
            row, new_cache = process_item(item_id, cache_df, active_cache, dup_logged, dup_log_file)
            results.append(row)

            last_remaining = globals().get("X-EBAY-C-REMAINING-REQUESTS", last_remaining)
            last_reset = globals().get("X-EBAY-C-RESET-TIME", last_reset)

            if i % 100 == 0:
                print(f"📊 API Usage Status: {last_remaining or 'N/A'} calls remaining | Reset at (UTC): {last_reset or '—'}")
                print(f"[API METERS] Browse={BROWSE_CALLS}, Revise={REVISE_CALLS}, Saved={REVISE_SAVED}")
                print_rate_limit_snapshot()

            if new_cache:
                if cache_df is None:
                    cache_df = pd.DataFrame(new_cache)
                else:
                    cache_df = pd.concat([cache_df, pd.DataFrame(new_cache)], ignore_index=True)

            if i % 10 == 0 and not SANDBOX_MODE:
                _autosave_write_atomic(pd.DataFrame(results), AUTOSAVE_FOLDER, autosave_base)
                print(f"   Autosaved progress after {i} items.")
                if active_cache is not None:
                    save_active_cache(active_cache, ACTIVE_CACHE_PATH)

            time.sleep(SLEEP_BETWEEN_CALLS_SEC)

    except KeyboardInterrupt:
        if not SANDBOX_MODE:
            print("\n⛔ Detected manual interrupt (CTRL+C). Saving progress and exiting gracefully...")
            _autosave_write_atomic(pd.DataFrame(results), AUTOSAVE_FOLDER, autosave_base)
            if active_cache is not None:
                save_active_cache(active_cache, ACTIVE_CACHE_PATH)
        else:
            print("\n⛔ Detected manual interrupt (CTRL+C) in SANDBOX mode. No autosave written.")

    # After completing the loop:
    if not SANDBOX_MODE and active_cache is not None:
        save_active_cache(active_cache, ACTIVE_CACHE_PATH)

    report_path = os.path.join(
        RESULTS_FOLDER,
        f"price_update_report_{VERSION}_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.csv"
    )
    final_df = pd.DataFrame(results)

    latest_autosave = _latest_autosave_path(AUTOSAVE_FOLDER, autosave_base)
    if latest_autosave and os.path.exists(latest_autosave):
        try:
            prev = pd.read_csv(latest_autosave, dtype={"item_id": str})
            final_df = pd.concat([prev, final_df], ignore_index=True)
        except Exception:
            pass

    final_df["item_id"]=df["item_id"].astype(str)
    df.to_csv(report_path, index=False)
    print(f"\nReport saved: {report_path}")
    if not SANDBOX_MODE and active_cache is not None:
        print(f"Active cache updated: {ACTIVE_CACHE_PATH}")

    print("\n================ API USAGE SUMMARY ================")
    print(f"Browse API calls used: {BROWSE_CALLS}")
    print(f"Revise API calls sent: {REVISE_CALLS}")
    print(f"Revise calls avoided (threshold): {REVISE_SAVED}")
    print(f"Total API hits today: {BROWSE_CALLS + REVISE_CALLS}")
    print("====================================================\n")

    # Detailed per-API snapshot (Browse / Finding / Trading)
    print_rate_limit_snapshot()

    print(f"✅ Finished processing. {last_remaining or 'N/A'} calls remaining. Next reset at (UTC): {last_reset or '—'}")
    if DRY_RUN:
        print("Test run complete (no live price updates sent).")
    else:
        print("Live update complete!")

def _get_autosave_progress(folder: str, base_name: str) -> int:
    """
    Return the number of rows in the current autosave file, or 0 if not present/invalid.
    """
    try:
        path = os.path.join(folder, base_name)
        if not os.path.exists(path):
            return 0
        df = pd.read_csv(path, dtype={"item_id": str})
        return len(df)
    except Exception:
        return 0


def _parse_trading_items_for_metadata(xml_text: str):
    """
    Very lightweight XML parsing for GetMyeBaySelling responses.
    Returns a list of dicts with keys:
      - item_id
      - start_time (ISO string or None)
      - sku (from <SKU>)
      - custom_label (from <CustomLabel>)
    """
    results = []
    if not xml_text:
        return results

    # Split on <Item>...</Item> blocks
    for m in re.finditer(r"<Item>(.*?)</Item>", xml_text, flags=re.DOTALL | re.IGNORECASE):
        block = m.group(1)

        def _find(tag):
            mm = re.search(rf"<{tag}>(.*?)</{tag}>", block, flags=re.DOTALL | re.IGNORECASE)
            return mm.group(1).strip() if mm else None

        item_id = _find("ItemID")
        start_time = _find("StartTime")
        sku = _find("SKU") or None
        custom_label = _find("CustomLabel") or None

        if item_id:
            results.append({
                "item_id": item_id.strip(),
                "start_time": start_time,
                "sku": sku,
                "custom_label": custom_label,
            })
    return results


def _parse_ebay_datetime(dt_str: str):
    """
    Parse an eBay-style UTC datetime string into a datetime, e.g. '2025-11-20T18:42:31.000Z'.
    Returns None on failure.
    """
    if not dt_str:
        return None
    dt_str = dt_str.strip()
    # Try a couple of common formats
    fmts = ["%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ"]
    for fmt in fmts:
        try:
            return datetime.strptime(dt_str, fmt).replace(tzinfo=timezone.utc)
        except Exception:
            continue
    return None


def fetch_recent_item_ids(days_back: int, max_items: int = 50000):
    """
    Fetch all active ItemIDs via GetMyeBaySelling and return only those
    whose StartTime is within the last `days_back` days.
    """
    url = "https://api.ebay.com/ws/api.dll"
    headers = {
        "X-EBAY-API-CALL-NAME": "GetMyeBaySelling",
        "X-EBAY-API-SITEID": "0",
        "X-EBAY-API-COMPATIBILITY-LEVEL": "967",
        "Content-Type": "text/xml",
        "Authorization": f"Bearer {OAUTH_TOKEN}",
    }

    all_ids = []
    page = 1
    entries_per_page = 200
    cutoff = datetime.now(timezone.utc) - timedelta(days=days_back)

    print(f"🔍 Fetching active item IDs started within last {days_back} days via GetMyeBaySelling...")

    while len(all_ids) < max_items:
        body = f"""<?xml version="1.0" encoding="utf-8"?>
        <GetMyeBaySellingRequest xmlns="urn:ebay:apis:eBLBaseComponents">
            <RequesterCredentials>
                <eBayAuthToken>{OAUTH_TOKEN}</eBayAuthToken>
            </RequesterCredentials>
            <ActiveList>
                <Include>true</Include>
                <Pagination>
                    <EntriesPerPage>{entries_per_page}</EntriesPerPage>
                    <PageNumber>{page}</PageNumber>
                </Pagination>
            </ActiveList>
        </GetMyeBaySellingRequest>"""

        r, hdrs = _request(
            "POST",
            url,
            headers=headers,
            data=body.encode("utf-8"),
            timeout=30,
            label=f"Trading/GetMyeBaySelling-New p{page}"
        )
        api_meter_browse()

        if not r or r.status_code != 200:
            print(f"   GetMyeBaySelling page {page} failed with status {r.status_code if r else 'N/A'}. Stopping.")
            break

        items_meta = _parse_trading_items_for_metadata(r.text or "")
        if not items_meta:
            print(f"   No <Item> blocks found on page {page}. Stopping pagination.")
            break

        added_this_page = 0
        for meta in items_meta:
            item_id = meta.get("item_id")
            st_raw = meta.get("start_time")
            dt = _parse_ebay_datetime(st_raw) if st_raw else None
            if dt is not None and dt >= cutoff:
                if item_id not in all_ids:
                    all_ids.append(item_id)
                    added_this_page += 1

        print(f"   Page {page}: added {added_this_page} recent IDs (total so far: {len(all_ids)})")

        if len(items_meta) < entries_per_page:
            break

        page += 1

    print(f"✅ Finished fetching recent active IDs. Total unique recent ItemIDs: {len(all_ids)}")
    return all_ids, f"recent GetMyeBaySelling (last {days_back} days)"


def fetch_item_ids_by_custom_sku(target_sku: str) -> Tuple[List[str], str]:
    """
    Fetch active ItemIDs for EXACT CustomLabel match.
    Uses GetMyeBaySelling (Trading API).
    Returns (item_ids, source_label).
    """
    url = "https://api.ebay.com/ws/api.dll"
    headers = {
        "X-EBAY-API-CALL-NAME": "GetMyeBaySelling",
        "X-EBAY-API-SITEID": "0",
        "X-EBAY-API-COMPATIBILITY-LEVEL": "967",
        "Content-Type": "text/xml",
        "Authorization": f"Bearer {OAUTH_TOKEN}",
    }

    page = 1
    entries_per_page = 200
    matched_ids: List[str] = []
    matched_skus: List[str] = []

    print(f"\n🔍 Searching for EXACT Custom SKU = '{target_sku}' via GetMyeBaySelling…")

    while True:
        body = f"""<?xml version="1.0" encoding="utf-8"?>
        <GetMyeBaySellingRequest xmlns="urn:ebay:apis:eBLBaseComponents">
            <RequesterCredentials>
                <eBayAuthToken>{OAUTH_TOKEN}</eBayAuthToken>
            </RequesterCredentials>
            <ActiveList>
                <Include>true</Include>
                <Pagination>
                    <EntriesPerPage>{entries_per_page}</EntriesPerPage>
                    <PageNumber>{page}</PageNumber>
                </Pagination>
                <Sort>TimeLeft</Sort>
            </ActiveList>
        </GetMyeBaySellingRequest>"""

        r, hdrs = _request(
            "POST",
            url,
            headers=headers,
            data=body.encode("utf-8"),
            timeout=25,
            label=f"Trading/SKU p{page}"
        )
        api_meter_browse()

        if not r or r.status_code != 200:
            print("❌ eBay Trading API error while fetching active items.")
            break

        text = r.text or ""
        item_ids = re.findall(r"<ItemID>(\d+)</ItemID>", text)
        skus = re.findall(r"<CustomLabel>(.*?)</CustomLabel>", text)

        if not item_ids:
            break

        # EXACT match only
        for iid, sku in zip(item_ids, skus):
            if sku.strip() == target_sku:
                matched_ids.append(iid)
                matched_skus.append(sku.strip())

        # stop early if fewer than a full page
        if len(item_ids) < entries_per_page:
            break

        page += 1

    if not matched_ids:
        return [], "No exact match"

    return matched_ids, "Exact CustomLabel match"


if __name__ == "__main__":
    main()