import os, sys
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.abspath(os.path.join(CURRENT_DIR, ".."))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)
import time
import math
import json
import pandas as pd
import requests
import platform
from dotenv import load_dotenv
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime, timedelta, timezone
import re
import glob
import hashlib
import csv as csv_mod

# ============================================================
# Helpers (import AFTER sys.path is fixed)
# ============================================================
from helpers_v10 import (
    normalize_title_global,
    extract_year_from_title,
    extract_card_number_from_title,
    extract_player_tokens_from_title,
    extract_set_tokens,
    extract_parallels_from_title,
    detect_insert_flag,
    detect_promo_flag,
    detect_oddball_flag,
)

# ------------------------------------------------------------
# Load structured classification rules (brands, sets, ignore tokens)
# ------------------------------------------------------------
try:
    CLASSIFICATION_RULES = load_token_rules(os.path.join(ROOT_DIR, "token_rules.json"))
except Exception:
    CLASSIFICATION_RULES = {
        "brands": [],
        "sets": [],
        "ignore_tokens": [],
    }

def load_token_rules(path: str = "token_rules.json") -> dict:
    """
    Load token_rules.json with guaranteed safe/default structure.
    """
    if not os.path.exists(path):
        return {
            "multiword_sets": [],
            "token_equivalents": [],
            "ignore_tokens": [],
        }

    try:
        with open(path, "r", encoding="utf-8") as f:
            rules = json.load(f)
    except Exception:
        return {
            "multiword_sets": [],
            "token_equivalents": [],
            "ignore_tokens": [],
        }

    # Ensure all keys exist
    rules.setdefault("multiword_sets", [])
    rules.setdefault("token_equivalents", [])
    rules.setdefault("ignore_tokens", [])

    return rules

def save_token_rules(path: str = "token_rules.json") -> None:
    """
    Write rules back to token_rules.json using pretty indenting.
    """
    try:
        rules = globals().get("token_rules") or load_token_rules(path)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(rules, f, indent=2, ensure_ascii=False)
    except Exception as e:
        print(f"[TokenRules] Failed to save {path}: {e}")

RESET  = "\033[0m"
BOLD   = "\033[1m"
DIM    = "\033[2m"

# Colors
RED    = "\033[31m"
GREEN  = "\033[32m"
YELLOW = "\033[33m"
BLUE   = "\033[34m"
MAGENTA= "\033[35m"
CYAN   = "\033[36m"
WHITE  = "\033[37m"

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

from helpers_v10 import load_manual_overrides, save_manual_overrides

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
# Track the last safe resume position (index) on disk
LAST_RESUME_INDEX_PATH = os.path.join(AUTOSAVE_FOLDER, "last_resume_index.txt")

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

# ---------- Manual Overrides ----------
manual_overrides_path = config_v24.get("manual_overrides_path", "manual_overrides.json")
manual_overrides = load_manual_overrides(os.path.join(BASE_DIR, manual_overrides_path))

CLASSIFICATION_RULES = {}

CLASSIFICATION_RULES = {}

# -----------------------------------------------------------
# TOKEN RULES (global shared dictionary)
# -----------------------------------------------------------
token_rules = load_token_rules()


# Diagnostic collector used only when diagnostic_mode=True
STRICT_DIAG = {
    "removed": []
}

def _diag_reset():
    STRICT_DIAG["removed"] = []

def _diag_log(title, price, reason):
    STRICT_DIAG["removed"].append({
        "title": title,
        "total": price,
        "reason": reason,
    })

def _load_classification_rules():
    global CLASSIFICATION_RULES
    rules_path = os.path.join(os.path.dirname(__file__), "classification_rules.json")
    try:
        with open(rules_path, "r", encoding="utf-8") as f:
            CLASSIFICATION_RULES = json.load(f)
    except Exception:
        CLASSIFICATION_RULES = {
            "oddball_terms": [],
            "promo_terms": [],
            "insert_terms": [],
            "parallel_color_terms": [],
            "parallel_pattern_terms": []
        }

# ---- Learned set phrases (v7) ----------------------------
SET_PHRASES = {}
SET_PHRASE_INDEX = {}

def _canonicalize_phrase_text(s: str) -> str:
    """
    Canonicalize text for phrase matching:
      - lowercase
      - normalize hyphens/underscores/slashes to spaces
      - collapse whitespace
    """
    s = (s or "").lower()
    s = re.sub(r"[\-_/]+", " ", s)
    s = re.sub(r"\s+", " ", s)
    return s.strip()


def _load_set_phrases():
    """
    Load pricing/set_phrases.json (from retro_learn_v7_set_phrases.py)
    and build an index:

        SET_PHRASE_INDEX = {
            "skybox": [
                ["skybox", "emotion"],
                ["skybox", "premium"],
                ...
            ],
            "topps": [
                ["topps", "chrome"],
                ["topps", "chrome", "platinum", "anniversary"],
                ...
            ],
            ...
        }

    Lists are sorted longest-first so we always prefer the longest match.
    """
    global SET_PHRASES, SET_PHRASE_INDEX

    path = os.path.join(os.path.dirname(__file__), "set_phrases.json")
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        SET_PHRASES = {}
        SET_PHRASE_INDEX = {}
        return

    # JSON can be {"phrase": count, ...} or ["phrase1", "phrase2", ...]
    if isinstance(data, dict):
        phrases = [str(k).strip() for k in data.keys()]
    elif isinstance(data, list):
        phrases = [str(x).strip() for x in data]
    else:
        phrases = []

    # Build first-token → list of token-lists index
    index = {}
    for phrase in phrases:
        norm = _canonicalize_phrase_text(phrase)
        toks = norm.split()
        if not toks:
            continue
        first = toks[0]
        index.setdefault(first, []).append(toks)

    # Sort candidates for each first-token by length DESC (longest phrase first)
    for first, lst in index.items():
        lst.sort(key=lambda t: len(t), reverse=True)

    SET_PHRASES = { _canonicalize_phrase_text(p): True for p in phrases }
    SET_PHRASE_INDEX = index

# ------------------------------------------------------------
# Brand families (v6) — Loaded from brand_families.json
# ------------------------------------------------------------
from typing import List  # should already be imported; safe if duplicated

BRAND_FAMILIES_PATH = os.path.join(os.path.dirname(__file__), "brand_families.json")
BRAND_FAMILIES: List[Dict[str, str]] = []


def _load_brand_families() -> None:
    """
    Load brand_families.json once at import-time.

    Expected structure (what you actually have):

    [
      { "pattern": "skybox", "canonical": "skybox" },
      { "pattern": "skybox impact", "canonical": "skybox impact" },
      ...
    ]
    """
    global BRAND_FAMILIES
    path = BRAND_FAMILIES_PATH
    families: List[Dict[str, str]] = []

    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        print(f"[BRAND_FAMILIES] Failed to load {path}: {e}")
        BRAND_FAMILIES = []
        return

    # Case 1: list of {pattern, canonical}  ← your current file
    if isinstance(data, list):
        for entry in data:
            if not isinstance(entry, dict):
                continue
            pattern = str(entry.get("pattern", "")).strip().lower()
            canonical = str(entry.get("canonical", "")).strip().lower()
            if not pattern or not canonical:
                continue
            families.append({"pattern": pattern, "canonical": canonical})

    # Case 2: optional dict fallback for future formats
    elif isinstance(data, dict):
        for canonical, patterns in data.items():
            canon = str(canonical).strip().lower()
            if not canon:
                continue
            if isinstance(patterns, str):
                patterns = [patterns]
            for pat in patterns or []:
                p = str(pat).strip().lower()
                if not p:
                    continue
                families.append({"pattern": p, "canonical": canon})

    BRAND_FAMILIES = families
    print(f"[BRAND_FAMILIES] Loaded {len(BRAND_FAMILIES)} brand patterns from {os.path.basename(path)}")

# Load once when module imports
_load_brand_families()
_load_classification_rules()
_load_set_phrases()

# ================= LOAD TOKEN =================
load_dotenv()
OAUTH_TOKEN = os.getenv("EBAY_OAUTH_TOKEN", "").strip()
APP_ID = os.getenv("EBAY_APP_ID", "").strip()

if not OAUTH_TOKEN.startswith("v^"):
    print("Warning: Invalid or missing token in .env")
if not APP_ID:
    print("Warning: Missing EBAY_APP_ID in .env")

def clear_screen():
    os.system('cls' if platform.system() == 'Windows' else 'clear')

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

def normalize_title(t):
    if isinstance(t, list):
        return " ".join([str(x).strip() for x in t if x])
    return str(t or "").strip()

def _is_graded(title: str) -> bool:
    if not title:
        return False
    title = title.upper()
    return any(term in title for term in ["PSA", "BGS", "SGC", "CGC", "CSG", "GMA", "BCCG"])

_SERIAL_RE = re.compile(r"(\d+\s*/\s*\d+|#\s*\d+\s*/\s*\d+)", re.I)

# Dynamic exclusion terms (to avoid high-end/graded comps when title isn't graded)
EXCLUDE_TERMS = ["auto", "autograph", "signature", "graded", "psa", "bgs", "sgc", "serial"]


def _extract_serial_fragment(title: str) -> Optional[str]:
    """
    Extract a normalized serial fragment like '12/250' from a title.
    Returns lowercase '12/250' or None.
    """
    if not title:
        return None
    m = _SERIAL_RE.search(title)
    if not m:
        return None
    frag = m.group(1)
    frag = frag.replace("#", "").replace(" ", "")
    return frag.lower()

# ---------------------------------------------------------
# SIMPLE TITLE PARSER FOR QUERY BUILDING
# ---------------------------------------------------------

def _parse_title_for_queries(title: str):
    """
    Extract (year, player, set_name) from the fully generalized title parser.

    Uses:
      - _extract_card_signature_from_title()  ← JSON-driven
      - normalize_token()
      - JSON token_rules["multiword_sets"]
      - JSON token_rules["token_equivalents"]
      - JSON token_rules["ignore_tokens"]

    Returns:
        (year: str or None,
         player: str or None,
         set_name: str or None)
    """
    if not title:
        return None, None, None

    sig = _extract_card_signature_from_title(title)
    if not sig:
        return None, None, None

    year = None
    if sig.get("year"):
        year = str(sig["year"])

    # --------------------------
    # PLAYER NAME
    # --------------------------
    # Join player tokens back into a name, properly capitalized
    player_tokens = sig.get("player_tokens") or []
    player = None
    if player_tokens:
        # Capitalize each token safely
        player = " ".join(t.capitalize() for t in player_tokens)

    # --------------------------
    # SET / FAMILY NAME
    # --------------------------
    fam_raw = sig.get("family_tokens") or []

    # We intentionally rebuild using SIMPLE tokens here.
    # The multiword_sets rule already captures pairs like:
    #   ["skybox","emotion"]
    # So we re-stitch them properly with Title Case.
    set_name = None
    if fam_raw:
        cleaned = [normalize_token(t) for t in fam_raw if normalize_token(t)]
        if cleaned:
            set_name = " ".join(w.capitalize() for w in cleaned)

    # --------------------------
    # Final return
    # --------------------------
    return year, player, set_name

# ============================================================
# v8 DYNAMIC QUERY BUILDER
# ============================================================

NEGATIVE_TOKENS = "-lot -lots -factory -break -case -sealed -bundle"

def _parse_title_for_queries(title: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Extract (year, player, set_name) from the v7 card signature.

    Uses:
      - _extract_card_signature_from_title()
      - detect_set_phrases_from_title() (via signature['set_phrase'])

    Returns:
        (year: str | None,
         player: str | None,
         set_name: str | None)
    """
    if not title:
        return None, None, None

    sig = _extract_card_signature_from_title(title)
    if not sig:
        return None, None, None

    # YEAR
    year_val = sig.get("year")
    year = str(year_val) if year_val is not None else None

    # PLAYER
    player_tokens = sig.get("player_tokens") or []
    player: Optional[str] = None
    if player_tokens:
        player = " ".join(t.capitalize() for t in player_tokens)

    # SET / FAMILY NAME
    set_phrase = sig.get("set_phrase")
    fam_raw = sig.get("family_tokens") or []
    set_name: Optional[str] = None

    if set_phrase:
        # strict canonical phrase → Title Case for query
        set_name = " ".join(w.capitalize() for w in set_phrase.split())
    elif fam_raw:
        cleaned = [str(t).lower() for t in fam_raw if t]
        if cleaned:
            set_name = " ".join(w.capitalize() for w in cleaned)

    return year, player, set_name

def _build_dynamic_query(title: str, rules: Optional[dict] = None) -> str:
    """
    Primary smart active-comp query.

    • Uses JSON-driven token builder for the base query
    • Appends negative terms from token_rules.json if present,
      otherwise falls back to the existing default lot/box/break filters.
    """
    base_candidates = _build_token_based_queries(title, rules)
    if base_candidates:
        base = base_candidates[0]
    else:
        base = normalize_title_global(title)

    if not base:
        base = ""

    # Negative filters
    if rules is None:
        rules = globals().get("token_rules") or {}

    negative_terms = rules.get("negative_terms")
    if not negative_terms:
        negative_terms = [
            "-lot",
            "-lots",
            "-factory",
            "-break",
            "-case",
            "-sealed",
        ]

    return " ".join([base] + negative_terms).strip()

def _build_active_fallback_queries(title: Any) -> List[str]:
    """
    v8 fallback builder — fully sanitized, string-safe.
    """
    if isinstance(title, dict):
        raw = title.get("value") or title.get("title") or ""
    else:
        raw = str(title or "")

    raw = raw.strip()
    if not raw:
        return []

    norm = normalize_title_global(raw)

    year = extract_year_from_title(norm) or ""
    player_tokens = extract_player_tokens_from_title(norm) or []
    set_tokens = extract_set_tokens(norm) or []

    filters = " -lot -lots -factory -break -case -sealed"

    queries = []

    # (1) Year + norm
    if year:
        queries.append(f"{year} {norm}{filters}")

    # (2) Player + set
    if player_tokens and set_tokens:
        queries.append(f"{' '.join(player_tokens)} {' '.join(set_tokens)}{filters}")

    # (3) Bare normalized
    queries.append(f"{norm}{filters}")

    # Dedupe
    out = []
    seen = set()
    for q in queries:
        qq = q.lower().strip()
        if qq not in seen:
            seen.add(qq)
            out.append(q.strip())
    return out

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
            # Option E: No forced cooldown. Warn and return True so caller retries immediately.
            print(f"No reset header returned for 429 on {label}. Continuing without forced sleep.")
            time.sleep(300)
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

# =======================================================
# TOKEN RULES LOADER (Auto-learning JSON brain)
# Loads token_rules.json once and allows appends
# =======================================================

RULES: Dict = None
TOKEN_RULES_PATH: Optional[str] = None

# =======================================================
# TOKEN NORMALIZATION (Step 3)
# =======================================================

# ---------------------------------------------------------
# TOKEN NORMALIZATION
# ---------------------------------------------------------
import re  # should already exist at top of file; if not, keep this

def normalize_token(tok: str) -> str:
    """
    JSON-driven normalization.
    - lowercase
    - remove punctuation/spacing variations
    - apply token equivalence groups from token_rules.json
    - skip ignore tokens
    """
    if not tok:
        return ""

    rules = load_token_rules()
    ignore = set(rules.get("ignore_tokens", []))
    equivalence_groups = rules.get("token_equivalents", [])
    punct_eq = rules.get("punctuation_equivalents", [])

    # ----------------------------
    # 1. Basic cleanup
    # ----------------------------
    s = str(tok).lower().strip()

    # Apply punctuation equivalences
    for old, new in punct_eq:
        s = s.replace(old, new)

    # Remove non-alphanumeric sequences → space
    s = re.sub(r"[^a-z0-9]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()

    # Skip ignored tokens
    if s in ignore:
        return ""

    # ----------------------------
    # 2. Token equivalence groups
    # ----------------------------
    for group in equivalence_groups:
        group_norm = [g.lower() for g in group]
        if s in group_norm:
            # normalize to first form in group
            return group_norm[0]

    return s

def normalize_title_for_learning(title: str) -> str:
    """
    Normalize a title for learning:
      - lowercase
      - remove punctuation
      - collapse spaces
      - standardize JR/SR
    """
    if isinstance(title, dict):
        # eBay / API shapes like {"value": "..."} or {"title": "..."}
        title = title.get("value") or title.get("title") or ""

    # If still empty after unwrap → bail
    if not title:
        return ""

    # Always work with a string
    t = str(title).lower()

    # Normalize jr/sr/iii formatting
    t = t.replace("jr.", "jr")
    t = t.replace("sr.", "sr")
    t = t.replace("iii", "iii")  # safe no-op but intentional

    # Replace all non-alphanumeric characters with spaces
    t = re.sub(r"[^a-z0-9 ]+", " ", t)

    # Collapse multiple spaces
    t = re.sub(r"\s+", " ", t).strip()

    return t

# ---------------------------------------------------------
# SET SIMILARITY / MATCH HELPER
# ---------------------------------------------------------

def sets_match(subject_set, comp_set, rules: dict | None = None) -> bool:
    """
    JSON-driven set family comparison.

    Uses:
      - normalize_token()
      - token_rules["similarity"]["min_jaccard"]
      - token_rules["similarity"]["min_levenshtein_ratio"]

    Logic:
      1. Normalize tokens
      2. Compute Jaccard overlap
      3. If Jaccard >= threshold → match
      4. If *any* token pair has Levenshtein ratio >= threshold → match
      5. Otherwise → no match
    """
    if not subject_set or not comp_set:
        return False

    # Load token rules (has Jaccard + Levenshtein thresholds)
    rules = rules or load_token_rules()
    sim_cfg = rules.get("similarity", {})

    min_jaccard = float(sim_cfg.get("min_jaccard", 0.40))
    min_lev = float(sim_cfg.get("min_levenshtein_ratio", 0.78))

    # --------------------------------------
    # Normalize sets to canonical form
    # --------------------------------------
    subj = {normalize_token(t) for t in subject_set if normalize_token(t)}
    comp = {normalize_token(t) for t in comp_set if normalize_token(t)}

    if not subj or not comp:
        return False

    # --------------------------------------
    # Jaccard similarity
    # --------------------------------------
    inter = subj.intersection(comp)
    union = subj.union(comp)
    jaccard = len(inter) / len(union) if union else 0.0

    if jaccard >= min_jaccard:
        return True

    # --------------------------------------
    # Levenshtein ratio across all token pairs
    # --------------------------------------
    def lev_ratio(a, b):
        # Simple optimized Levenshtein ratio
        if a == b:
            return 1.0
        la, lb = len(a), len(b)
        if la == 0 or lb == 0:
            return 0.0

        # DP matrix
        prev = list(range(lb + 1))
        for i in range(1, la + 1):
            curr = [i] + [0] * lb
            for j in range(1, lb + 1):
                cost = 0 if a[i-1] == b[j-1] else 1
                curr[j] = min(
                    prev[j] + 1,
                    curr[j-1] + 1,
                    prev[j-1] + cost
                )
            prev = curr

        dist = prev[-1]
        max_len = max(la, lb)
        return 1 - (dist / max_len)

    # If ANY token pair is similar enough, accept
    for s in subj:
        for c in comp:
            if lev_ratio(s, c) >= min_lev:
                return True

    return False

# ================= STRICT CARD SIGNATURE HELPERS (Option A) =================
def detect_set_phrases_from_title(title: str) -> List[str]:
    """
    Use SET_PHRASE_INDEX (mined from ActiveListings.csv) to detect
    multi-word set phrases in a title.

    Returns phrases in canonicalized, space-separated form, e.g.:

        ["skybox emotion", "topps chrome platinum anniversary"]
    """
    if not title or not SET_PHRASE_INDEX:
        return []

    norm = _canonicalize_phrase_text(title)
    tokens = norm.split()
    n = len(tokens)
    i = 0
    phrases: List[str] = []

    while i < n:
        first = tokens[i]
        candidates = SET_PHRASE_INDEX.get(first)
        best_match: Optional[List[str]] = None

        if candidates:
            # candidates already sorted longest-first
            for cand in candidates:
                clen = len(cand)
                if i + clen <= n and tokens[i:i+clen] == cand:
                    best_match = cand
                    break

        if best_match:
            phrase = " ".join(best_match)
            phrases.append(phrase)
            i += len(best_match)
        else:
            i += 1

    return phrases


from typing import Optional  # at top of file if not already present

def extract_set_phrase_from_title(title: str) -> Optional[str]:
    """
    Use v7 SET_PHRASE_INDEX via detect_set_phrases_from_title().
    Longest phrase wins; returns canonical phrase or None.
    """
    phrases = detect_set_phrases_from_title(title)
    # Prefer phrase with most tokens, then longest string
    return max(
        phrases,
        key=lambda p: (len(p.split()), len(p)),
        default=None,
    )

def _extract_card_signature_from_title(title: str) -> Dict[str, Any]:
    """
    Universal signature extractor (v8).

    Works for:
      • raw listing titles
      • dynamic query strings

    Uses only JSON + helper functions (helpers_v10).
    """
    if not title:
        return {}

    norm_title = normalize_title_global(title)

    # Core structured pieces
    year = extract_year_from_title(title)
    card_num = extract_card_number_from_title(title)
    players = extract_player_tokens_from_title(title)          # always from original wording
    set_tokens = extract_set_tokens(title)                     # e.g. ['skybox', 'e', 'motion']
    parallels = extract_parallels_from_title(title)
    is_insert = detect_insert_flag(title)
    is_promo = detect_promo_flag(title)
    is_oddball = detect_oddball_flag(title)

    # ---------- BRAND FAMILY (from brand_families.json) ----------
    brand_family: Optional[str] = None
    lowered = norm_title.lower()

    for bf in BRAND_FAMILIES:
        # bf is a dict: {"pattern": "...", "canonical": "..."}
        pattern = str(bf.get("pattern", "")).strip().lower()
        canonical = str(bf.get("canonical", "")).strip().lower()
        if not pattern or not canonical:
            continue

        # Simple substring match on normalized title
        if pattern in lowered:
            brand_family = canonical
            break

    # ---------- SET PHRASE (from set_phrases.json via helper) ----------
    # This uses SET_PHRASE_INDEX / detect_set_phrases_from_title()
    # and returns a canonical phrase like "skybox e motion".
    set_phrase = extract_set_phrase_from_title(title)

    return {
        "year": year,
        "card_num": card_num,
        "player_tokens": players or [],
        "set_tokens": set_tokens or [],
        "parallels": parallels or [],
        "is_insert": bool(is_insert),
        "is_promo": bool(is_promo),
        "is_oddball": bool(is_oddball),
        "brand_family": brand_family,
        "set_phrase": set_phrase,
    }

def update_token_rules_from_signature(sig: Optional[Dict]) -> None:
    """
    JSON-driven update of token_rules from a card's parsed signature.
    MODE B (Kevin): learn ONLY set/brand structures.

    Learns:
      • multiword set names  (e.g., ["skybox","emotion"])
      • brand + family combos
      • composite families discovered in titles

    DOES NOT LEARN:
      • player names
      • single-word tokens
      • junk tokens
    """
    if not sig:
        return

    rules = load_token_rules()
    changed = False

    # RULE CONTAINERS
    multi_sets = rules.setdefault("multiword_sets", [])
    ignore_tokens = set(rules.setdefault("ignore_tokens", []))
    token_equivs = rules.setdefault("token_equivalents", [])

    # SUBJECT TOKENS
    fam_tokens = sig.get("family_tokens") or []
    brand_tokens = sig.get("brand_tokens") or []
    set_tokens  = sig.get("set_tokens")  or []

    # ---------------------------------------------------------
    # Helper: normalize + dedupe
    # ---------------------------------------------------------
    def _norm_list(lst):
        return [normalize_token(t) for t in lst if normalize_token(t)]

    # ---------------------------------------------------------
    # 1) LEARN MULTIWORD FAMILY TOKENS
    # e.g. ["skybox","emotion"], ["upper","deck"]
    # ---------------------------------------------------------
    fam_norm = _norm_list(fam_tokens)

    if len(fam_norm) >= 2:
        pair = fam_norm[:2]
        if pair not in multi_sets:
            multi_sets.append(pair)
            changed = True

    # ---------------------------------------------------------
    # 2) LEARN BRAND + SET TOKENS
    # e.g. brand=["skybox"], set_tokens=["emotion"]
    # ---------------------------------------------------------
    if brand_tokens and set_tokens:
        bn = normalize_token(brand_tokens[0])
        sn = normalize_token(set_tokens[0])
        if bn and sn and bn not in ignore_tokens and sn not in ignore_tokens:
            pair = [bn, sn]
            if pair not in multi_sets:
                multi_sets.append(pair)
                changed = True

    # ---------------------------------------------------------
    # 3) EXPAND EXISTING MULTIWORD SETS USING TOKEN EQUIVALENTS
    #
    # If we have ["emotion","skybox"] and "e-motion" appears,
    # add a normalized variant.
    # ---------------------------------------------------------
    for fam in [fam_norm]:
        for group in token_equivs:
            norm_group = [normalize_token(g) for g in group]
            inter = set(fam).intersection(norm_group)
            if inter:
                # Expand multiword sets using equivalences
                new = [normalize_token(t) for t in fam]
                if new not in multi_sets:
                    multi_sets.append(new)
                    changed = True

    # ---------------------------------------------------------
    # SAVE IF ANYTHING CHANGED
    # ---------------------------------------------------------
    if changed:
        save_token_rules()

# =======================================================
# OPTIONAL LEARNING ENGINE FOR GUI (Step 3)
# Accepts optional callback (GUI learn log)
# =======================================================
def learn_from_title(title: str, learn_callback=None):
    """
    JSON-driven learning engine.
    Automatically expands token_rules.json with:

       • multiword set names   (e.g., ["skybox","emotion"])
       • token equivalence     (e.g., ["emotion","e-motion","e motion"])
       • safe ignore tokens    (e.g., "official", "limited")
       • brand+family patterns (SkyBox + Emotion)
    """
    # ---- NEW: unwrap dict titles safely ----
    if isinstance(title, dict):
        title = title.get("value") or title.get("title") or ""

    if not title:
        return

    # From here down, title is guaranteed string
    sig = _extract_card_signature_from_title(title)
    if not sig:
        return

    rules = load_token_rules()
    changed = False
    events = []
    raw = str(title).lower()

    # ------------------------------------------------------------
    # 1) LEARN MULTIWORD SET NAMES
    # ------------------------------------------------------------
    fam = sig.get("family_tokens") or []
    brand = sig.get("brand_tokens") or []
    set_tokens = sig.get("set_tokens") or []

    # Example:
    #   fam = ["skybox","emotion"]   → multiword_set
    #   brand = ["skybox"], set_tokens=["emotion"] → multiword_set
    #
    # Learn ANY 2–3 token sequence that looks like a set/family.
    if len(fam) >= 2:
        pair = [normalize_token(fam[0]), normalize_token(fam[1])]
        if pair not in rules["multiword_sets"]:
            rules["multiword_sets"].append(pair)
            changed = True
            events.append(f"[LEARN] Added multiword set: {pair}")

    # brand + set tokens → also a multiword set
    if brand and set_tokens:
        pair = [normalize_token(brand[0]), normalize_token(set_tokens[0])]
        if pair not in rules["multiword_sets"]:
            rules["multiword_sets"].append(pair)
            changed = True
            events.append(f"[LEARN] Added brand+set multiword: {pair}")

    # ------------------------------------------------------------
    # 2) LEARN TOKEN EQUIVALENCE GROUPS
    # ------------------------------------------------------------
    eq_map = {
        "emotion": ["emotion", "e-motion", "e'motion", "e motion"],
        "chrome":  ["chrome", "chromium"],
        "prizm":   ["prizm", "prism"],
    }

    for key, variants in eq_map.items():
        if any(v in raw for v in variants):
            group_norm = [normalize_token(v) for v in variants]
            if group_norm not in rules["token_equivalents"]:
                rules["token_equivalents"].append(group_norm)
                changed = True
                events.append(f"[LEARN] Added token-equivalent: {group_norm}")

    # ------------------------------------------------------------
    # 3) LEARN IGNORE TOKENS (SAFE ONLY)
    # ------------------------------------------------------------
    # Words safe to ignore in set matching (only if title contains them)
    safe_ignore = [
        "official", "collectors", "limited", "special",
        "bonus", "exclusive", "member", "promo", "edition",
    ]

    for tok in safe_ignore:
        if tok in raw:
            nt = normalize_token(tok)
            if nt and nt not in rules["ignore_tokens"]:
                rules["ignore_tokens"].append(nt)
                changed = True
                events.append(f"[LEARN] Added ignore token: {nt}")

    # ------------------------------------------------------------
    # 4) SAVE + FORWARD EVENTS TO GUI
    # ------------------------------------------------------------
    if changed:
        save_token_rules()
        if learn_callback:
            for ev in events:
                learn_callback(ev)

def _normalize_spaces(s: str) -> str:
    return re.sub(r"\s+", " ", s or "").strip()


def _normalize_token_for_query(tok: str) -> str:
    """
    Normalize a token for querying:
      - lowercase
      - strip punctuation like '-' and '.'
      - keeps alphanumerics
    This lets 'E-Motion', 'E Motion', 'E.Motion' all become 'emotion' in the query.
    """
    if not tok:
        return ""
    t = tok.lower()
    t = re.sub(r"[^\w]+", "", t)  # keep letters/numbers/underscore
    return t

def _canonicalize_brand_text(s: str) -> str:
    """
    Canonicalize text for brand-family matching:
      - lowercase
      - normalize hyphens/underscores/slashes to spaces
      - collapse multiple spaces
    """
    s = (s or "").lower()
    s = re.sub(r"[\-_/]+", " ", s)
    s = re.sub(r"\s+", " ", s)
    return s.strip()

def detect_brand_family(title: str) -> Dict[str, Optional[str]]:
    """
    Use BRAND_FAMILIES (from brand_families.json) to detect a canonical brand family
    from a raw title string.

    Returns:
        {
          "brand_family": "topps",         # canonical key from BRAND_FAMILIES
          "brand_tokens": ["topps"],       # tokenized canonical family
          "matched_variant": "topps"       # exact variant text matched in title
        }
    or all-None/empty if nothing detected.
    """
    txt = _canonicalize_brand_text(title)
    if not txt or not BRAND_FAMILIES:
        return {"brand_family": None, "brand_tokens": [], "matched_variant": None}

    padded = f" {txt} "
    best_family = None
    best_variant = None
    best_len = 0

    for canon, variants in BRAND_FAMILIES.items():
        # Consider both the canonical key and any stored variants
        all_variants = set(variants or [])
        all_variants.add(canon)

        for var in all_variants:
            v = _canonicalize_brand_text(var)
            if not v:
                continue

            pattern = f" {v} "
            if pattern in padded:
                # Prefer the longest matched phrase to avoid 'topps' beating 'rookies & stars'
                if len(v) > best_len:
                    best_len = len(v)
                    best_family = _canonicalize_brand_text(canon)
                    best_variant = v

    if not best_family:
        return {"brand_family": None, "brand_tokens": [], "matched_variant": None}

    return {
        "brand_family": best_family,
        "brand_tokens": best_family.split(),
        "matched_variant": best_variant,
    }

def _build_token_based_queries(title: str, rules: Optional[dict] = None) -> List[str]:
    """
    Build ranked candidate queries from the JSON-driven card signature.

    Uses:
      • year
      • set_phrase / family_tokens
      • brand_family / brand_tokens
      • player_tokens
      • card_num

    Then we normalize + dedupe.
    """
    sig = _extract_card_signature_from_title(title)
    if not sig:
        norm = normalize_title_global(title)
        return [norm] if norm else []

    year = sig.get("year")
    card_num = sig.get("card_num")
    player_tokens = sig.get("player_tokens") or []
    family_tokens = sig.get("family_tokens") or []
    set_phrase = sig.get("set_phrase") or ""
    brand_family = sig.get("brand_family") or ""
    brand_tokens = sig.get("brand_tokens") or []

    player = " ".join(player_tokens).strip()
    family = " ".join(family_tokens).strip()
    brand = " ".join(brand_tokens).strip()

    # Use explicit set_phrase if present, otherwise fall back to family tokens
    if set_phrase:
        core_phrase = set_phrase
    else:
        parts = [brand, family]
        core_phrase = " ".join(p for p in parts if p).strip()

    if not core_phrase:
        core_phrase = normalize_title_global(title)

    core_phrase = normalize_title_global(core_phrase)

    queries: List[str] = []

    # (1) year + phrase + #card + player
    if year and card_num and player:
        queries.append(f"{year} {core_phrase} #{card_num} {player}")
        queries.append(f"{year} {core_phrase} {card_num} {player}")

    # (2) phrase + #card + player
    if card_num and player:
        queries.append(f"{core_phrase} #{card_num} {player}")

    # (3) player + phrase + #card
    if player and card_num:
        queries.append(f"{player} {core_phrase} #{card_num}")

    # (4) year + phrase + player
    if year and player:
        queries.append(f"{year} {core_phrase} {player}")

    # (5) year + phrase
    if year:
        queries.append(f"{year} {core_phrase}")

    # (6) phrase + player
    if player:
        queries.append(f"{core_phrase} {player}")

    # Fallback: normalized title
    norm_title = normalize_title_global(title)
    if norm_title:
        queries.append(norm_title)

    # Normalize + dedupe, preserve order
    seen: set[str] = set()
    out: List[str] = []
    for q in queries:
        qn = normalize_title_global(q)
        if not qn:
            continue
        if qn in seen:
            continue
        seen.add(qn)
        out.append(qn)

    return out

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

def _titles_match_strict(
    subject_sig: Dict[str, Any],
    comp_title: str,
    comp_price: float
) -> bool:
    """
    Strict v8/v7-style match, driven entirely by the signature dicts.

    Rules (when the subject has the field set):

      • Same year
      • Same card number
      • Same brand_family
      • Same set_phrase (canonicalized)
      • Compatible parallels (comp must include all subject parallels)
      • Same insert/promo/oddball flags
      • At least one shared player token

    If subject_sig is empty, we accept everything (no additional filter).
    """
    if not subject_sig:
        return True

    comp_sig = _extract_card_signature_from_title(comp_title)
    if not comp_sig:
        return False

    # ---------- YEAR ----------
    sy = subject_sig.get("year")
    cy = comp_sig.get("year")
    if sy and cy and sy != cy:
        return False

    # ---------- CARD NUMBER ----------
    sn = subject_sig.get("card_num")
    cn = comp_sig.get("card_num")
    if sn and cn and sn != cn:
        return False

    # ---------- BRAND FAMILY ----------
    sb = subject_sig.get("brand_family")
    cb = comp_sig.get("brand_family")
    if sb and cb and sb != cb:
        return False

    # ---------- SET PHRASE (canonical, if subject has one) ----------
    sp = subject_sig.get("set_phrase")
    cp = comp_sig.get("set_phrase")
    if sp:
        if not cp:
            return False
        if normalize_title_global(sp) != normalize_title_global(cp):
            return False

    # ---------- PARALLELS ----------
    s_par = set(subject_sig.get("parallels") or [])
    c_par = set(comp_sig.get("parallels") or [])
    if s_par and c_par and not s_par.issubset(c_par):
        return False

    # ---------- INSERT / PROMO / ODDBALL ----------
    for flag in ("is_insert", "is_promo", "is_oddball"):
        sv = subject_sig.get(flag)
        cv = comp_sig.get(flag)
        if sv is not None and cv is not None and sv != cv:
            return False

    # ---------- PLAYER OVERLAP ----------
    s_players = set(subject_sig.get("player_tokens") or [])
    c_players = set(comp_sig.get("player_tokens") or [])
    if s_players and c_players and s_players.isdisjoint(c_players):
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
    # print(f"[BROWSE QUERY] {query}")

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
        title_it = normalize_title(it.get("title"))
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
def search_active(
    title: str,
    limit: int = ACTIVE_LIMIT,
    active_cache: Optional[Dict] = None,
) -> Tuple[List[float], str, int]:
    """
    Unified active-comp search with cache & fallback queries.
    Fully patched with v7 strict signature matching layer.
    """

    # --- FIX: convert dict titles into string FIRST ---
    if isinstance(title, dict):
        title = title.get("value") or title.get("title") or ""

    title = str(title or "").strip()
    raw_title = title

    supply_count = 0

    # ------------------ CACHE CHECK ------------------
    if active_cache is not None:
        cached, from_cache = maybe_use_active_cache(title, active_cache, ACTIVE_CACHE_TTL_MIN)
        if from_cache and cached:
            supply_count = len(cached)
            return cached, "ActiveCache (Merged)", supply_count

    # ------------------ QUERY BUILDER ------------------
    raw_title = (title or "").strip()
    dynamic_query = _build_dynamic_query(title)

    print("Query builder loaded from:", _build_token_based_queries.__code__.co_filename)

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

    # ------------------ BROWSE MERGE ------------------
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

    # ------------------ FINDING MERGE ------------------
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

    # =====================================================
    # STRICT FILTERING LAYER (v7 STRICT MATCHER — FINAL)
    # =====================================================
    subject_sig = _extract_card_signature_from_title(title)
    subject_serial = _extract_serial_fragment(title)

    filtered_items: List[Dict] = []
    removed_items: List[Dict] = []

    for it in merged_items:
        comp_title = it["title"] or ""
        price = it["total"]
        reason = None

        # SERIAL CHECK (enforced only if subject is serial-numbered)
        comp_serial = _extract_serial_fragment(comp_title)
        if subject_serial and comp_serial and comp_serial != subject_serial:
            reason = f"Serial mismatch: subject '{subject_serial}' vs comp '{comp_serial}'"
        else:
            # STRICT MATCH (v7 signature)
            comp_sig = _extract_card_signature_from_title(comp_title)
            if subject_sig is not None and not _titles_match_strict(subject_sig, comp_sig):
                reason = "Failed _titles_match_strict (v7 signature mismatch)"

            # PREMIUM GUARDRAIL (safe hybrid)
            elif price >= 10 and not safe_hybrid_filter(title, comp_title, price):
                reason = "Rejected by safe_hybrid_filter"

        if reason:
            removed_items.append({
                "title": comp_title,
                "total": price,
                "reason": reason,
            })
        else:
            filtered_items.append(it)

    # No strict survivors
    if not filtered_items:
        return [], "No actives", supply_count

    # Sort final actives (A2 lowest-k expects ascending)
    filtered_items.sort(key=lambda x: x["total"])
    active_totals = [it["total"] for it in filtered_items]
    supply_count = len(active_totals)

    # Determine merged source string
    source_bits = []
    if any_browse:
        source_bits.append("Browse")
    if any_finding:
        source_bits.append("Finding")

    act_source = " + ".join(source_bits) + " (Merged)" if source_bits else "No actives"

    # Save active list to cache
    if active_cache is not None and active_totals:
        update_active_cache(title, active_totals, active_cache)

    return active_totals, act_source, supply_count

def debug_capture_from_title(title: str) -> Dict[str, Any]:
    """
    v8 debug capture:
    Fully traces:
      • raw merge (Browse + Finding)
      • strict signature filtering
      • removed comps + reasons
      • final active_totals
      • A2 median computation
    Returns a dict consumed by the GUI.
    """
    # --- Step 1: Build all queries like search_active() does ---
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

    # --- Step 2: RAW MERGE (NORMALIZED FORMAT) ---
    merged_items: List[Dict] = []
    seen_keys = set()

    def normalize(it):
        """Normalize API item into {'title': str, 'total': float}."""
        if isinstance(it, dict):
            t = it.get("title") or ""
            p = it.get("total")
        else:
            # If the API ever passes raw strings, normalize safely
            t = str(it)
            p = None

        try:
            p = float(p)
        except:
            p = 0.0

        return {"title": t.strip(), "total": p}

    # Browse merge
    for q in browse_queries:
        if not q:
            continue
        items = _fetch_active_items_browse_for_query(q, ACTIVE_LIMIT)
        for it in items:
            it2 = normalize(it)
            key = (it2["title"].lower(), it2["total"])
            if key in seen_keys:
                continue
            seen_keys.add(key)
            merged_items.append(it2)

    # Finding merge
    for q in finding_queries:
        if not q:
            continue
        items = _fetch_active_items_finding_for_query(q, ACTIVE_LIMIT)
        for it in items:
            it2 = normalize(it)
            key = (it2["title"].lower(), it2["total"])
            if key in seen_keys:
                continue
            seen_keys.add(key)
            merged_items.append(it2)

    # --- Step 3: STRICT FILTER LAYER ---
    subject_sig = _extract_card_signature_from_title(title)
    subject_serial = _extract_serial_fragment(title)

    filtered_items: List[Dict] = []
    removed_items: List[Dict] = []

    for it in merged_items:
        comp_title = it["title"]
        price = it["total"]
        reason = None

        # SERIAL CHECK
        comp_serial = _extract_serial_fragment(comp_title)
        if subject_serial and comp_serial and comp_serial != subject_serial:
            reason = (
                f"Serial mismatch: subject '{subject_serial}' vs comp '{comp_serial}'"
            )
        else:
            comp_sig = _extract_card_signature_from_title(comp_title)

            # STRICT SIGNATURE MATCH
            if subject_sig is not None and not _titles_match_strict(subject_sig, comp_sig):
                reason = "Failed _titles_match_strict (v8 mismatch)"

            # SAFE HYBRID FILTER (only for higher-value comps)
            elif price >= 10 and not safe_hybrid_filter(title, comp_title, price):
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
            filtered_items.append(it)

    # --- Step 4: Extract active_totals ---
    filtered_items_sorted = sorted(filtered_items, key=lambda x: x["total"])
    active_totals = [float(it["total"]) for it in filtered_items_sorted]

    # --- Step 5: A2 median computation ---
    act_values = sorted(active_totals)
    lowest_k = act_values[:5]
    if not lowest_k:
        median_active = None
    else:
        n = len(lowest_k)
        mid = n // 2
        if n % 2 == 1:
            median_active = round(lowest_k[mid], 2)
        else:
            median_active = round((lowest_k[mid - 1] + lowest_k[mid]) / 2, 2)

    # --- Step 6: RETURN STRUCTURE ---
    return {
        "raw_items": merged_items,
        "filtered_items": filtered_items_sorted,
        "removed_items": removed_items,
        "active_totals": active_totals,
        "act_values": act_values,
        "lowest_k": lowest_k,
        "median_active": median_active,
    }

def _fetch_active_items_browse_for_query(query: str, limit: int = ACTIVE_LIMIT) -> List[Dict]:
    """
    v8 — Browse API for ACTIVE comps

    IMPORTANT:
    • Uses the exact request shape that we just verified manually in REPL.
    • No hard-coding of player names or sets.
    • Still applies:
        – ungraded only
        – no auctions / variations / lots / boxes
        – v8 strict signature match (subject vs comp)
    """
    if not query:
        return []

    print(f"[BROWSE MERGED] {query}")

    # --- 1) Build headers exactly like your working REPL test ---
    headers = _headers().copy()  # includes Authorization
    # make sure marketplace header is present (your REPL added this)
    if "X-EBAY-C-MARKETPLACE-ID" not in headers:
        headers["X-EBAY-C-MARKETPLACE-ID"] = "EBAY_US"

    params = {
        "q": query,
        "limit": str(limit),
        # NOTE: we let Browse return everything, then filter in Python.
        # This matches your working REPL call.
    }

    # Use requests.get directly here to avoid any surprises in _request()
    try:
        r = requests.get(
            EBAY_BROWSE_SEARCH,
            headers=headers,
            params=params,
            timeout=ACTIVE_TIMEOUT,
        )
    except Exception as e:
        print(f"[BROWSE ERROR] Exception during active lookup: {e}")
        return []

    api_meter_browse()

    if not r or r.status_code != 200:
        print(f"[BROWSE ERROR] HTTP {getattr(r, 'status_code', '??')}")
        return []

    data = r.json() or {}
    items = data.get("itemSummaries", []) or []
    print(f"[BROWSE RAW COUNT] {len(items)} items")

    results: List[Dict] = []

    bad_condition_terms = [
        "poor", "fair", "filler", "filler card", "crease", "creased",
        "damage", "damaged", "bent", "writing", "pen", "marker",
        "tape", "miscut", "off-center", "oc", "kid card",
    ]
    lot_like_terms = [
        " lot", "lot of", "lots", "complete set", "factory set", "team set", "set ",
        "sealed box", "hobby box", "blaster box", "mega box", "hanger box", "value box",
        "cello box", "rack pack", "value pack", "fat pack", "jumbo box",
        "case break", "player break", "team break", "group break", "box break",
        "box", "case",
    ]

    # SUBJECT SIGNATURE IS BASED ON THE QUERY STRING
    # (which came from the actual listing title via _build_dynamic_query)
    subject_sig = _extract_card_signature_from_title(ORIGINAL_TITLE)

    for it in items:
        title_it = normalize_title(it.get("title"))
        lower_title = title_it.lower()

        # 1) Exclude graded
        if _is_graded(title_it):
            continue

        # 2) Require a fixed-price option
        opts = it.get("buyingOptions") or []
        if "FIXED_PRICE" not in opts:
            continue
        if "AUCTION" in opts:
            continue

        # 3) No variation parent groups (they make pricing noisy)
        group_type = it.get("itemGroupType")
        if group_type:
            continue

        # 4) Exclude auction URLs / bid links / variation URLs
        web_url = (it.get("itemWebUrl") or "").lower()
        if "variation" in web_url:
            continue
        if "auction" in web_url or "bid=" in web_url or "bids=" in web_url:
            continue

        # 5) Kill obvious lots / boxes / sets
        if any(term in lower_title for term in lot_like_terms):
            continue
        if re.search(r"\b\d+\s*(card|cards)\b", lower_title):
            continue
        if re.search(r"\bx\d{1,3}\b", lower_title):
            continue
        if any(term in lower_title for term in bad_condition_terms):
            continue

        # 6) Extract price
        price = _extract_total_price(it)
        if not price:
            continue

        # 7) v8 strict signature enforcement
        comp_sig = _extract_card_signature_from_title(title_it)
        if subject_sig and not _titles_match_strict(subject_sig, comp_sig):
            continue

        results.append({"title": title_it, "total": price})

    return results

def _fetch_active_items_finding_for_query(query: str, limit: int = ACTIVE_LIMIT) -> List[Dict]:
    """
    Finding API fallback for merged actives.

    • Uses Finding to backfill actives when Browse is thin
    • Applies generic BIN / no-lot / no-box filters
    • DOES NOT do strict card-level matching – that is handled later in search_active.
    """
    if not query:
        return []

    print(f"[FINDING MERGED] {query}")

    params = {
        "OPERATION-NAME": "findItemsByKeywords",
        "SERVICE-VERSION": "1.0.0",
        "SECURITY-APPNAME": os.getenv("EBAY_APP_ID"),
        "RESPONSE-DATA-FORMAT": "JSON",
        "REST-PAYLOAD": "",
        "paginationInput.entriesPerPage": str(limit),
        "keywords": query,
    }

    r, hdrs = _request(
        "GET",
        EBAY_FINDING_API,
        headers={"X-EBAY-SOA-REQUEST-DATA-FORMAT": "JSON"},
        params=params,
        timeout=ACTIVE_TIMEOUT,
        label="Finding/ActiveMerged",
    )

    if not r or r.status_code != 200:
        return []

    data = r.json()
    items = (
        data.get("findItemsByKeywordsResponse", [{}])[0]
        .get("searchResult", [{}])[0]
        .get("item", [])
    )

    results: List[Dict] = []

    bad_condition_terms = [
        "poor", "fair", "filler", "filler card", "crease", "creased",
        "damage", "damaged", "bent", "writing", "pen", "marker",
        "tape", "miscut", "off-center", "oc", "kid card",
    ]

    lot_like_terms = [
    " lot", "lot of", "lots",
    "complete set", "factory set", "team set", "set ",
    "sealed box", "hobby box", "blaster box", "mega box", "hanger box", "value box",
    "cello box", "rack pack", "value pack", "fat pack", "jumbo box",
    "case break", "player break", "team break", "group break", "box break",
    # REMOVED:
    # "box",  ← breaks SkyBox
    # "case", ← breaks 'Showcase', 'Timeless Treasures', etc.
    ]

    for it in items:
        # Finding titles sometimes come as list → normalize
        raw_title = it.get("title") or ""
        if isinstance(raw_title, list):
            raw_title = raw_title[0] if raw_title else ""
        title_it = normalize_title(raw_title)
        lower_title = title_it.lower()

        price = _extract_total_price_from_finding(it)
        if not price:
            continue

        # Exclude graded
        if _is_graded(title_it):
            continue

        # Exclude obvious lots / boxes / breaks
        if any(term in lower_title for term in lot_like_terms):
            continue
        if re.search(r"\b\d+\s*(card|cards)\b", lower_title):
            continue
        if re.search(r"\bx\d{1,3}\b", lower_title):
            continue

        # Condition filters
        if any(term in lower_title for term in bad_condition_terms):
            continue

        # NOTE: no _titles_match_strict here – strict happens later.
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

def save_last_resume_index(index: int) -> None:
    """
    Persist the NEXT index to resume from (1-based) so that
    auto-resume and manual-resume can use a consistent pointer.
    """
    try:
        os.makedirs(AUTOSAVE_FOLDER, exist_ok=True)
        with open(LAST_RESUME_INDEX_PATH, "w", encoding="utf-8") as f:
            f.write(str(int(index)))
    except Exception as e:
        print(f"[RESUME] Could not save last resume index ({e}).")


def load_last_resume_index() -> Optional[int]:
    """
    Return the last saved resume index (1-based), or None if missing/invalid.
    """
    try:
        if not os.path.exists(LAST_RESUME_INDEX_PATH):
            return None
        with open(LAST_RESUME_INDEX_PATH, "r", encoding="utf-8") as f:
            txt = f.read().strip()
        if not txt:
            return None
        val = int(txt)
        return val if val > 0 else None
    except Exception:
        return None

# ================= ITEM HANDLERS =================
def read_item_ids() -> List[str]:
    txt_path = os.path.join(IDS_FOLDER, "item_ids.txt")
    if os.path.exists(txt_path):
        with open(txt_path, "r", encoding="utf-8") as f:
            return [x.strip() for x in f.readlines() if x.strip()]
    print("No batch item_ids.txt found in IDs folder.")
    return []

def get_item_details(item_id: str) -> Dict[str, Any]:
    """
    Fetch full item details (title, price, sku, URL) using Browse API
    with safe structure for GUI + pricing engine.
    Always returns a dictionary.
    """

    # Proper browse ID format for item-level retrieval
    browse_id = f"v1|{item_id}|0"

    details = {
        "title": "",
        "price": None,
        "sku": "",
        "viewItemURL": "",
    }

    # --- Perform request ---
    r, hdrs = _request(
        "GET",
        f"https://api.ebay.com/buy/browse/v1/item/{browse_id}",
        headers=_headers(),
        timeout=15,
        label="Browse/Item",
    )
    api_meter_browse()

    # --- Parse response ---
    if r and r.status_code == 200:
        try:
            data = r.json()

            details["title"] = data.get("title") or ""
            details["viewItemURL"] = data.get("itemWebUrl") or ""

            if "price" in data:
                details["price"] = _safe_float(data["price"].get("value"))

            # SKU is inside itemSku or seller provided fields
            if "itemSku" in data:
                details["sku"] = data["itemSku"]

            # Sometimes available under itemOffered.sku
            if "itemOffered" in data:
                details["sku"] = data["itemOffered"].get("sku", details["sku"])

        except Exception:
            pass

    return details

def process_item(
    item_id: str,
    cache_df: pd.DataFrame,
    active_cache: Dict,
    dup_logged: set,
    dup_log_file: str,
    manual_overrides: set,
    manual_overrides_path: str,
    learn_callback=None   # ← optional GUI callback
) -> Tuple[Dict, List[Dict]]:

    try:
        # --------------------------------------------
        # LOAD TITLE + CURRENT PRICE
        # --------------------------------------------
        title, current_price = get_item_details(item_id)

        # --------------------------------------------
        # Load token rules once per item
        # --------------------------------------------
        token_rules = load_token_rules()

        # --------------------------------------------
        # Learning pass
        # --------------------------------------------
        learned = learn_from_title(title, learn_callback=learn_callback)

        if learned:
            save_token_rules(token_rules)
            # Retry the entire item exactly once
            return process_item(
                item_id,
                cache_df,
                active_cache,
                dup_logged,
                dup_log_file,
                manual_overrides,
                manual_overrides_path,
                learn_callback
            )

        # --------------------------------------------
        # MANUAL OVERRIDE SKIP LOGIC
        # --------------------------------------------
        if item_id in manual_overrides:
            print(f"   SKIPPED (manual override): {item_id}")
            return {
                "item_id": item_id,
                "Title": title,
                "CurrentPrice": current_price or "",
                "MedianSold": "",
                "ActiveAvg": "",
                "SuggestedPrice": current_price or "",
                "SupplyCount": "",
                "Note": "Manual override active",
                "UpdateStatus": "SKIPPED",
            }, []

        # --------------------------------------------
        # RUN SOLD + ACTIVE SEARCHES
        # --------------------------------------------
        sold_totals, sold_source, new_cache_rows = search_sold(
            title, limit=SOLD_LIMIT, cache_df=cache_df
        )
        time.sleep(SLEEP_BETWEEN_CALLS_SEC)

        active_totals, act_source, supply_count = search_active(
            title, limit=ACTIVE_LIMIT, active_cache=active_cache
        )

        # --------------------------------------------
        # PRICE CALCULATION
        # --------------------------------------------
        median_sold, median_active, suggested, note = get_price_strict(
            active_totals,
            sold_totals,
            current_price=current_price
        )

        combined_note = note or ""

        # --------------------------------------------
        # PRINT SUMMARY LINE
        # --------------------------------------------
        print(
            f"-> {title} | Source: {sold_source} | Current: {current_price or '-'} | "
            f"Sold(med): {median_sold or '-'} | Active(med): {median_active or '-'} | Suggest: {suggested or '-'}"
        )

        update_status = ""

        # --------------------------------------------
        # PRICE UPDATE (LIVE)
        # --------------------------------------------
        if not DRY_RUN and suggested and current_price:
            diff_abs = abs(suggested - current_price)
            diff_pct = diff_abs / current_price if current_price > 0 else 0.0

            if diff_abs >= 0.30 or diff_pct >= (PERCENT_THRESHOLD / 100):
                success, update_status = update_ebay_price(item_id, suggested)
                print(f"   {update_status}")

                # Duplicate detection log
                if "Duplicate Listing policy" in update_status and item_id not in dup_logged:
                    dup_logged.add(item_id)
                    sku = get_custom_label(item_id)

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
                    pd.DataFrame([dup_entry]).to_csv(
                        dup_log_file,
                        mode="a",
                        index=False,
                        header=not os.path.exists(dup_log_file),
                        quoting=csv_mod.QUOTE_MINIMAL,
                    )

            else:
                api_meter_revise_saved()
                print(f"   Change below threshold (Δ${diff_abs:.2f}, {diff_pct*100:.1f}%). No update sent.")

        elif DRY_RUN:
            print("   Dry run - no live update sent.")

        # --------------------------------------------
        # RETURN RESULT ROW
        # --------------------------------------------
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

    # ---------- CLI ARGUMENTS ----------
    args = sys.argv[1:]

    # Fast meters-only mode
    if args and args[0] in ("--meters", "-m", "--meter", "--limits"):
        print("Fetching live API rate-limit headers (Browse, Finding, Trading)…")
        return

    # Optional resume index from CLI
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

    # Optional manual full-store ID refresh
    force_refresh_ids = any(
        a in ("--refresh-ids", "--refresh_ids", "--refresh-full-store-ids", "--refresh_all_ids")
        for a in args
    )

    # ---------- MENU LOOP ----------
    while True:

        has_batch_file = os.path.exists(os.path.join(IDS_FOLDER, "item_ids.txt"))
        autosave_base = "msc_autosave_temp.csv"
        autosave_rows = _get_autosave_progress(AUTOSAVE_FOLDER, autosave_base)

        mode_label = f"{BOLD}{GREEN}LIVE MODE{RESET}" if not DRY_RUN else f"{YELLOW}{BOLD}TEST MODE{RESET}"
        sandbox_status = "🟢 ON" if SANDBOX_MODE else "🔴 OFF"

        clear_screen()
        print("\n")
        print(f"{BOLD}{CYAN}MONTEREY SPORTS CARDS {RESET}{BOLD}{WHITE}–{RESET} {BOLD}{CYAN}CONTROL PANEL{RESET} {WHITE}({mode_label}){RESET}")

        # Load last resume index (safe even if missing)
        saved_resume_index = load_last_resume_index()
        resume_display = saved_resume_index if saved_resume_index is not None else "None"

        # Sandbox ICONS
        sandbox_icon = "🟢" if SANDBOX_MODE else "🔴"
        sandbox_text = f"{sandbox_icon} {'ON' if SANDBOX_MODE else 'OFF'}"

        # Full status line
        print(f"{WHITE}   [ Sandbox: {sandbox_text} | Last Resume: {resume_display} ]{RESET}")

        print(f"{BLUE}{'=' * 49}{RESET}")
        print("\n" * 0)


        print(f"{CYAN}{BOLD}Choose a mode:{RESET}")

        print(f"{BLUE}0){RESET} {BOLD}Toggle Live Updates (LIVE/TEST){RESET}")
        print(f"{BLUE}1){RESET} {BOLD}Full-Store Scan{RESET} {WHITE}(all active listings){RESET}")

        if autosave_rows > 0:
            print(f"{BLUE}2){RESET} {BOLD}Resume from Autosave{RESET} {WHITE}(~{autosave_rows} items already processed){RESET}")
        else:
            print(f"{BLUE}2){RESET} {BOLD}Resume from Autosave{RESET} {WHITE}(no valid autosave found){RESET}")

        print(f"{BLUE}3){RESET} {BOLD}Newly Listed Items Only{RESET} {WHITE}(created within last N days){RESET}")
        print(f"{BLUE}4){RESET} {BOLD}Custom SKU Only{RESET} {WHITE}(exact match){RESET}")

        if has_batch_file:
            print(f"{BLUE}5){RESET} {BOLD}Batch File Mode{RESET} {WHITE}(item_ids.txt present){RESET}")
        else:
            print(f"{BLUE}5){RESET} {BOLD}Batch File Mode{RESET} {WHITE}(no item_ids.txt found){RESET}")

        print(f"{BLUE}6){RESET} {BOLD}Resume From Manual Index{RESET} {WHITE}(Advanced){RESET}")
        print(f"{BLUE}S){RESET} {BOLD}Sandbox Test Mode{RESET} {WHITE}(no autosave, no updates){RESET}")
        print(f"{BLUE}7){RESET} {BOLD}Show API Limit Snapshot Only{RESET}")
        print(f"{BLUE}8){RESET} {BOLD}Reset Autosave + Full-Store Scan{RESET}")
        print(f"{BLUE}9){RESET} {BOLD}Exit{RESET}")

        print(f"{BLUE}{'=' * 37}{RESET}")

        # ---------- FIXED: single input, no duplicates ----------
        default_choice = "5" if has_batch_file else "1"
        choice = input(f"{CYAN}Enter choice [default {default_choice}]:{RESET} ").strip().upper() or default_choice

        # Validate choice
        valid_choices = {"0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "S"}
        while choice not in valid_choices:
            choice = input(f"{RED}Invalid choice. Enter 0–9 or S:{RESET} ").strip().upper()

        # --------------------------------------------------------
        # 0) TOGGLE LIVE / TEST   (>>> NOW WORKS CORRECTLY <<<)
        # --------------------------------------------------------
        if choice == "0":
            prev_mode_live = not DRY_RUN
            DRY_RUN = not DRY_RUN
            mode_label = "LIVE MODE" if not DRY_RUN else "TEST MODE"

            print(f"\n🔄 Live Updates Toggled: Now running in {mode_label}.")

            if (not prev_mode_live) and (not DRY_RUN) and SANDBOX_MODE:
                SANDBOX_MODE = False
                print("Sandbox mode is only available in TEST mode. Turning Sandbox OFF.")

            time.sleep(1)
            continue   # <— THIS NOW REDRAWS THE MENU PROPERLY

        # Toggle sandbox
        if choice == "S":
            if not DRY_RUN:
                print(f"{RED}{BOLD}Sandbox mode is only available in TEST mode. Please switch to TEST mode first.{RESET}")
            else:
                SANDBOX_MODE = not SANDBOX_MODE
                on_off = "ON" if SANDBOX_MODE else "OFF"
                print(f"🧪 Sandbox Mode is now {on_off}.")
                
            print("\nPress Enter to continue...")
            input()   # <-- ONE clean pause, no double input
            continue

        # Show meters only
        if choice == "7":
            print("Showing API meter snapshot only…")
            # check_meters_only()
            return

        # Reset autosave then treat as full-store
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

            # Also clear the saved resume index pointer
            try:
                if os.path.exists(LAST_RESUME_INDEX_PATH):
                    os.remove(LAST_RESUME_INDEX_PATH)
                    print("🧹 Cleared saved resume index pointer.")
            except Exception as e:
                print(f"Could not delete last resume index file: {e}")

            # After reset, behave like Full-Store Scan
            choice = "1"

        # Exit
        if choice == "9":
            print("Exiting without running pricing engine.")
            return

        # Manual index resume
        if choice == "6":
            last_idx = load_last_resume_index() if not SANDBOX_MODE else None
            if last_idx is not None:
                print(f"\nLast saved resume index: {last_idx}")

            while True:
                idx_str = input("Enter starting index (1-based) to resume from (e.g., 500): ").strip()
                try:
                    idx_val = int(idx_str)
                    if idx_val <= 0:
                        print("Please enter a positive integer for index.")
                        continue

                    # P1 behavior: if manual < saved -> warn / require confirmation
                    if last_idx is not None and idx_val < last_idx:
                        diff = last_idx - idx_val
                        print(
                            f"\n⚠ You previously stopped at index {last_idx}, "
                            f"but you entered {idx_val}."
                        )
                        print(f"This would reprocess approximately {diff} items.")
                        confirm = input("Do you still want to resume from the EARLIER index? (Y/N): ").strip().upper()

                        if confirm != "Y":
                            resume_from_index = last_idx
                            print(f"\nUsing saved resume index {last_idx} instead.")
                        else:
                            resume_from_index = idx_val
                            print(f"\nManual backward resume confirmed. Starting at index {resume_from_index}.")
                    else:
                        # Either no last_idx or manual >= last_idx → accept manual
                        resume_from_index = idx_val
                        print(f"\nManual resume selected. Will start from index {resume_from_index}.")

                    break
                except ValueError:
                    print("Invalid number. Please enter a positive integer.")

        # ---------- ID SELECTION LOGIC ----------
        ids: List[str] = []
        ids_source = ""

        # Modes 1 and 2 share the full-store base (cached full_store_ids.json)
        if choice in ("1", "2"):
            if choice == "1":
                # Full-store submenu
                while True:
                    clear_screen()

                    # Header
                    print(f"{BOLD}{CYAN}MONTEREY SPORTS CARDS {RESET}{BOLD}{WHITE}–{RESET} {BOLD}{CYAN}FULL-STORE SCAN MENU{RESET}")
                    print(f"{BLUE}{'=' * 44}{RESET}")
                    print()

                    # Submenu options
                    print(f"{CYAN}{BOLD}Choose an action:                         Description:{RESET}")
                    print(f"{BLUE}A){RESET} {BOLD}New Full Scan{RESET} "
                        f"{WHITE}             (Full Overwrite – Rebuild full_store_ids.json){RESET}")

                    print(f"{BLUE}B){RESET} {BOLD}Append NEW ItemIDs Only{RESET} "
                        f"{WHITE}   (Append Mode – merges with existing data){RESET}")

                    print(f"{BLUE}C){RESET} {BOLD}Return to Main Menu{RESET}")

                    print()

                    sub_choice = input(f"{WHITE}Select {BLUE}A{WHITE}, {BLUE}B{WHITE}, or {BLUE}C{WHITE}: {RESET}").strip().upper()

                    if sub_choice == "C":
                        print("Returning to Main Menu...")
                        time.sleep(1)
                        # Abort this run and return to the top-level control panel
                        return "BACK"

                    if sub_choice == "A":
                        print(f"\n{BOLD}{RED}⚠ WARNING{RESET} — This will {BOLD}{RED}REBUILD{RESET} full_store_ids.json")
                        print(f"This will {BOLD}{RED}OVERWRITE{RESET} the existing cached file.")
                        print(f"Type {BOLD}{RED}REBUILD{RESET} to continue or anything else to cancel.")
                        confirm = input("Confirm: ").strip().upper()
                        if confirm != "REBUILD":
                            print("Cancelled. Returning to Full-Store Scan menu...")
                            time.sleep(1)
                            continue
                        force_refresh_ids = True
                        break

                    if sub_choice == "B":
                        clear_screen()
                        print("🔎 Safe Append Mode: Loading cached full-store IDs...")

                        cached_ids = _load_full_store_ids_from_cache()
                        cached_set = set(cached_ids)

                        print("🔎 Fetching fresh active IDs from eBay for comparison...")
                        fresh_ids = fetch_all_active_item_ids()

                        new_ids = [iid for iid in fresh_ids if iid not in cached_set]
                        total_before = len(cached_ids)
                        total_after = total_before + len(new_ids)

                        print(f"Found {len(new_ids)} NEW ItemIDs!")
                        print(f"Total before: {total_before} → Total after: {total_after}")

                        merged = cached_ids + new_ids
                        _save_full_store_ids_to_cache(merged)

                        print("Append Mode complete. Returning…")
                        time.sleep(1)

                        force_refresh_ids = False
                        break

                    print("Invalid choice. Please pick A, B, or C.")
                    time.sleep(1)

            # After submenu (or for Resume mode), load full-store IDs
            ids, ids_source = load_or_fetch_full_store_ids(force_refresh=force_refresh_ids)
            if not ids:
                print("No active items found or failed to fetch active IDs. Exiting.")
                return

            # Choice 2: Resume from autosave / saved index pointers
            if choice == "2":
                last_idx = load_last_resume_index() if not SANDBOX_MODE else None

                if last_idx is not None:
                    # Use the saved resume pointer as the authoritative index
                    resume_from_index = last_idx
                    print(f"Resume mode selected. Using saved resume index: {last_idx}.")
                elif autosave_rows > 0:
                    # Fallback: infer resume index from autosave row count
                    resume_from_index = autosave_rows + 1
                    print(
                        f"Resume mode selected. No saved index found, but autosave has ~{autosave_rows} rows; "
                        f"starting at index {resume_from_index}."
                    )
                else:
                    print("No valid autosave or saved resume index found; cannot resume. Consider running Full-Store Scan instead.")
                    return

        elif choice == "3":
            # Newly listed items based on StartTime
            clear_screen()

            # Header
            print(f"{BOLD}{CYAN}MONTEREY SPORTS CARDS {RESET}{BOLD}{WHITE}–{RESET} "
                f"{BOLD}{CYAN}NEW LISTINGS FILTER{RESET}")
            print(f"{BLUE}{'=' * 43}{RESET}")
            print()

            print(f"{CYAN}{BOLD}How many days back do you want to scan?     Description:{RESET}")
            # Options
            print(f"{BLUE}1){RESET}  {BOLD}1 ➜  Day{RESET} "
                f"{WHITE}                             (Last 24 Hours){RESET}")

            print(f"{BLUE}2){RESET}  {BOLD}2 ➜  Days{RESET} "
                f"{WHITE}                            (Last 48 Hours){RESET}")

            print(f"{BLUE}3){RESET}  {BOLD}3 ➜  Days{RESET} "
                f"{WHITE}                            (Last 72 Hours){RESET}")

            print(f"{BLUE}7){RESET}  {BOLD}7 ➜  Days{RESET} "
                f"{WHITE}                            (Last 7 Days - Weekly Scan){RESET}")

            print(f"{BLUE}30){RESET} {BOLD}30 ➜  Days{RESET} "
                f"{WHITE}                           (Last 30 days - Monthly Scan){RESET}")

            print(f"{BLUE}90){RESET} {BOLD}90 ➜  Days{RESET} "
                f"{WHITE}                           (Last 90 days - Quarterly Scan){RESET}")

            print(f"{BLUE}C){RESET}  {BOLD}Return to Previous Menu{RESET}")
            print()

            print(f"{BLUE}{'=' * 37}{RESET}")

            # ---- INPUT LOOP (unified) ----
            while True:
                days_input = input(
                    f"{CYAN}Enter choice [{WHITE}1,2,3,7,30,90{CYAN} or {WHITE}C{CYAN}]:{RESET} "
                ).strip().upper()

                if days_input == "C":
                    return "BACK"

                # Validate numeric
                try:
                    days_back = int(days_input)
                    if days_back <= 0:
                        print("Please enter a positive integer.")
                        continue
                    break

                except ValueError:
                    print("Invalid number. Please enter an integer or C.")
                    continue

            # ---- FETCH NEW LISTINGS ----
            ids, ids_source = fetch_recent_item_ids(days_back)

            if not ids:
                print(f"No active items found started in the last {days_back} days. Returning...")
                time.sleep(1)
                return "BACK"

            print(f"Loaded {len(ids)} newly listed items from last {days_back} days.")
            time.sleep(1)

            return ids, ids_source



        elif choice == "4":
            # Custom SKU mode
            while True:
                print("\nExample SKU format: 041425-MJ")
                custom_sku = input("Enter the exact Custom SKU to process (or press Enter to return to menu): ").strip()

                if not custom_sku:
                    print("Returning to main menu...")
                    return

                ids, ids_source = fetch_item_ids_by_custom_sku(custom_sku)
                if not ids:
                    print(f"❌ No active items found with Custom SKU/Label = '{custom_sku}'. Try another SKU.\n")
                    continue

                print(f"Loaded {len(ids)} items with Custom SKU/Label = '{custom_sku}'.")
                break

        elif choice == "5":
            # Batch file mode
            ids = read_item_ids()
            ids_source = "batch file item_ids.txt"
            if not ids:
                print("No batch item_ids.txt found or file was empty. Exiting.")
                return

        elif choice == "6":
            # Manual index resume uses the same full-store ID base, but without the A/B/C submenu.
            ids, ids_source = load_or_fetch_full_store_ids(force_refresh=False)
            if not ids:
                print("No active items found or failed to fetch active IDs. Exiting.")
                return

        else:
            print(f"Unhandled choice: {choice}")
            return

        # ---------- SHARED SETUP AFTER IDs ARE CHOSEN ----------
        if not SANDBOX_MODE:
            os.makedirs(CACHE_FOLDER, exist_ok=True)
            active_cache = load_active_cache(ACTIVE_CACHE_PATH, ACTIVE_CACHE_TTL_MIN)
        else:
            active_cache = None

        # Reorder by quantity if possible (Inventory API)
        numeric_ids = []
        for raw in ids:
            raw_clean = str(raw).strip().replace("v1|", "").replace("|0", "")
            try:
                numeric_ids.append(int(raw_clean))
            except Exception:
                continue

        if numeric_ids:
            chunk_size = 1000
            full_df_list = []

            for start in range(0, len(numeric_ids), chunk_size):
                chunk = numeric_ids[start:start + chunk_size]
                id_str = ",".join(str(x) for x in chunk)

                r, hdrs = _request(
                    "GET",
                    "https://api.ebay.com/sell/inventory/v1/inventory_item",
                    headers=_headers(),  # your existing headers helper
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
                df_qty = pd.DataFrame(full_df_list)
                df_qty = df_qty.sort_values("qty", ascending=False)
                ids = df_qty["item_id"].astype(str).tolist()

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

        run_stamp = datetime.now().strftime("%Y-%m-%d")
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

        # ---------- MAIN LOOP ----------
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
                row, new_cache = process_item(
                    item_id,
                    cache_df,
                    active_cache,
                    dup_logged,
                    dup_log_file,
                    manual_overrides,
                    os.path.join(BASE_DIR, manual_overrides_path)
                )
                results.append(row)

                # update headers-based meter snapshots if you want (left as in your original)
                # last_remaining = globals().get("X-EBAY-C-REMAINING-REQUESTS", last_remaining)
                # last_reset = globals().get("X-EBAY-C-RESET-TIME", last_reset)

                if new_cache:
                    if cache_df is None:
                        cache_df = pd.DataFrame(new_cache)
                    else:
                        cache_df = pd.concat([cache_df, pd.DataFrame(new_cache)], ignore_index=True)

                if i % 10 == 0 and not SANDBOX_MODE:
                    _autosave_write_atomic(pd.DataFrame(results), AUTOSAVE_FOLDER, autosave_base)
                    print(f"   Autosaved progress after {i} items.")

                    # Save the NEXT safe resume index (1-based)
                    save_last_resume_index(i + 1)

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

        # After completing the loop
        if not SANDBOX_MODE and active_cache is not None:
            save_active_cache(active_cache, ACTIVE_CACHE_PATH)

        # ---------- FINAL REPORT SAVE ----------
        report_path = os.path.join(
            RESULTS_FOLDER,
            f"price_update_report_{VERSION}_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.csv"
        )
        final_df = pd.DataFrame(results)

        # (Optional) you can still merge latest autosave here if you want,
        # using _latest_autosave_path(...) like in your earlier version.

        if not final_df.empty:
            final_df["item_id"] = final_df["item_id"].astype(str)
        final_df.to_csv(report_path, index=False)
        print(f"\nReport saved: {report_path}")

        print("\n================ API USAGE SUMMARY ================")
        print(f"Browse API calls used: {BROWSE_CALLS}")
        print(f"Revise API calls sent: {REVISE_CALLS}")
        print(f"Revise calls avoided (threshold): {REVISE_SAVED}")
        print(f"Total API hits today: {BROWSE_CALLS + REVISE_CALLS}")
        print("====================================================\n")

        print_rate_limit_snapshot()

        print("✅ Finished processing.")
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
    Two-stage SKU lookup:

      Stage 1 → Inventory API (fast, exact SKU match)
        - GET /sell/inventory/v1/inventory_item?sku=TARGETSKU
        - If a matching inventoryItem has offers with legacyItemId, return that ItemID immediately.

      Stage 2 → Trading API GetMyeBaySelling (limited page scan)
        - Scan up to 15 pages (15 × 200 = 3,000 items max)
        - For each <Item>, check BOTH <CustomLabel> and <SKU> for an EXACT match
        - Stop early as soon as a match is found

    Returns:
        (item_id_list, source_label)

        source_label ∈ {
            "Inventory API SKU match",
            "Trading API SKU match",
            "No exact match",
            "API error",
        }
    """
    target_sku = (target_sku or "").strip()
    if not target_sku:
        return [], "No SKU provided"

    # Normalize for exact comparison
    target_norm = target_sku.upper()

    # ============================================================
    # STAGE 1 — Inventory API (FAST, EXACT)
    # ============================================================
    inv_url = "https://api.ebay.com/sell/inventory/v1/inventory_item"
    r, hdrs = _request(
        "GET",
        inv_url,
        headers=_headers(),
        params={"sku": target_sku},
        timeout=12,
        label="Inventory/SKU"
    )
    api_meter_browse()

    if r:
        try:
            data = r.json()
            items = data.get("inventoryItems") or []
            if items:
                # Use the first matching inventory item
                it = items[0]
                offers = it.get("offers") or []
                if offers:
                    # Prefer legacyItemId when present
                    legacy = offers[0].get("legacyItemId")
                    if legacy:
                        return [str(legacy)], "Inventory API SKU match"
        except Exception:
            # If anything goes wrong parsing Inventory response,
            # we silently fall back to Stage 2.
            pass
    else:
        # _request returned None => token/rate/HTTP error
        # We still attempt Trading as a fallback.
        pass

    # ============================================================
    # STAGE 2 — Trading API (LIMITED PAGE SCAN)
    # ============================================================
    print(f"🔍 Falling back to Trading API for SKU = '{target_sku}'...")

    url = "https://api.ebay.com/ws/api.dll"
    headers = {
        "X-EBAY-API-CALL-NAME": "GetMyeBaySelling",
        "X-EBAY-API-SITEID": "0",
        "X-EBAY-API-COMPATIBILITY-LEVEL": "967",
        "Content-Type": "text/xml",
        "Authorization": f"Bearer {OAUTH_TOKEN}",
    }

    found_ids: List[str] = []
    entries_per_page = 200
    MAX_PAGES = 15

    for page in range(1, MAX_PAGES + 1):
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
            timeout=20,
            label=f"Trading/SKU p{page}"
        )
        api_meter_browse()

        # If Trading is unavailable (token, rate limit, server error),
        # bail out with a clear label instead of looping forever.
        if not r:
            return [], "API error"

        text = r.text or ""

        # If there are no items on this page at all, we can safely stop.
        if "<ItemID>" not in text:
            break

        # Split into <Item> blocks for independent parsing
        item_blocks = re.findall(r"<Item>(.*?)</Item>", text, flags=re.DOTALL | re.IGNORECASE)

        for blk in item_blocks:
            # Look at BOTH <CustomLabel> and <SKU>
            m_custom = re.search(r"<CustomLabel>(.*?)</CustomLabel>", blk, flags=re.DOTALL | re.IGNORECASE)
            m_sku = re.search(r"<SKU>(.*?)</SKU>", blk, flags=re.DOTALL | re.IGNORECASE)

            sku_in_list = None
            if m_custom:
                sku_in_list = m_custom.group(1).strip()
            elif m_sku:
                sku_in_list = m_sku.group(1).strip()

            if sku_in_list and sku_in_list.upper() == target_norm:
                m_id = re.search(r"<ItemID>(\d+)</ItemID>", blk, flags=re.DOTALL | re.IGNORECASE)
                if m_id:
                    iid = m_id.group(1).strip()
                    if iid not in found_ids:
                        found_ids.append(iid)

        # Stop early as soon as we find one or more matching items
        if found_ids:
            return found_ids, "Trading API SKU match"

    # ============================================================
    # RESULT — NOTHING FOUND
    # ============================================================
    return [], "No exact match"


if __name__ == "__main__":
    main()