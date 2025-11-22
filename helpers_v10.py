import os
import json
from datetime import datetime
from typing import Dict, Any, Tuple, List


def _ensure_dir(path: str):
    folder = os.path.dirname(path)
    if folder and not os.path.exists(folder):
        os.makedirs(folder)


# ================= CONFIG =================

DEFAULT_CONFIG: Dict[str, Any] = {
    "version": 1,
    "enable_velocity": True,
    "enable_supply_logic": True,
    "enable_stale_deflation": False,
    "enable_rare_boost": True,

    "velocity_high_threshold": 4,
    "velocity_medium_threshold": 2,
    "velocity_window_days": 45,
    "velocity_boost_high": 1.15,
    "velocity_boost_medium": 1.08,

    "oversupply_threshold": 15,
    "oversupply_discount": 0.92,

    "rare_supply_threshold": 3,
    "rare_boost_factor": 1.10,

    "max_daily_increase_pct": 30,
    "max_daily_decrease_pct": 30,

    "active_cache_ttl_minutes": 720  # 12 hours
}


def load_config(path: str) -> Dict[str, Any]:
    """
    Load V10 config from JSON, falling back to defaults and writing default file if missing.
    """
    cfg = DEFAULT_CONFIG.copy()
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                user_cfg = json.load(f)
            if isinstance(user_cfg, dict):
                cfg.update(user_cfg)
        except Exception as e:
            print(f"[V10] Could not load config file ({e}). Using defaults.")
    else:
        try:
            _ensure_dir(path)
            with open(path, "w", encoding="utf-8") as f:
                json.dump(cfg, f, indent=2)
            print(f"[V10] Created default config at {os.path.basename(path)}")
        except Exception as e:
            print(f"[V10] Could not write default config file ({e}).")
    return cfg


# ================= ACTIVE CACHE =================

def load_active_cache(path: str, ttl_minutes: int) -> Dict[str, Any]:
    """
    Load active price cache {title: {"prices": [...], "ts": ISO}}.
    Prunes entries older than ttl_minutes.
    """
    now = datetime.utcnow()
    cache: Dict[str, Any] = {}
    if not os.path.exists(path):
        return cache

    try:
        with open(path, "r", encoding="utf-8") as f:
            raw = json.load(f)
        if not isinstance(raw, dict):
            return {}
        for title, data in raw.items():
            ts_str = (data or {}).get("ts")
            prices = (data or {}).get("prices", [])
            if not ts_str or not isinstance(prices, list):
                continue
            try:
                ts = datetime.fromisoformat(ts_str)
            except Exception:
                continue
            age_min = (now - ts).total_seconds() / 60.0
            if age_min <= ttl_minutes:
                cache[title] = {"prices": prices, "ts": ts_str}
        if raw and not cache:
            print("[V10] Active cache exists but all entries were stale. Starting fresh.")
        elif cache:
            print(f"[V10] Loaded active cache with {len(cache)} entries.")
    except Exception as e:
        print(f"[V10] Failed to load active cache ({e}). Starting empty.")
        cache = {}
    return cache


def save_active_cache(cache: Dict[str, Any], path: str):
    """
    Persist active cache to disk.
    """
    try:
        _ensure_dir(path)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(cache, f, indent=2)
    except Exception as e:
        print(f"[V10] Could not save active cache ({e}).")


def maybe_use_active_cache(
    title: str,
    cache: Dict[str, Any],
    ttl_minutes: int
) -> Tuple[List[float], bool]:
    """
    Return (prices, from_cache_flag) if title is in cache and still fresh.
    """
    if not cache:
        return [], False
    data = cache.get(title)
    if not data:
        return [], False

    ts_str = data.get("ts")
    prices = data.get("prices", [])
    if not ts_str or not isinstance(prices, list):
        return [], False

    try:
        ts = datetime.fromisoformat(ts_str)
        age_min = (datetime.utcnow() - ts).total_seconds() / 60.0
    except Exception:
        return [], False

    if age_min > ttl_minutes:
        return [], False

    return [float(p) for p in prices if isinstance(p, (int, float))], True


def update_active_cache(title: str, prices: List[float], cache: Dict[str, Any]):
    """
    Update the in-memory active cache (caller must save it later).
    """
    if not prices:
        return
    cache[title] = {
        "prices": [float(p) for p in prices],
        "ts": datetime.utcnow().isoformat()
    }


#########################################
# SAFE-HYBRID COMP FILTER (Mode A)
#########################################

# Strict words always excluded from comps unless very specific exceptions
HARD_EXCLUDE = [
    "refractor", "xfractor", "pulsar", "mojo", "wave",
    "cracked ice", "atomic", "prism", "prizm",
    "serial", "#/", "/", "foilboard",
    "psa", "bgs", "sgc", "graded",
    "lot", "set of", "bundle", "collection"
]

# Soft words: parallel-ish color/foil words
SOFT_EXCLUDE = [
    "gold", "black", "blue", "green", "red", "silver",
    "rainbow", "holo", "foil", "purple", "orange",
    "pink", "lime", "aqua", "teal"
]


def is_serial_numbered(title: str) -> bool:
    # catches: "/150", "#/299", "/99", "#33/250"
    t = title or ""
    return ("/" in t) or ("#/" in t)


def comp_matches_parallel_type(user_title: str, comp_title: str) -> bool:
    """
    If user's title indicates the card IS a specific parallel,
    then comps with that same parallel keyword are allowed.
    Otherwise they are excluded.

    This is the HEART of keeping base vs parallel separate.
    """
    u = (user_title or "").lower()
    c = (comp_title or "").lower()

    # User card is Chrome
    if "chrome" in u:
        # Only allow Chrome comps
        if "chrome" not in c:
            return False

        # Allow Chrome parallels ONLY when user title also includes that word
        for w in SOFT_EXCLUDE:
            if w in c and w not in u:
                return False

        # Allow refractors ONLY if user title has refractor
        if "refractor" in c and "refractor" not in u:
            return False

        # Chrome base vs Chrome base, or same parallel set
        return True

    # Non-Chrome (Base Topps, Bowman, Score, etc.)
    if "chrome" in c and "chrome" not in u:
        return False

    # If user card is NOT a parallel:
    # exclude all soft parallel terms from comps
    if not any(w in u for w in SOFT_EXCLUDE):
        if any(w in c for w in SOFT_EXCLUDE):
            return False

    # Exclude serials unless user also has serial
    if is_serial_numbered(c) and not is_serial_numbered(u):
        return False

    # If user listed a specific parallel keyword, comps must also contain it
    for w in SOFT_EXCLUDE:
        if w in u and w not in c:
            return False

    return True


def comp_price_sane(user_title: str, comp_price: float) -> bool:
    """
    Mode A: maximum safety.

    - Base cards (no chrome / no refractor / no color words):
        reject comps > $2.99
    - Chrome base (chrome but not refractor / color):
        reject comps > $3.99

    Higher-end parallels still have a safety net from other logic,
    but this keeps base/cheap stuff from drifting upward.
    """
    title = (user_title or "").lower()
    p = float(comp_price)

    # Is this essentially a base card?
    is_base = (
        "chrome" not in title
        and "refractor" not in title
        and not any(w in title for w in SOFT_EXCLUDE)
    )

    if is_base:
        if p > 2.99:
            return False

    # Chrome base sanity limit (no refractor / color in title)
    if "chrome" in title and "refractor" not in title and not any(w in title for w in SOFT_EXCLUDE):
        if p > 3.99:
            return False

    # For true parallels (chrome refractors, gold, etc.), still have a big global cap
    # (Extract_total_price already rejects > 100, but we clamp more for safety if needed)
    if p > 100.0:
        return False

    return True


def safe_hybrid_filter(user_title: str, comp_title: str, comp_price: float) -> bool:
    """
    FINAL DECISION: returns True if comp is allowed.
    Combines:
      - HARD_EXCLUDE keywords
      - Chrome/base/parallel compatibility
      - Price sanity for Mode A
    """
    t1 = (user_title or "").lower()
    t2 = (comp_title or "").lower()

    # Hard filter first
    if any(w in t2 for w in HARD_EXCLUDE):
        # Only exception: chrome refractors allowed if user card is chrome refractor
        if not ("refractor" in t2 and "chrome" in t1 and "refractor" in t1):
            return False

    # Parallel/base matching logic
    if not comp_matches_parallel_type(t1, t2):
        return False

    # Price sanity
    if not comp_price_sane(t1, comp_price):
        return False

    return True


# ================= PRICING ENHANCEMENTS =================

def classify_velocity(sold_totals: List[float], config: Dict[str, Any]) -> Tuple[str, int]:
    """
    Very simple velocity classification using number of sold comps as a proxy.
    (We don't have per-day timestamps from Browse, so we approximate.)
    """
    count = len(sold_totals or [])
    high = int(config.get("velocity_high_threshold", 4))
    med = int(config.get("velocity_medium_threshold", 2))

    if count >= high:
        return "HIGH", count
    if count >= med:
        return "MEDIUM", count
    if count > 0:
        return "LOW", count
    return "NONE", 0


def adjust_price_with_enhancements(
    suggested: float,
    median_sold: float,
    median_active: float,
    sold_totals: List[float],
    active_totals: List[float],
    supply_count: int,
    config: Dict[str, Any]
) -> Tuple[float, str, Dict[str, Any]]:
    """
    Apply V10 enhancements (velocity, supply, rare boost) on top of the baseline suggested price.

    Returns: (adjusted_price, extra_note, metrics_dict)
    """
    if suggested is None:
        return suggested, "", {
            "supply_count": supply_count,
            "velocity_bucket": "NONE",
            "velocity_count": 0,
            "oversupply": False,
            "rare": False
        }

    base_price = float(suggested)
    adj = base_price
    reasons: List[str] = []

    vel_bucket, vel_count = classify_velocity(sold_totals, config)
    oversupply = False
    rare = False

    # ---- Velocity logic ----
    if config.get("enable_velocity", True) and vel_bucket in ("MEDIUM", "HIGH"):
        if vel_bucket == "HIGH":
            factor = float(config.get("velocity_boost_high", 1.15))
        else:
            factor = float(config.get("velocity_boost_medium", 1.08))
        adj *= factor
        reasons.append(f"Velocity {vel_bucket} (x{factor:.2f})")

    # ---- Supply / oversupply logic ----
    if config.get("enable_supply_logic", True):
        oversupply_threshold = int(config.get("oversupply_threshold", 15))
        oversupply_discount = float(config.get("oversupply_discount", 0.92))
        if supply_count >= oversupply_threshold:
            adj *= oversupply_discount
            oversupply = True
            reasons.append(f"Oversupply {supply_count} actives (x{oversupply_discount:.2f})")

    # ---- Rare / low-supply boost ----
    if config.get("enable_rare_boost", True) and supply_count > 0:
        rare_threshold = int(config.get("rare_supply_threshold", 3))
        rare_boost = float(config.get("rare_boost_factor", 1.10))
        if supply_count <= rare_threshold:
            adj *= rare_boost
            rare = True
            reasons.append(f"Low supply {supply_count} actives (x{rare_boost:.2f})")

    # ---- Clamp around baseline to avoid wild swings ----
    max_inc_pct = float(config.get("max_daily_increase_pct", 30))
    max_dec_pct = float(config.get("max_daily_decrease_pct", 30))

    max_price = base_price * (1 + max_inc_pct / 100.0)
    min_price = base_price * (1 - max_dec_pct / 100.0)

    if adj > max_price:
        adj = max_price
        reasons.append(f"Clamped to +{max_inc_pct:.0f}% of baseline")
    if adj < min_price:
        adj = min_price
        reasons.append(f"Clamped to -{max_dec_pct:.0f}% of baseline")

    metrics = {
        "supply_count": supply_count,
        "velocity_bucket": vel_bucket,
        "velocity_count": vel_count,
        "oversupply": oversupply,
        "rare": rare,
    }

    return round(adj, 2), " | ".join(reasons), metrics