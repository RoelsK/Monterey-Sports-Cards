import os, sys
import json
import re
from datetime import datetime
from typing import Dict, Any, Tuple, List

# ------------------------------------------------------------
# PATH BOOTSTRAP
# ------------------------------------------------------------
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.abspath(os.path.join(CURRENT_DIR, ".."))

if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

# ------------------------------------------------------------
# GENERIC JSON LOADER
# ------------------------------------------------------------
def _load_json(name: str):
    path = os.path.join(ROOT_DIR, name)
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except:
        return {}

# ------------------------------------------------------------
# LOAD TOKEN RULES (player tokens, stopwords, suffixes)
# ------------------------------------------------------------
TOKEN_RULES = _load_json("token_rules.json")
if not TOKEN_RULES:
    TOKEN_RULES = {
        "remove_words": [],
        "player_suffixes": ["jr", "sr", "ii", "iii", "iv"],
        "max_name_word_length": 4,
        "normalization_table": []
    }

# ------------------------------------------------------------
# LOAD ALL OTHER JSON RESOURCES
# ------------------------------------------------------------
SET_PHRASES          = _load_json("set_phrases.json")
BRAND_FAMILIES       = _load_json("brand_families.json")
CLASSIFICATION_RULES = _load_json("classification_rules.json")

# ------------------------------------------------------------
# PARALLEL RULES (.py, not JSON)
# ------------------------------------------------------------
try:
    import parallel_rules as PARALLEL_RULES
except Exception:
    PARALLEL_RULES = {}

# ------------------------------------------------------------
# IMPORT PRICING ENGINE FOR SIGNATURE WRAPPER
# ------------------------------------------------------------
from pricing import pricing_engine

def _extract_card_signature_from_title(title: str):
    """Expose pricing_engine’s signature extractor through helpers_v10."""
    return pricing_engine._extract_card_signature_from_title(title)

# ============================================================
# GLOBAL NORMALIZATION TABLE
# Merged and de-duplicated version of your previous two systems
# ============================================================

NORMALIZATION_TABLE: List[Tuple[str, str]] = []

# 1. Inject any normalization from token_rules.json
for pair in TOKEN_RULES.get("normalization_table", []):
    if isinstance(pair, list) and len(pair) == 2:
        NORMALIZATION_TABLE.append((pair[0], pair[1]))

# 2. Built-in canonical normalization (v8 strict)
NORMALIZATION_TABLE.extend([
    (r"\bsky[\s\-]*box\b", "skybox"),
    (r"\be[\s\-]*motion\b", "e motion"),
    (r"\btopps? chrome\b", "topps chrome"),
    (r"\btopps? finest\b", "topps finest"),
    (r"[^a-z0-9]+", " "),
])

def normalize_title_global(text: str) -> str:
    """Full canonical normalization used across set/player extraction."""
    if not text:
        return ""
    t = text.lower()
    for pattern, repl in NORMALIZATION_TABLE:
        t = re.sub(pattern, repl, t)
    return re.sub(r"\s+", " ", t).strip()

# ================= CONFIG SYSTEM =================

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

def _ensure_dir(path: str):
    folder = os.path.dirname(path)
    if folder and not os.path.exists(folder):
        os.makedirs(folder)

def load_config(path: str) -> Dict[str, Any]:
    """Load config_v10.json with fallback to DEFAULT_CONFIG."""
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


# ------------------------------  
# Manual Override Helpers
# ------------------------------

def load_manual_overrides(path):
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
            return set(data) if isinstance(data, list) else set()
    except Exception:
        return set()


def save_manual_overrides(data, path):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(list(data), f, indent=2)
    os.replace(tmp, path)


def maybe_use_active_cache(
    title: str,
    cache: Dict[str, Any],
    ttl_minutes: int
) -> Tuple[List[float], bool]:

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
    if not prices:
        return
    cache[title] = {
        "prices": [float(p) for p in prices],
        "ts": datetime.utcnow().isoformat()
    }


#########################################
# SAFE-HYBRID COMP FILTER (Mode A)
#########################################

HARD_EXCLUDE = [
    "refractor", "xfractor", "pulsar", "mojo", "wave",
    "cracked ice", "atomic", "prism", "prizm",
    "serial", "#/", "/", "foilboard",
    "psa", "bgs", "sgc", "graded",
    "lot", "set of", "bundle", "collection"
]

SOFT_EXCLUDE = [
    "gold", "black", "blue", "green", "red", "silver",
    "rainbow", "holo", "foil", "purple", "orange",
    "pink", "lime", "aqua", "teal"
]


def is_serial_numbered(title: str) -> bool:
    t = title or ""
    return ("/" in t) or ("#/" in t)


def comp_matches_parallel_type(user_title: str, comp_title: str) -> bool:
    u = (user_title or "").lower()
    c = (comp_title or "").lower()

    if "chrome" in u:
        if "chrome" not in c:
            return False
        for w in SOFT_EXCLUDE:
            if w in c and w not in u:
                return False
        if "refractor" in c and "refractor" not in u:
            return False
        return True

    if "chrome" in c and "chrome" not in u:
        return False

    if not any(w in u for w in SOFT_EXCLUDE):
        if any(w in c for w in SOFT_EXCLUDE):
            return False

    if is_serial_numbered(c) and not is_serial_numbered(u):
        return False

    for w in SOFT_EXCLUDE:
        if w in u and w not in c:
            return False

    return True


def comp_price_sane(user_title: str, comp_price: float) -> bool:
    title = (user_title or "").lower()
    p = float(comp_price)

    is_base = (
        "chrome" not in title
        and "refractor" not in title
        and not any(w in title for w in SOFT_EXCLUDE)
    )

    if is_base:
        if p > 2.99:
            return False

    if "chrome" in title and "refractor" not in title and not any(w in title for w in SOFT_EXCLUDE):
        if p > 3.99:
            return False

    if p > 100.0:
        return False

    return True


def safe_hybrid_filter(user_title: str, comp_title: str, comp_price: float) -> bool:
    t1 = (user_title or "").lower()
    t2 = (comp_title or "").lower()

    if any(w in t2 for w in HARD_EXCLUDE):
        if not ("refractor" in t2 and "chrome" in t1 and "refractor" in t1):
            return False

    if not comp_matches_parallel_type(t1, t2):
        return False

    if not comp_price_sane(t1, comp_price):
        return False

    return True


# ================= PRICING ENHANCEMENTS =================

def classify_velocity(sold_totals: List[float], config: Dict[str, Any]) -> Tuple[str, int]:
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

    if config.get("enable_velocity", True) and vel_bucket in ("MEDIUM", "HIGH"):
        factor = (
            float(config.get("velocity_boost_high", 1.15))
            if vel_bucket == "HIGH"
            else float(config.get("velocity_boost_medium", 1.08))
        )
        adj *= factor
        reasons.append(f"Velocity {vel_bucket} (x{factor:.2f})")

    if config.get("enable_supply_logic", True):
        oversupply_threshold = int(config.get("oversupply_threshold", 15))
        oversupply_discount = float(config.get("oversupply_discount", 0.92))
        if supply_count >= oversupply_threshold:
            adj *= oversupply_discount
            oversupply = True
            reasons.append(f"Oversupply {supply_count} actives (x{oversupply_discount:.2f})")

    if config.get("enable_rare_boost", True) and supply_count > 0:
        rare_threshold = int(config.get("rare_supply_threshold", 3))
        rare_boost = float(config.get("rare_boost_factor", 1.10))
        if supply_count <= rare_threshold:
            adj *= rare_boost
            rare = True
            reasons.append(f"Low supply {supply_count} actives (x{rare_boost:.2f})")

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

# ============================================================
# TITLE PARSING HELPERS (restored for strict matcher v7/v8)
# ============================================================

def _basic_tokens(text: str):
    """
    Shared tokenizer using global normalization.
    Returns lowercased word tokens with punctuation stripped.
    """
    norm = normalize_title_global(text or "")
    return [t for t in norm.split() if t]

def extract_year_from_title(title: str):
    """
    Extract a 4-digit year from the title.
    More permissive and stable than previous version.
    """
    import re

    if not title:
        return None

    # allow punctuation between digits
    clean = normalize_title_global(title)
    
    m = re.search(r"(19[0-9]{2}|20[0-4][0-9])", clean)
    if m:
        return int(m.group(1))

    return None

def extract_card_number_from_title(title: str):
    """
    Extract the card number like '#8', 'No. 8', 'Card 8'.
    Returns the canonical string (e.g., '8', '8A') or None.
    """
    import re

    if not title:
        return None

    t = title.lower()

    # Avoid catching serials like #/5000
    m = re.search(r"(?<!/)\#\s*([0-9]{1,4}[a-z]?)", t)
    if m:
        return m.group(1).upper()

    m = re.search(r"\bno\.?\s*([0-9]{1,4}[a-z]?)", t)
    if m:
        return m.group(1).upper()

    m = re.search(r"\bcard\s*#?\s*([0-9]{1,4}[a-z]?)", t)
    if m:
        return m.group(1).upper()

    return None

def extract_player_tokens_from_title(title: str) -> List[str]:
    normalized = normalize_title_global(title)
    words = normalized.split()
    words = [w for w in words if w not in TOKEN_RULES["remove_words"] and not w.isdigit()
             and len(w) <= TOKEN_RULES.get("max_name_word_length", 20)]

    # Exclude known set tokens
    set_tokens = extract_set_tokens(title)
    words = [w for w in words if w not in set_tokens]

    tokens = []
    skip_next = False
    for i, word in enumerate(words):
        if skip_next:
            skip_next = False
            continue
        if i + 1 < len(words) and words[i + 1] in TOKEN_RULES["player_suffixes"]:
            tokens.append(f"{word} {words[i + 1]}")
            skip_next = True
        else:
            tokens.append(word)
    return tokens

def extract_set_tokens(title: str) -> List[str]:
    """
    Detect set phrases using set_phrases.json.
    Supports multi-word set names and normalization.
    """
    if not title:
        return []

    t = normalize_title_global(title)

    tokens = []
    for phrase in SET_PHRASES:
        pat = phrase.get("pattern")
        out_tokens = phrase.get("tokens")
        if not pat or not out_tokens:
            continue
        if re.search(pat, t):
            tokens.extend(out_tokens)

    return list(dict.fromkeys(tokens))

def extract_parallels_from_title(title: str):
    """
    Extract simple parallel descriptors (refractor, gold, silver, etc.).
    Returns a list of canonical parallel tokens.
    """
    if not title:
        return []

    t = normalize_title_global(title)
    parallels = []

    # canonical parallel keywords
    candidates = [
        "refractor", "xfractor", "mojo", "wave", "pulsar", "atomic",
        "prizm", "prism",
        "silver", "gold", "blue", "green", "red", "purple", "orange",
        "pink", "black", "white", "aqua", "teal", "lime", "rainbow",
        "holo", "foil"
    ]

    for word in candidates:
        if word in t:
            parallels.append(word)

    # dedupe while preserving order
    seen = set()
    out = []
    for p in parallels:
        if p not in seen:
            seen.add(p)
            out.append(p)

    return out


def detect_insert_flag(title: str):
    """
    True if this looks like an insert subset (not base).
    """
    if not title:
        return False

    t = normalize_title_global(title)
    keywords = [
        "insert", "inserts", "insert set",
        "inser", "subset",
    ]
    return any(k in t for k in keywords)


def detect_promo_flag(title: str):
    """
    True if this looks like a promo / sample / proof card.
    """
    if not title:
        return False

    t = normalize_title_global(title)
    keywords = [
        "promo", "promotional", "sample", "proof",
        "test issue", "test card", "preview",
    ]
    return any(k in t for k in keywords)


def detect_oddball_flag(title: str):
    """
    True if this looks like an oddball (food issue, police, etc.).
    """
    if not title:
        return False

    t = normalize_title_global(title)
    keywords = [
        "police", "cereal", "hostess", "post", "kelloggs",
        "dennys", "mcdonald", "izzy", "slurpee", "magazine",
        "book", "disc", "coin", "sticker", "food issue",
    ]
    return any(k in t for k in keywords)

# ------------------------------------------------------------
# SIGNATURE WRAPPER (exposes pricing_engine._extract_card_signature_from_title)
# ------------------------------------------------------------
try:
    from pricing import pricing_engine
except Exception as e:
    pricing_engine = None

def _extract_card_signature_from_title(title: str):
    """
    Wrapper so other modules can call the signature extractor
    through helpers_v10 instead of reaching into pricing_engine.
    """
    if pricing_engine is None:
        return None
    helper = getattr(pricing_engine, "_extract_card_signature_from_title", None)
    if helper is None:
        return None
    return helper(title)