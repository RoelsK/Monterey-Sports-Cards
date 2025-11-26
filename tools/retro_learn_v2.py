"""
retro_learn_v2_optimizer.py

Post-processing optimizer for token_rules.json.
This script:
  • Loads token_rules.json
  • Deduplicates multiword_sets
  • Removes noise sets (numbers, players, mislearned stuff)
  • Normalizes casing + punctuation
  • Groups multiword_sets by brand families
  • Sorts the final ruleset for stability

Does NOT delete real card sets such as:
  "stadium club chrome", "upper deck mvp", "topps heritage", etc.
"""

import json
from pathlib import Path
import re
import sys

ROOT = Path(__file__).resolve().parent.parent
RULES_PATH = ROOT / "pricing" / "token_rules.json"

# ---------------------------------------------------------
# Helpers
# ---------------------------------------------------------

BRAND_PREFIXES = [
    "topps", "bowman", "panini", "donruss", "fleer", "upper deck", "skybox",
    "pacific", "score", "pinnacle", "press pass", "classic", "leaf", "action packed"
]

# patterns to remove:
REMOVE_PATTERNS = [
    r"^\d{4}$",                 # years: 1987, 1991
    r"^\d{1,2}$",               # single-number fragments
    r"^\d+[a-z]$",              # 89a, 92b etc
]

# full-token blacklist
BAD_TOKENS = {
    "bo jackson", "michael jordan", "kobe bryant", "barry sanders",
    "rookies", "jackson", "new", "allstars", "alltime", "victory:",
    "heroes:", "update:", "checklist", "misc"
}

def normalize(s: str) -> str:
    s = s.lower().strip()
    s = s.replace(":", "").replace("/", " ")
    s = re.sub(r"\s+", " ", s)
    return s


def is_noise_set(pair: list[str]) -> bool:
    """Decide if a learned multiword_set should be deleted."""
    if not pair or len(pair) != 2:
        return True

    a, b = normalize(pair[0]), normalize(pair[1])
    combined = f"{a} {b}".strip()

    # obvious garbage
    if combined in BAD_TOKENS:
        return True

    # numbers, years, trivial words
    for pat in REMOVE_PATTERNS:
        if re.match(pat, a) or re.match(pat, b) or re.match(pat, combined):
            return True

    # player names (2 tokens)
    if any(name in combined for name in ["jackson", "jordan", "bryant", "griffey", "sanders"]):
        return True

    return False


def brand_weight(pair: list[str]) -> int:
    """Return sort weight by BRAND prefix match."""
    text = normalize(" ".join(pair))
    for i, brand in enumerate(BRAND_PREFIXES):
        if text.startswith(brand):
            return i
    return 999  # unknown brand last


# ---------------------------------------------------------
# Main Optimizer
# ---------------------------------------------------------

def optimize_token_rules():
    if not RULES_PATH.exists():
        print(f"❌ Cannot find token_rules.json at: {RULES_PATH}")
        sys.exit(1)

    with RULES_PATH.open("r", encoding="utf-8") as f:
        rules = json.load(f)

    mws = rules.get("multiword_sets", [])
    print(f"Loaded {len(mws):,} learned multiword sets.")

    # Normalize + dedupe
    cleaned = []
    seen = set()

    for pair in mws:
        if not isinstance(pair, list) or len(pair) != 2:
            continue

        a, b = normalize(pair[0]), normalize(pair[1])
        pair_norm = (a, b)

        # skip junk
        if is_noise_set([a, b]):
            continue

        if pair_norm not in seen:
            cleaned.append([a, b])
            seen.add(pair_norm)

    print(f"After clean/dedupe: {len(cleaned):,}")

    # Sort by brand → then alphabetically
    cleaned_sorted = sorted(cleaned, key=lambda p: (brand_weight(p), p[0], p[1]))

    rules["multiword_sets"] = cleaned_sorted

    # Write optimized file
    with RULES_PATH.open("w", encoding="utf-8") as f:
        json.dump(rules, f, indent=2)

    print(f"💾 Optimized token_rules saved. Final multiword_sets count: {len(cleaned_sorted):,}")


if __name__ == "__main__":
    optimize_token_rules()
