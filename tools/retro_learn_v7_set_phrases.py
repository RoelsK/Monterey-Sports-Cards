"""
retro_learn_v7_set_phrases.py

PURPOSE:
    Automatically discover REAL multi-word SET / SUBSET phrases from your
    ActiveListings.csv titles.

    Examples it can learn:
        "skybox e-motion"
        "stadium club"
        "topps chrome"
        "fleer ultra"
        "upper deck sp"
        "donruss elite"
        "bowman best"
        "panini contenders"
        ...and hundreds more.

    These phrases are used inside pricing_engine for:
        • strict matching
        • query building
        • avoiding card-specific hacks
        • correctly grouping sets

INPUT:
    • ActiveListings.csv  (must contain column "Title")

OUTPUT:
    • pricing/set_phrases.json
      {
        "skybox e-motion": 14,
        "stadium club": 291,
        "topps chrome": 184,
        ...
      }

BEHAVIOR:
    • Offline only
    • Mines 2–4 word n-grams
    • Filters out generic junk ("trading card", "baseball card", etc.)
    • Keeps phrases that occur ≥ MIN_FREQ times
    • Saves JSON sorted by frequency
"""

from pathlib import Path
import sys
import re
import json
import pandas as pd
from collections import Counter
from helpers_v10 import normalize_title_global

# ---------------------------------------------
# Paths
# ---------------------------------------------
TOOLS_DIR = Path(__file__).resolve().parent
ROOT = TOOLS_DIR.parent

ACTIVE_CSV = ROOT / "ActiveListings.csv"
OUTPUT_PATH = ROOT / "pricing" / "set_phrases.json"

# ---------------------------------------------
# CONFIG
# ---------------------------------------------
# Minimum number of occurrences needed to accept a set phrase
MIN_FREQ = 3

# Tokens that suggest a SET or BRAND phrase
SET_HINT_TOKENS = {
    "topps", "bowman", "chrome", "stadium", "club", "heritage", "archives",
    "update", "series", "leaf", "fleer", "ultra", "donruss", "elite",
    "pinnacle", "select", "prizm", "optic", "mosaic", "finest", "gallery",
    "upper", "deck", "skybox", "impact", "certified", "absolute",
    "contenders", "limited", "phoenix", "zenith", "e-motion", "emotion"
}

# Generic multi-word phrases we never want to store
STOP_PHRASES = {
    "trading card", "trading cards",
    "baseball card", "basketball card", "football card", "hockey card",
    "sports card", "sports cards",
}

# Individual junk tokens
STOP_TOKENS = {
    "trading", "card", "cards", "baseball", "basketball",
    "football", "hockey", "mlb", "nfl", "nba", "nhl",
}


# ---------------------------------------------
# Helpers
# ---------------------------------------------
def normalize_for_ngrams(text: str) -> list[str]:
    """
    Use the shared global normalizer from helpers_v10,
    then split into tokens for n-gram mining.
    """
    if not text:
        return []

    norm = normalize_title_global(text)
    if not norm:
        return []

    return norm.split()

def iter_ngrams(tokens: list[str], min_n=2, max_n=4):
    """Yield all 2–4 word contiguous phrases."""
    L = len(tokens)
    for n in range(min_n, max_n + 1):
        if L < n:
            break
        for i in range(L - n + 1):
            yield tokens[i:i+n]


def looks_like_set_phrase(ng: list[str]) -> bool:
    """Heuristic: must include at least one set-hint token and not only junk."""
    if len(ng) < 2:
        return False

    # All junk? reject
    if all(tok in STOP_TOKENS for tok in ng):
        return False

    # Must contain some set-related keyword
    if not any(tok in SET_HINT_TOKENS for tok in ng):
        return False

    return True


# ---------------------------------------------
# MAIN LOGIC
# ---------------------------------------------
def main():
    print("📄 retro_learn_v7_set_phrases.py — Auto Set-Phrase Miner")
    print("--------------------------------------------------------")
    print("This will:")
    print("  • Scan all titles in ActiveListings.csv")
    print("  • Automatically discover REAL multi-word set names")
    print("  • Store output → pricing/set_phrases.json\n")

    if not ACTIVE_CSV.exists():
        print(f"❌ ActiveListings.csv not found at: {ACTIVE_CSV}")
        sys.exit(1)

    # Load CSV
    try:
        df = pd.read_csv(ACTIVE_CSV, dtype=str)
    except Exception as e:
        print("❌ Failed to load CSV:", e)
        sys.exit(1)

    if "Title" not in df.columns:
        print("❌ CSV missing 'Title' column.")
        print("Columns:", df.columns.tolist())
        sys.exit(1)

    titles = df["Title"].dropna().astype(str).tolist()
    print(f"Total rows: {len(titles)}\n")

    counter = Counter()

    print("Mining 2–4 word set phrases...\n")
    for idx, tt in enumerate(titles):
        tokens = normalize_for_ngrams(tt)
        if not tokens:
            continue

        for ng in iter_ngrams(tokens):
            if not looks_like_set_phrase(ng):
                continue

            phrase = " ".join(ng)
            if phrase in STOP_PHRASES:
                continue

            counter[phrase] += 1

        if (idx + 1) % 500 == 0:
            print(f"   Progress: {idx+1}/{len(titles)}")

    # Filter by frequency threshold
    final = {p: c for p, c in counter.items() if c >= MIN_FREQ}

    if not final:
        print("\n⚠ No set phrases met the MIN_FREQ threshold.")
        return

    # Sort by frequency desc
    sorted_items = dict(sorted(final.items(), key=lambda kv: (-kv[1], kv[0])))

    # Ensure output folder exists
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    # Save JSON
    with OUTPUT_PATH.open("w", encoding="utf-8") as f:
        json.dump(sorted_items, f, indent=2, ensure_ascii=False)

    # Report summary
    print("\n💾 Saved set phrases →", OUTPUT_PATH)
    print(f"Total distinct set phrases: {len(sorted_items)}\n")

    print("Top examples:")
    for i, (phrase, cnt) in enumerate(sorted_items.items()):
        if i >= 20:
            break
        print(f"  {phrase!r:35} → {cnt}")
    print()


if __name__ == "__main__":
    main()