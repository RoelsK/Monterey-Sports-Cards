"""
retro_learn_v6_brand_families.py

PURPOSE:
    Build, normalize, and maintain a BRAND FAMILY classifier
    for strict-matching inside pricing_engine.py.

INPUT:
    • ActiveListings.csv
    • Natural brand/product names inside titles

OUTPUT:
    • pricing/brand_families.json
      {
        "topps chrome": ["chrome", "topps chromium", "chrome topps"],
        "bowman chrome": ["chrome bowman"],
        ...
      }

BEHAVIOR:
    • Offline only (NO API calls)
    • Extracts brand-family patterns from titles
    • Canonicalizes each family
    • Removes duplicates
    • Saves brand_families.json
"""

from pathlib import Path
import json
import sys
import re
import pandas as pd

# ---------------------------------------------
# Paths
# ---------------------------------------------
TOOLS_DIR = Path(__file__).resolve().parent
ROOT = TOOLS_DIR.parent
ACTIVE_CSV = ROOT / "ActiveListings.csv"
OUTPUT_PATH = ROOT / "pricing" / "brand_families.json"

# ---------------------------------------------
# Canonical brand roots
# ---------------------------------------------
BRAND_ROOTS = {
    # Topps / Bowman universe
    "topps",
    "bowman",
    "stadium club",
    "gypsy queen",
    "heritage",
    "archives",
    "chrome",
    "finest",

    # Panini universe
    "donruss",
    "donruss optic",
    "optic",
    "select",
    "mosaic",
    "prizm",
    "contenders",
    "rookies & stars",
    "absolute",
    "revolution",
    "threads",
    "spectra",
    "phoenix",

    # Upper Deck / Fleer universe
    "upper deck",
    "fleer ultra",
    "fleer",
    "skybox",
    "skybox impact",
    "collector's choice",
    "sp",
    "spx",
    "spa",
    "ex",
    "ex2001",
    "gold label",
    "mvp",
}

# Variants (normalize to canonical)
VARIANT_MAP = {
    "topps chrome": ["chrome topps", "toppscr", "t chrome", "chrome"],
    "bowman chrome": ["bowmancr", "chrome bowman", "bowman ch"],
    "donruss optic": ["optic donruss", "optic holo", "optic hollow"],
    "skybox impact": ["impact skybox"],
    "stadium club": ["stadiumclub"],
    "fleer ultra": ["ultra fleer"],
    "upper deck": ["ud", "u.d.", "upper-deck"],
    "spx": ["sp x", "sp-x"],
    "ex": ["ex2000", "ex-2001"],
}

# ---------------------------------------------
# Helpers
# ---------------------------------------------
def canonicalize(s: str) -> str:
    s = s.lower().strip()
    s = re.sub(r"[\-_/]+", " ", s)
    s = re.sub(r"\s+", " ", s)
    return s


def extract_brand_candidates(title: str) -> set[str]:
    if not title:
        return set()

    lower = canonicalize(title)
    tokens = lower.split()
    found = set()

    # Check n-gram windows: 1-word, 2-word, 3-word
    for n in (1, 2, 3):
        for i in range(len(tokens) - n + 1):
            gram = " ".join(tokens[i:i+n])
            if gram in BRAND_ROOTS:
                found.add(gram)

            # Also detect variants
            for canon, variants in VARIANT_MAP.items():
                for v in variants:
                    if gram == canonicalize(v):
                        found.add(canon)

    return found


# ---------------------------------------------
# MAIN
# ---------------------------------------------
def main():
    if not ACTIVE_CSV.exists():
        print(f"❌ ActiveListings.csv not found at: {ACTIVE_CSV}")
        sys.exit(1)

    print("📄 retro_learn_v6_brand_families.py — Brand Family Classifier")
    print("------------------------------------------------------------")
    print("This will:")
    print("  • Scan all active listing titles")
    print("  • Detect brand-family tokens")
    print("  • Normalize & group related variants")
    print("  • Save output → pricing/brand_families.json\n")

    try:
        df = pd.read_csv(ACTIVE_CSV, dtype=str)
    except Exception as e:
        print("❌ Failed to load CSV:", e)
        sys.exit(1)

    if "Title" not in df.columns:
        print("❌ CSV missing 'Title' column.")
        print("Columns:", df.columns.tolist())
        sys.exit(1)

    print(f"Total rows: {len(df)}")

    brand_map: dict[str, set[str]] = {}

    for idx, row in df.iterrows():
        title = row.get("Title", "")
        candidates = extract_brand_candidates(title)

        if not candidates:
            continue

        # Group into canonical → variants form
        for c in candidates:
            norm = canonicalize(c)

            # Try to map variant → canonical
            canon_root = None
            for root in BRAND_ROOTS:
                if norm == canonicalize(root):
                    canon_root = canonicalize(root)
                    break

            if canon_root is None:
                # Check variant map
                for base, variants in VARIANT_MAP.items():
                    if norm in [canonicalize(v) for v in variants] or norm == canonicalize(base):
                        canon_root = canonicalize(base)
                        break

            if canon_root is None:
                continue

            brand_map.setdefault(canon_root, set()).add(norm)

        if (idx + 1) % 500 == 0:
            print(f"   Progress: {idx+1}/{len(df)}")

    # Convert to sorted lists
    final_map = {k: sorted(list(v)) for k, v in brand_map.items()}

    # Save
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with OUTPUT_PATH.open("w", encoding="utf-8") as f:
        json.dump(final_map, f, indent=2, ensure_ascii=False)

    print(f"\n💾 Saved brand families → {OUTPUT_PATH}")
    print(f"Total brand families detected: {len(final_map)}\n")

    print("Sample families:")
    for i, (brand, variants) in enumerate(final_map.items()):
        if i >= 10:
            break
        print(f"  {brand}: {variants}")


if __name__ == "__main__":
    main()
