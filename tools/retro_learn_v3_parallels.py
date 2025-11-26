"""
retro_learn_v3_parallels.py

Offline learner for PARALLEL PATTERN TERMS.

- NO API CALLS.
- Reads ActiveListings.csv from the project root.
- Scans titles for known parallel-style pattern words (mojo, wave, disco, shimmer, scope, etc.).
- Updates pricing/classification_rules.json → "parallel_pattern_terms" only.

This is intentionally conservative:
  • We do NOT auto-add color words like "blue" / "red" to avoid mis-tagging team names.
  • We only focus on pattern-style tokens that nearly always indicate a parallel.
"""

from pathlib import Path
import json
import sys
import pandas as pd

# ---------------------------------------------
# Paths
# ---------------------------------------------
TOOLS_DIR = Path(__file__).resolve().parent
ROOT = TOOLS_DIR.parent                          # project root
ACTIVE_CSV = ROOT / "ActiveListings.csv"
CLASS_RULES_PATH = ROOT / "pricing" / "classification_rules.json"


# ---------------------------------------------
# Helpers to load/save classification rules
# ---------------------------------------------
def load_classification_rules() -> dict:
    if CLASS_RULES_PATH.exists():
        try:
            with CLASS_RULES_PATH.open("r", encoding="utf-8") as f:
                rules = json.load(f)
        except Exception as e:
            print(f"⚠ Failed to read existing classification_rules.json: {e}")
            rules = {}
    else:
        rules = {}

    # Ensure expected keys exist
    rules.setdefault("oddball_terms", [])
    rules.setdefault("promo_terms", [])
    rules.setdefault("insert_terms", [])
    rules.setdefault("parallel_color_terms", [])
    rules.setdefault("parallel_pattern_terms", [])

    return rules


def save_classification_rules(rules: dict) -> None:
    try:
        CLASS_RULES_PATH.parent.mkdir(parents=True, exist_ok=True)
        with CLASS_RULES_PATH.open("w", encoding="utf-8") as f:
            json.dump(rules, f, indent=2, ensure_ascii=False)
        print(f"💾 Saved updated classification_rules.json → {CLASS_RULES_PATH}")
    except Exception as e:
        print(f"❌ Failed to save classification_rules.json: {e}")
        sys.exit(1)


# ---------------------------------------------
# Seed pattern tokens (safe, rarely ambiguous)
# ---------------------------------------------
SEED_PATTERN_TOKENS = {
    # Generic pattern words
    "mojo",
    "wave",
    "hyper",
    "laser",
    "scope",
    "pulsar",
    "disco",
    "velocity",
    "flash",
    "swirl",
    "cracked",
    "checkered",
    "checkerboard",
    "kaleidoscope",
    "cosmic",
    "tiger",
    "zebra",
    "giraffe",
    "snakeskin",
    "lava",
    "ice",
    "atomic",
    "finest",
    "mosaic",
    "optic",
    "holo",
    "holofoil",
    "shimmer",
    "sparkle",
    "fireworks",
    "stained",
    "glass",
    "fracture",
    "fractured",
    "xfractor",
    "x-fractor",
}


def extract_pattern_candidates(title: str) -> set[str]:
    """
    Extract candidate pattern-style parallel tokens from a single title.
    This is deliberately simple and conservative.
    """
    if not title:
        return set()

    lower = title.lower()
    raw_tokens = [t.strip(" ,./-:;!()[]{}\"'") for t in lower.split()]
    tokens = [t for t in raw_tokens if t]

    found: set[str] = set()

    for tok in tokens:
        base = tok

        # Normalize some known variants
        if base in {"xfractor", "x-fractor"}:
            base = "xfractor"

        if base in {"holofoil", "holo", "holos"}:
            base = "holo"

        if base in {"checkerboard", "checkered"}:
            base = "checkerboard"

        if base in {"fracture", "fractured"}:
            base = "fracture"

        if base in SEED_PATTERN_TOKENS:
            found.add(base)

    # Also detect some multi-word patterns as single tokens (for substring checks later),
    # but we just record the core word here. Titles already contain both words.
    if "cracked ice" in lower:
        found.add("cracked")
        found.add("ice")
    if "stained glass" in lower:
        found.add("stained")
        found.add("glass")

    return found


def main():
    if not ACTIVE_CSV.exists():
        print("❌ ActiveListings.csv not found in project root.")
        print(f"   Expected here: {ACTIVE_CSV}")
        sys.exit(1)

    print("📄 retro_learn_v3_parallels.py — Offline pattern-parallel learner")
    print("-----------------------------------------------------------------")
    print(f"CSV Path: {ACTIVE_CSV}")
    print("This will:")
    print("  • Load classification_rules.json (or create defaults)")
    print("  • Scan all titles in ActiveListings.csv")
    print("  • Collect pattern-style parallel tokens (mojo, wave, disco, shimmer, etc.)")
    print("  • Merge them into parallel_pattern_terms (deduped, sorted)")
    print("No API calls are made.\n")

    try:
        df = pd.read_csv(ACTIVE_CSV, dtype=str)
    except Exception as e:
        print(f"❌ Failed to read CSV: {e}")
        sys.exit(1)

    if "Title" not in df.columns:
        print("❌ CSV does not contain a 'Title' column.")
        print("   First few columns detected:")
        print("   " + ", ".join(df.columns[:10].tolist()))
        sys.exit(1)

    rules = load_classification_rules()
    existing_patterns = set(t.lower() for t in rules.get("parallel_pattern_terms", []))

    total_rows = len(df)
    titles_seen = 0
    new_patterns: set[str] = set()

    print(f"Total rows in CSV: {total_rows}")
    print("Starting parallel-pattern learning pass...\n")

    for idx, row in df.iterrows():
        title = (row.get("Title") or "").strip()
        if not title:
            continue

        titles_seen += 1
        candidates = extract_pattern_candidates(title)

        for c in candidates:
            if c not in existing_patterns:
                new_patterns.add(c)

        if (idx + 1) % 500 == 0:
            print(f"   Progress: {idx + 1}/{total_rows} rows processed...")

    print("\n✅ Pattern-parallel scan complete.")
    print(f"   Titles processed: {titles_seen}")
    print(f"   Existing pattern terms: {len(existing_patterns)}")
    print(f"   New pattern terms discovered: {len(new_patterns)}")

    if new_patterns:
        print("   New pattern tokens found (sample):")
        for t in sorted(list(new_patterns))[:25]:
            print(f"     • {t}")
    else:
        print("   No new pattern tokens found beyond what already exists.")

    # Merge and save
    merged = sorted(existing_patterns | new_patterns)
    rules["parallel_pattern_terms"] = merged
    save_classification_rules(rules)

    print(f"\nFinal parallel_pattern_terms count: {len(merged)}")
    print("Done. Future strict matching will now better understand pattern parallels.")


if __name__ == "__main__":
    main()
