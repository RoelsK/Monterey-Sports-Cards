"""
retro_learn_v4_inserts.py

Offline learner for INSERT TERMS.

- NO API CALLS.
- Reads ActiveListings.csv from the project root.
- Scans titles for strongly insert-like phrases.
- Updates pricing/classification_rules.json → "insert_terms" (substring-based).

Heuristics (SAFE, conservative):
  • We look for phrases containing 'insert', 'die-cut', 'die cut',
    'holofoil', 'holoview', 'gold medallion', 'electric diamond',
    'x-fractor', 'xfractor', 'spotlight', etc.
  • We skip ultra-generic subset names like 'team leaders' for now.
"""

from pathlib import Path
import json
import sys
import pandas as pd

TOOLS_DIR = Path(__file__).resolve().parent
ROOT = TOOLS_DIR.parent
ACTIVE_CSV = ROOT / "ActiveListings.csv"
CLASS_RULES_PATH = ROOT / "pricing" / "classification_rules.json"


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


# Strongly insert-like substrings (safe)
SEED_INSERT_SUBSTRINGS = {
    "insert",
    "inserts",
    "insertt",
    "die cut",
    "die-cut",
    "diecut",
    "holofoil",
    "holoview",
    "gold medallion",
    "electric diamond",
    "gold team",
    "spotlight",
    "spot light",
    "power plus",
    "power surge",
    "power zone",
    "hitting machines",
    "all-star insert",
    "proto-star",
}


def extract_insert_phrases(title: str) -> set[str]:
    """
    Extract conservative insert-like substrings from a title.
    We return phrases we will store directly in insert_terms for substring matching.
    """
    if not title:
        return set()

    lower = title.lower()
    found: set[str] = set()

    # Direct seed substring hits
    for sub in SEED_INSERT_SUBSTRINGS:
        if sub in lower:
            found.add(sub)

    # Additional heuristic: any phrase 'something insert'
    # We'll capture the segment around the word 'insert'/'inserts'.
    words = lower.split()
    for i, w in enumerate(words):
        if w in {"insert", "inserts"}:
            # take up to 2 words before and 2 after and join them
            start = max(0, i - 2)
            end = min(len(words), i + 3)
            phrase = " ".join(words[start:end]).strip()
            if "insert" in phrase:
                found.add(phrase)

    # Normalize whitespace
    cleaned = set()
    for s in found:
        cleaned.add(" ".join(s.split()))

    return cleaned


def main():
    if not ACTIVE_CSV.exists():
        print("❌ ActiveListings.csv not found in project root.")
        print(f"   Expected here: {ACTIVE_CSV}")
        sys.exit(1)

    print("📄 retro_learn_v4_inserts.py — Offline insert-term learner")
    print("---------------------------------------------------------")
    print(f"CSV Path: {ACTIVE_CSV}")
    print("This will:")
    print("  • Load classification_rules.json (or create defaults)")
    print("  • Scan all titles in ActiveListings.csv")
    print("  • Collect strongly insert-like phrases (substring-based)")
    print("  • Merge them into insert_terms (deduped, sorted)")
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
    existing_inserts = set(s.lower() for s in rules.get("insert_terms", []))

    total_rows = len(df)
    titles_seen = 0
    new_phrases: set[str] = set()

    print(f"Total rows in CSV: {total_rows}")
    print("Starting insert-term learning pass...\n")

    for idx, row in df.iterrows():
        title = (row.get("Title") or "").strip()
        if not title:
            continue

        titles_seen += 1
        candidates = extract_insert_phrases(title)

        for c in candidates:
            if c not in existing_inserts:
                new_phrases.add(c)

        if (idx + 1) % 500 == 0:
            print(f"   Progress: {idx + 1}/{total_rows} rows processed...")

    print("\n✅ Insert-term scan complete.")
    print(f"   Titles processed: {titles_seen}")
    print(f"   Existing insert terms: {len(existing_inserts)}")
    print(f"   New insert terms discovered: {len(new_phrases)}")

    if new_phrases:
        print("   New insert phrases found (sample):")
        for s in sorted(list(new_phrases))[:25]:
            print(f"     • {s}")
    else:
        print("   No new insert phrases found beyond what already exists.")

    merged = sorted(existing_inserts | new_phrases)
    rules["insert_terms"] = merged
    save_classification_rules(rules)

    print(f"\nFinal insert_terms count: {len(merged)}")
    print("Done. Strict matching can now better separate base vs inserts.")
    

if __name__ == "__main__":
    main()
