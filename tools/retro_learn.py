"""
retro_learn.py

Offline token-rule "pretraining" from your full ActiveListings.csv.

- NO API CALLS.
- Reads eBay "Download Active Listings" CSV:
  Columns like: Item number,Title,Custom label (SKU),Current price,...
- For each non-empty Title, calls learn_from_title(...)
- Uses your existing pricing_engine token_rules helpers so everything
  stays in sync with live pricing runs.
"""

from pathlib import Path
import sys
import pandas as pd

# ---------------------------------------------
# Project root + import pricing_engine helpers
# ---------------------------------------------
# ---------------------------------------------
# Project root = parent of /tools/
# ---------------------------------------------
SCRIPT_DIR = Path(__file__).resolve().parent
ROOT = SCRIPT_DIR.parent  # <— this is the actual project root

# Add ROOT to sys.path so "pricing" becomes importable
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

try:
    from pricing.pricing_engine import (
        load_token_rules,
        save_token_rules,
        learn_from_title,
    )
except ImportError as e:
    print("❌ Could not import from pricing.pricing_engine.")
    print("   Make sure your project structure is:")
    print("   ROOT/")
    print("     pricing/")
    print("       pricing_engine.py")
    print("     retro_learn.py")
    print("     ActiveListings.csv")
    print(f"   Import error: {e}")
    sys.exit(1)


# ---------------------------------------------
# CONFIG
# ---------------------------------------------
ACTIVE_CSV = ROOT / "ActiveListings.csv"  # <— uses RELATIVE path only

def main():
    if not ACTIVE_CSV.exists():
        print("❌ ActiveListings.csv not found in project root.")
        print(f"   Expected here: {ACTIVE_CSV}")
        sys.exit(1)

    print("📄 retro_learn.py — Offline token-rule learning")
    print("------------------------------------------------")
    print(f"CSV Path: {ACTIVE_CSV}")
    print("This will:")
    print("  • Load all active titles from the CSV")
    print("  • Feed each TITLE into learn_from_title(...)")
    print("  • Save updated token_rules via save_token_rules(...)")
    print("No API calls are made.\n")

    # -----------------------------------------
    # Load CSV
    # -----------------------------------------
    try:
        df = pd.read_csv(ACTIVE_CSV, dtype=str)  # keep everything as string
    except Exception as e:
        print(f"❌ Failed to read CSV: {e}")
        sys.exit(1)

    if "Title" not in df.columns:
        print("❌ CSV does not contain a 'Title' column.")
        print("   First few columns detected:")
        print("   " + ", ".join(df.columns[:10].tolist()))
        sys.exit(1)

    # -----------------------------------------
    # Load existing token rules
    # -----------------------------------------
    try:
        token_rules = load_token_rules()
    except Exception as e:
        print(f"⚠ Warning: load_token_rules() failed: {e}")
        print("   Starting with empty token_rules dict.")
        token_rules = {}

    total_rows = len(df)
    non_empty_titles = 0
    learned_events = 0

    print(f"Total rows in CSV: {total_rows}")
    print("Starting offline learning pass...\n")

    # -----------------------------------------
    # Emitter for capturing learning output
    # -----------------------------------------
    def emit(msg: str):
        nonlocal learned_events
        learned_events += 1
        if learned_events <= 50:
            print("   " + msg)
        elif learned_events == 51:
            print("   ... (further learn logs suppressed)")

    # -----------------------------------------
    # Iterate titles
    # -----------------------------------------
    for idx, row in df.iterrows():
        title = (row.get("Title") or "").strip()
        if not title:
            continue

        non_empty_titles += 1

        # Run your existing learning logic
        try:
            # Correct signature in pricing_engine.py:
            # learn_from_title(title, learn_callback=None)
            learn_from_title(title, learn_callback=emit)
        except Exception as e:
            print(f"⚠ Row {idx}: error during learn_from_title → {e}")

        # Progress every 500 rows
        if (idx + 1) % 500 == 0:
            print(f"   Progress: {idx + 1}/{total_rows} rows processed...")

    print("\n✅ Offline learning pass complete.")
    print(f"   Titles processed: {non_empty_titles}")
    print(f"   Learning events emitted: {learned_events}")

    # -----------------------------------------
    # Save updated token rules
    # -----------------------------------------
    try:
        save_token_rules()  # <— CORRECT: no args
        print("💾 token_rules saved via save_token_rules().")
    except Exception as e:
        print(f"❌ Failed to save token_rules: {e}")
        sys.exit(1)

    print("\nDone. Future live runs + the GUI debugger now benefit from this pretraining.")


if __name__ == "__main__":
    main()