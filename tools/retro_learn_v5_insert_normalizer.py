"""
retro_learn_v5_insert_normalizer.py

Purpose:
  Normalize, canonicalize, group, and dedupe insert terms inside
  pricing/classification_rules.json.

Problem solved:
  After v4 learning, many inserts appear as variants:
     "die cut", "die-cut", "diecut"
     "gold medallion", "gold  medallion"
     "holofoil", "holo foil"
     "power surge", "power   surge"
     etc.

This script:
  • Loads classification_rules.json
  • Normalizes whitespace, hyphens, casing
  • Unifies known variant families
  • Removes pure-duplicate variants
  • Sorts the final list
  • Saves a clean, canonical insert_terms list
"""

from pathlib import Path
import json
import sys
import re

# ---------------------------------------------
# Paths
# ---------------------------------------------
TOOLS_DIR = Path(__file__).resolve().parent
ROOT = TOOLS_DIR.parent
CLASS_RULES_PATH = ROOT / "pricing" / "classification_rules.json"

# ---------------------------------------------
# Insert variant groups
# ---------------------------------------------
INSERT_GROUPS = [
    # Die-cut family
    {"die cut", "die-cut", "diecut", "die  cut", "die  -  cut"},
    
    # Holofoil family
    {"holofoil", "holo foil", "holo-foil", "holo  foil"},
    
    # Holoview family
    {"holoview", "holo view", "holo-view"},
    
    # Gold Medallion family
    {"gold medallion", "gold  medallion", "medallion gold"},
    
    # Electric Diamond family
    {"electric diamond", "electric  diamond", "diamond electric"},
    
    # Power Surge family
    {"power surge", "power  surge", "surge power"},
    
    # Power Plus family
    {"power plus", "power  plus", "plus power"},
    
    # Golden Greats family
    {"golden greats", "golden  greats", "greats golden"},
    
    # Curtain Call family
    {"curtain call", "curtain  call", "call curtain"},

    # Instant Impact
    {"instant impact", "instant  impact", "impact instant"},
]


# ---------------------------------------------
# Canonicalization Helper
# ---------------------------------------------
def canonicalize_term(s: str) -> str:
    """Normalize casing, whitespace, hyphens."""
    s = s.lower().strip()
    s = s.replace("-", "-").replace("–", "-").replace("—", "-")
    s = re.sub(r"\s+", " ", s)
    s = s.replace(" - ", " ")
    s = s.strip()
    return s


def group_normalize(term: str) -> str:
    """Return canonical version of a term using INSERT_GROUPS if it matches."""
    c = canonicalize_term(term)

    for group in INSERT_GROUPS:
        group_c = {canonicalize_term(x) for x in group}
        if c in group_c:
            # Canonical = first entry (sorted)
            return sorted(list(group_c))[0]

    return c  # fallback


# ---------------------------------------------
# Normalizer
# ---------------------------------------------
def normalize_insert_terms():
    if not CLASS_RULES_PATH.exists():
        print(f"❌ classification_rules.json not found at: {CLASS_RULES_PATH}")
        sys.exit(1)

    with CLASS_RULES_PATH.open("r", encoding="utf-8") as f:
        rules = json.load(f)

    insert_terms = rules.get("insert_terms", [])
    print(f"Loaded {len(insert_terms)} insert_terms...")

    normalized = set()

    for t in insert_terms:
        # canonicalize + group normalize
        c = group_normalize(t)
        normalized.add(c)

    # sort alphabetically, ignoring case
    cleaned_sorted = sorted(normalized)

    rules["insert_terms"] = cleaned_sorted

    # Save
    with CLASS_RULES_PATH.open("w", encoding="utf-8") as f:
        json.dump(rules, f, indent=2, ensure_ascii=False)

    print(f"💾 Normalized insert_terms saved to: {CLASS_RULES_PATH}")
    print(f"Final insert_terms count: {len(cleaned_sorted)}")


if __name__ == "__main__":
    normalize_insert_terms()
