import json
import os
from datetime import datetime
from typing import List, Set

import requests  # assuming you use this already

FULL_STORE_PATH = "ids/full_store_ids.json"


# ------------------------------
# Helper: Load existing ItemIDs
# ------------------------------
def load_existing_ids() -> Set[str]:
    if not os.path.exists(FULL_STORE_PATH):
        print(f"⚠ No full_store_ids.json found at {FULL_STORE_PATH}. Creating a new one.")
        return set()

    try:
        with open(FULL_STORE_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
            return set(str(x) for x in data)
    except Exception as e:
        print(f"❌ Error reading {FULL_STORE_PATH}: {e}")
        return set()


# ------------------------------------------
# Helper: Write updated ID list back to file
# ------------------------------------------
def save_ids_to_file(all_ids: Set[str]):
    all_ids_list = sorted(all_ids, key=lambda x: int(x))  # keep ordering consistent
    os.makedirs(os.path.dirname(FULL_STORE_PATH), exist_ok=True)

    with open(FULL_STORE_PATH, "w", encoding="utf-8") as f:
        json.dump(all_ids_list, f, indent=2)

    print(f"💾 Saved {len(all_ids_list)} total ItemIDs → {FULL_STORE_PATH}")


# --------------------------------------------------------
# FETCH ACTIVE IDS — you may use your Finding/Browse logic
# --------------------------------------------------------
def fetch_active_item_ids() -> Set[str]:
    """
    Placeholder: Replace with your EXISTING function that fetches ItemIDs.
    
    This is intentionally simple so you can plug in:
    - Browse API
    - Finding API
    - Multi-call paging
    - Your existing helpers

    IMPORTANT:
    This must return a Python SET of item IDs (strings).
    """
    print("⚠ fetch_active_item_ids() is a placeholder. Replace with your actual API fetch function.")
    return set()

    # Example structure:
    #
    # ids = set()
    # for page in range(total_pages):
    #     call API
    #     add item.itemId
    # return ids


# ------------------------------------------
# MAIN ENTRY: Append NEW ItemIDs only
# ------------------------------------------
def append_new_ids():
    print("\n🔍 Scanning for NEW ItemIDs…")

    existing_ids = load_existing_ids()
    print(f"📁 Loaded {len(existing_ids)} existing IDs")

    fetched_ids = fetch_active_item_ids()
    print(f"🌐 Fetched {len(fetched_ids)} active IDs from eBay")

    new_ids = fetched_ids - existing_ids

    if not new_ids:
        print("✅ No new ItemIDs found. Your full_store_ids.json is up-to-date.")
        return

    print(f"\n✨ Found {len(new_ids)} NEW ItemIDs:")
    for x in sorted(new_ids):
        print(f"   + {x}")

    updated_ids = existing_ids | new_ids
    save_ids_to_file(updated_ids)

    print("\n🎉 Done!")
    print(f"🆕 Added: {len(new_ids)}")
    print(f"📦 Total: {len(updated_ids)}\n")