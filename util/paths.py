import os

# Absolute BASE folder where the entire project lives
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Master folders (stay backward compatible)
CACHE_DIR = os.path.join(BASE_DIR, "cache")
RESULTS_FOLDER = os.path.join(BASE_DIR, "results")
AUTOSAVE_FOLDER = os.path.join(BASE_DIR, "autosave")
LOGS_FOLDER = os.path.join(BASE_DIR, "logs")

# Ensure folders exist
for folder in (CACHE_DIR, RESULTS_FOLDER, AUTOSAVE_FOLDER, LOGS_FOLDER):
    os.makedirs(folder, exist_ok=True)

# Specific important files (names unchanged)
FULL_STORE_IDS_PATH = os.path.join(CACHE_DIR, "full_store_ids.json")
ACTIVE_CACHE_PATH = os.path.join(CACHE_DIR, "active_cache.json")
TOKENS_PATH = os.path.join(BASE_DIR, ".env")