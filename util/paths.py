import os

# Absolute BASE folder where the entire project lives
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Master folders
CACHE_DIR = os.path.join(BASE_DIR, "cache")
RESULTS_DIR = os.path.join(BASE_DIR, "results")
AUTOSAVE_DIR = os.path.join(BASE_DIR, "autosave")   # YES — this is the missing one
LOGS_DIR = os.path.join(BASE_DIR, "logs")

# Ensure folders exist
for folder in (CACHE_DIR, RESULTS_DIR, AUTOSAVE_DIR, LOGS_DIR):
    os.makedirs(folder, exist_ok=True)

# Specific important files
FULL_STORE_IDS_PATH = os.path.join(CACHE_DIR, "full_store_ids.json")
ACTIVE_CACHE_PATH = os.path.join(CACHE_DIR, "active_cache.json")
TOKENS_PATH = os.path.join(BASE_DIR, ".env")