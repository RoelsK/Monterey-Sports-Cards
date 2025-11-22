import os
import glob
import pandas as pd

def detect_last_resume_position(autosave_folder: str):
    """
    Detects the last processed index by reading the autosave file.
    Returns:
      - integer index (1-based) to resume from
      - OR None if no resume available
    """

    pattern = os.path.join(autosave_folder, "msc_autosave_temp.csv")
    files = glob.glob(pattern)

    if not files:
        return None

    autosave_path = files[0]

    try:
        df = pd.read_csv(autosave_path)
        processed = len(df)

        if processed > 0:
            return processed  # 1-based since your script uses 1-based outputs
        return None
    except Exception:
        return None
