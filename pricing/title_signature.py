"""Title signature utilities.

These thin wrappers call into pricing_engine's internal helpers so that
other modules can use a stable API without touching private names.
"""

from typing import Dict, Optional
from pricing import pricing_engine


def extract_card_signature(title: str) -> Optional[Dict]:
    """Extract a normalized card signature from a title string.

    Delegates to pricing_engine._extract_card_signature_from_title.
    Returns None if the underlying helper is missing.
    """
    helper = getattr(pricing_engine, "_extract_card_signature_from_title", None)
    if helper is None:
        return None
    return helper(title)


def compute_signature_hash(title: str, sku: Optional[str] = None) -> str:
    """Compute a stable hash for duplicate detection.

    Delegates to pricing_engine._compute_signature_hash if available.
    Falls back to an empty string if not present.
    """
    helper = getattr(pricing_engine, "_compute_signature_hash", None)
    if helper is None:
        return ""
    return helper(title, sku)
