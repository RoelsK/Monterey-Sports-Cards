"""Browse API helpers.

For now, this simply re-exports the merged active search from pricing_engine
so that callers can import it from fetch.ebay_browse without touching the
core repricer module directly.
"""

from typing import Dict, List, Tuple
from pricing import pricing_engine


def search_active(title: str, limit: int = None, active_cache: Dict = None) -> Tuple[List[float], str, int]:
    """Delegate to pricing_engine.search_active.

    The signature matches the underlying helper:
        (active_totals, act_source, supply_count)
    """
    helper = getattr(pricing_engine, "search_active", None)
    if helper is None:
        return [], "No actives (helper missing)", 0
    if limit is None:
        return helper(title, active_cache=active_cache)
    return helper(title, limit=limit, active_cache=active_cache)
