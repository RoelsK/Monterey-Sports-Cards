"""Update actions (revise price, custom label lookup).

These helpers delegate directly into pricing_engine so that higher-level
code can import them from a dedicated fetch namespace without changing the
core repricer behavior.
"""

from pricing import pricing_engine


update_ebay_price = getattr(pricing_engine, "update_ebay_price", None)
get_custom_label = getattr(pricing_engine, "get_custom_label", None)

__all__ = ["update_ebay_price", "get_custom_label"]
