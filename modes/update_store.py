"""Update Store mode.

Thin wrapper that delegates to pricing_engine.main() so that the
existing repricer logic runs unchanged.
"""

from pricing import pricing_engine


def main():
    """Run the main eBay store repricer (live listings)."""
    pricing_engine.main()  # When repricer exits (e.g. via "C"), we return to main menu.
    return  # Explicit, clear return to caller (main.py)


if __name__ == "__main__":
    main()