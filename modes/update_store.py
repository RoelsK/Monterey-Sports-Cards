"""Update Store mode.

Thin wrapper that delegates to pricing_engine.main() so that the
existing repricer logic runs unchanged.
"""

from pricing import pricing_engine


def main():
    """Run the main eBay store repricer (live listings)."""
    pricing_engine.main()


if __name__ == "__main__":
    main()
