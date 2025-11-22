"""Logging helpers.

At the moment, logging is still done directly via print() inside
pricing_engine and cdp_mode. This module is provided as a future home for
structured logging if you decide to centralize it.

For now, we expose a simple log() helper that just prints to stdout.
"""


def log(msg: str):
    print(msg)
