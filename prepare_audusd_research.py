#!/usr/bin/env python3
"""Compatibility wrapper for the generic research_asset master command.

This prepares the full offline workflow only. It does not start AUD/USD research.
"""
from __future__ import annotations

import argparse
import sys

from research_asset import main as research_asset_main


START = "2026-07-29T00:00:00Z"
END = "2026-08-29T23:59:59Z"


def main() -> None:
    parser = argparse.ArgumentParser(description="Prepare AUD/USD offline research without executing it")
    parser.add_argument("--cache", required=True)
    parser.add_argument("--workspace")
    parser.add_argument("--python", default=sys.executable)
    args = parser.parse_args()
    translated = [
        sys.argv[0], "AUD_USD", "--cache", args.cache, "--start", START,
        "--end", END, "--horizon", "240", "--prepare-only", "--python", args.python,
    ]
    if args.workspace:
        translated.extend(["--workspace", args.workspace])
    sys.argv = translated
    research_asset_main()


if __name__ == "__main__":
    main()
