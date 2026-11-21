#!/usr/bin/env python
"""Run the logistic-map test suite with the standard library test runner."""

from __future__ import annotations

import argparse
import sys
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parent
TESTS = HERE / "tests"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("-p", "--pattern", default="test_*.py", help="test file pattern")
    parser.add_argument("-v", "--verbose", action="store_true", help="verbose output")
    parser.add_argument("test", nargs="?", default="", help="optional test name filter")
    args = parser.parse_args()

    sys.path.insert(0, str(TESTS))
    loader = unittest.TestLoader()
    suite = loader.discover(str(TESTS), pattern=args.pattern, top_level_dir=str(TESTS))
    if args.test:
        suite = loader.loadTestsFromName(args.test, suite)
        if suite.countTestCases() == 0:
            print(f"no tests matched {args.test!r}", file=sys.stderr)
            return 2

    runner = unittest.TextTestRunner(verbosity=2 if args.verbose else 1)
    result = runner.run(suite)
    return 0 if result.wasSuccessful() else 1


if __name__ == "__main__":
    raise SystemExit(main())
