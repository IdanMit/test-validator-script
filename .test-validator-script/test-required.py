#!/usr/bin/env python3
"""
Test-required validator: ensures tests/ exists, has test files, and all tests pass.
Exit 0 on success, 1 on failure. Respects VALIDATION_SKIP=test-required.
"""

import os
import subprocess
import sys

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.dirname(_SCRIPT_DIR)
if _SCRIPT_DIR not in sys.path:
    sys.path.insert(0, _SCRIPT_DIR)
if os.getcwd() != _REPO_ROOT:
    os.chdir(_REPO_ROOT)

from testing_expressions import TESTS_DIR, has_test_files, should_skip, tests_dir_exists


def run_pytest() -> bool:
    """Run pytest on tests/. Return True if exit code is 0."""
    result = subprocess.run(
        [sys.executable, "-m", "pytest", TESTS_DIR, "-v"],
        capture_output=True,
        text=True,
        timeout=120,
    )
    if result.returncode != 0:
        print(result.stdout or "")
        print(result.stderr or "", file=sys.stderr)
        return False
    return True


def main() -> int:
    if should_skip("test-required"):
        print("VALIDATION_SKIP=test-required: skipping test-required validator.")
        return 0

    if not tests_dir_exists():
        print("FAIL: tests/ directory not found. Create tests/ and add test files (test_*.py or *_test.py).")
        return 1

    if not has_test_files():
        print("FAIL: tests/ has no test files. Add at least one test_*.py or *_test.py.")
        return 1

    if not run_pytest():
        print("FAIL: One or more tests failed. Fix tests and run: pytest tests/ -v")
        return 1

    print("PASS: test-required validator.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
