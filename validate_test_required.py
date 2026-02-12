#!/usr/bin/env python3
"""
Test-required validator: ensures tests exist and pass.
PR fails if there are no tests or if the test suite fails.
Exit 0 on success, non-zero on failure (CI-friendly).
"""
from pathlib import Path
import subprocess
import sys


def main() -> int:
    tests_dir = Path("tests")
    if not tests_dir.is_dir():
        print("FAIL: tests/ directory not found.")
        print("WHY: Validator requires a tests/ directory with test modules.")
        print("FIX: Create tests/ and add at least one test file (e.g. test_*.py).")
        return 1

    py_tests = list(tests_dir.glob("test_*.py")) + list(tests_dir.glob("*_test.py"))
    if not py_tests:
        print("FAIL: No test files found in tests/ (expected test_*.py or *_test.py).")
        print("WHY: Code changes should include or trigger appropriate tests.")
        print("FIX: Add test modules under tests/ and run: pytest")
        return 1

    result = subprocess.run(
        [sys.executable, "-m", "pytest", "tests/", "-v", "--tb=short"],
        capture_output=False,
    )
    if result.returncode != 0:
        print("\nFAIL: Test suite did not pass.")
        print("WHY: All tests must pass before merge.")
        print("FIX: Run 'pytest tests/ -v' locally and fix failing tests.")
        return result.returncode

    print("PASS: Tests exist and passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
