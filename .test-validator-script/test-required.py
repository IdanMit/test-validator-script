#!/usr/bin/env python3
"""
Test-required validator: ensures tests exist and pass.
Run from project root for correct paths. Exit 0 on success, non-zero on failure.
"""
import os
import subprocess
import sys
from pathlib import Path

# Run from project root when invoked as script
ROOT = Path(__file__).resolve().parent.parent
os.chdir(ROOT)


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
        cwd=ROOT,
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
