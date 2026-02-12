#!/usr/bin/env python3
"""
Wrapper for the test-required validator. Invokes the project-root
validate_test_required.py so the single source of truth stays at root.
"""
import os
import subprocess
import sys
from pathlib import Path

# Project root is parent of .test-validator-script
ROOT = Path(__file__).resolve().parent.parent
VALIDATOR = ROOT / "validate_test_required.py"


def main() -> int:
    if not VALIDATOR.is_file():
        print("FAIL: validate_test_required.py not found at project root.")
        print("WHY: Test-required validator must exist at repo root.")
        print("FIX: Add validate_test_required.py at project root.")
        return 1
    os.chdir(ROOT)
    return subprocess.run(
        [sys.executable, str(VALIDATOR)],
        cwd=ROOT,
    ).returncode


if __name__ == "__main__":
    sys.exit(main())
