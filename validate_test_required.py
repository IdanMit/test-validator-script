#!/usr/bin/env python3
"""Root entry point for the test-required validator. Delegates to .test-validator-script/test-required.py."""

import os
import subprocess
import sys

REPO_ROOT = os.path.dirname(os.path.abspath(__file__))
SCRIPT = os.path.join(REPO_ROOT, ".test-validator-script", "test-required.py")


def main() -> int:
    if not os.path.isfile(SCRIPT):
        print("FAIL: Validator script not found at .test-validator-script/test-required.py", file=sys.stderr)
        return 1
    result = subprocess.run([sys.executable, SCRIPT], cwd=REPO_ROOT)
    return result.returncode


if __name__ == "__main__":
    sys.exit(main())
