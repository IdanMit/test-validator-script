"""
Testing expression utilities for validators.
Used for test discovery patterns and validation logic.
"""
from pathlib import Path

# Glob patterns that identify test modules
TEST_FILE_GLOBS = ("test_*.py", "*_test.py")


def find_test_files(tests_dir: Path) -> list[Path]:
    """Return list of Python test files under tests_dir."""
    if not tests_dir.is_dir():
        return []
    out = []
    for pattern in TEST_FILE_GLOBS:
        out.extend(tests_dir.glob(pattern))
    return sorted(set(out))


def has_required_tests(tests_dir: Path) -> bool:
    """Return True if tests_dir exists and contains at least one test file."""
    return len(find_test_files(tests_dir)) > 0
