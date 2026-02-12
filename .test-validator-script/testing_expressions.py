"""
Testing expression utilities: path and pattern helpers for validators.
"""
from pathlib import Path


def is_test_file(path: Path) -> bool:
    """Return True if path looks like a test module (test_*.py or *_test.py)."""
    name = path.name
    return name.startswith("test_") and name.endswith(".py") or (
        name.endswith("_test.py")
    )


def collect_test_files(tests_dir: Path):
    """Yield test files under tests_dir (test_*.py and *_test.py)."""
    if not tests_dir.is_dir():
        return
    for p in tests_dir.glob("test_*.py"):
        yield p
    for p in tests_dir.glob("*_test.py"):
        yield p


def has_tests(tests_dir: Path) -> bool:
    """Return True if tests_dir exists and contains at least one test file."""
    return any(collect_test_files(tests_dir))
