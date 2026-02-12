"""Shared expressions and helpers for test validation."""

import os

TESTS_DIR = "tests"
VALIDATION_SKIP_ENV = "VALIDATION_SKIP"


def should_skip(validator_name: str) -> bool:
    """Return True if VALIDATION_SKIP contains this validator (e.g. 'test-required')."""
    skip = os.environ.get(VALIDATION_SKIP_ENV, "")
    return validator_name in [s.strip() for s in skip.split(",") if s.strip()]


def tests_dir_exists() -> bool:
    """Return True if the tests directory exists."""
    return os.path.isdir(TESTS_DIR)


def has_test_files() -> bool:
    """Return True if tests/ contains at least one test file (test_*.py or *_test.py)."""
    if not tests_dir_exists():
        return False
    for name in os.listdir(TESTS_DIR):
        if name.startswith("test_") and name.endswith(".py"):
            return True
        if name.endswith("_test.py"):
            return True
    return False
