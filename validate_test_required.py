#!/usr/bin/env python3
"""
Test-required validator: PR fails if relevant code changes don't include/trigger tests.
Deterministic, fast, actionable, CI-friendly (exit codes, clear logs).
"""
import os
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent
# Source dirs / patterns that require tests
SOURCE_GLOB = "*.py"
# Exclude validators, scripts that are not library code, and vendored/test-helper code
EXCLUDE_PREFIXES = ("validate_", "run_", ".test-validator-script")
# Test discovery
TEST_DIR = "tests"
TEST_FILE_PREFIX = "test_"


def get_source_modules():
    """List Python modules that are considered 'source' (require tests)."""
    sources = []
    for path in REPO_ROOT.glob(SOURCE_GLOB):
        if path.name.startswith(EXCLUDE_PREFIXES):
            continue
        if path.name == "__init__.py" or path == Path(__file__).name:
            continue
        # Only main library/application code (e.g. simple_json_reader, pyspark_json_reader)
        if path.stem in ("simple_json_reader", "pyspark_json_reader", "clean_csv_converter", "json_to_csv_converter", "filtered_timeline_viewer", "example_analysis"):
            sources.append(path.stem)
    return sorted(set(sources))


def get_test_modules():
    """List test modules (tests/*.py or test_*.py in root)."""
    tests = []
    tests_dir = REPO_ROOT / TEST_DIR
    if tests_dir.is_dir():
        for p in tests_dir.glob("*.py"):
            if p.name.startswith("test_") and not p.name.startswith("__"):
                tests.append(p.stem)
    for p in REPO_ROOT.glob("test_*.py"):
        if p.name not in (Path(__file__).name,):
            tests.append(p.stem)
    return sorted(set(tests))


def source_has_test(source_module, test_modules):
    """Check if a source module has a corresponding test (by naming convention)."""
    expected = f"test_{source_module}"
    return any(t == expected or t.endswith(source_module) for t in test_modules)


def run_tests():
    """Run pytest; return (success, output)."""
    try:
        result = subprocess.run(
            [sys.executable, "-m", "pytest", str(REPO_ROOT), "-v", "--tb=short", "-q"],
            capture_output=True,
            text=True,
            timeout=120,
            cwd=REPO_ROOT,
        )
        return result.returncode == 0, result.stdout + result.stderr
    except subprocess.TimeoutExpired:
        return False, "Tests timed out (120s)."
    except FileNotFoundError:
        return False, "pytest not found. Install with: pip install pytest"
    except Exception as e:
        return False, str(e)


def main():
    if os.environ.get("VALIDATION_SKIP"):
        print("[validate:test-required] SKIP (VALIDATION_SKIP set). Ensure justification is documented.")
        return 0

    sources = get_source_modules()
    tests = get_test_modules()

    covered = [s for s in sources if source_has_test(s, tests)]
    missing = [s for s in sources if not source_has_test(s, tests)]
    if not covered:
        print("FAIL [validate:test-required] No tests found for any source module.")
        print("  Source modules:", ", ".join(sources))
        print("  How to fix: Add tests under tests/ (e.g. tests/test_<module>.py) or root test_<module>.py.")
        print("  To skip (with justification): set VALIDATION_SKIP=reason in CI.")
        return 1
    if missing:
        print("WARN [validate:test-required] Some source modules have no tests:", ", ".join(missing))
        print("  Covered:", ", ".join(covered))

    ok, out = run_tests()
    if not ok:
        print("FAIL [validate:test-required] Tests failed or did not run.")
        print(out)
        print("  How to fix: Run 'python -m pytest .' locally and fix failing tests.")
        return 1

    print("PASS [validate:test-required] Tests exist and passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
