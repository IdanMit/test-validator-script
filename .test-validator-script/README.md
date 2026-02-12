# Test Validator Scripts

This directory holds validator scripts and utilities used by the project's CI and local checks.

## Contents

| File | Purpose |
|------|---------|
| `test-required.py` | Main test-required validator (ensures `tests/` exists, has test files, and all tests pass). |
| `testing_expressions.py` | Utilities for test discovery and validation expressions. |
| `VALIDATOR-SKILL.md` | Skill reference for validator behavior and CI integration. |

## Running validators

From the **project root** (not this directory):

```bash
# Run the test-required validator (recommended)
python validate_test_required.py

# Or run the script in this directory (same behavior)
python .test-validator-script/test-required.py
```

## CI

Validators run on every PR and push to `main`/`master` via `.github/workflows/validate.yml`. The job name is `validate:test-required`.

## Fail messaging

- **tests/ missing**: Create `tests/` and add at least one `test_*.py` or `*_test.py`.
- **No test files**: Add test modules under `tests/`.
- **Tests failed**: Run `pytest tests/ -v` locally and fix failing tests.
