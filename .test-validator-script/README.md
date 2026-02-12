# Test Validator Scripts

This directory holds validator script assets and skill reference documentation for the Logs project.

## Validators

| Validator        | Script (project root)     | Purpose                                      |
|------------------|---------------------------|----------------------------------------------|
| test-required    | `validate_test_required.py` | Ensures `tests/` exists, has test files, and all tests pass. |

## Running from this directory

The main validator is at project root. From the repo root:

```bash
python validate_test_required.py
```

Or use the wrapper in this directory (runs the root script):

```bash
python .test-validator-script/test-required.py
```

## Files

- **README.md** (this file) – Documentation for validator scripts.
- **VALIDATOR-SKILL.md** – Skill reference for the Validator Test Helper.
- **test-required.py** – Wrapper that invokes the root test-required validator.
- **testing_expressions.py** – Utilities for test path/expression handling.

## CI

Validators run in GitHub Actions (`.github/workflows/validate.yml`) on pull requests and pushes to `main`/`master`. Required checks must pass before merge.
