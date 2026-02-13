# Test validator scripts

This directory holds the validator implementation and skill reference used by CI and the root `validate_test_required.py`.

## Contents

- **README.md** — This file.
- **VALIDATOR-SKILL.md** — Validator skill reference and standards.
- **test-required.py** — Test-required validator: ensures `tests/` exists, contains test files, and all tests pass.
- **testing_expressions.py** — Shared expressions/helpers for test validation.

## Usage

From repo root:

```bash
python validate_test_required.py
```

Or run the script directly:

```bash
python .test-validator-script/test-required.py
```

## Skip validation

Set `VALIDATION_SKIP=test-required` to bypass the test-required check (e.g. for docs-only PRs).
