# Logs

Utilities and scripts for log conversion and timeline analysis (JSON/CSV).

## Validators

Required checks run on PR and push to `main`/`master`:

| Validator | Command | Purpose |
|-----------|--------|---------|
| **test-required** | `python validate_test_required.py` | Ensures `tests/` exists, has test files, and all tests pass. |

### Running validators locally

```bash
# Install dependencies
pip install -r requirements.txt

# Run test-required validator (runs pytest)
python validate_test_required.py
```

### If a validator fails

- **validate_test_required**: Create or fix tests under `tests/`. Run `pytest tests/ -v` to reproduce. PR is blocked until tests pass.

## Tests

```bash
pytest tests/ -v
```
