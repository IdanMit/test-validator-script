# Validator Skill Reference

## Validator standards

- **Deterministic**: No flaky network calls; stable inputs.
- **Fast**: PR checks finish quickly.
- **Actionable**: Fail output states what failed, why, and how to fix.
- **CI-friendly**: Exit codes (0 = pass, non-zero = fail), clear logs.

## Naming and outputs

- Script: `validate_<thing>.py` or `test-required.py` in this directory.
- CI job: `validate:<thing>` (e.g. `validate:test-required`).
- On failure: print **what failed**, **why**, and **how to fix**.

## Recommended validators

### A) Tests-required (core)

- **Goal**: PR fails if there are no tests or if the test suite fails.
- **Implementation**: Ensure `tests/` exists, contains `test_*.py` or `*_test.py`, and `pytest tests/` passes.
- **Skip**: Use `VALIDATION_SKIP` with justification only when necessary (e.g. docs-only PRs).

### B) Coverage gate (optional)

- Run only if coverage tooling exists (e.g. pytest-cov).
- Default threshold: 80% for new/changed code.

### C) Security / performance

- Add when requested (SAST, dependency scan, secrets, performance regression).

## CI checklist

- Validators run on **PR** (blocking) and on **main**/master (full).
- Results visible as **required checks**.
- Fast validators first; cache deps to keep runtimes low.
