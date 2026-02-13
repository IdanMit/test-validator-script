# Validator skill reference

## Standards

- **Deterministic** — No flaky network calls.
- **Fast** — PR checks finish quickly.
- **Actionable** — Fail output tells the developer what to fix.
- **CI-friendly** — Exit codes and clear logs.

## Naming and outputs

- Script: `validate_<thing>.py` or `test-required.py` in this directory.
- CI job: `validate:<thing>` (e.g. `validate:test-required`).
- On failure: Print what failed, why, and how to fix.

## Validators

### Test-required (core)

PR fails if code changes do not include or trigger tests. Exceptions via `VALIDATION_SKIP=test-required`.

### Coverage gate (optional)

Block merges below a coverage threshold when coverage tooling is present.

### Security / performance (optional)

SAST, dependency scan, secrets; performance regression checks on critical paths.
