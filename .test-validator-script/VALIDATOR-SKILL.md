# Validator Test Helper Skill Reference

## Purpose

Create and enforce **validators** (required checks) for test expectations, coverage gates, and security/performance guardrails.

## When to use

- Adding or tightening PR gates (e.g. tests required, coverage)
- Wiring validators into CI (GitHub Actions, GitLab, Jenkins)
- Reviewing why a PR should pass or fail on required validations

## Validator standards

- **Deterministic** – no flaky network; stable inputs
- **Fast** – PR checks finish quickly
- **Actionable** – failure output says what failed, why, and how to fix
- **CI-friendly** – exit codes, clear logs

## Naming and outputs

- Script: `validate_<thing>.py` (or `.sh`/`.js`)
- CI job: `validate:<thing>`
- On failure: print **what failed**, **why**, and **how to fix**

## Recommended validators

1. **Tests-required (core)** – PR fails if there are no tests or tests don’t pass.
2. **Coverage gate** – Block merges that reduce coverage or fall below threshold (e.g. 80%).
3. **Security** – SAST, dependency scan, secrets detection (when requested).
4. **Performance** – For critical paths or nightly; fail on regression threshold.

## CI checklist

- Validators run on **PR** (blocking) and on **main** (full).
- Results visible as **required checks**.
- Run fast validators first; cache deps/build to keep runtimes low.
