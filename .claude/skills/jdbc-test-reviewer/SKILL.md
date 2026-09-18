---
name: jdbc-test-reviewer
description: >
  Reviews JDBC/Java test code for quality, flakiness, and correctness.
  Use when user says 'review jdbc tests', 'review this jdbc test',
  'is this jdbc test flaky', 'check this java test for flakiness',
  'jdbc test review', or 'review my jdbc test file'.
---

# JDBC test review

Review tests under `src/test/java` (and related `src/test/resources`).
Load `.ai/review/jdbc-java.yaml` and `.ai/review/jdbc-flaky-tests.yaml`
before commenting. Mock hosts: `.ai/review/jdbc-test-mock-domain.yaml`.

## Workflow

1. If the user names files, use those. Otherwise
   `git diff --name-only origin/master...HEAD -- 'src/test/java/**/*.java'`.
2. Flag issues introduced or worsened by the diff. Map findings to Arctic
   Owl rule ids.

## Checks

- Unique Snowflake object names; no shared default-connection mutation
  without isolation.
- ResultSets closed or fully consumed; try-with-resources.
- WireMock uses a dynamic port and resets between tests.
- No bare `Thread.sleep` for async waits; poll status instead.
- `SQLException` assertions include SQLSTATE and error code.
- Multi-row `SELECT` has `ORDER BY`; `wasNull()` after nullable getters.
- No hardcoded credentials; mock hosts use `snowflake.com`.
- Test method names describe behavior (`should…` when adding JUnit tests).

## Output

Per file, High / Medium / Low with rule ids, then a short checklist of
flakiness risks remaining.
