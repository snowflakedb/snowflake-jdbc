---
name: java-clean-code
description: Refactors and reviews Java code for cohesive methods, consistent abstraction levels, and behavior-preserving helper extraction. Use when implementing or reviewing Java/JDBC code, simplifying a long method, extracting helpers, or when the user asks for Java clean code or a Java refactor.
---

# Java clean code

Improve readability without changing observable behavior. Treat method length as a signal, never as the rule.

Load `.ai/review/jdbc-java.yaml` before proposing extractions.

## Workflow

1. Read the changed method, its callers, tests, and nearby helpers.
2. State the behavior that must remain stable: return values, exception types and order, side effects, input mutation, null handling, and public visibility.
3. Identify cohesive phases. Common phases are input guards, resolution/parsing, validation, domain decisions, result construction, and side effects.
4. Keep the entry method at one abstraction level. Give each extracted helper one purpose and a name that describes the domain action.
5. Choose the narrowest boundary:
   - use a `private static` helper for stateless logic local to one class when it is not worth independent tests;
   - extract a cohesive package-private owner when helpers own state/dependencies, are reused, or merit focused unit tests;
   - do not widen visibility solely to test an implementation detail.
6. Preserve execution order and data flow. Do not combine cleanup, renaming, or semantic changes with extraction unless tests explicitly cover them.
7. Run focused tests and formatting. Review the diff for accidental API or behavior changes.

## Extract testable helpers to a package owner

Do not leave testable domain helpers as `private static` methods on a public JDBC class. Extract them to a cohesive owner so they can be unit-tested at package visibility.

Prefer `net.snowflake.client.internal.util` or `net.snowflake.client.internal.jdbc` next to existing helpers. Name the owner after the domain (`DriverPropertyInfoUtil`), not a generic `Utils` dump. Keep helpers package-private when same-package tests are enough.

## Utilities

Reuse `net.snowflake.client.internal.jdbc.SnowflakeUtil.isNullOrEmpty(value)` instead of repeating `value == null || value.isEmpty()`. Do not replace that with `isBlank`.

Prefer `putAll`, `addAll`, or a copy constructor when a loop only copies every entry unchanged.

## Do not over-refactor

Do not extract a trivial expression, every branch to satisfy a line-count threshold, or untouched existing code solely because it could be cleaner.

## Behavior-preservation checklist

- [ ] Public signatures and visibility are unchanged unless requested.
- [ ] Validation and exception precedence are unchanged.
- [ ] Null, empty, and malformed inputs follow the same paths.
- [ ] Mutable inputs are copied or mutated exactly as before.
- [ ] Side effects and resource cleanup occur in the same order.
- [ ] Existing tests pass; focused tests cover any newly exposed seam.

## Review output

Report only actionable findings introduced or materially worsened by the diff. Explain the mixed responsibilities, propose cohesive helper boundaries, and note the behavior that must be preserved. Do not report method length alone.
