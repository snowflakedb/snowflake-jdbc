---
name: jdbc-impl-reviewer
description: >
  Reviews JDBC source (src/main/java) for Effective Java / Clean Code
  issues: concurrency, resources, exception translation, and god-class
  growth. Use when the user says 'review jdbc impl', 'review JDBC source',
  'review this Java implementation', 'jdbc impl review',
  'review src/main/java', or asks for a Java good-practices review of the
  Snowflake JDBC driver (not tests).
---

# JDBC implementation review

Review JDBC **source** under `src/main/java` only. Tests belong to
`jdbc-test-reviewer`. Load `.ai/review/jdbc-java-impl.yaml`,
`.ai/review/jdbc-logging.yaml`, and `.ai/context/jdbc-java-practices.md`
before commenting.

This repository is the previous major version of the Snowflake JDBC driver.
Do not apply conventions that belong only to the next major version on the
Universal Core (`@JdbcBoundary`, `SFSQLException` carriers, `Decorators.*`,
or unicore JNI).

## Workflow

1. If the user names files, use those. Otherwise
   `git diff --name-only origin/master...HEAD -- 'src/main/java/**/*.java'`.
2. Review each file in full. Named classes are samples of a pattern; they
   do not limit the review. Map a finding to an Arctic Owl rule id when
   one exists.

## Hotspot examples

- **Seams** — no new public mutable statics (`jdbc-java-no-public-mutable-static-seam`).
- **Concurrency** — no static `SimpleDateFormat` / `DateFormat` / `Calendar`
  (`jdbc-java-no-thread-unsafe-jdk-dateformat`).
- **Exceptions** — specific catches or translate to `SQLException` with cause
  (`jdbc-java-catch-specific-or-translate`).
- **Lookup** — multi-alias property reads walk the declared name list
  (`jdbc-java-property-alias-declared-order`).
- **Resources** — bounded Arrow allocators; close aggregates remaining
  resources (`jdbc-java-arrow-allocator-must-be-bounded`,
  `jdbc-java-resource-close-aggregates`).
- **Logging** — never log secrets, tokens, or query parameters
  (`jdbc-log-never-log-secrets`).

## Output

Per file, group High / Medium / Low with rule ids:

```
## src/main/java/.../Foo.java

### High
- [jdbc-java-no-thread-unsafe-jdk-dateformat] static SimpleDateFormat at line N

### Medium
- [jdbc-java-catch-specific-or-translate] catch (Exception) at line N
```
