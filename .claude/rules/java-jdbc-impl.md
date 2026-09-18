# JDBC source implementation

Apply when editing `src/main/java`. Arctic Owl source of truth:
`.ai/review/jdbc-java-impl.yaml`. Tests stay under `jdbc-java.yaml` and the
`jdbc-test-reviewer` skill.

## Seams and concurrency

- No new `public static` mutable fields as test/source overrides. Inject
  collaborators via constructor or factory.
- No static `SimpleDateFormat` / `DateFormat` / `Calendar`. Use
  `DateTimeFormatter` or a per-call clone.
- Catch specific exceptions, or translate to `SQLException` with cause.
  Do not swallow `Exception` / `Throwable` without mapping.

## Resources

- Close aggregations must not drop the first failure (close remaining
  resources, then throw the primary error).
- Arrow allocators used for result decoding must be size-bounded.
- When a connection property has aliases, lookup walks the declared
  name list in order.

## Utilities

Prefer `net.snowflake.client.internal.jdbc.SnowflakeUtil.isNullOrEmpty`
over repeating `value == null || value.isEmpty()`. Do not switch to
`isBlank` — that changes whitespace handling.
