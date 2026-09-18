# JDBC Java practices

This is the Java-side contract for `src/main/java` in this repository — the
previous major version of the Snowflake JDBC driver. Review procedure is the
`jdbc-impl-reviewer` skill; enforceable rules are `.ai/review/jdbc-java-impl.yaml`.

Do not apply conventions that belong only to the next major version of JDBC
(the Universal Core wrapper): `@JdbcBoundary` decorators, `SFSQLException`
carriers, or the unicore protobuf JNI bridge.

## Layout

- Public JDBC API and driver entry: `src/main/java/net/snowflake/client/...`
- Internals: `src/main/java/net/snowflake/client/internal/...`
- Shared helpers: `net.snowflake.client.internal.jdbc.SnowflakeUtil` (including
  `isNullOrEmpty`) and `net.snowflake.client.internal.util`

## Conventions that do apply

- No public mutable static test/source seams; inject collaborators.
- No static `SimpleDateFormat` / `DateFormat` / `Calendar`.
- Catch specific exceptions or translate to `SQLException` with cause.
- Resource close must aggregate failures; Arrow allocators must be bounded.
- Property aliases are looked up in declared order.
