package net.snowflake.client;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.matchesPattern;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import net.snowflake.client.core.SFException;
import net.snowflake.client.jdbc.SnowflakeUtil;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;
import org.hamcrest.MatcherAssert;

public class TestUtil {
  private static final SFLogger logger = SFLoggerFactory.getLogger(TestUtil.class);

  private static final Pattern QUERY_ID_REGEX =
      Pattern.compile("[a-z0-9]{8}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{12}");

  public static final String GENERATED_SCHEMA_PREFIX = "GENERATED_";
  public static final String ESCAPED_GENERATED_SCHEMA_PREFIX =
      GENERATED_SCHEMA_PREFIX.replaceAll("_", "\\\\_");

  private static final List<String> schemaGeneratedInTestsPrefixes =
      Arrays.asList(
          GENERATED_SCHEMA_PREFIX,
          "GITHUB_", // created by JDBC CI jobs before tests
          "GH_JOB_", // created by other drivers tests e.g. Python
          "JDBCPERF", // created in JDBC perf tests
          "SCHEMA_" // created by other drivers tests e.g. Scala
          );

  public static boolean isSchemaGeneratedInTests(String schema) {
    return schemaGeneratedInTestsPrefixes.stream().anyMatch(prefix -> schema.startsWith(prefix));
  }

  /**
   * Util function to assert a piece will throw exception and assert on the error code
   *
   * @param errorCode expected error code
   * @param testCode the code that will run and throws exception
   */
  public static void assertSFException(int errorCode, TestRunInterface testCode) {
    SFException e = assertThrows(SFException.class, testCode::run);
    assertThat(e.getVendorCode(), is(errorCode));
  }

  /** Functional interface used to run a piece of code which throws SFException */
  @FunctionalInterface
  public interface TestRunInterface {
    void run() throws SFException;
  }

  /**
   * System.getenv wrapper. If System.getenv raises an SecurityException, it is ignored and returns
   * null.
   *
   * @deprecated This method should be replaced by SnowflakeUtil.systemGetEnv.
   *     <p>This is replicated from SnowflakeUtil.systemGetEnv, because the old driver doesn't have
   *     that function for the tests to use it. Replace this function call with
   *     SnowflakeUtil.systemGetEnv when it is available.
   * @param env the environment variable name.
   * @return the environment variable value if set, otherwise null.
   */
  @Deprecated
  public static String systemGetEnv(String env) {
    try {
      return System.getenv(env);
    } catch (SecurityException ex) {
      logger.debug(
          "Failed to get environment variable {}. Security exception raised: {}",
          env,
          ex.getMessage());
    }
    return null;
  }

  public static void assertValidQueryId(String queryId) {
    assertNotNull(queryId);
    MatcherAssert.assertThat(
        "Expecting " + queryId + " is a valid UUID", queryId, matchesPattern(QUERY_ID_REGEX));
  }

  /**
   * Creates schema and deletes it at the end of the passed function execution
   *
   * @param statement statement
   * @param schemaName name of schema to create and delete after lambda execution
   * @param action action to execute when schema was created
   * @throws Exception when any error occurred
   */
  public static void withSchema(Statement statement, String schemaName, ThrowingRunnable action)
      throws Exception {
    try {
      statement.execute("CREATE OR REPLACE SCHEMA " + schemaName);
      action.run();
    } finally {
      statement.execute("DROP SCHEMA " + schemaName);
    }
  }

  /**
   * Creates schema and deletes it at the end of the passed function execution
   *
   * @param statement statement
   * @param action action to execute when schema was created
   * @throws Exception when any error occurred
   */
  public static void withRandomSchema(
      Statement statement, ThrowingConsumer<String, Exception> action) throws Exception {
    String customSchema =
        GENERATED_SCHEMA_PREFIX + SnowflakeUtil.randomAlphaNumeric(5).toUpperCase();
    try {
      statement.execute("CREATE OR REPLACE SCHEMA " + customSchema);
      action.accept(customSchema);
    } finally {
      statement.execute("DROP SCHEMA " + customSchema);
    }
  }

  public interface MethodRaisesSQLException {
    void run() throws SQLException;
  }

  public static void expectSnowflakeLoggedFeatureNotSupportedException(MethodRaisesSQLException f) {
    SQLException ex = assertThrows(SQLException.class, f::run);
    assertEquals(ex.getClass().getSimpleName(), "SnowflakeLoggedFeatureNotSupportedException");
  }

  /**
   * Compares two string values both values are cleaned of whitespaces
   *
   * @param expected expected value
   * @param actual actual value
   */
  public static void assertEqualsIgnoringWhitespace(String expected, String actual) {
    assertEquals(expected.replaceAll("\\s+", ""), actual.replaceAll("\\s+", ""));
  }

  public static String randomTableName(String jiraId) {
    return ("TEST_" + (jiraId != null ? jiraId : "") + "_" + UUID.randomUUID())
        .replaceAll("-", "_");
  }

  public static List<Integer> randomIntList(int length, int modulo) {
    return new Random()
        .ints()
        .limit(length)
        .mapToObj(i -> Math.abs(i) % modulo)
        .collect(Collectors.toList());
  }
}
