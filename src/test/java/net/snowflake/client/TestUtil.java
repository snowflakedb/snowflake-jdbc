/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.client;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.sql.Statement;
import java.util.regex.Pattern;
import net.snowflake.client.core.SFException;
import net.snowflake.client.jdbc.SnowflakeUtil;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;
import org.junit.Assert;

public class TestUtil {
  private static final SFLogger logger = SFLoggerFactory.getLogger(TestUtil.class);

  private static final Pattern QUERY_ID_REGEX =
      Pattern.compile("[a-z0-9]{8}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{12}");

  /**
   * Util function to assert a piece will throw exception and assert on the error code
   *
   * @param errorCode expected error code
   * @param testCode the code that will run and throws exception
   */
  public static void assertSFException(int errorCode, TestRunInterface testCode) {
    try {
      testCode.run();
      Assert.fail();
    } catch (SFException e) {
      assertThat(e.getVendorCode(), is(errorCode));
    }
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
   * <p>This is replicated from SnowflakeUtil.systemGetEnv, because the old driver doesn't have that
   * function for the tests to use it. Replace this function call with SnowflakeUtil.systemGetEnv
   * when it is available.
   *
   * @param env the environment variable name.
   * @return the environment variable value if set, otherwise null.
   */
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
    assertTrue(
        "Expecting " + queryId + " is a valid UUID", QUERY_ID_REGEX.matcher(queryId).matches());
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
  public static void withRandomSchema(Statement statement, ThrowingConsumer<String> action)
      throws Exception {
    String customSchema = "TEST_SCHEMA_" + SnowflakeUtil.randomAlphaNumeric(5);
    try {
      statement.execute("CREATE OR REPLACE SCHEMA " + customSchema);
      action.call(customSchema);
    } finally {
      statement.execute("DROP SCHEMA " + customSchema);
    }
  }
}
