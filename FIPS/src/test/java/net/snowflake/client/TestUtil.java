package net.snowflake.client;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import net.snowflake.client.core.SFException;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;
import org.junit.jupiter.api.Assertions;

public class TestUtil {
  private static final SFLogger logger = SFLoggerFactory.getLogger(TestUtil.class);
  /**
   * Util function to assert a piece will throw exception and assert on the error code
   *
   * @param errorCode expected error code
   * @param testCode the code that will run and throws exception
   */
  public static void assertSFException(int errorCode, TestRunInterface testCode) {
    try {
      testCode.run();
      Assertions.fail();
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
   * @deprecated This method should be replaced by SnowflakeUtil.systemGetEnv.
   * <p>This is replicated from SnowflakeUtil.systemGetEnv, because the old driver doesn't have that
   * function for the tests to use it. Replace this function call with SnowflakeUtil.systemGetEnv
   * when it is available.
   *
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
}
