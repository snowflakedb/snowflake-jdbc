package net.snowflake.client.suites;

import java.util.Arrays;
import net.snowflake.client.providers.SimpleFormatProvider;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.platform.suite.api.AfterSuite;
import org.junit.platform.suite.api.BeforeSuite;

@BaseTestSuite
public abstract class OldDriverTestSuite {
  @BeforeSuite
  public static void beforeAll() {
    SimpleFormatProvider.setSupportedFormats(Arrays.asList(Arguments.of("JSON")));
  }

  @AfterSuite
  public static void afterAll() {
    SimpleFormatProvider.resetSupportedFormats();
  }
}
