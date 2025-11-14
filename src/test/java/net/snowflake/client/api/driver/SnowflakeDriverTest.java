package net.snowflake.client.api.driver;

import static net.snowflake.client.api.driver.SnowflakeDriver.getClientVersionStringFromManifest;
import static net.snowflake.client.api.driver.SnowflakeDriver.implementVersion;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

public class SnowflakeDriverTest {

  @Test
  public void testSuppressIllegalReflectiveAccessWarning() {
    // Just to make sure this function won't break anything
    SnowflakeDriver.disableIllegalReflectiveAccessWarning();
  }

  @Test
  public void testStaticVersionMatchesManifest() {
    assertEquals(implementVersion, getClientVersionStringFromManifest().replace("-SNAPSHOT", ""));
  }
}
