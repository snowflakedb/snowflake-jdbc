package net.snowflake.client.api.driver;

import static net.snowflake.client.api.driver.SnowflakeDriver.implementVersion;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.junit.jupiter.api.Test;

/**
 * Test class for package-private methods in SnowflakeDriver. This class must be in the same package
 * as SnowflakeDriver to access package-private methods.
 */
public class SnowflakeDriverTest {

  @Test
  public void testSuppressIllegalReflectiveAccessWarning() {
    // Just to make sure this function won't break anything
    SnowflakeDriver.disableIllegalReflectiveAccessWarning();
  }

  @Test
  public void testStaticVersionMatchesManifest() {
    String manifestVersion =
        SnowflakeDriver.versionResourceBundleManager.getLocalizedMessage("version");
    assertNotNull(manifestVersion, "Manifest version should not be null");

    // Remove -SNAPSHOT suffix if present for comparison
    String normalizedManifestVersion = manifestVersion.replace("-SNAPSHOT", "");
    assertEquals(
        implementVersion,
        normalizedManifestVersion,
        "Static version should match manifest version");
  }
}
