package net.snowflake.client.api.driver;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.junit.jupiter.api.Test;

/** Test class for SnowflakeDriver methods. */
public class SnowflakeDriverTest {

  @Test
  public void testStaticVersionMatchesManifest() {
    String manifestVersion =
        SnowflakeDriver.versionResourceBundleManager.getLocalizedMessage("version");
    assertNotNull(manifestVersion, "Manifest version should not be null");

    // Remove -SNAPSHOT suffix if present for comparison
    String normalizedManifestVersion = manifestVersion.replace("-SNAPSHOT", "");
    assertEquals(
        SnowflakeDriver.getImplementationVersion(),
        normalizedManifestVersion,
        "Static version should match manifest version");
  }
}
