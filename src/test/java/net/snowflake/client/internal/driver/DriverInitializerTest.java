package net.snowflake.client.internal.driver;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import net.snowflake.client.category.TestTags;
import net.snowflake.client.internal.core.minicore.Minicore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(TestTags.CORE)
public class DriverInitializerTest {

  @AfterEach
  public void tearDown() {
    DriverInitializer.resetForTesting();
    Minicore.resetForTesting();
  }

  @Test
  public void testInitializeDoesNotTriggerMinicore() {
    DriverInitializer.resetForTesting();
    Minicore.resetForTesting();

    DriverInitializer.initialize();

    assertTrue(DriverInitializer.isInitialized(), "DriverInitializer should be initialized");
    assertFalse(
        Minicore.hasInitializationStarted(),
        "Minicore should not be started by DriverInitializer — it loads lazily on first connection");
  }
}
