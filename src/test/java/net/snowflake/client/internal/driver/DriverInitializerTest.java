package net.snowflake.client.internal.driver;

import static org.junit.jupiter.api.Assertions.assertTrue;

import net.snowflake.client.category.TestTags;
import net.snowflake.client.internal.core.minicore.Minicore;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(TestTags.CORE)
public class DriverInitializerTest {

  @Test
  public void testInitializeTriggersMinicore() {
    DriverInitializer.resetForTesting();
    Minicore.resetForTesting();

    DriverInitializer.initialize();

    assertTrue(DriverInitializer.isInitialized(), "DriverInitializer should be initialized");
    assertTrue(
        Minicore.hasInitializationStarted(),
        "Minicore async initialization should have been started by DriverInitializer.initialize()");
  }
}
