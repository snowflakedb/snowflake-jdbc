package net.snowflake.client.internal.driver;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Properties;
import net.snowflake.client.category.TestTags;
import net.snowflake.client.internal.core.minicore.Minicore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(TestTags.CORE)
public class ConnectionFactoryTest {

  @BeforeEach
  public void setUp() {
    Minicore.resetForTesting();
  }

  @AfterEach
  public void tearDown() {
    Minicore.resetForTesting();
  }

  @Test
  public void testCreateConnectionTriggersMinicoreForSnowflakeUrl() {
    assertFalse(
        Minicore.hasInitializationStarted(), "Minicore should not be started before connection");

    try {
      ConnectionFactory.createConnection(
          "jdbc:snowflake://fake.account.snowflakecomputing.com", new Properties());
    } catch (Exception e) {
      // Expected — the fake host will fail during connection setup.
      // We only care that minicore was triggered before the failure.
    }

    assertTrue(
        Minicore.hasInitializationStarted(),
        "Minicore should be started when ConnectionFactory processes a Snowflake URL");
  }

  @Test
  public void testCreateConnectionDoesNotTriggerMinicoreForNonSnowflakeUrl() throws Exception {
    assertFalse(
        Minicore.hasInitializationStarted(), "Minicore should not be started before connection");

    assertNull(
        ConnectionFactory.createConnection("jdbc:mysql://localhost:3306/db", new Properties()),
        "Non-Snowflake URL should return null per JDBC spec");

    assertFalse(
        Minicore.hasInitializationStarted(),
        "Minicore should not be started for non-Snowflake URLs");
  }
}
