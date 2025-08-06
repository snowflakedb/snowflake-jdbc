package net.snowflake.client.core.auth.oauth;

import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class RandomStateProviderTest {

  private RandomStateProvider randomStateProvider;

  @BeforeEach
  public void setUp() {
    // Initialize the RandomStateProvider before each test
    randomStateProvider = new RandomStateProvider();
  }

  @Test
  public void testGetState_ShouldReturnNonNullState() {
    // Call getState() and verify that it returns a non-null value
    String state = randomStateProvider.getState();
    assertNotNull(state, "State should not be null");
  }

  @Test
  public void testGetState_ShouldReturnUniqueValues() {
    // Ensure that the state is random and different each time
    String state1 = randomStateProvider.getState();
    String state2 = randomStateProvider.getState();

    // Assert that the two generated states are different
    assertNotEquals(state1, state2, "State should be different on subsequent calls");
  }
}
