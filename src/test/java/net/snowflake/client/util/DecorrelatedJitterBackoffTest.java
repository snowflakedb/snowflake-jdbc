package net.snowflake.client.util;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class DecorrelatedJitterBackoffTest {

  static Stream<Arguments> testParameters() {
    long base = 1000L; // 1 second base
    long cap = 20000L; // 20 second cap

    return Stream.of(
        // attemptNumber, base, cap, currentSleepTime, expectedMin (always base), expectedMax
        // (min(cap, sleep*3))
        // Retry 1: Starting with 1 second
        Arguments.of(1, base, cap, 1000L, base, 3000L),
        // Retry 2: Sleep time has increased to ~2 seconds
        Arguments.of(2, base, cap, 2000L, base, 6000L),
        // Retry 3: Sleep time increased to ~3 seconds
        Arguments.of(3, base, cap, 3000L, base, 9000L),
        // Retry 4: Sleep time increased to ~4 seconds
        Arguments.of(4, base, cap, 4000L, base, 12000L),
        // Retry 5: Sleep time increased to ~5 seconds
        Arguments.of(5, base, cap, 5000L, base, 15000L),
        // Retry 6: Sleep time increased to ~6 seconds
        Arguments.of(6, base, cap, 6000L, base, 18000L),
        // Retry 7: Sleep time at 7 seconds - approaching cap
        Arguments.of(7, base, cap, 7000L, base, cap), // 7000 * 3 = 21000 > cap, so capped at 20000
        // Retry 8: Sleep time at 8 seconds - beyond cap
        Arguments.of(8, base, cap, 8000L, base, cap), // 8000 * 3 = 24000 > cap, so capped at 20000
        // Retry 9: Sleep time at 10 seconds - well beyond cap
        Arguments.of(
            9, base, cap, 10000L, base, cap), // 10000 * 3 = 30000 > cap, so capped at 20000
        // Retry 10: Sleep time at 15 seconds
        Arguments.of(
            10, base, cap, 15000L, base, cap)); // 15000 * 3 = 45000 > cap, so capped at 20000
  }

  @ParameterizedTest(name = "Retry {0}: currentSleep={3}ms, expecting [{4}ms, {5}ms]")
  @MethodSource("testParameters")
  public void testNextSleepTime(
      int attemptNumber,
      long base,
      long cap,
      long currentSleepTime,
      long expectedMin,
      long expectedMax) {
    DecorrelatedJitterBackoff backoff = new DecorrelatedJitterBackoff(base, cap);

    // Run the test multiple times since nextSleepTime uses random values
    for (int i = 0; i < 100; i++) {
      long result = backoff.nextSleepTime(currentSleepTime);

      assertTrue(
          result >= expectedMin,
          String.format(
              "Retry %d, iteration %d: Result %dms should be >= %dms (base) for currentSleepTime=%dms",
              attemptNumber, i, result, expectedMin, currentSleepTime));

      assertTrue(
          result <= expectedMax,
          String.format(
              "Retry %d, iteration %d: Result %dms should be <= %dms (min(cap=%d, sleep*3=%d)) for currentSleepTime=%dms",
              attemptNumber, i, result, expectedMax, cap, currentSleepTime * 3, currentSleepTime));
    }
  }
}
