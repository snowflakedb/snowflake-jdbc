package net.snowflake.client.internal.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.junit.jupiter.api.Test;

public class HeartbeatIntervalSelectorTest {

  // Helper method for Java 8 compatibility (setOf() is Java 9+)
  private static Set<Long> setOf(Long... values) {
    return new HashSet<>(Arrays.asList(values));
  }

  // === Validation Tests ===

  @Test
  public void testSelectBestInterval_NullExistingIntervals_ThrowsException() {
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> HeartbeatIntervalSelector.selectBestInterval(10, null));
    assertTrue(exception.getMessage().contains("cannot be null"));
  }

  @Test
  public void testSelectBestInterval_EmptyExistingIntervals_ThrowsException() {
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> HeartbeatIntervalSelector.selectBestInterval(10, new HashSet<>()));
    assertTrue(exception.getMessage().contains("cannot be null or empty"));
  }

  @Test
  public void testSelectBestInterval_NegativeRequestedInterval_ThrowsException() {
    Set<Long> existing = setOf(10L);
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> HeartbeatIntervalSelector.selectBestInterval(-5, existing));
    assertTrue(exception.getMessage().contains("must be positive"));
  }

  @Test
  public void testSelectBestInterval_ZeroRequestedInterval_ThrowsException() {
    Set<Long> existing = setOf(10L);
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> HeartbeatIntervalSelector.selectBestInterval(0, existing));
    assertTrue(exception.getMessage().contains("must be positive"));
  }

  // === Core Behavior Tests ===

  @Test
  public void testSelectBestInterval_ExactMatch_ReturnsExactMatch() {
    Set<Long> existing = setOf(5L, 10L, 15L);
    long result = HeartbeatIntervalSelector.selectBestInterval(10, existing);
    assertEquals(10, result, "Should return exact match when it exists");
  }

  @Test
  public void testSelectBestInterval_RequestedBetweenTwo_ReturnsClosestSmaller() {
    Set<Long> existing = setOf(5L, 15L);
    long result = HeartbeatIntervalSelector.selectBestInterval(10, existing);
    assertEquals(
        5, result, "Should return closest smaller interval (more frequent heartbeats are safer)");
  }

  @Test
  public void testSelectBestInterval_RequestedBetweenMany_ReturnsClosestSmaller() {
    Set<Long> existing = setOf(3L, 5L, 8L, 15L, 20L);
    long result = HeartbeatIntervalSelector.selectBestInterval(10, existing);
    assertEquals(8, result, "Should return 8 (closest to 10 but still smaller)");
  }

  @Test
  public void testSelectBestInterval_RequestedSmallerThanAll_ReturnsShortestInterval() {
    // Critical case: If requested is 9s but all existing are [10s, 15s, 20s],
    // there is no interval that is less than or equal to the requested value.
    // In that case, return the shortest available interval (10s) rather than a longer one.
    Set<Long> existing = setOf(10L, 15L, 20L);
    long result = HeartbeatIntervalSelector.selectBestInterval(9, existing);
    assertEquals(10, result, "Should return shortest interval when no smaller exists");
  }

  @Test
  public void testSelectBestInterval_RequestedLargerThanAll_ReturnsLargestSmallerInterval() {
    Set<Long> existing = setOf(5L, 10L, 15L);
    long result = HeartbeatIntervalSelector.selectBestInterval(20, existing);
    assertEquals(
        15, result, "Should return largest of the smaller intervals (closest but still safe)");
  }

  // === Edge Cases ===

  @Test
  public void testSelectBestInterval_SingleExistingIntervalSmaller_ReturnsIt() {
    Set<Long> existing = setOf(5L);
    long result = HeartbeatIntervalSelector.selectBestInterval(10, existing);
    assertEquals(5, result, "Should return the only existing interval");
  }

  @Test
  public void testSelectBestInterval_SingleExistingIntervalLarger_ReturnsIt() {
    Set<Long> existing = setOf(15L);
    long result = HeartbeatIntervalSelector.selectBestInterval(10, existing);
    assertEquals(
        15,
        result,
        "Should return the only existing interval even if it's larger (no other choice)");
  }

  @Test
  public void testSelectBestInterval_SingleExistingIntervalEqual_ReturnsIt() {
    Set<Long> existing = setOf(10L);
    long result = HeartbeatIntervalSelector.selectBestInterval(10, existing);
    assertEquals(10, result, "Should return exact match");
  }

  @Test
  public void testSelectBestInterval_RequestedVeryLarge_ReturnsLargestSmaller() {
    Set<Long> existing = setOf(1L, 5L, 10L);
    long result = HeartbeatIntervalSelector.selectBestInterval(1000, existing);
    assertEquals(10, result, "Should return largest of smaller intervals");
  }

  @Test
  public void testSelectBestInterval_RequestedVerySmall_ReturnsSmallestAvailable() {
    Set<Long> existing = setOf(10L, 20L, 30L);
    long result = HeartbeatIntervalSelector.selectBestInterval(1, existing);
    assertEquals(10, result, "Should return smallest available interval");
  }

  // === Real-World Scenarios ===

  @Test
  public void testSelectBestInterval_RealWorldScenario1_ShortLivedSession() {
    // Short-lived session: 60s validity, wants 10s heartbeat
    // But we have threads at [5s, 30s, 3600s]
    Set<Long> existing = setOf(5L, 30L, 3600L);
    long result = HeartbeatIntervalSelector.selectBestInterval(10, existing);
    assertEquals(5, result, "Should use 5s (more frequent than needed, but safe)");
  }

  @Test
  public void testSelectBestInterval_RealWorldScenario2_LongLivedSession() {
    // Long-lived session: 4h validity, wants 3600s (1h) heartbeat
    // But we have threads at [10s, 60s, 300s]
    Set<Long> existing = setOf(10L, 60L, 300L);
    long result = HeartbeatIntervalSelector.selectBestInterval(3600, existing);
    assertEquals(300, result, "Should use 300s (closest smaller interval)");
  }

  @Test
  public void testSelectBestInterval_RealWorldScenario3_CriticalBugCase() {
    // This was the original bug scenario:
    // Session needs 10s heartbeat (short validity)
    // Existing threads: [12s] (from a longer-lived session)
    // PROBLEM: 12s is LONGER than requested 10s, which could cause expiration
    // BEHAVIOR: Return 12s as only available option (telemetry will alert on this)
    Set<Long> existing = setOf(12L);
    long result = HeartbeatIntervalSelector.selectBestInterval(10, existing);
    assertEquals(
        12,
        result,
        "Should select 12s as only option (session might expire - telemetry will alert)");
  }

  @Test
  public void testSelectBestInterval_RealWorldScenario4_MaxThreadsReached() {
    // 10 threads already exist with various intervals
    // New session needs 25s heartbeat
    Set<Long> existing = setOf(5L, 10L, 15L, 20L, 30L, 60L, 120L, 300L, 600L, 3600L);
    long result = HeartbeatIntervalSelector.selectBestInterval(25, existing);
    assertEquals(20, result, "Should select 20s (closest smaller to 25s)");
  }

  @Test
  public void testSelectBestInterval_RealWorldScenario5_EdgeOfEquality() {
    // Requested = 10, existing = [9, 10, 11]
    Set<Long> existing = setOf(9L, 10L, 11L);
    long result = HeartbeatIntervalSelector.selectBestInterval(10, existing);
    assertEquals(10, result, "Should select exact match (10s)");
  }

  // === Boundary Tests ===

  @Test
  public void testSelectBestInterval_OneSmallerOneEqual_ReturnsEqual() {
    Set<Long> existing = setOf(5L, 10L);
    long result = HeartbeatIntervalSelector.selectBestInterval(10, existing);
    assertEquals(10, result, "Should prefer exact match over smaller");
  }

  @Test
  public void testSelectBestInterval_OneEqualOneLarger_ReturnsEqual() {
    Set<Long> existing = setOf(10L, 15L);
    long result = HeartbeatIntervalSelector.selectBestInterval(10, existing);
    assertEquals(10, result, "Should return exact match");
  }

  @Test
  public void testSelectBestInterval_AllSmaller_ReturnsLargestOfSmaller() {
    Set<Long> existing = setOf(1L, 2L, 3L, 4L, 5L);
    long result = HeartbeatIntervalSelector.selectBestInterval(10, existing);
    assertEquals(5, result, "Should return largest of all smaller intervals");
  }

  @Test
  public void testSelectBestInterval_AllLarger_ReturnsSmallestOfLarger() {
    Set<Long> existing = setOf(20L, 30L, 40L, 50L);
    long result = HeartbeatIntervalSelector.selectBestInterval(10, existing);
    assertEquals(20, result, "Should return smallest of all larger intervals (best fallback)");
  }

  // === Safety Verification Tests ===

  @Test
  public void testSelectBestInterval_NeverReturnsLongerWhenShorterExists() {
    // Verify that we NEVER select a longer interval when shorter exists
    Set<Long> existing = setOf(5L, 20L);
    long result = HeartbeatIntervalSelector.selectBestInterval(10, existing);
    assertEquals(5, result, "Must select 5s (shorter/safer), never 20s (longer/risky)");
  }

  @Test
  public void testSelectBestInterval_AlwaysReturnsValidInterval() {
    // Result should always be one of the existing intervals
    Set<Long> existing = setOf(5L, 10L, 15L);
    long result = HeartbeatIntervalSelector.selectBestInterval(12, existing);
    assertTrue(existing.contains(result), "Result must be from existing intervals");
  }

  @Test
  public void testSelectBestInterval_ConsistentResults() {
    // Same inputs should always produce same output
    Set<Long> existing = setOf(5L, 10L, 15L, 20L);
    long result1 = HeartbeatIntervalSelector.selectBestInterval(12, existing);
    long result2 = HeartbeatIntervalSelector.selectBestInterval(12, existing);
    long result3 = HeartbeatIntervalSelector.selectBestInterval(12, existing);
    assertEquals(result1, result2, "Results should be consistent");
    assertEquals(result2, result3, "Results should be consistent");
  }
}
