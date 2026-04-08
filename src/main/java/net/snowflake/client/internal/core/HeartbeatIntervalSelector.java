package net.snowflake.client.internal.core;

import java.util.Set;

/**
 * Selects the best existing heartbeat interval when a new interval cannot be created due to thread
 * limits.
 *
 * <p>The selector always prefers shorter (more frequent) intervals to prevent session token
 * expiration. It never selects an interval longer than requested unless no shorter alternatives
 * exist.
 */
class HeartbeatIntervalSelector {

  /**
   * Find the best existing interval to use when the requested interval cannot be created.
   *
   * <p>Selection strategy: 1. First choice: Closest interval that is <= requested (more frequent or
   * equal) 2. Fallback: If no smaller intervals exist, use the SHORTEST available interval (most
   * frequent), even if it is still longer than requested
   *
   * <p>This is a best-effort strategy to keep heartbeats as frequent as possible with existing
   * intervals, but it cannot guarantee avoiding less frequent heartbeats or token expiration when
   * only longer intervals are available.
   *
   * @param requestedInterval The interval that was requested but cannot be created
   * @param existingIntervals The set of intervals that currently have threads
   * @return The best existing interval to use
   * @throws IllegalArgumentException if existingIntervals is null, empty, or requestedInterval <= 0
   */
  static long selectBestInterval(long requestedInterval, Set<Long> existingIntervals) {
    if (requestedInterval <= 0) {
      throw new IllegalArgumentException(
          "Requested interval must be positive: " + requestedInterval);
    }
    if (existingIntervals == null || existingIntervals.isEmpty()) {
      throw new IllegalArgumentException("Existing intervals cannot be null or empty");
    }

    Long closestSmallerOrEqual = null;
    Long shortestOverall = null;

    for (Long existingInterval : existingIntervals) {
      // Track the shortest interval overall (fallback)
      if (shortestOverall == null || existingInterval < shortestOverall) {
        shortestOverall = existingInterval;
      }

      // Find the closest interval that is <= requested (more frequent or equal)
      if (existingInterval <= requestedInterval) {
        if (closestSmallerOrEqual == null || existingInterval > closestSmallerOrEqual) {
          closestSmallerOrEqual = existingInterval;
        }
      }
    }

    // Prefer smaller-or-equal interval; fallback to shortest if none exists
    return closestSmallerOrEqual != null ? closestSmallerOrEqual : shortestOverall;
  }
}
