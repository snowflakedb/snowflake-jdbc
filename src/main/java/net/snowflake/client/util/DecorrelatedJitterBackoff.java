package net.snowflake.client.util;

import java.util.concurrent.ThreadLocalRandom;

/**
 * Decorrelated Jitter backoff
 *
 * <p>https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/
 */
public class DecorrelatedJitterBackoff {
  private final long base;
  private final long cap;

  public DecorrelatedJitterBackoff(long base, long cap) {
    this.base = base;
    this.cap = cap;
  }

  public long nextSleepTime(long sleep) {
    return Math.min(cap, ThreadLocalRandom.current().nextLong(base, sleep * 3));
  }
}
