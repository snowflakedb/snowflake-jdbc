package net.snowflake.client.internal.util;

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
    // A caller may feed back a timeout-trimmed value below base; clamp so the bound stays > origin.
    long boundedSleep = Math.max(sleep, base);
    return Math.min(cap, ThreadLocalRandom.current().nextLong(base, boundedSleep * 3));
  }

  public long getJitterForLogin(long currentTime) {
    double multiplicationFactor = chooseRandom(-1, 1);
    long jitter = (long) (multiplicationFactor * currentTime * 0.5);
    return jitter;
  }

  public double chooseRandom(double min, double max) {
    return min + (Math.random() * (max - min));
  }
}
