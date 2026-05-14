package net.snowflake.client.internal.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

public class SFBaseSessionEventLoopGroupTest {

  /** Factory runs at most once; subsequent calls return the same instance. */
  @Test
  public void shouldCreateGroupLazilyAndReuseIt() {
    SFSession session = new SFSession();
    AtomicInteger calls = new AtomicInteger();
    CountingCloseable first =
        session.getOrCreateS3EventLoopGroup(
            () -> {
              calls.incrementAndGet();
              return new CountingCloseable();
            });
    CountingCloseable second =
        session.getOrCreateS3EventLoopGroup(
            () -> {
              calls.incrementAndGet();
              return new CountingCloseable();
            });

    assertSame(first, second);
    assertEquals(1, calls.get(), "factory should be invoked exactly once, got " + calls.get());
    assertFalse(first.closed);
  }

  /** {@code closeS3EventLoopGroup} closes the held resource and is idempotent. */
  @Test
  public void shouldCloseGroupOnceAndBeIdempotent() {
    TestSession session = new TestSession();
    CountingCloseable holder = session.getOrCreateS3EventLoopGroup(CountingCloseable::new);

    session.invokeCloseS3EventLoopGroup();
    session.invokeCloseS3EventLoopGroup();

    assertTrue(holder.closed);
    assertEquals(1, holder.closeCount, "close should run exactly once, got " + holder.closeCount);
  }

  /**
   * After close, attempts to (re)create the group must throw — otherwise we'd resurrect a group
   * that nothing will ever close.
   */
  @Test
  public void shouldRejectGetOrCreateAfterClose() {
    TestSession session = new TestSession();
    session.invokeCloseS3EventLoopGroup();

    assertThrows(
        IllegalStateException.class,
        () -> session.getOrCreateS3EventLoopGroup(CountingCloseable::new));
  }

  /**
   * A failure in the holder's close() must not propagate out of {@code closeS3EventLoopGroup}, and
   * the session must still be poisoned so no caller can resurrect the group. A second close() must
   * be a no-op.
   */
  @Test
  public void shouldSwallowExceptionFromHolderClose() {
    TestSession session = new TestSession();
    session.getOrCreateS3EventLoopGroup(
        () ->
            () -> {
              throw new RuntimeException("boom");
            });

    session.invokeCloseS3EventLoopGroup();

    assertThrows(
        IllegalStateException.class,
        () -> session.getOrCreateS3EventLoopGroup(CountingCloseable::new));
    session.invokeCloseS3EventLoopGroup();
  }

  /** Test subclass that exposes the protected close hook so we can drive lifecycle directly. */
  private static class TestSession extends SFSession {
    void invokeCloseS3EventLoopGroup() {
      closeS3EventLoopGroup();
    }
  }

  private static class CountingCloseable implements AutoCloseable {
    boolean closed;
    int closeCount;

    @Override
    public void close() {
      closed = true;
      closeCount++;
    }
  }
}
