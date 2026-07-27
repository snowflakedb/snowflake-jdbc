package net.snowflake.client.internal.jdbc;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.SQLException;
import net.snowflake.client.internal.api.implementation.connection.SnowflakeConnectionImpl;
import net.snowflake.client.internal.core.SFBaseSession;
import net.snowflake.client.internal.core.SFException;
import net.snowflake.client.internal.jdbc.MockConnectionTest.MockSnowflakeConnectionImpl;
import net.snowflake.client.internal.jdbc.MockConnectionTest.MockSnowflakeSFSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

public class SnowflakeConnectionImplIsValidTest {

  @AfterEach
  public void clearInterruptFlag() {
    Thread.interrupted();
  }

  @Test
  public void isValidPreservesInterruptFlagWhenHeartbeatIsInterrupted() throws SQLException {
    try (SnowflakeConnectionImpl connection =
        new SnowflakeConnectionImpl(new InterruptingHandler())) {
      assertFalse(Thread.currentThread().isInterrupted());

      boolean valid = connection.isValid(0);

      assertFalse(valid, "isValid should return false when the heartbeat is interrupted");
      assertTrue(
          Thread.currentThread().isInterrupted(),
          "isValid must restore the thread's interrupt flag after catching InterruptedException");
    }
  }

  @Test
  public void isValidDoesNotClearPreExistingInterruptOnSuccess() throws SQLException {
    try (SnowflakeConnectionImpl connection =
        new SnowflakeConnectionImpl(new MockSnowflakeConnectionImpl())) {
      Thread.currentThread().interrupt();
      boolean valid = connection.isValid(0);

      assertTrue(valid, "isValid should return true when the heartbeat succeeds");
      assertTrue(
          Thread.currentThread().isInterrupted(),
          "isValid must not clear a pre-existing interrupt flag when the heartbeat succeeds");
    }
  }

  private static class InterruptingHandler extends MockSnowflakeConnectionImpl {
    private final InterruptingSession session = new InterruptingSession(this);

    @Override
    public SFBaseSession getSFSession() {
      return session;
    }
  }

  private static class InterruptingSession extends MockSnowflakeSFSession {
    InterruptingSession(SFConnectionHandler handler) {
      super(handler);
    }

    @Override
    public void callHeartBeat(int timeout) throws SFException, SQLException {
      sneakyThrow(new InterruptedException("simulated heartbeat interruption"));
    }

    @SuppressWarnings("unchecked")
    private static <E extends Throwable> void sneakyThrow(Throwable t) throws E {
      throw (E) t;
    }
  }
}
