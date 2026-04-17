package net.snowflake.client.internal.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.SQLException;
import net.snowflake.client.api.exception.SnowflakeSQLException;
import net.snowflake.client.internal.jdbc.SnowflakeReauthenticationRequest;
import net.snowflake.client.internal.jdbc.telemetry.InternalApiTelemetryTracker.InternalCallMarker;
import net.snowflake.common.core.SqlState;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SFStatementTest {

  private SFSession mockSession;
  private SFStatement statement;

  @BeforeEach
  void setUp() {
    mockSession = mock(SFSession.class);
    when(mockSession.getQueryTimeout()).thenReturn(0);
    statement = new SFStatement(mockSession);
  }

  @Test
  void testRenewSessionOnExpiry_rethrowsNonSessionExpiryError() {
    SnowflakeSQLException otherException =
        new SnowflakeSQLException("unknown", "Some other error", SqlState.INTERNAL_ERROR, 12345);

    SnowflakeSQLException thrown =
        assertThrows(
            SnowflakeSQLException.class,
            () -> statement.renewSessionOnExpiry(otherException, "token"));
    assertSame(otherException, thrown);
  }

  @Test
  void testRenewSessionOnExpiry_renewsSessionOnSessionExpired() throws SFException, SQLException {
    SnowflakeSQLException sessionExpiredException =
        new SnowflakeSQLException(
            "unknown",
            "Your session has expired",
            SqlState.INTERNAL_ERROR,
            Constants.SESSION_EXPIRED_GS_CODE);

    doNothing().when(mockSession).renewSession("old-token");

    // Should not throw — session renewal succeeds
    statement.renewSessionOnExpiry(sessionExpiredException, "old-token");

    verify(mockSession).renewSession("old-token");
  }

  @Test
  void testRenewSessionOnExpiry_handlesReauthForExternalBrowser() throws SFException, SQLException {
    SnowflakeSQLException sessionExpiredException =
        new SnowflakeSQLException(
            "unknown",
            "Your session has expired",
            SqlState.INTERNAL_ERROR,
            Constants.SESSION_EXPIRED_GS_CODE);

    doThrow(
            new SnowflakeReauthenticationRequest(
                "qid", "reauth needed", SqlState.INTERNAL_ERROR, 390110))
        .when(mockSession)
        .renewSession("old-token");
    when(mockSession.isExternalbrowserOrOAuthFullFlowAuthenticator()).thenReturn(true);

    // Should call session.open() and not throw
    statement.renewSessionOnExpiry(sessionExpiredException, "old-token");

    verify(mockSession).open(any(InternalCallMarker.class));
  }

  @Test
  void testRenewSessionOnExpiry_throwsReauthWhenNotExternalBrowser()
      throws SFException, SQLException {
    SnowflakeSQLException sessionExpiredException =
        new SnowflakeSQLException(
            "unknown",
            "Your session has expired",
            SqlState.INTERNAL_ERROR,
            Constants.SESSION_EXPIRED_GS_CODE);

    SnowflakeReauthenticationRequest reauthEx =
        new SnowflakeReauthenticationRequest(
            "qid", "reauth needed", SqlState.INTERNAL_ERROR, 390110);
    doThrow(reauthEx).when(mockSession).renewSession("old-token");
    when(mockSession.isExternalbrowserOrOAuthFullFlowAuthenticator()).thenReturn(false);

    // Should re-throw the SnowflakeReauthenticationRequest
    SnowflakeSQLException thrown =
        assertThrows(
            SnowflakeReauthenticationRequest.class,
            () -> statement.renewSessionOnExpiry(sessionExpiredException, "old-token"));
    assertSame(reauthEx, thrown);
  }

  @Test
  void testRenewSessionOnExpiry_preservesErrorCodeOnRethrow() throws SFException {
    int originalErrorCode = 99999;
    SnowflakeSQLException otherException =
        new SnowflakeSQLException(
            "qid-123", "Something went wrong", SqlState.INTERNAL_ERROR, originalErrorCode);

    SnowflakeSQLException thrown =
        assertThrows(
            SnowflakeSQLException.class,
            () -> statement.renewSessionOnExpiry(otherException, "token"));
    assertEquals(originalErrorCode, thrown.getErrorCode());
    assertEquals("qid-123", thrown.getQueryId());
  }
}
