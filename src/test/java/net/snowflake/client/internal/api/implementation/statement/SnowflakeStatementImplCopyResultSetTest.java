package net.snowflake.client.internal.api.implementation.statement;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.ResultSet;
import java.sql.SQLException;
import net.snowflake.client.internal.api.implementation.resultset.SnowflakeBaseResultSet;
import net.snowflake.client.internal.api.implementation.connection.SnowflakeConnectionImpl;
import net.snowflake.client.internal.core.SFBaseResultSet;
import net.snowflake.client.internal.core.SFBaseSession;
import net.snowflake.client.internal.core.SFBaseStatement;
import net.snowflake.client.internal.core.SFException;
import net.snowflake.client.internal.core.SFStatementType;
import net.snowflake.client.internal.jdbc.SFConnectionHandler;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SnowflakeStatementImplCopyResultSetTest {

  private SnowflakeConnectionImpl mockConnection;
  private SFConnectionHandler mockHandler;
  private SFBaseStatement mockSFStatement;
  private SFBaseResultSet mockSFResultSet;
  private SFBaseSession mockSession;
  private SnowflakeBaseResultSet mockJdbcResultSet;
  private SnowflakeStatementImpl statement;

  @BeforeEach
  void setUp() throws SQLException, SFException {
    mockConnection = mock(SnowflakeConnectionImpl.class);
    mockHandler = mock(SFConnectionHandler.class);
    mockSFStatement = mock(SFBaseStatement.class);
    mockSFResultSet = mock(SFBaseResultSet.class);
    mockSession = mock(SFBaseSession.class);
    mockJdbcResultSet = mock(SnowflakeBaseResultSet.class);

    when(mockConnection.getHandler(any())).thenReturn(mockHandler);
    when(mockHandler.getSFStatement()).thenReturn(mockSFStatement);
    when(mockConnection.getSFBaseSession(any())).thenReturn(mockSession);
    when(mockConnection.getSessionID()).thenReturn("test-session-id");
    when(mockConnection.isClosed()).thenReturn(false);

    when(mockSFStatement.execute(any(), any(), any(), any())).thenReturn(mockSFResultSet);
    when(mockSFStatement.hasChildren()).thenReturn(false);

    when(mockSFResultSet.getQueryId()).thenReturn("query-id");
    when(mockSFResultSet.isClosed()).thenReturn(false);
    when(mockSFResultSet.next()).thenReturn(false);

    when(mockHandler.createResultSet(any(SFBaseResultSet.class), any())).thenReturn(mockJdbcResultSet);
    when(mockJdbcResultSet.isClosed()).thenReturn(false);

    statement = new SnowflakeStatementImpl(
        mockConnection,
        ResultSet.TYPE_FORWARD_ONLY,
        ResultSet.CONCUR_READ_ONLY,
        ResultSet.CLOSE_CURSORS_AT_COMMIT);
  }

  @Test
  void copyWithFlagDisabled_executeReturnsFalseAndResultSetIsNull() throws SQLException {
    when(mockSFResultSet.getStatementType()).thenReturn(SFStatementType.COPY);
    when(mockSession.isEnableCopyResultSet()).thenReturn(false);
    when(mockSession.isSfSQLMode()).thenReturn(false);

    boolean result = statement.execute("COPY INTO ...");

    assertFalse(result, "execute() should return false for COPY when flag is disabled");
    assertNull(statement.getResultSet(), "getResultSet() should be null when execute() returns false");
  }

  @Test
  void copyWithFlagEnabled_executeReturnsTrueAndResultSetIsNonNull() throws SQLException {
    when(mockSFResultSet.getStatementType()).thenReturn(SFStatementType.COPY);
    when(mockSession.isEnableCopyResultSet()).thenReturn(true);

    boolean result = statement.execute("COPY INTO ...");

    assertTrue(result, "execute() should return true for COPY when flag is enabled");
    assertNotNull(statement.getResultSet(), "getResultSet() should be non-null when execute() returns true");
  }

  @Test
  void insertWithFlagEnabled_executeReturnsFalse() throws SQLException {
    when(mockSFResultSet.getStatementType()).thenReturn(SFStatementType.INSERT);
    when(mockSession.isEnableCopyResultSet()).thenReturn(true);
    when(mockSession.isSfSQLMode()).thenReturn(false);

    boolean result = statement.execute("INSERT INTO t VALUES (1)");

    assertFalse(result, "execute() should return false for INSERT regardless of enableCopyResultSet flag");
  }
}
