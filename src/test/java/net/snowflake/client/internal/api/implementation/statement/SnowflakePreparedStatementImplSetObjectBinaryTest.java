package net.snowflake.client.internal.api.implementation.statement;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import net.snowflake.client.internal.api.implementation.connection.SnowflakeConnectionImpl;
import net.snowflake.client.internal.core.ParameterBindingDTO;
import net.snowflake.client.internal.core.SFBaseSession;
import net.snowflake.client.internal.core.SFBaseStatement;
import net.snowflake.client.internal.jdbc.SFConnectionHandler;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Regression tests for SNOW-3982613: setObject(idx, byte[], Types.BINARY/VARBINARY) must hex-encode
 * the byte[] (as setBytes does), not bind String.valueOf(byte[]) ("[B@..").
 */
class SnowflakePreparedStatementImplSetObjectBinaryTest {

  private static final byte[] BYTES = {
    (byte) 0xDE, (byte) 0xAD, (byte) 0xBE, (byte) 0xEF, 0x00, 0x01, 0x02, 0x03
  };
  private static final String EXPECTED_HEX = "DEADBEEF00010203";

  private SnowflakeConnectionImpl mockConnection;

  @BeforeEach
  void setUp() throws SQLException {
    mockConnection = mock(SnowflakeConnectionImpl.class);
    SFConnectionHandler mockHandler = mock(SFConnectionHandler.class);
    SFBaseStatement mockSFStatement = mock(SFBaseStatement.class);
    SFBaseSession mockSession = mock(SFBaseSession.class);

    when(mockConnection.getHandler(any())).thenReturn(mockHandler);
    when(mockHandler.getSFStatement()).thenReturn(mockSFStatement);
    when(mockConnection.getSFBaseSession(any())).thenReturn(mockSession);
    when(mockConnection.getSessionID()).thenReturn("test-session-id");
    when(mockConnection.isClosed()).thenReturn(false);
    when(mockConnection.getShowStatementParameters()).thenReturn(false);
  }

  private SnowflakePreparedStatementImpl newStatement() throws SQLException {
    return new SnowflakePreparedStatementImpl(
        mockConnection,
        "insert into t values (?)",
        false,
        ResultSet.TYPE_FORWARD_ONLY,
        ResultSet.CONCUR_READ_ONLY,
        ResultSet.CLOSE_CURSORS_AT_COMMIT);
  }

  private static void assertHexBinding(ParameterBindingDTO binding) {
    String value = String.valueOf(binding.getValue());
    assertEquals("BINARY", binding.getType());
    assertEquals(EXPECTED_HEX, value.toUpperCase());
    assertFalse(value.contains("[B@"), "byte[] must not be bound via String.valueOf");
  }

  @ParameterizedTest
  @ValueSource(ints = {Types.BINARY, Types.VARBINARY})
  void setObjectByteArrayWithTypeBindsHex(int sqlType) throws SQLException {
    SnowflakePreparedStatementImpl stmt = newStatement();
    stmt.setObject(1, BYTES, sqlType);
    assertHexBinding(stmt.getParameterBindings().get("1"));
  }

  @Test
  void setObjectByteArrayWithScaleDelegatesAndBindsHex() throws SQLException {
    SnowflakePreparedStatementImpl stmt = newStatement();
    stmt.setObject(1, BYTES, Types.BINARY, 0);
    assertHexBinding(stmt.getParameterBindings().get("1"));
  }
}
