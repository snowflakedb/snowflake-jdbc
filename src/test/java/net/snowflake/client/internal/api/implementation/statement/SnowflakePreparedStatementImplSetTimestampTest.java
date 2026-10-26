package net.snowflake.client.internal.api.implementation.statement;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.util.Calendar;
import java.util.Map;
import java.util.TimeZone;
import net.snowflake.client.api.resultset.SnowflakeType;
import net.snowflake.client.internal.api.implementation.connection.SnowflakeConnectionImpl;
import net.snowflake.client.internal.core.ParameterBindingDTO;
import net.snowflake.client.internal.core.SFBaseSession;
import net.snowflake.client.internal.core.SFBaseStatement;
import net.snowflake.client.internal.jdbc.SFConnectionHandler;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/** Regression tests for SNOW-4352287: timestamps before 1970 must not bind one second late. */
class SnowflakePreparedStatementImplSetTimestampTest {

  private SnowflakeConnectionImpl mockConnection;
  private SFBaseSession mockSession;

  @BeforeEach
  void setUp() throws SQLException {
    mockConnection = mock(SnowflakeConnectionImpl.class);
    SFConnectionHandler mockHandler = mock(SFConnectionHandler.class);
    SFBaseStatement mockSFStatement = mock(SFBaseStatement.class);
    mockSession = mock(SFBaseSession.class);

    when(mockConnection.getHandler(any())).thenReturn(mockHandler);
    when(mockHandler.getSFStatement()).thenReturn(mockSFStatement);
    when(mockConnection.getSFBaseSession(any())).thenReturn(mockSession);
    when(mockConnection.getSessionID()).thenReturn("test-session-id");
    when(mockConnection.isClosed()).thenReturn(false);
    when(mockConnection.getShowStatementParameters()).thenReturn(false);
    when(mockSession.getTimestampMappedType()).thenReturn(SnowflakeType.TIMESTAMP_LTZ);
  }

  private SnowflakePreparedStatementImpl newStatement() throws SQLException {
    return new SnowflakePreparedStatementImpl(
        mockConnection,
        "insert into t values (?, ?, ?, ?, ?)",
        false,
        ResultSet.TYPE_FORWARD_ONLY,
        ResultSet.CONCUR_READ_ONLY,
        ResultSet.CLOSE_CURSORS_AT_COMMIT);
  }

  private static void assertBinding(ParameterBindingDTO binding, String type, String value) {
    assertEquals(type, binding.getType());
    assertEquals(value, binding.getValue());
  }

  @ParameterizedTest
  @CsvSource({
    "1969-12-31T23:59:59.123456Z, -876544000",
    "1969-12-31T23:59:59.001Z, -999000000",
    "1969-12-31T23:59:59.999999999Z, -1",
    "1950-06-15T12:00:00.500Z, -616852799500000000",
    "1969-12-31T23:59:59.000123Z, -999877000",
    "1969-12-31T23:59:59Z, -1000000000",
    "1970-01-01T00:00:00Z, 0",
    "1970-01-01T00:00:00.123456Z, 123456000",
    "2024-02-29T12:34:56.789Z, 1709210096789000000"
  })
  void shouldBindTimestampAsEpochNanos(String instant, String expectedValue) throws SQLException {
    Timestamp ts = Timestamp.from(Instant.parse(instant));
    SnowflakePreparedStatementImpl stmt = newStatement();
    stmt.setTimestamp(1, ts);
    stmt.setObject(2, ts);
    stmt.setObject(3, ts, Types.TIMESTAMP);
    stmt.setObject(4, ts, SnowflakeType.EXTRA_TYPES_TIMESTAMP_LTZ);
    stmt.setObject(5, ts, SnowflakeType.EXTRA_TYPES_TIMESTAMP_NTZ);

    Map<String, ParameterBindingDTO> bindings = stmt.getParameterBindings();
    assertBinding(bindings.get("1"), "TIMESTAMP_LTZ", expectedValue);
    assertBinding(bindings.get("2"), "TIMESTAMP_LTZ", expectedValue);
    assertBinding(bindings.get("3"), "TIMESTAMP_LTZ", expectedValue);
    assertBinding(bindings.get("4"), "TIMESTAMP_LTZ", expectedValue);
    assertBinding(bindings.get("5"), "TIMESTAMP_NTZ", expectedValue);
  }

  @ParameterizedTest
  @CsvSource({
    "1969-12-31T23:59:59.123456Z, UTC, TIMESTAMP_TZ, -876544000 1440",
    "1969-12-31T23:59:59.123456Z, UTC, TIMESTAMP_LTZ, -876544000",
    "1969-12-31T23:59:59.123456Z, GMT-05:00, TIMESTAMP_TZ, -876544000 1140",
    "1969-12-31T23:59:59.123456Z, GMT-05:00, TIMESTAMP_NTZ, -18000876544000",
    "1970-01-01T03:00:00.500Z, GMT-05:00, TIMESTAMP_LTZ, -7199500000000",
    "1970-01-01T03:00:00.500Z, GMT-05:00, TIMESTAMP_TZ, 10800500000000 1140",
    "1969-12-31T23:59:59.000123Z, GMT-05:00, TIMESTAMP_LTZ, -18000999877000",
    "2024-02-29T12:34:56.789Z, GMT+01:00, TIMESTAMP_TZ, 1709210096789000000 1500",
    "2024-02-29T12:34:56.789Z, GMT+01:00, TIMESTAMP_LTZ, 1709213696789000000"
  })
  void shouldBindTimestampWithCalendarAsEpochNanos(
      String instant, String zone, SnowflakeType mappedType, String expectedValue)
      throws SQLException {
    when(mockSession.getTimestampMappedType()).thenReturn(mappedType);
    SnowflakePreparedStatementImpl stmt = newStatement();
    stmt.setTimestamp(
        1,
        Timestamp.from(Instant.parse(instant)),
        Calendar.getInstance(TimeZone.getTimeZone(zone)));

    assertBinding(stmt.getParameterBindings().get("1"), mappedType.name(), expectedValue);
  }

  @Test
  void shouldBindNullTimestampAsNullValue() throws SQLException {
    SnowflakePreparedStatementImpl stmt = newStatement();
    stmt.setTimestamp(1, null);
    stmt.setTimestamp(2, null, Calendar.getInstance(TimeZone.getTimeZone("UTC")));

    assertNull(stmt.getParameterBindings().get("1").getValue());
    assertNull(stmt.getParameterBindings().get("2").getValue());
  }
}
