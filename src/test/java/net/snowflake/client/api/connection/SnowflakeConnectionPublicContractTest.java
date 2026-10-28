package net.snowflake.client.api.connection;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import net.snowflake.client.api.exception.ErrorCode;
import net.snowflake.client.api.resultset.QueryStatus;
import net.snowflake.client.internal.api.implementation.connection.SnowflakeConnectionImpl;
import net.snowflake.client.internal.core.SessionParameterTestUtil;
import net.snowflake.client.internal.jdbc.MockConnectionTest.MockSnowflakeConnectionImpl;
import net.snowflake.client.internal.jdbc.MockConnectionTest.MockSnowflakeSFSession;
import net.snowflake.client.internal.jdbc.telemetry.TelemetryData;
import net.snowflake.client.internal.jdbc.telemetry.TelemetryUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

/**
 * Offline unit tests for the stable public JDBC contract on {@link SnowflakeConnection}.
 *
 * <p>Tests use {@link MockSnowflakeConnectionImpl} — no network connections are made. All
 * assertions operate through the public {@link SnowflakeConnection} interface or through {@link
 * java.sql.Connection}, never through the concrete {@link SnowflakeConnectionImpl} class, to ensure
 * the contract is implementable without coupling to internals.
 *
 * <p>The {@link DefaultOnlySnowflakeConnection} inner class provides a minimal, compilable
 * implementation of {@link SnowflakeConnection} that deliberately does <em>not</em> override {@code
 * getSessionParameter} or {@code submitTelemetry}. This lets tests verify that the default methods
 * throw {@link SQLFeatureNotSupportedException} explicitly rather than returning {@code null} or
 * silently doing nothing — ensuring third-party / mock implementors fail loudly when they have not
 * yet added the new capability.
 */
class SnowflakeConnectionPublicContractTest {

  private Connection rawConnection;
  private SnowflakeConnection sfConnection;
  private MockSnowflakeConnectionImpl mockHandler;

  @BeforeEach
  void setUp() throws SQLException {
    mockHandler = new MockSnowflakeConnectionImpl();
    rawConnection = new SnowflakeConnectionImpl(mockHandler);
    sfConnection = rawConnection.unwrap(SnowflakeConnection.class);
  }

  // -----------------------------------------------------------------------
  // Minimal interface implementation – does NOT override the new default methods.
  // Used to verify the default "explicit failure" contract.
  // -----------------------------------------------------------------------

  /**
   * A compilable stub that implements every pre-existing abstract method of {@link
   * SnowflakeConnection} but intentionally inherits the default implementations of {@code
   * getSessionParameter} and {@code submitTelemetry}. Any test that constructs this class and calls
   * either new method must receive {@link SQLFeatureNotSupportedException}.
   */
  private static class DefaultOnlySnowflakeConnection implements SnowflakeConnection {
    @Override
    public void uploadStream(String stageName, String destFileName, InputStream inputStream)
        throws SQLException {}

    @Override
    public void uploadStream(
        String stageName, String destFileName, InputStream inputStream, UploadStreamConfig config)
        throws SQLException {}

    @Override
    public InputStream downloadStream(String stageName, String sourceFileName) throws SQLException {
      return null;
    }

    @Override
    public InputStream downloadStream(
        String stageName, String sourceFileName, DownloadStreamConfig config) throws SQLException {
      return null;
    }

    @Override
    public String getSessionID() throws SQLException {
      return null;
    }

    @Override
    public QueryStatus getQueryStatus(String queryID) throws SQLException {
      return null;
    }

    @Override
    public ResultSet createResultSet(String queryID) throws SQLException {
      return null;
    }

    @Override
    public String[] getChildQueryIds(String queryID) throws SQLException {
      return new String[0];
    }

    @Override
    public int getDatabaseMajorVersion() throws SQLException {
      return 0;
    }

    @Override
    public int getDatabaseMinorVersion() throws SQLException {
      return 0;
    }

    @Override
    public String getDatabaseVersion() throws SQLException {
      return null;
    }

    @Override
    public String getRole() throws SQLException {
      return null;
    }

    @Override
    public String getWarehouse() throws SQLException {
      return null;
    }

    @Override
    public String getDatabase() throws SQLException {
      return null;
    }
    // getSessionParameter and submitTelemetry are intentionally NOT overridden;
    // they inherit the default implementations that throw SQLFeatureNotSupportedException.
  }

  // -----------------------------------------------------------------------
  // unwrap / isWrapperFor
  // -----------------------------------------------------------------------

  /** java.sql.Connection returned by the driver must advertise SnowflakeConnection via unwrap. */
  @Test
  void connectionIsUnwrappableToSnowflakeConnection() throws SQLException {
    assertTrue(
        rawConnection.isWrapperFor(SnowflakeConnection.class),
        "Connection must claim to be a wrapper for SnowflakeConnection");
    assertNotNull(
        rawConnection.unwrap(SnowflakeConnection.class),
        "unwrap(SnowflakeConnection.class) must return non-null");
  }

  // -----------------------------------------------------------------------
  // Default-method "explicit failure" contract (no override)
  // -----------------------------------------------------------------------

  /**
   * Tests that verify the default implementations of the new methods throw {@link
   * SQLFeatureNotSupportedException}. This is the binary/source-safe evolution strategy: existing
   * third-party or mock {@link SnowflakeConnection} implementors that have not yet added the new
   * methods inherit an explicit failure rather than silently returning {@code null} or doing
   * nothing.
   */
  @Nested
  class DefaultMethodsThrowExplicitly {

    private final SnowflakeConnection stub = new DefaultOnlySnowflakeConnection();

    @Test
    void getSessionParameterDefaultThrowsSQLFeatureNotSupportedException() {
      assertThrows(
          SQLFeatureNotSupportedException.class,
          () -> stub.getSessionParameter("QUERY_TAG"),
          "default getSessionParameter must throw SQLFeatureNotSupportedException,"
              + " not return null or throw a different exception");
    }

    @Test
    void getSessionParameterDefaultRejectsNullName() {
      assertThrows(IllegalArgumentException.class, () -> stub.getSessionParameter(null));
    }

    @Test
    void submitTelemetryDefaultThrowsSQLFeatureNotSupportedException() {
      assertThrows(
          SQLFeatureNotSupportedException.class,
          () -> stub.submitTelemetry("{\"type\":\"test\"}"),
          "default submitTelemetry must throw SQLFeatureNotSupportedException,"
              + " not silently drop the payload");
    }

    @Test
    void submitTelemetryDefaultRejectsNullPayload() {
      assertThrows(IllegalArgumentException.class, () -> stub.submitTelemetry(null));
    }
  }

  private static void assertConnectionClosed(Executable call) {
    SQLException ex = assertThrows(SQLException.class, call);
    assertEquals(
        ErrorCode.CONNECTION_CLOSED.getMessageCode().intValue(),
        ex.getErrorCode(),
        "closed connection must fail with CONNECTION_CLOSED, not another SQLException");
  }

  // -----------------------------------------------------------------------
  // getSessionParameter
  // -----------------------------------------------------------------------

  @Nested
  class GetSessionParameter {

    private void applyParameterBlock(Map<String, Object> parameters) {
      SessionParameterTestUtil.applyParameterBlock(parameters, mockHandler.getSFSession());
    }

    /**
     * Runs a parameter block through the login path. {@code TIMEZONE} is stored only in the
     * leftover map, while the driver also copies {@code AUTOCOMMIT} and {@code
     * CLIENT_SESSION_KEEP_ALIVE} into dedicated fields; all of them must be visible.
     */
    @Test
    void returnsParametersAppliedThroughLoginPath() throws SQLException {
      Map<String, Object> parameters = new HashMap<>();
      parameters.put("TIMEZONE", "America/Los_Angeles");
      parameters.put("AUTOCOMMIT", Boolean.FALSE);
      parameters.put("CLIENT_SESSION_KEEP_ALIVE", Boolean.TRUE);
      applyParameterBlock(parameters);

      assertEquals("America/Los_Angeles", sfConnection.getSessionParameter("TIMEZONE"));
      assertEquals("false", sfConnection.getSessionParameter("AUTOCOMMIT"));
      assertEquals("true", sfConnection.getSessionParameter("CLIENT_SESSION_KEEP_ALIVE"));
    }

    /** Integer parameters are returned in their decimal string form. */
    @Test
    void returnsNumericParameterAsDecimalString() throws SQLException {
      applyParameterBlock(Collections.singletonMap("CLIENT_PREFETCH_THREADS", 4));

      assertEquals("4", sfConnection.getSessionParameter("CLIENT_PREFETCH_THREADS"));
    }

    /** Lookup is case-insensitive in both directions, including mixed-case stored names. */
    @Test
    void isCaseInsensitiveLookup() throws SQLException {
      Map<String, Object> parameters = new HashMap<>();
      parameters.put("TIMEZONE", "America/Los_Angeles");
      parameters.put("Mixed_Case_Param", "value");
      applyParameterBlock(parameters);

      assertEquals("America/Los_Angeles", sfConnection.getSessionParameter("timezone"));
      assertEquals("America/Los_Angeles", sfConnection.getSessionParameter("Timezone"));
      assertEquals("value", sfConnection.getSessionParameter("MIXED_CASE_PARAM"));
      assertEquals("value", sfConnection.getSessionParameter("mixed_case_param"));
    }

    /** A name dropped from the latest parameter block must no longer be returned. */
    @Test
    void reflectsOnlyLatestParameterBlock() throws SQLException {
      applyParameterBlock(Collections.singletonMap("QUERY_TAG", "first"));
      applyParameterBlock(Collections.singletonMap("TIMEZONE", "UTC"));

      assertNull(sfConnection.getSessionParameter("QUERY_TAG"));
      assertEquals("UTC", sfConnection.getSessionParameter("TIMEZONE"));
    }

    /** An absent parameter must return null, not throw. */
    @Test
    void returnsNullForAbsentParameter() throws SQLException {
      assertNull(
          sfConnection.getSessionParameter("NONEXISTENT_PARAMETER_XYZ"),
          "absent parameter must yield null before any parameter block is applied");

      applyParameterBlock(Collections.singletonMap("TIMEZONE", "UTC"));
      assertNull(
          sfConnection.getSessionParameter("NONEXISTENT_PARAMETER_XYZ"),
          "absent parameter must yield null");
    }

    /** A null parameter name must throw IllegalArgumentException, not NullPointerException. */
    @Test
    void throwsIllegalArgumentOnNullParameterName() {
      assertThrows(
          IllegalArgumentException.class,
          () -> sfConnection.getSessionParameter(null),
          "null parameterName must produce IllegalArgumentException");
    }

    /** After close, getSessionParameter must fail with CONNECTION_CLOSED, even for a null name. */
    @Test
    void throwsConnectionClosedOnClosedConnection() throws SQLException {
      rawConnection.close();
      assertConnectionClosed(() -> sfConnection.getSessionParameter("QUERY_TAG"));
      assertConnectionClosed(() -> sfConnection.getSessionParameter(null));
    }
  }

  // -----------------------------------------------------------------------
  // submitTelemetry
  // -----------------------------------------------------------------------

  @Nested
  class SubmitTelemetry {

    private MockSnowflakeSFSession session() {
      return (MockSnowflakeSFSession) mockHandler.getSFSession();
    }

    /**
     * A valid JSON object payload must reach the telemetry client, be flushed, and carry the
     * driver-injected {@value TelemetryUtil#SOURCE_KEY} marker.
     */
    @Test
    void submitsAndFlushesValidJsonObjectPayload() throws SQLException {
      sfConnection.submitTelemetry("{\"type\":\"snowpark_test\",\"value\":42}");

      List<TelemetryData> logs = session().getTelemetryLogs();
      assertEquals(1, logs.size(), "exactly one telemetry entry must be submitted");
      assertEquals("snowpark_test", logs.get(0).getMessage().get("type").asText());
      assertEquals(42, logs.get(0).getMessage().get("value").asInt());
      // The driver must inject a source marker so server-side consumers can distinguish
      // application-submitted events from driver-internal diagnostic events.
      assertEquals(
          SnowflakeConnectionImpl.SUBMIT_TELEMETRY_SOURCE_VALUE,
          logs.get(0).getMessage().get(TelemetryUtil.SOURCE_KEY).asText(),
          "driver must inject \"" + TelemetryUtil.SOURCE_KEY + "\" source marker");
      assertEquals(1, session().getTelemetryFlushCount(), "each submitTelemetry call must flush");
    }

    /** A malformed JSON payload must be discarded without throwing. */
    @Test
    void discardsInvalidJsonPayload() throws SQLException {
      sfConnection.submitTelemetry("not-a-valid-json-object");

      assertTrue(session().getTelemetryLogs().isEmpty(), "malformed payload must not be submitted");
      assertEquals(0, session().getTelemetryFlushCount());
    }

    /** Valid JSON that is not an object must be discarded without throwing. */
    @Test
    void discardsNonObjectJsonPayload() throws SQLException {
      sfConnection.submitTelemetry("[1, 2, 3]");
      sfConnection.submitTelemetry("42");

      assertTrue(session().getTelemetryLogs().isEmpty(), "non-object JSON must not be submitted");
      assertEquals(0, session().getTelemetryFlushCount());
    }

    /** A payload exceeding the size limit must be discarded without throwing. */
    @Test
    void discardsOversizedPayload() throws SQLException {
      // Build a JSON object whose serialized length exceeds the documented limit.
      // Use a Java-8-compatible way to build the repeated string (String.repeat is Java 11+).
      char[] chars = new char[SnowflakeConnectionImpl.SUBMIT_TELEMETRY_MAX_PAYLOAD_LENGTH];
      java.util.Arrays.fill(chars, 'x');
      String bigValue = new String(chars);
      sfConnection.submitTelemetry("{\"data\":\"" + bigValue + "\"}");

      assertTrue(session().getTelemetryLogs().isEmpty(), "oversized payload must not be submitted");
      assertEquals(0, session().getTelemetryFlushCount());
    }

    /** A null payload must throw IllegalArgumentException. */
    @Test
    void throwsIllegalArgumentOnNullPayload() {
      assertThrows(
          IllegalArgumentException.class,
          () -> sfConnection.submitTelemetry(null),
          "null jsonPayload must produce IllegalArgumentException");
    }

    /** After close, submitTelemetry must fail with CONNECTION_CLOSED, even for a null payload. */
    @Test
    void throwsConnectionClosedOnClosedConnection() throws SQLException {
      rawConnection.close();
      assertConnectionClosed(() -> sfConnection.submitTelemetry("{\"type\":\"test\"}"));
      assertConnectionClosed(() -> sfConnection.submitTelemetry(null));
    }
  }
}
