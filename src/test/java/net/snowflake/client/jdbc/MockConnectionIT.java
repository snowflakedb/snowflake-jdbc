package net.snowflake.client.jdbc;

import com.fasterxml.jackson.databind.JsonNode;
import net.snowflake.client.category.TestCategoryConnection;
import net.snowflake.client.core.*;
import net.snowflake.client.jdbc.telemetry.Telemetry;
import net.snowflake.common.core.SnowflakeDateTimeFormat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.*;
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * IT test for testing the "pluggable" implementation of SnowflakeConnection, SnowflakeStatement,
 * and ResultSet. These tests will query Snowflake normally, retrieve the JSON result, and replay it
 * back using a custom implementation of these objects that simply echoes a given JSON response.
 */
@Category(TestCategoryConnection.class)
public class MockConnectionIT extends BaseJDBCTest {

  private static final String testTableName = "test_custom_conn_table";

  private static SFResultSetMetaData getRSMDFromResponse(JsonNode rootNode, SFSession sfSession)
      throws SnowflakeSQLException {

    String queryId = rootNode.path("data").path("queryId").asText();

    Map<String, Object> parameters =
        SessionUtil.getCommonParams(rootNode.path("data").path("parameters"));

    String sqlTimestampFormat =
        (String) ResultUtil.effectiveParamValue(parameters, "TIMESTAMP_OUTPUT_FORMAT");

    // Special handling of specialized formatters, use a helper function
    SnowflakeDateTimeFormat ntzFormat =
        ResultUtil.specializedFormatter(
            parameters, "timestamp_ntz", "TIMESTAMP_NTZ_OUTPUT_FORMAT", sqlTimestampFormat);

    SnowflakeDateTimeFormat ltzFormat =
        ResultUtil.specializedFormatter(
            parameters, "timestamp_ltz", "TIMESTAMP_LTZ_OUTPUT_FORMAT", sqlTimestampFormat);

    SnowflakeDateTimeFormat tzFormat =
        ResultUtil.specializedFormatter(
            parameters, "timestamp_tz", "TIMESTAMP_TZ_OUTPUT_FORMAT", sqlTimestampFormat);

    String sqlDateFormat =
        (String) ResultUtil.effectiveParamValue(parameters, "DATE_OUTPUT_FORMAT");

    SnowflakeDateTimeFormat dateFormatter =
        SnowflakeDateTimeFormat.fromSqlFormat(Objects.requireNonNull(sqlDateFormat));

    String sqlTimeFormat =
        (String) ResultUtil.effectiveParamValue(parameters, "TIME_OUTPUT_FORMAT");

    SnowflakeDateTimeFormat timeFormatter =
        SnowflakeDateTimeFormat.fromSqlFormat(Objects.requireNonNull(sqlTimeFormat));

    List<SnowflakeColumnMetadata> resultColumnMetadata = new ArrayList<>();
    int columnCount = rootNode.path("data").path("rowtype").size();
    for (int i = 0; i < columnCount; i++) {
      JsonNode colNode = rootNode.path("data").path("rowtype").path(i);

      SnowflakeColumnMetadata columnMetadata =
          SnowflakeUtil.extractColumnMetadata(
              colNode, sfSession.isJdbcTreatDecimalAsInt(), sfSession);

      resultColumnMetadata.add(columnMetadata);
    }

    return new SFResultSetMetaData(
        resultColumnMetadata,
        queryId,
        sfSession,
        false,
        ntzFormat,
        ltzFormat,
        tzFormat,
        dateFormatter,
        timeFormatter);
  }

  public Connection initStandardConnection() throws SQLException {
    Connection conn = BaseJDBCTest.getConnection(BaseJDBCTest.DONT_INJECT_SOCKET_TIMEOUT);
    conn.createStatement().execute("alter session set jdbc_query_result_format = json");
    return conn;
  }

  public Connection initMockConnection(ConnectionImplementationFactory implementation)
      throws SQLException {
    return new SnowflakeConnectionV1(implementation);
  }

  @Before
  public void setUp() throws SQLException {
    Connection con = initStandardConnection();

    con.createStatement().execute("alter session set jdbc_query_result_format = json");

    // Create a table of two rows containing a varchar column and an int column
    con.createStatement()
        .execute("create or replace table " + testTableName + " (colA string, colB int)");
    con.createStatement().execute("insert into " + testTableName + " values('rowOne', 1)");
    con.createStatement().execute("insert into " + testTableName + " values('rowTwo', 2)");

    con.close();
  }

  /**
   * Test running some queries, and plugging in the raw JSON response from those queries into a
   * MockConnection. The results retrieved from the MockConnection should be the same as those from
   * the original connection.
   */
  @Test
  public void testReuseResponse() throws SQLException {
    Connection con = initStandardConnection();
    Statement stmt = con.createStatement();

    assertTrue(stmt instanceof SnowflakeStatementV1);
    SFStatementImpl sfStatement = (SFStatementImpl) ((SnowflakeStatementV1) stmt).getSfStatement();
    sfStatement.enableJsonResponseCapture();

    ResultSet result = stmt.executeQuery("select count(*) from " + testTableName);
    assertTrue(result instanceof SnowflakeResultSetV1);

    JsonNode rawResponse = sfStatement.getCapturedResponse();

    result.next();
    int count = result.getInt(1);
    assertEquals("row-count was not what was expected", 2, count);

    MockSnowflakeConnectionImpl mockImpl = new MockSnowflakeConnectionImpl(rawResponse);
    Connection mockConnection = initMockConnection(mockImpl);

    ResultSet fakeResultSet = mockConnection.prepareStatement("blah").executeQuery();
    fakeResultSet.next();
    int secondCount = fakeResultSet.getInt(1);
    assertEquals("row-count was not what was expected", secondCount, count);

    con.close();
    mockConnection.close();
  }

  @After
  public void tearDown() throws SQLException {
    Connection con = initStandardConnection();
    con.createStatement().execute("drop table if exists " + testTableName);
    con.close();
  }

  private static class MockedSFStatement implements SFStatement {
    JsonNode mockedResponse;
    MockSnowflakeSession sfSession;

    MockedSFStatement(JsonNode mockedResponse, MockSnowflakeSession session) {
      this.mockedResponse = mockedResponse;
      this.sfSession = session;
    }

    @Override
    public void addProperty(String propertyName, Object propertyValue) {}

    @Override
    public SFStatementMetaData describe(String sql) {
      return null;
    }

    @Override
    public Object executeHelper(
        String sql,
        String mediaType,
        Map<String, ParameterBindingDTO> bindValues,
        boolean describeOnly,
        boolean internal,
        boolean asyncExec) {
      return null;
    }

    @Override
    public int getConservativePrefetchThreads() {
      return 0;
    }

    @Override
    public long getConservativeMemoryLimit() {
      return 0;
    }

    @Override
    public SFBaseResultSet execute(
        String sql,
        boolean asyncExec,
        Map<String, ParameterBindingDTO> parametersBinding,
        CallingMethod caller)
        throws SQLException {
      return new MockJsonResultSet(mockedResponse, sfSession);
    }

    @Override
    public void close() {}

    @Override
    public void cancel() {}

    @Override
    public void executeSetProperty(String sql) {}

    @Override
    public SFSession getSession() {
      return sfSession;
    }

    @Override
    public boolean getMoreResults(int current) {
      return false;
    }

    @Override
    public SFBaseResultSet getResultSet() {
      return null;
    }

    @Override
    public boolean hasChildren() {
      return false;
    }
  }

  private static class MockJsonResultSet extends SFJsonResultSet {

    JsonNode resultJson;
    int currentRowIdx = -1;
    int rowCount;

    public MockJsonResultSet(JsonNode mockedJsonResponse, MockSnowflakeSession sfSession)
        throws SnowflakeSQLException {
      setSession(sfSession);
      this.resultJson = mockedJsonResponse.path("data").path("rowset");
      this.resultSetMetaData = MockConnectionIT.getRSMDFromResponse(mockedJsonResponse, session);
      this.rowCount = resultJson.size();
    }

    @Override
    public boolean next() {
      currentRowIdx++;
      return currentRowIdx < rowCount;
    }

    @Override
    protected Object getObjectInternal(int columnIndex) {
      return JsonResultChunk.extractCell(resultJson, currentRowIdx, columnIndex - 1);
    }

    @Override
    public boolean isLast() {
      return (currentRowIdx + 1) == rowCount;
    }

    @Override
    public boolean isAfterLast() {
      return (currentRowIdx >= rowCount);
    }

    @Override
    public SFStatementType getStatementType() {
      return null;
    }

    @Override
    public void setStatementType(SFStatementType statementType) {}

    @Override
    public String getQueryId() {
      return null;
    }
  }

  private static class MockSnowflakeSession implements SFSession {

    @Override
    public boolean isSafeToClose() {
      return false;
    }

    @Override
    public QueryStatus getQueryStatus(String queryID) {
      return null;
    }

    @Override
    public void addProperty(SFSessionProperty sfSessionProperty, Object propertyValue) {}

    @Override
    public void addProperty(String propertyName, Object propertyValue) {}

    @Override
    public boolean containProperty(String key) {
      return false;
    }

    @Override
    public boolean isStringQuoted() {
      return false;
    }

    @Override
    public boolean isJdbcTreatDecimalAsInt() {
      return false;
    }

    @Override
    public void open() {}

    @Override
    public List<DriverPropertyInfo> checkProperties() {
      return null;
    }

    @Override
    public String getDatabaseVersion() {
      return null;
    }

    @Override
    public int getDatabaseMajorVersion() {
      return 0;
    }

    @Override
    public int getDatabaseMinorVersion() {
      return 0;
    }

    @Override
    public String getSessionId() {
      return null;
    }

    @Override
    public void close() {}

    @Override
    public Properties getClientInfo() {
      return null;
    }

    @Override
    public String getClientInfo(String name) {
      return null;
    }

    @Override
    public void setSFSessionProperty(String propertyName, boolean propertyValue) {}

    @Override
    public Object getSFSessionProperty(String propertyName) {
      return null;
    }

    @Override
    public boolean isClosed() {
      return false;
    }

    @Override
    public boolean getAutoCommit() {
      return false;
    }

    @Override
    public void setAutoCommit(boolean autoCommit) {}

    @Override
    public String getDatabase() {
      return null;
    }

    @Override
    public void setDatabase(String database) {}

    @Override
    public String getSchema() {
      return null;
    }

    @Override
    public void setSchema(String schema) {}

    @Override
    public String getRole() {
      return null;
    }

    @Override
    public void setRole(String role) {}

    @Override
    public String getUser() {
      return null;
    }

    @Override
    public String getUrl() {
      return null;
    }

    @Override
    public String getWarehouse() {
      return null;
    }

    @Override
    public void setWarehouse(String warehouse) {}

    @Override
    public Telemetry getTelemetryClient() {
      return null;
    }

    @Override
    public boolean getMetadataRequestUseConnectionCtx() {
      return false;
    }

    @Override
    public boolean getMetadataRequestUseSessionDatabase() {
      return false;
    }

    @Override
    public boolean getPreparedStatementLogging() {
      return false;
    }

    @Override
    public List<SFException> getSqlWarnings() {
      return null;
    }

    @Override
    public void clearSqlWarnings() {}

    @Override
    public void setInjectFileUploadFailure(String fileToFail) {}

    @Override
    public void setInjectedDelay(int delay) {}

    @Override
    public boolean isSfSQLMode() {
      return false;
    }

    @Override
    public void setSfSQLMode(boolean booleanV) {}

    @Override
    public boolean isResultColumnCaseInsensitive() {
      return false;
    }

    @Override
    public String getServerUrl() {
      return null;
    }

    @Override
    public String getSessionToken() {
      return null;
    }

    @Override
    public String getServiceName() {
      return null;
    }

    @Override
    public String getIdToken() {
      return null;
    }

    @Override
    public SnowflakeType getTimestampMappedType() {
      return null;
    }
  }

  private static class MockSnowflakeConnectionImpl implements ConnectionImplementationFactory {

    JsonNode jsonResponse;
    MockSnowflakeSession session;

    public MockSnowflakeConnectionImpl(JsonNode jsonResponse) {
      this.jsonResponse = jsonResponse;
      this.session = new MockSnowflakeSession();
    }

    @Override
    public SFSession getSFSession() {
      return session;
    }

    @Override
    public SFStatement createSFStatement() {
      return new MockedSFStatement(jsonResponse, session);
    }

    @Override
    public SnowflakeFileTransferAgent getFileTransferAgent(String command, SFStatement statement) {
      return null;
    }
  }
}
