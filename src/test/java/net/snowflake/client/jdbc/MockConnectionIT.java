package net.snowflake.client.jdbc;

import com.fasterxml.jackson.databind.JsonNode;
import net.snowflake.client.category.TestCategoryConnection;
import net.snowflake.client.core.*;
import net.snowflake.client.jdbc.telemetry.Telemetry;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.*;
import java.util.List;
import java.util.Map;
import java.util.Properties;

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

  private static class MockedSFStatement implements SFStatement {
    SFResultSetMetaData rsmd;
    JsonNode mockedResponse;

    MockedSFStatement(SFResultSetMetaData rsmd, JsonNode mockedResponse) throws SQLException {
      this.rsmd = rsmd;
      this.mockedResponse = mockedResponse;
    }

    @Override
    public void addProperty(String propertyName, Object propertyValue) throws SFException {}

    @Override
    public SFStatementMetaData describe(String sql) throws SFException, SQLException {
      return null;
    }

    @Override
    public Object executeHelper(
        String sql,
        String mediaType,
        Map<String, ParameterBindingDTO> bindValues,
        boolean describeOnly,
        boolean internal,
        boolean asyncExec)
        throws SnowflakeSQLException, SFException {
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
        throws SQLException, SFException {
      return new MockJsonResultSet(mockedResponse, rsmd);
    }

    @Override
    public void close() {}

    @Override
    public void cancel() throws SFException, SQLException {}

    @Override
    public void executeSetProperty(String sql) {}

    @Override
    public SFSession getSession() {
      return null;
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
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

    public MockJsonResultSet(JsonNode mockedJsonResponse, SFResultSetMetaData rsmd) {
      this.resultJson = mockedJsonResponse.path("data").path("rowset");
      this.resultSetMetaData = rsmd;
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
    public QueryStatus getQueryStatus(String queryID) throws SQLException {
      return null;
    }

    @Override
    public void addProperty(SFSessionProperty sfSessionProperty, Object propertyValue)
        throws SFException {}

    @Override
    public void addProperty(String propertyName, Object propertyValue) throws SFException {}

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
    public void open() throws SFException, SnowflakeSQLException {}

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
    public void close() throws SFException, SnowflakeSQLException {}

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
    public void setSfSQLMode(boolean booleanV) {}

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
    SFResultSetMetaData resultSetMetaData;

    public MockSnowflakeConnectionImpl(JsonNode jsonResponse, SFResultSetMetaData rsmd) {
      this.resultSetMetaData = rsmd;
      this.jsonResponse = jsonResponse;
    }

    @Override
    public SFSession getSFSession() {
      return new MockSnowflakeSession();
    }

    @Override
    public SFStatement createSFStatement() throws SQLException {
      return new MockedSFStatement(resultSetMetaData, jsonResponse);
    }

    @Override
    public SnowflakeFileTransferAgent getFileTransferAgent(String command, SFStatement statement)
        throws SQLNonTransientConnectionException, SnowflakeSQLException {
      return null;
    }
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
   *
   * @throws SQLException
   * @throws SFException
   */
  @Test
  public void testReuseResponse() throws SQLException, SFException {
    Connection con = initStandardConnection();
    Statement stmt = con.createStatement();

    assertTrue(stmt instanceof SnowflakeStatementV1);
    SFStatementImpl sfStatement = (SFStatementImpl) ((SnowflakeStatementV1) stmt).getSfStatement();
    sfStatement.enableJsonResponseCapture();

    ResultSet result = stmt.executeQuery("select count(*) from " + testTableName);
    assertTrue(result instanceof SnowflakeResultSetV1);

    SFResultSetMetaData resultSetMetaData =
        ((SnowflakeResultSetV1) result).getSfBaseResultSet().getMetaData();
    JsonNode rawResponse = sfStatement.getCapturedResponse();

    result.next();
    int count = result.getInt(1);
    assertEquals("row-count was not what was expected", 2, count);

    MockSnowflakeConnectionImpl mockImpl =
        new MockSnowflakeConnectionImpl(rawResponse, resultSetMetaData);
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
}
