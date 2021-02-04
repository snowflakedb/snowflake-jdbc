package net.snowflake.client.jdbc;

import static org.junit.Assert.*;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.util.*;
import net.snowflake.client.category.TestCategoryConnection;
import net.snowflake.client.core.*;
import net.snowflake.client.jdbc.telemetry.NoOpTelemetryClient;
import net.snowflake.client.jdbc.telemetry.Telemetry;
import net.snowflake.common.core.SnowflakeDateTimeFormat;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * IT test for testing the "pluggable" implementation of SnowflakeConnection, SnowflakeStatement,
 * and ResultSet. These tests will query Snowflake normally, retrieve the JSON result, and replay it
 * back using a custom implementation of these objects that simply echoes a given JSON response.
 */
@Category(TestCategoryConnection.class)
public class MockConnectionTest extends BaseJDBCTest {

  private static final String testTableName = "test_custom_conn_table";

  private static SFResultSetMetaData getRSMDFromResponse(JsonNode rootNode, SFBaseSession sfSession)
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

  private static ObjectNode getJsonFromDataType(DataType dataType) {

    ObjectMapper mapper = new ObjectMapper();
    ObjectNode type = mapper.createObjectNode();

    if (dataType == DataType.INT) {
      type.put("name", "someIntColumn");
      type.put("database", "");
      type.put("schema", "");
      type.put("table", "");
      type.put("scale", 0);
      type.put("precision", 18);
      type.put("type", "fixed");
      type.put("length", (Integer) null);
      type.put("byteLength", (Integer) null);
      type.put("nullable", true);
      type.put("collation", (String) null);
    } else if (dataType == DataType.STRING) {
      type.put("name", "someStringColumn");
      type.put("database", "");
      type.put("schema", "");
      type.put("table", "");
      type.put("scale", (Integer) null);
      type.put("precision", (Integer) null);
      type.put("length", 16777216);
      type.put("type", "text");
      type.put("byteLength", 16777216);
      type.put("nullable", true);
      type.put("collation", (String) null);
    }

    return type;
  }

  public Connection initMockConnection(SFConnectionHandler implementation) throws SQLException {
    return new SnowflakeConnectionV1(implementation);
  }

  /**
   * Test running some queries, and plugging in the raw JSON response from those queries into a
   * MockConnection. The results retrieved from the MockConnection should be the same as those from
   * the original connection.
   */
  @Test
  public void testMockResponse() throws SQLException, JsonProcessingException {
    ObjectMapper mapper = new ObjectMapper();
    JsonNode rawResponse =
        mapper.readTree(
            "{\n"
                + "   \"data\":{\n"
                + "      \"parameters\":[\n"
                + "         {\n"
                + "            \"name\":\"TIMESTAMP_OUTPUT_FORMAT\",\n"
                + "            \"value\":\"DY, DD MON YYYY HH24:MI:SS TZHTZM\"\n"
                + "         },\n"
                + "         {\n"
                + "            \"name\":\"TIME_OUTPUT_FORMAT\",\n"
                + "            \"value\":\"HH24:MI:SS\"\n"
                + "         },\n"
                + "         {\n"
                + "            \"name\":\"TIMESTAMP_TZ_OUTPUT_FORMAT\",\n"
                + "            \"value\":\"\"\n"
                + "         },\n"
                + "         {\n"
                + "            \"name\":\"TIMESTAMP_NTZ_OUTPUT_FORMAT\",\n"
                + "            \"value\":\"\"\n"
                + "         },\n"
                + "         {\n"
                + "            \"name\":\"DATE_OUTPUT_FORMAT\",\n"
                + "            \"value\":\"YYYY-MM-DD\"\n"
                + "         },\n"
                + "         {\n"
                + "            \"name\":\"TIMESTAMP_LTZ_OUTPUT_FORMAT\",\n"
                + "            \"value\":\"\"\n"
                + "         }\n"
                + "      ],\n"
                + "      \"rowtype\":[\n"
                + "         {\n"
                + "            \"name\":\"COLA\",\n"
                + "            \"database\":\"TESTDB\",\n"
                + "            \"schema\":\"TESTSCHEMA\",\n"
                + "            \"table\":\"TEST_CUSTOM_CONN_TABLE\",\n"
                + "            \"scale\":null,\n"
                + "            \"precision\":null,\n"
                + "            \"length\":16777216,\n"
                + "            \"type\":\"text\",\n"
                + "            \"byteLength\":16777216,\n"
                + "            \"nullable\":true,\n"
                + "            \"collation\":null\n"
                + "         },\n"
                + "         {\n"
                + "            \"name\":\"COLB\",\n"
                + "            \"database\":\"TESTDB\",\n"
                + "            \"schema\":\"TESTSCHEMA\",\n"
                + "            \"table\":\"TEST_CUSTOM_CONN_TABLE\",\n"
                + "            \"scale\":0,\n"
                + "            \"precision\":38,\n"
                + "            \"length\":null,\n"
                + "            \"type\":\"fixed\",\n"
                + "            \"byteLength\":null,\n"
                + "            \"nullable\":true,\n"
                + "            \"collation\":null\n"
                + "         }\n"
                + "      ],\n"
                + "      \"rowset\":[\n"
                + "         [\n"
                + "            \"rowOne\",\n"
                + "            \"1\"\n"
                + "         ],\n"
                + "         [\n"
                + "            \"rowTwo\",\n"
                + "            \"2\"\n"
                + "         ]\n"
                + "      ],\n"
                + "      \"total\":2,\n"
                + "      \"returned\":2,\n"
                + "      \"queryId\":\"0199922f-015a-7715-0000-0014000123ca\",\n"
                + "      \"databaseProvider\":null,\n"
                + "      \"finalDatabaseName\":\"TESTDB\",\n"
                + "      \"finalSchemaName\":\"TESTSCHEMA\",\n"
                + "      \"finalWarehouseName\":\"DEV\",\n"
                + "      \"finalRoleName\":\"SYSADMIN\",\n"
                + "      \"numberOfBinds\":0,\n"
                + "      \"arrayBindSupported\":false,\n"
                + "      \"statementTypeId\":4096,\n"
                + "      \"version\":1,\n"
                + "      \"sendResultTime\":1610498856446,\n"
                + "      \"queryResultFormat\":\"json\"\n"
                + "   },\n"
                + "   \"code\":null,\n"
                + "   \"message\":null,\n"
                + "   \"success\":true\n"
                + "}");

    SFConnectionHandler mockImpl = new MockSnowflakeConnectionImpl(rawResponse);
    Connection mockConnection = initMockConnection(mockImpl);

    ResultSet fakeResultSet =
        mockConnection.prepareStatement("select count(*) from " + testTableName).executeQuery();
    fakeResultSet.next();
    String val = fakeResultSet.getString(1);
    assertEquals("colA value from the mock connection was not what was expected", "rowOne", val);

    mockConnection.close();
  }

  /**
   * Fabricates fake JSON responses with some int data, and asserts the correct results via
   * retrieval from MockJsonResultSet
   */
  @Test
  public void testMockedResponseWithRows() throws SQLException {
    // Test with some ints
    List<DataType> dataTypes = Arrays.asList(DataType.INT, DataType.INT, DataType.INT);
    List<Object> row1 = Arrays.asList(1, 2, null);
    List<Object> row2 = Arrays.asList(4, null, 6);
    List<List<Object>> rowsToTest = Arrays.asList(row1, row2);

    JsonNode responseWithRows = createDummyResponseWithRows(rowsToTest, dataTypes);

    SFConnectionHandler mockImpl = new MockSnowflakeConnectionImpl(responseWithRows);
    Connection mockConnection = initMockConnection(mockImpl);

    ResultSet fakeResultSet =
        mockConnection.prepareStatement("select * from fakeTable").executeQuery();
    compareResultSets(fakeResultSet, rowsToTest, dataTypes);

    mockConnection.close();

    // Now test with some strings
    dataTypes = Arrays.asList(DataType.STRING, DataType.STRING);
    row1 = Arrays.asList("hi", "bye");
    row2 = Arrays.asList(null, "snowflake");
    List<Object> row3 = Arrays.asList("is", "great");
    rowsToTest = Arrays.asList(row1, row2, row3);

    responseWithRows = createDummyResponseWithRows(rowsToTest, dataTypes);

    mockImpl = new MockSnowflakeConnectionImpl(responseWithRows);
    mockConnection = initMockConnection(mockImpl);

    fakeResultSet = mockConnection.prepareStatement("select * from fakeTable").executeQuery();
    compareResultSets(fakeResultSet, rowsToTest, dataTypes);

    mockConnection.close();

    // Mixed data
    dataTypes = Arrays.asList(DataType.STRING, DataType.INT);
    row1 = Arrays.asList("foo", 2);
    row2 = Arrays.asList("bar", 4);
    row3 = Arrays.asList("baz", null);
    rowsToTest = Arrays.asList(row1, row2, row3);

    responseWithRows = createDummyResponseWithRows(rowsToTest, dataTypes);

    mockImpl = new MockSnowflakeConnectionImpl(responseWithRows);
    mockConnection = initMockConnection(mockImpl);

    fakeResultSet = mockConnection.prepareStatement("select * from fakeTable").executeQuery();
    compareResultSets(fakeResultSet, rowsToTest, dataTypes);

    mockConnection.close();
  }

  /** Tests the MockFileTransferInterface with PUT/GET on random byte arrays. */
  @Test
  public void testMockTransferAgent() throws SQLException, IOException {
    SFConnectionHandler mockImpl = new MockSnowflakeConnectionImpl();
    SnowflakeConnection mockConnection =
        initMockConnection(mockImpl).unwrap(SnowflakeConnectionV1.class);

    byte[] inputBytes1 = new byte[] {0, 1, 2};
    InputStream uploadStream1 = new ByteArrayInputStream(inputBytes1);
    mockConnection.uploadStream("@fakeStage", "", uploadStream1, "file1", false);

    InputStream downloadStream1 = mockConnection.downloadStream("@fakeStage", "file1", false);
    byte[] outputBytes1 = new byte[downloadStream1.available()];
    downloadStream1.read(outputBytes1);
    assertArrayEquals("downloaded bytes not what was expected", outputBytes1, inputBytes1);
  }

  private JsonNode createDummyResponseWithRows(List<List<Object>> rows, List<DataType> dataTypes) {
    ObjectMapper mapper = new ObjectMapper();
    ObjectNode rootNode = mapper.createObjectNode();
    ObjectNode dataNode = rootNode.putObject("data");

    createResultSetMetadataResponse(dataNode, dataTypes);
    createRowsetJson(dataNode, rows, dataTypes);

    return rootNode;
  }

  /**
   * Creates the metadata portion of the response, i.e.,
   *
   * <p>parameters: [time format, date format, timestamp format, timestamp_ltz format, timestamp_tz
   * format, timestamp_ntz format, ] queryId rowType
   *
   * @param dataNode ObjectNode representing the "data" portion of the JSON response
   * @param dataTypes datatypes of the rows used in the generated response
   */
  private void createResultSetMetadataResponse(ObjectNode dataNode, List<DataType> dataTypes) {
    ArrayNode parameters = dataNode.putArray("parameters");

    parameters.add(createParameterJson("TIME_OUTPUT_FORMAT", "HH24:MI:SS"));
    parameters.add(createParameterJson("DATE_OUTPUT_FORMAT", "YYYY-MM-DD"));
    parameters.add(
        createParameterJson("TIMESTAMP_OUTPUT_FORMAT", "DY, DD MON YYYY HH24:MI:SS TZHTZM"));
    parameters.add(createParameterJson("TIMESTAMP_LTZ_OUTPUT_FORMAT", ""));
    parameters.add(createParameterJson("TIMESTAMP_NTZ_OUTPUT_FORMAT", ""));
    parameters.add(createParameterJson("TIMESTAMP_TZ_OUTPUT_FORMAT", ""));

    dataNode.put("queryId", "81998ae8-01e5-e08d-0000-10140001201a");

    ArrayNode rowType = dataNode.putArray("rowtype");

    for (DataType type : dataTypes) {
      rowType.add(getJsonFromDataType(type));
    }
  }

  /**
   * Creates a parameter key-value pairing in JSON, with name and value
   *
   * @return an ObjectNode with the parameter name and value
   */
  private ObjectNode createParameterJson(String parameterName, String parameterValue) {
    ObjectMapper mapper = new ObjectMapper();
    ObjectNode parameterObject = mapper.createObjectNode();
    parameterObject.put("name", parameterName);
    parameterObject.put("value", parameterValue);

    return parameterObject;
  }

  /**
   * Adds the data portion of the mocked response JSON
   *
   * @param dataNode The ObjectNode representing the "data" portion of the JSON response
   * @param rows The rows to add to the rowset.
   * @param dataTypes datatypes of the provided set of rows
   */
  private void createRowsetJson(
      ObjectNode dataNode, List<List<Object>> rows, List<DataType> dataTypes) {
    ArrayNode rowsetNode = dataNode.putArray("rowset");

    if (rows == null || rows.isEmpty()) {
      return;
    }

    for (List<Object> row : rows) {
      Iterator<Object> rowData = row.iterator();
      ArrayNode rowJson = rowsetNode.addArray();
      for (DataType type : dataTypes) {
        if (type == DataType.INT) {
          rowJson.add((Integer) rowData.next());
        } else if (type == DataType.STRING) {
          rowJson.add((String) rowData.next());
        }
      }
    }
  }

  /**
   * Utility method to check that the integer result set is equivalent to the given list of list of
   * ints
   */
  private void compareResultSets(
      ResultSet resultSet, List<List<Object>> expectedRows, List<DataType> dataTypes)
      throws SQLException {
    if (expectedRows == null || expectedRows.size() == 0) {
      assertFalse(resultSet.next());
      return;
    }

    int numRows = expectedRows.size();

    int resultSetRows = 0;

    Iterator<List<Object>> rowIterator = expectedRows.iterator();

    while (resultSet.next() && rowIterator.hasNext()) {
      List<Object> expectedRow = rowIterator.next();
      int columnIdx = 0;
      for (DataType type : dataTypes) {
        Object expected = expectedRow.get(columnIdx);
        columnIdx++;
        if (type == DataType.INT) {
          if (expected == null) {
            expected = 0;
          }
          int actual = resultSet.getInt(columnIdx);
          assertEquals(expected, actual);
        } else if (type == DataType.STRING) {
          String actual = resultSet.getString(columnIdx);
          assertEquals(expected, actual);
        }
      }

      resultSetRows++;
    }

    // If the result set has more rows than expected, finish the count
    while (resultSet.next()) {
      resultSetRows++;
    }

    assertEquals("row-count was not what was expected", numRows, resultSetRows);
  }

  // DataTypes supported with mock responses in test:
  // Currently only String and Integer are supported
  private enum DataType {
    INT,
    STRING
  }

  private static class MockedSFBaseStatement extends SFBaseStatement {
    JsonNode mockedResponse;
    MockSnowflakeSFSession sfSession;

    MockedSFBaseStatement(JsonNode mockedResponse, MockSnowflakeSFSession session) {
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
    public SFBaseResultSet execute(
        String sql, Map<String, ParameterBindingDTO> parametersBinding, CallingMethod caller)
        throws SQLException, SFException {
      return new MockJsonResultSet(mockedResponse, sfSession);
    }

    @Override
    public SFBaseResultSet asyncExecute(
        String sql, Map<String, ParameterBindingDTO> parametersBinding, CallingMethod caller)
        throws SQLException, SFException {
      return null;
    }

    @Override
    public void close() {}

    @Override
    public void cancel() {}

    @Override
    public void executeSetProperty(String sql) {}

    @Override
    public boolean hasChildren() {
      return false;
    }

    @Override
    public SFBaseSession getSFBaseSession() {
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
  }

  private static class MockJsonResultSet extends SFJsonResultSet {

    JsonNode resultJson;
    int currentRowIdx = -1;
    int rowCount;

    public MockJsonResultSet(JsonNode mockedJsonResponse, MockSnowflakeSFSession sfSession)
        throws SnowflakeSQLException {
      setSession(sfSession);
      this.resultJson = mockedJsonResponse.path("data").path("rowset");
      this.resultSetMetaData = MockConnectionTest.getRSMDFromResponse(mockedJsonResponse, session);
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

  private static class MockSnowflakeSFSession extends SFBaseSession {

    @Override
    public boolean isSafeToClose() {
      return false;
    }

    @Override
    public List<DriverPropertyInfo> checkProperties() {
      return null;
    }

    @Override
    public void close() {}

    @Override
    public void raiseError(Throwable exc, String jobId, String requestId) {}

    @Override
    public Telemetry getTelemetryClient() {
      return new NoOpTelemetryClient();
    }

    @Override
    public List<SFException> getSqlWarnings() {
      return null;
    }

    @Override
    public void clearSqlWarnings() {}
  }

  private static class MockSFFileTransferAgent extends SFBaseFileTransferAgent {

    private final String filePath;
    private final Map<String, byte[]> fileMap;

    // Takes the entire command, PUT and all, and encodes it in the file path
    // We could strip the GET/PUT in front of things, but
    public MockSFFileTransferAgent(
        Map<String, byte[]> fileMap, String filePath, CommandType commandType) {
      this.filePath = filePath;
      this.fileMap = fileMap;
      this.commandType = commandType;
    }

    @Override
    public boolean execute() throws SQLException {
      // Uploads a ByteArrayInputStream with available() bytes
      // to the fake "file store" represented by the Map
      if (commandType == CommandType.UPLOAD) {
        try {
          byte[] fileBytes = new byte[sourceStream.available()];
          sourceStream.read(fileBytes);
          // string-parsing logic skipped, so we use a dummy key for now
          fileMap.put("fileName", fileBytes);
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
      return false;
    }

    @Override
    public InputStream downloadStream(String fileName) throws SnowflakeSQLException {
      if (commandType == CommandType.DOWNLOAD) {
        // string-parsing logic skipped, so we use a dummy key for now
        byte[] bytes = fileMap.get("fileName");
        return new ByteArrayInputStream(bytes);
      }
      return null;
    }
  }

  private static class MockSnowflakeConnectionImpl implements SFConnectionHandler {
    JsonNode jsonResponse;
    MockSnowflakeSFSession session;
    // Map to store the bytes that are "uploaded"
    private Map<String, byte[]> fileMap = new HashMap<>();

    public MockSnowflakeConnectionImpl() {
      this.session = new MockSnowflakeSFSession();
    }

    public MockSnowflakeConnectionImpl(JsonNode jsonResponse) {
      this();
      this.jsonResponse = jsonResponse;
    }

    @Override
    public boolean supportsAsyncQuery() {
      return false;
    }

    @Override
    public void initializeConnection(String url, Properties info) throws SQLException {}

    @Override
    public SFBaseSession getSFSession() {
      return session;
    }

    @Override
    public SFBaseStatement getSFStatement() {
      return new MockedSFBaseStatement(jsonResponse, session);
    }

    @Override
    public ResultSet createResultSet(String queryID, Statement statement) throws SQLException {
      return null;
    }

    @Override
    public SnowflakeBaseResultSet createResultSet(SFBaseResultSet resultSet, Statement statement)
        throws SQLException {
      return new SnowflakeResultSetV1(resultSet, statement);
    }

    @Override
    public SnowflakeBaseResultSet createAsyncResultSet(
        SFBaseResultSet resultSet, Statement statement) throws SQLException {
      return null;
    }

    @Override
    public SFBaseFileTransferAgent getFileTransferAgent(String command, SFBaseStatement statement)
        throws SQLNonTransientConnectionException, SnowflakeSQLException {
      SFBaseFileTransferAgent.CommandType commandType =
          command.substring(0, 3).equalsIgnoreCase("PUT")
              ? SFBaseFileTransferAgent.CommandType.UPLOAD
              : SFBaseFileTransferAgent.CommandType.DOWNLOAD;
      return new MockSFFileTransferAgent(fileMap, "fileName", commandType);
    }
  }
}
