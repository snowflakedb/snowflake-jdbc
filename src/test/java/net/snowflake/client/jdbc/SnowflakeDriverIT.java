package net.snowflake.client.jdbc;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.math.BigDecimal;
import java.nio.channels.FileChannel;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;
import net.snowflake.client.AbstractDriverIT;
import net.snowflake.client.annotations.DontRunOnGithubActions;
import net.snowflake.client.annotations.DontRunOnTestaccount;
import net.snowflake.client.category.TestTags;
import net.snowflake.common.core.SqlState;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** General integration tests */
@Tag(TestTags.OTHERS)
public class SnowflakeDriverIT extends BaseJDBCTest {
  private static final int MAX_CONCURRENT_QUERIES_PER_USER = 50;
  private static final String getCurrenTransactionStmt = "SELECT CURRENT_TRANSACTION()";
  private static Logger logger = Logger.getLogger(SnowflakeDriverIT.class.getName());

  private static String ORDERS_JDBC = "ORDERS_JDBC";

  @TempDir private File tmpFolder;
  private ObjectMapper mapper = new ObjectMapper();

  @TempDir public File tmpFolder2;

  public String testStageName =
      String.format("test_stage_%s", UUID.randomUUID().toString()).replaceAll("-", "_");

  @BeforeAll
  public static void setUp() throws Throwable {
    try (Connection connection = getConnection()) {
      try (Statement statement = connection.createStatement()) {

        statement.execute(
            "create or replace table orders_jdbc"
                + "(C1 STRING NOT NULL COMMENT 'JDBC', "
                + "C2 STRING, C3 STRING, C4 STRING, C5 STRING, C6 STRING, "
                + "C7 STRING, C8 STRING, C9 STRING) "
                + "stage_file_format = (field_delimiter='|' "
                + "error_on_column_count_mismatch=false)");

        statement.execute(
            "create or replace table clustered_jdbc " + "(c1 number, c2 number) cluster by (c1)");

        // put files
        assertTrue(
            statement.execute(
                "PUT file://" + getFullPathFileInResource(TEST_DATA_FILE) + " @%orders_jdbc"),
            "Failed to put a file");
        assertTrue(
            statement.execute(
                "PUT file://" + getFullPathFileInResource(TEST_DATA_FILE_2) + " @%orders_jdbc"),
            "Failed to put a file");

        int numRows = statement.executeUpdate("copy into orders_jdbc");

        assertEquals(73, numRows, "Unexpected number of rows copied: " + numRows);
      }
    }
  }

  @AfterAll
  public static void tearDown() throws SQLException {
    try (Connection connection = getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("drop table if exists clustered_jdbc");
      statement.execute("drop table if exists orders_jdbc");
    }
  }

  public static Connection getConnection(int injectSocketTimeout) throws SQLException {
    Connection connection = AbstractDriverIT.getConnection(injectSocketTimeout);

    try (Statement statement = connection.createStatement()) {
      statement.execute(
          "alter session set "
              + "TIMEZONE='America/Los_Angeles',"
              + "TIMESTAMP_TYPE_MAPPING='TIMESTAMP_LTZ',"
              + "TIMESTAMP_OUTPUT_FORMAT='DY, DD MON YYYY HH24:MI:SS TZHTZM',"
              + "TIMESTAMP_TZ_OUTPUT_FORMAT='DY, DD MON YYYY HH24:MI:SS TZHTZM',"
              + "TIMESTAMP_LTZ_OUTPUT_FORMAT='DY, DD MON YYYY HH24:MI:SS TZHTZM',"
              + "TIMESTAMP_NTZ_OUTPUT_FORMAT='DY, DD MON YYYY HH24:MI:SS TZHTZM'");
    }
    return connection;
  }

  public static Connection getConnection() throws SQLException {
    return getConnection(AbstractDriverIT.DONT_INJECT_SOCKET_TIMEOUT);
  }

  /** Test connection to database using Snowflake Oauth instead of username/pw * */
  @Test
  @DontRunOnGithubActions
  public void testOauthConnection() throws SQLException {
    Map<String, String> params = getConnectionParameters();
    String role = null;
    String token = null;

    try (Connection con = getConnection("s3testaccount");
        Statement statement = con.createStatement()) {
      statement.execute("use role accountadmin");
      statement.execute(
          "create or replace security integration jdbc_oauth_integration\n"
              + "  type=oauth\n"
              + "  oauth_client=CUSTOM\n"
              + "  oauth_client_type=CONFIDENTIAL\n"
              + "  oauth_redirect_uri='https://localhost.com/oauth'\n"
              + "  oauth_issue_refresh_tokens=true\n"
              + "  enabled=true oauth_refresh_token_validity=86400;");
      role = params.get("role");
      try (ResultSet rs =
          statement.executeQuery(
              "select system$it('create_oauth_access_token', 'JDBC_OAUTH_INTEGRATION', '"
                  + role
                  + "')")) {
        assertTrue(rs.next());
        token = rs.getString(1);
      }
    }
    Properties props = new Properties();
    props.put("authenticator", "OAUTH");
    props.put("token", token);
    props.put("role", role);
    try (Connection con = getConnection("s3testaccount", props);
        Statement statement = con.createStatement()) {
      statement.execute("select 1");
    }
  }

  @Disabled
  @Test
  public void testConnections() throws Throwable {
    ExecutorService executorService = Executors.newFixedThreadPool(MAX_CONCURRENT_QUERIES_PER_USER);

    List<Future<?>> futures = new ArrayList<>();

    // create 30 threads, each open a connection and submit a query that
    // runs for 10 seconds
    for (int idx = 0; idx < MAX_CONCURRENT_QUERIES_PER_USER; idx++) {
      logger.info("open a new connection and submit query " + idx);

      final int queryIdx = idx;

      futures.add(
          executorService.submit(
              () -> {
                try (Connection connection = getConnection();
                    Statement statement = connection.createStatement();
                    ResultSet resultSet = statement.executeQuery("SELECT system$sleep(10) % 1")) {
                  ResultSetMetaData resultSetMetaData = resultSet.getMetaData();

                  // assert column count
                  assertEquals(1, resultSetMetaData.getColumnCount());

                  // assert we get 1 row
                  for (int i = 0; i < 1; i++) {
                    assertTrue(resultSet.next());

                    // assert each column is not null except the last one
                    for (int j = 1; j < 2; j++) {
                      assertEquals(0, resultSet.getInt(j));
                    }
                  }

                  logger.info("Query " + queryIdx + " passed ");
                }
                return true;
              }));
    }

    executorService.shutdown();

    for (int idx = 0; idx < MAX_CONCURRENT_QUERIES_PER_USER; idx++) {
      futures.get(idx).get();
    }
  }

  /** Test show columns */
  @Test
  public void testShowColumns() throws Throwable {
    Properties paramProperties = new Properties();
    try (Connection connection = getConnection(paramProperties);
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("show columns in clustered_jdbc")) {
      assertEquals(2, countRows(resultSet), "number of columns");
    }
  }

  private int countRows(ResultSet rset) throws Throwable {
    int cnt = 0;
    while (rset.next()) {
      cnt++;
    }
    return cnt;
  }

  @Test
  public void testRowsPerResultset() throws Throwable {
    try (Connection connection = getConnection()) {
      connection.createStatement().execute("alter session set rows_per_resultset=2048");

      try (Statement statement = connection.createStatement();
          ResultSet resultSet = statement.executeQuery("SELECT * FROM orders_jdbc")) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        int numColumns = resultSetMetaData.getColumnCount();
        assertEquals(9, numColumns);
        assertEquals(73, countRows(resultSet), "number of columns");
      }
    }
  }

  @Test
  public void testDDLs() throws Throwable {
    try (Connection connection = getConnection();
        Statement statement = connection.createStatement()) {
      try {
        statement.execute("CREATE OR REPLACE TABLE testDDLs(version number, name string)");
      } finally {
        statement.execute("DROP TABLE testDDLs");
      }
    }
  }

  private long getCurrentTransaction(Connection connection) throws SQLException {
    try (Statement statement = connection.createStatement()) {
      statement.execute(getCurrenTransactionStmt);
      try (ResultSet rs = statement.getResultSet()) {
        if (rs.next()) {
          String txnId = rs.getString(1);
          return txnId != null ? Long.valueOf(txnId) : 0L;
        }
      }
    }

    throw new SQLException(getCurrenTransactionStmt + " didn't return a result.");
  }

  /** Tests autocommit */
  @Test
  public void testAutocommit() throws Throwable {
    try (Connection connection = getConnection();
        Statement statement = connection.createStatement()) {
      try {
        // 1. test commit
        connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
        assertEquals(Connection.TRANSACTION_READ_COMMITTED, connection.getTransactionIsolation());
        connection.setAutoCommit(false); // disable autocommit
        assertFalse(connection.getAutoCommit());

        assertEquals(0, getCurrentTransaction(connection));

        // create a table, this should not start a transaction
        statement.executeUpdate("CREATE OR REPLACE TABLE AUTOCOMMIT_API_TEST (i int)");
        assertEquals(0, getCurrentTransaction(connection));

        // insert into it this should start a transaction.
        statement.executeUpdate("INSERT INTO AUTOCOMMIT_API_TEST VALUES (1)");
        assertNotEquals(0, getCurrentTransaction(connection));

        // commit it using the api
        connection.commit();
        assertFalse(connection.getAutoCommit());
        assertEquals(0, getCurrentTransaction(connection));
        try (ResultSet resultSet =
            statement.executeQuery("SELECT COUNT(*) FROM AUTOCOMMIT_API_TEST WHERE i = 1")) {
          assertTrue(resultSet.next());
          assertEquals(1, resultSet.getInt(1));
        }
        // 2. test rollback ==
        // delete from the table, should start a transaction.
        statement.executeUpdate("DELETE FROM AUTOCOMMIT_API_TEST");
        assertNotEquals(0, getCurrentTransaction(connection));

        // roll it back using the api
        connection.rollback();
        assertFalse(connection.getAutoCommit());
        assertEquals(0, getCurrentTransaction(connection));
        try (ResultSet resultSet =
            statement.executeQuery("SELECT COUNT(*) FROM AUTOCOMMIT_API_TEST WHERE i = 1")) {
          assertTrue(resultSet.next());
          assertEquals(1, resultSet.getInt(1));
        }
      } finally {
        statement.execute("DROP TABLE AUTOCOMMIT_API_TEST");
      }
    }
  }

  /**
   * Assert utility function for constraints. It asserts that result contains the specified number
   * of rows, and for each row the primary key table name and foreign key table name matches the
   * expected input.
   */
  private void assertConstraintResults(
      ResultSet resultSet, int numRows, int numCols, String pkTableName, String fkTableName)
      throws Throwable {
    ResultSetMetaData resultSetMetaData = resultSet.getMetaData();

    // assert column count
    assertEquals(numCols, resultSetMetaData.getColumnCount());

    // primary key for testConstraintsP1 should contain two rows
    for (int i = 0; i < numRows; i++) {
      assertTrue(resultSet.next(), "get constraint result row count");

      if (pkTableName != null) {
        assertTrue(
            pkTableName.equalsIgnoreCase(resultSet.getString(3)),
            "get constraint result primary table name");
      }

      if (fkTableName != null) {
        assertTrue(
            fkTableName.equalsIgnoreCase(resultSet.getString(7)),
            "get constraint result foreign table name");
      }
    }
  }

  @Test
  public void testBoolean() throws Throwable {

    try (Connection connection = getConnection();
        Statement statement = connection.createStatement()) {
      try {
        statement.execute("alter SESSION set CLIENT_METADATA_REQUEST_USE_CONNECTION_CTX=true");

        DatabaseMetaData metadata = connection.getMetaData();

        // Create a table with boolean columns
        statement.execute("create or replace table testBooleanT1(c1 boolean)");

        // Insert values into the table
        statement.execute("insert into testBooleanT1 values(true), (false), (null)");

        // Get values from the table
        try (PreparedStatement preparedStatement =
            connection.prepareStatement("select c1 from testBooleanT1")) {

          // I. Test ResultSetMetaData interface
          try (ResultSet resultSet = preparedStatement.executeQuery()) {
            ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
            // Verify the column type is Boolean
            assertEquals(Types.BOOLEAN, resultSetMetaData.getColumnType(1));

            // II. Test DatabaseMetadata interface
            try (ResultSet columnMetaDataResultSet =
                metadata.getColumns(
                    null, // catalog
                    null, // schema
                    "TESTBOOLEANT1", // table
                    null // column
                    )) {
              resultSetMetaData = columnMetaDataResultSet.getMetaData();
              // assert column count
              assertEquals(24, resultSetMetaData.getColumnCount());

              assertTrue(columnMetaDataResultSet.next());
              assertEquals(Types.BOOLEAN, columnMetaDataResultSet.getInt(5));
            }
          }
        }
      } finally {
        statement.execute("drop table testBooleanT1");
      }
    }
  }

  @Test
  public void testConstraints() throws Throwable {
    ResultSet manualResultSet = null;

    try (Connection connection = getConnection();
        Statement statement = connection.createStatement()) {
      try {
        statement.execute("alter SESSION set CLIENT_METADATA_REQUEST_USE_CONNECTION_CTX=true");

        DatabaseMetaData metadata = connection.getMetaData();

        // Create primary key tables
        statement.execute(
            "CREATE OR REPLACE TABLE testConstraintsP1(c1 number unique, c2 "
                + "number, constraint cons0 primary key (c1, c2))");

        statement.execute(
            "CREATE OR REPLACE TABLE testConstraintsP2(c1 number "
                + "constraint cons1 primary key, c2 number)");

        // Create foreign key tables
        statement.execute(
            "CREATE OR REPLACE TABLE testConstraintsF1(c1 number, c2 number, "
                + "constraint cons3 foreign key (c1, c2) references "
                + "testConstraintsP1(c1, c2))");

        statement.execute(
            "CREATE OR REPLACE TABLE testConstraintsF2(c1 number, c2 number, "
                + "constraint cons4 foreign key (c1, c2) references "
                + "testConstraintsP1(c1, c2), constraint cons5 "
                + "foreign key (c2) references testConstraintsP2(c1))");

        // show primary keys
        try (ResultSet resultSet = metadata.getPrimaryKeys(null, null, "TESTCONSTRAINTSP1")) {

          // primary key for testConstraintsP1 should contain two rows
          assertConstraintResults(resultSet, 2, 6, "testConstraintsP1", null);
        }

        ResultSet resultSet1 = metadata.getPrimaryKeys(null, null, "TESTCONSTRAINTSP2");

        // primary key for testConstraintsP2 contains 1 row
        assertConstraintResults(resultSet1, 1, 6, "testConstraintsP2", null);
        resultSet1.close();
        assertFalse(resultSet1.next());

        // Show imported keys
        try (ResultSet resultSet = metadata.getImportedKeys(null, null, "TESTCONSTRAINTSF1")) {
          assertConstraintResults(resultSet, 2, 14, null, "testConstraintsF1");
        }

        manualResultSet = metadata.getImportedKeys(null, null, "TESTCONSTRAINTSF2");

        assertConstraintResults(manualResultSet, 3, 14, null, "testConstraintsF2");
        manualResultSet.close();
        assertFalse(manualResultSet.next());

        // show exported keys
        try (ResultSet resultSet = metadata.getExportedKeys(null, null, "TESTCONSTRAINTSP1")) {
          assertConstraintResults(resultSet, 4, 14, "testConstraintsP1", null);
        }

        manualResultSet = metadata.getExportedKeys(null, null, "TESTCONSTRAINTSP2");

        assertConstraintResults(manualResultSet, 1, 14, "testConstraintsP2", null);
        manualResultSet.close();
        assertFalse(manualResultSet.next());

        // show cross references
        try (ResultSet resultSet =
            metadata.getCrossReference(
                null, null, "TESTCONSTRAINTSP1", null, null, "TESTCONSTRAINTSF1")) {
          assertConstraintResults(resultSet, 2, 14, "testConstraintsP1", "testConstraintsF1");
        }

        try (ResultSet resultSet =
            metadata.getCrossReference(
                null, null, "TESTCONSTRAINTSP2", null, null, "TESTCONSTRAINTSF2")) {
          assertConstraintResults(resultSet, 1, 14, "testConstraintsP2", "testConstraintsF2");
        }

        try (ResultSet resultSet =
            metadata.getCrossReference(
                null, null, "TESTCONSTRAINTSP1", null, null, "TESTCONSTRAINTSF2")) {
          assertConstraintResults(resultSet, 2, 14, "testConstraintsP1", "testConstraintsF2");
        }

        manualResultSet =
            metadata.getCrossReference(
                null, null, "TESTCONSTRAINTSP2", null, null, "TESTCONSTRAINTSF1");

        assertFalse(
            manualResultSet.next(),
            "cross reference from testConstraintsP2 to " + "testConstraintsF2 should be empty");
        manualResultSet.close();
        assertFalse(manualResultSet.next());
      } finally {
        statement.execute("DROP TABLE TESTCONSTRAINTSF1");
        statement.execute("DROP TABLE TESTCONSTRAINTSF2");
        statement.execute("DROP TABLE TESTCONSTRAINTSP1");
        statement.execute("DROP TABLE TESTCONSTRAINTSP2");
      }
    }
  }

  @Test
  public void testQueryWithMaxRows() throws Throwable {
    final int maxRows = 30;
    try (Connection connection = getConnection();
        Statement statement = connection.createStatement()) {
      statement.setMaxRows(maxRows);
      try (ResultSet resultSet = statement.executeQuery("SELECT * FROM orders_jdbc")) {

        // assert column count
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        assertEquals(9, resultSetMetaData.getColumnCount());
        assertEquals(maxRows, countRows(resultSet));
      }
    }
  }

  @Test
  @DontRunOnGithubActions
  public void testCancelQueryBySystemFunction() throws Throwable {
    try (Connection connection = getConnection();
        Statement getSessionIdStmt = connection.createStatement()) {
      getSessionIdStmt.setMaxRows(30);
      try (ResultSet resultSet = getSessionIdStmt.executeQuery("SELECT current_session()")) {
        assertTrue(resultSet.next());
        final long sessionId = resultSet.getLong(1);
        Timer timer = new Timer();
        timer.schedule(
            new TimerTask() {
              @Override
              public void run() {
                try {
                  PreparedStatement cancelAll;
                  cancelAll = connection.prepareStatement("call system$cancel_all_queries(?)");

                  // bind integer
                  cancelAll.setLong(1, sessionId);
                  cancelAll.executeQuery();
                } catch (SQLException ex) {
                  logger.log(Level.SEVERE, "Cancel failed with exception {}", ex);
                }
              }
            },
            5000);
      }
      // execute a query for 120s
      try (Statement statement = connection.createStatement()) {
        statement.setMaxRows(30);
        SQLException ex =
            assertThrows(
                SQLException.class,
                () ->
                    statement
                        .executeQuery("SELECT count(*) FROM TABLE(generator(timeLimit => 120))")
                        .close());
        assertEquals(SqlState.QUERY_CANCELED, ex.getSQLState(), "sqlstate mismatch");
      }
    }
  }

  @Test
  public void testDBMetadata() throws Throwable {
    int cnt = 0;
    try (Connection connection = getConnection()) {
      try (Statement statement = connection.createStatement()) {
        statement.execute("alter SESSION set CLIENT_METADATA_REQUEST_USE_CONNECTION_CTX=true");
      }
      // get database metadata
      DatabaseMetaData metaData = connection.getMetaData();

      // the following will issue
      try (ResultSet databaseSet = metaData.getCatalogs()) {
        assertTrue(databaseSet.next(), "databases shouldn't be empty");

        // "show schemas in [databaseName]"
        ResultSet schemaSet = metaData.getSchemas(connection.getCatalog(), connection.getSchema());
        assertTrue(schemaSet.next(), "schemas shouldn't be empty");
        assertTrue(
            connection.getCatalog().equalsIgnoreCase(schemaSet.getString(2)),
            "database should be " + connection.getCatalog());
        assertTrue(
            connection.getSchema().equalsIgnoreCase(schemaSet.getString(1)),
            "schema should be " + connection.getSchema());
        // snow tables in a schema
        try (ResultSet tableSet =
            metaData.getTables(
                connection.getCatalog(), connection.getSchema(), ORDERS_JDBC, null)) { // types
          assertTrue(
              tableSet.next(),
              String.format(
                  "table %s should exists in db: %s, schema: %s",
                  ORDERS_JDBC, connection.getCatalog(), connection.getSchema()));
          assertTrue(
              connection.getCatalog().equalsIgnoreCase(schemaSet.getString(2)),
              "database should be " + connection.getCatalog());
          assertTrue(
              connection.getSchema().equalsIgnoreCase(schemaSet.getString(1)),
              "schema should be " + connection.getSchema());
          assertTrue(
              ORDERS_JDBC.equalsIgnoreCase(tableSet.getString(3)), "table should be orders_jdbc");
        }
      }

      try (ResultSet tableMetaDataResultSet =
          metaData.getTables(
              null, // catalog
              null, // schema
              ORDERS_JDBC, // table
              null)) { // types

        ResultSetMetaData resultSetMetaData = tableMetaDataResultSet.getMetaData();

        assertEquals(10, resultSetMetaData.getColumnCount());

        // assert we get 1 rows
        cnt = 0;
        while (tableMetaDataResultSet.next()) {
          assertTrue(ORDERS_JDBC.equalsIgnoreCase(tableMetaDataResultSet.getString(3)));
          ++cnt;
        }
        assertEquals(1, cnt, "number of tables");
      }
      // test pattern
      try (ResultSet tableMetaDataResultSet =
          metaData.getTables(
              null, // catalog
              null, // schema
              "%", // table
              null)) { // types

        ResultSetMetaData resultSetMetaData = tableMetaDataResultSet.getMetaData();

        // assert column count
        assertEquals(10, resultSetMetaData.getColumnCount());

        // assert we get orders_jdbc
        boolean found = false;
        while (tableMetaDataResultSet.next()) {
          // assert the table name
          if (ORDERS_JDBC.equalsIgnoreCase(tableMetaDataResultSet.getString(3))) {
            found = true;
            break;
          }
        }
        assertTrue(found, "orders_jdbc not found");
      }

      // get column metadata
      try (ResultSet columnMetaDataResultSet = metaData.getColumns(null, null, ORDERS_JDBC, null)) {

        ResultSetMetaData resultSetMetaData = columnMetaDataResultSet.getMetaData();

        // assert column count
        assertEquals(24, resultSetMetaData.getColumnCount());

        // assert we get 9 rows
        cnt = 0;
        while (columnMetaDataResultSet.next()) {
          // SNOW-16881: assert database name
          assertTrue(
              connection.getCatalog().equalsIgnoreCase(columnMetaDataResultSet.getString(1)));

          // assert the table name and column name, data type and type name
          assertTrue(ORDERS_JDBC.equalsIgnoreCase(columnMetaDataResultSet.getString(3)));

          assertTrue(columnMetaDataResultSet.getString(4).startsWith("C"));

          assertEquals(Types.VARCHAR, columnMetaDataResultSet.getInt(5));

          assertTrue("VARCHAR".equalsIgnoreCase(columnMetaDataResultSet.getString(6)));

          if (cnt == 0) {
            // assert comment
            assertEquals("JDBC", columnMetaDataResultSet.getString(12));

            // assert nullable
            assertEquals(DatabaseMetaData.columnNoNulls, columnMetaDataResultSet.getInt(11));

            // assert is_nullable
            assertEquals("NO", columnMetaDataResultSet.getString(18));
          }
          ++cnt;
        }
        assertEquals(9, cnt);
      }

      // create a table with mix cases
      try (Statement statement = connection.createStatement()) {
        statement.execute("create or replace table \"testDBMetadata\" (a timestamp_ltz)");
        try (ResultSet columnMetaDataResultSet =
            metaData.getColumns(null, null, "testDBMetadata", null)) {

          // assert we get 1 row
          cnt = 0;
          while (columnMetaDataResultSet.next()) {
            // assert the table name and column name, data type and type name
            assertTrue("testDBMetadata".equalsIgnoreCase(columnMetaDataResultSet.getString(3)));

            assertEquals(Types.TIMESTAMP, columnMetaDataResultSet.getInt(5));

            assertTrue(columnMetaDataResultSet.getString(4).equalsIgnoreCase("a"));
            cnt++;
          }
          assertEquals(1, cnt);
        }
      }
      connection.createStatement().execute("DROP TABLE IF EXISTS \"testDBMetadata\"");
    }
  }

  @Test
  @DontRunOnGithubActions
  public void testPutWithWildcardGCP() throws Throwable {
    Properties _connectionProperties = new Properties();
    _connectionProperties.put("inject_wait_in_put", 5);
    _connectionProperties.put("ssl", "off");
    try (Connection connection =
            getConnection(
                DONT_INJECT_SOCKET_TIMEOUT, _connectionProperties, false, false, "gcpaccount");
        Statement statement = connection.createStatement()) {
      try {
        String sourceFilePath = getFullPathFileInResource(TEST_DATA_FILE);
        // replace file name with wildcard character
        sourceFilePath = sourceFilePath.replace("orders_100.csv", "orders_10*.csv");

        File destFolder = new File(tmpFolder, "dest");
        destFolder.mkdirs();
        String destFolderCanonicalPath = destFolder.getCanonicalPath();
        String destFolderCanonicalPathWithSeparator = destFolderCanonicalPath + File.separator;
        statement.execute("alter session set ENABLE_GCP_PUT_EXCEPTION_FOR_OLD_DRIVERS=false");
        statement.execute("CREATE OR REPLACE STAGE wildcard_stage");
        assertTrue(
            statement.execute("PUT file://" + sourceFilePath + " @wildcard_stage"),
            "Failed to put a file");

        findFile(statement, "ls @wildcard_stage/");

        assertTrue(
            statement.execute(
                "GET @wildcard_stage 'file://" + destFolderCanonicalPath + "' parallel=8"),
            "Failed to get files");

        File downloaded;
        // download the files we just uploaded to stage
        for (int i = 0; i < fileNames.length; i++) {
          // Make sure that the downloaded file exists, it should be gzip compressed
          downloaded = new File(destFolderCanonicalPathWithSeparator + fileNames[i] + ".gz");
          assertTrue(downloaded.exists());

          Process p =
              Runtime.getRuntime()
                  .exec("gzip -d " + destFolderCanonicalPathWithSeparator + fileNames[i] + ".gz");
          p.waitFor();

          String individualFilePath = sourceFilePath.replace("orders_10*.csv", fileNames[i]);

          File original = new File(individualFilePath);
          File unzipped = new File(destFolderCanonicalPathWithSeparator + fileNames[i]);
          assertEquals(original.length(), unzipped.length());
          assertTrue(FileUtils.contentEquals(original, unzipped));
        }
      } finally {
        statement.execute("DROP STAGE IF EXISTS wildcard_stage");
      }
    }
  }

  /**
   * helper function for creating large file in Java. Copies info from 1 file to another
   *
   * @param file1 file with info to be copied
   * @param file2 file to be copied into
   * @throws Exception
   */
  private void copyContentFrom(File file1, File file2) throws Exception {
    FileInputStream inputStream = new FileInputStream(file1);
    FileOutputStream outputStream = new FileOutputStream(file2);
    try (FileChannel fIn = inputStream.getChannel();
        FileChannel fOut = outputStream.getChannel()) {
      fOut.transferFrom(fIn, 0, fIn.size());
      fIn.position(0);
      fOut.transferFrom(fIn, fIn.size(), fIn.size());
    }
  }

  @Test
  @DontRunOnGithubActions
  public void testPutGetLargeFileGCP() throws Throwable {
    try (Connection connection = getConnection("gcpaccount");
        Statement statement = connection.createStatement()) {
      try {
        File destFolder = new File(tmpFolder, "dest");
        destFolder.mkdirs();
        String destFolderCanonicalPath = destFolder.getCanonicalPath();
        String destFolderCanonicalPathWithSeparator = destFolderCanonicalPath + File.separator;

        File largeTempFile = new File(tmpFolder, "largeFile.csv");
        largeTempFile.createNewFile();
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(largeTempFile))) {
          bw.write("Creating large test file for GCP PUT/GET test");
          bw.write(System.lineSeparator());
          bw.write("Creating large test file for GCP PUT/GET test");
          bw.write(System.lineSeparator());
        }
        File largeTempFile2 = new File(tmpFolder, "largeFile2.csv");
        largeTempFile2.createNewFile();

        String sourceFilePath = largeTempFile.getCanonicalPath();

        // copy info from 1 file to another and continue doubling file size until we reach ~1.5GB,
        // which is a large file
        for (int i = 0; i < 12; i++) {
          copyContentFrom(largeTempFile, largeTempFile2);
          copyContentFrom(largeTempFile2, largeTempFile);
        }

        statement.execute("alter session set ENABLE_GCP_PUT_EXCEPTION_FOR_OLD_DRIVERS=false");

        // create a stage to put the file in
        statement.execute("CREATE OR REPLACE STAGE largefile_stage");
        assertTrue(
            statement.execute("PUT file://" + sourceFilePath + " @largefile_stage"),
            "Failed to put a file");

        // check that file exists in stage after PUT
        findFile(statement, "ls @largefile_stage/");

        // create a new table with columns matching CSV file
        statement.execute("create or replace table large_table (colA string)");
        // copy rows from file into table
        statement.execute("copy into large_table from @largefile_stage/largeFile.csv.gz");
        // copy back from table into different stage
        statement.execute("create or replace stage extra_stage");
        statement.execute("copy into @extra_stage/bigFile.csv.gz from large_table single=true");

        // get file from new stage
        assertTrue(
            statement.execute(
                "GET @extra_stage 'file://" + destFolderCanonicalPath + "' parallel=8"),
            "Failed to get files");

        // Make sure that the downloaded file exists; it should be gzip compressed
        File downloaded = new File(destFolderCanonicalPathWithSeparator + "bigFile.csv.gz");
        assertTrue(downloaded.exists());

        // unzip the file
        Process p =
            Runtime.getRuntime()
                .exec("gzip -d " + destFolderCanonicalPathWithSeparator + "bigFile.csv.gz");
        p.waitFor();

        // compare the original file with the file that's been uploaded, copied into a table, copied
        // back into a stage,
        // downloaded, and unzipped
        File unzipped = new File(destFolderCanonicalPathWithSeparator + "bigFile.csv");
        assertEquals(largeTempFile.length(), unzipped.length());
        assertTrue(FileUtils.contentEquals(largeTempFile, unzipped));
      } finally {
        statement.execute("DROP STAGE IF EXISTS largefile_stage");
        statement.execute("DROP STAGE IF EXISTS extra_stage");
        statement.execute("DROP TABLE IF EXISTS large_table");
      }
    }
  }

  @Test
  @DontRunOnGithubActions
  public void testPutOverwrite() throws Throwable {
    // create 2 files: an original, and one that will overwrite the original
    File file1 = new File(tmpFolder, "testfile.csv");
    file1.createNewFile();
    try (BufferedWriter bw = new BufferedWriter(new FileWriter(file1))) {
      bw.write("Writing original file content. This should get overwritten.");
    }

    File file2 = new File(tmpFolder2, "testfile.csv");
    file2.createNewFile();
    try (BufferedWriter bw = new BufferedWriter(new FileWriter(file2))) {
      bw.write("This is all new! This should be the result of the overwriting.");
    }

    String sourceFilePathOriginal = file1.getCanonicalPath();
    String sourceFilePathOverwrite = file2.getCanonicalPath();

    File destFolder = new File(tmpFolder, "dest");
    destFolder.mkdirs();
    String destFolderCanonicalPath = destFolder.getCanonicalPath();
    String destFolderCanonicalPathWithSeparator = destFolderCanonicalPath + File.separator;

    List<String> accounts = Arrays.asList(null, "s3testaccount", "azureaccount", "gcpaccount");
    for (int i = 0; i < accounts.size(); i++) {
      try (Connection connection = getConnection(accounts.get(i));
          Statement statement = connection.createStatement()) {
        try {
          statement.execute("alter session set ENABLE_GCP_PUT_EXCEPTION_FOR_OLD_DRIVERS=false");

          // create a stage to put the file in
          statement.execute("CREATE OR REPLACE STAGE testing_stage");
          assertTrue(
              statement.execute("PUT file://" + sourceFilePathOriginal + " @testing_stage"),
              "Failed to put a file");
          // check that file exists in stage after PUT
          findFile(statement, "ls @testing_stage/");

          // put another file in same stage with same filename with overwrite = true
          assertTrue(
              statement.execute(
                  "PUT file://" + sourceFilePathOverwrite + " @testing_stage overwrite=true"),
              "Failed to put a file");

          // check that file exists in stage after PUT
          findFile(statement, "ls @testing_stage/");

          // get file from new stage
          assertTrue(
              statement.execute(
                  "GET @testing_stage 'file://" + destFolderCanonicalPath + "' parallel=8"),
              "Failed to get files");

          // Make sure that the downloaded file exists; it should be gzip compressed
          File downloaded = new File(destFolderCanonicalPathWithSeparator + "testfile.csv.gz");
          assertTrue(downloaded.exists());

          // unzip the file
          Process p =
              Runtime.getRuntime()
                  .exec("gzip -d " + destFolderCanonicalPathWithSeparator + "testfile.csv.gz");
          p.waitFor();

          File unzipped = new File(destFolderCanonicalPathWithSeparator + "testfile.csv");
          assertTrue(FileUtils.contentEqualsIgnoreEOL(file2, unzipped, null));
        } finally {
          statement.execute("DROP TABLE IF EXISTS testLoadToLocalFS");
        }
      }
    }
  }

  @Test
  @DontRunOnGithubActions
  public void testPut() throws Throwable {

    List<String> accounts = Arrays.asList(null, "s3testaccount", "azureaccount", "gcpaccount");
    for (int i = 0; i < accounts.size(); i++) {
      try (Connection connection = getConnection(accounts.get(i));
          Statement statement = connection.createStatement()) {
        try {
          // load file test
          // create a unique data file name by using current timestamp in millis
          statement.execute("alter session set ENABLE_GCP_PUT_EXCEPTION_FOR_OLD_DRIVERS=false");
          // test external table load
          statement.execute("CREATE OR REPLACE TABLE testLoadToLocalFS(a number)");

          // put files
          assertTrue(
              statement.execute(
                  "PUT file://"
                      + getFullPathFileInResource(TEST_DATA_FILE)
                      + " @%testLoadToLocalFS/orders parallel=10"),
              "Failed to put a file");

          try (ResultSet resultSet = statement.getResultSet()) {

            ResultSetMetaData resultSetMetaData = resultSet.getMetaData();

            // assert column count
            assertTrue(resultSetMetaData.getColumnCount() > 0);

            assertTrue(resultSet.next()); // one row
            assertFalse(resultSet.next());
          }
          findFile(
              statement, "ls @%testLoadToLocalFS/ pattern='.*orders/" + TEST_DATA_FILE + ".g.*'");

          // remove files
          try (ResultSet resultSet =
              statement.executeQuery(
                  "rm @%testLoadToLocalFS/ pattern='.*orders/" + TEST_DATA_FILE + ".g.*'")) {

            ResultSetMetaData resultSetMetaData = resultSet.getMetaData();

            // assert column count
            assertTrue(resultSetMetaData.getColumnCount() >= 1);

            // assert we get 1 row for the file we copied
            assertTrue(resultSet.next());
            assertNotNull(resultSet.getString(1));
            assertFalse(resultSet.next());
            SQLException ex = assertThrows(SQLException.class, () -> resultSet.getString(1));
            assertEquals((int) ErrorCode.COLUMN_DOES_NOT_EXIST.getMessageCode(), ex.getErrorCode());

            Thread.sleep(100);
          }
          // show files again
          try (ResultSet resultSet =
              statement.executeQuery("ls @%testLoadToLocalFS/ pattern='.*orders/orders.*'")) {

            // assert we get 0 row
            assertFalse(resultSet.next());
          }
        } finally {
          statement.execute("DROP TABLE IF EXISTS testLoadToLocalFS");
        }
      }
    }
  }

  static void findFile(Statement statement, String checkSQL) throws Throwable {
    boolean fileFound = false;

    // tolerate at most 60 tries for the following loop
    for (int numSecs = 0; numSecs <= 60; numSecs++) {
      // show files
      try (ResultSet resultSet = statement.executeQuery(checkSQL)) {

        if (resultSet.next()) {
          fileFound = true;
          break;
        }
        // give enough time for s3 eventual consistency for US region
        Thread.sleep(1000);
        assertTrue(fileFound, "Could not find a file");

        // assert the first column not null
        assertNotNull(resultSet.getString(1), "Null result");
      }
    }
  }

  @Test
  @DontRunOnGithubActions
  public void testSQLError42S02() throws SQLException {

    try (Connection connection = getConnection();
        Statement statement = connection.createStatement()) {
      // execute a bad query
      SQLException ex =
          assertThrows(
              SQLException.class,
              () -> statement.executeQuery("SELECT * FROM nonexistence").close());
      assertEquals(SqlState.BASE_TABLE_OR_VIEW_NOT_FOUND, ex.getSQLState(), "sqlstate mismatch");
    }
  }

  @Test
  @DontRunOnGithubActions
  public void testExplainPlan() throws Throwable {
    try (Connection connection = getConnection();
        Statement statement = connection.createStatement();

        // test explain plan: sorry not available for general but debugging purpose only
        ResultSet resultSet =
            statement.executeQuery("EXPLAIN PLAN FOR SELECT c1 FROM orders_jdbc")) {

      ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
      assertTrue(resultSetMetaData.getColumnCount() >= 4, "must return more than 4 columns");
      assertTrue(countRows(resultSet) > 3, "must return more than 3 rows");
    }
  }

  @Test
  public void testTimestampParsing() throws Throwable {
    try (Connection connection = getConnection();
        Statement statement = connection.createStatement();
        ResultSet resultSet =
            statement.executeQuery(
                "select to_timestamp('2013-05-08T15:39:20.123-07:00') from orders_jdbc")) {

      assertTrue(resultSet.next());
      assertEquals("Wed, 08 May 2013 15:39:20 -0700", resultSet.getString(1));
    }
  }

  @Test
  public void testDateParsing() throws Throwable {
    try (Connection connection = getConnection();
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("select to_date('0001-01-01')")) {
      assertTrue(resultSet.next());
      assertEquals("0001-01-01", resultSet.getString(1));
    }
  }

  @Test
  public void testTimeParsing() throws Throwable {

    try (Connection connection = getConnection();
        Statement statement = connection.createStatement();
        ResultSet resultSet =
            statement.executeQuery("select to_time('15:39:20.123') from orders_jdbc")) {
      assertTrue(resultSet.next());
      assertEquals("15:39:20", resultSet.getString(1));
    }
  }

  @Test
  public void testClientSideSorting() throws Throwable {
    ResultSetMetaData resultSetMetaData = null;

    try (Connection connection = getConnection();
        Statement statement = connection.createStatement()) {

      // turn on sorting mode
      statement.execute("set-sf-property sort on");

      try (ResultSet resultSet = statement.executeQuery("SELECT c3 FROM orders_jdbc")) {
        resultSetMetaData = resultSet.getMetaData();

        // assert column count
        assertEquals(1, resultSetMetaData.getColumnCount());

        // assert the values for the first 5 rows
        for (int i = 0; i < 5; i++) {
          assertTrue(resultSet.next());

          // assert each column is 'F'
          assertEquals("F", resultSet.getString(1));
        }
      }
      // turn off sorting mode
      statement.execute("set-sf-property sort off");

      try (ResultSet resultSet =
          statement.executeQuery("SELECT c3 FROM orders_jdbc order by c3 desc")) {

        resultSetMetaData = resultSet.getMetaData();

        // assert column count
        assertEquals(1, resultSetMetaData.getColumnCount());

        // assert the values for the first 4 rows
        for (int i = 0; i < 4; i++) {
          assertTrue(resultSet.next());

          // assert each column is 'P'
          assertEquals("P", resultSet.getString(1));
        }
      }
    }
  }

  @Test
  public void testUpdateCount() throws Throwable {
    try (Connection connection = getConnection();
        Statement statement = connection.createStatement()) {
      try {
        // create test table
        statement.execute("CREATE OR REPLACE TABLE testUpdateCount(version number, name string)");

        // insert two rows
        int numRows =
            statement.executeUpdate("INSERT INTO testUpdateCount values (1, 'a'), (2, 'b')");

        assertEquals(2, numRows, "Unexpected number of rows inserted: " + numRows);
      } finally {
        statement.execute("DROP TABLE if exists testUpdateCount");
      }
    }
  }

  @Test
  public void testSnow4245() throws Throwable {
    try (Connection connection = getConnection();
        Statement statement = connection.createStatement()) {
      try {
        // set timestamp format
        statement.execute("alter session set timestamp_input_format = 'YYYY-MM-DD HH24:MI:SS';");

        // create test table with different time zone flavors
        String createSQL =
            "create or replace table testSnow4245(t timestamp with local time "
                + "zone,ntz timestamp without time zone,tz  timestamp with time zone)";
        statement.execute(createSQL);

        // populate
        int numRows =
            statement.executeUpdate(
                "insert into testSnow4245 values(NULL,NULL,NULL),"
                    + "('2013-06-04 01:00:04','2013-06-04 01:00:04','2013-06-04 01:00:04'),"
                    + "('2013-06-05 23:00:05','2013-06-05 23:00:05','2013-06-05 23:00:05')");
        assertEquals(3, numRows, "Unexpected number of rows inserted: " + numRows);

        // query the data
        try (ResultSet resultSet =
            statement.executeQuery(
                "SELECT * FROM testSnow4245 order by 1 "
                    + "nulls first, 2 nulls first, 3 nulls first")) {

          int i = 0;
          // assert we get 3 rows

          while (resultSet.next()) {
            // assert each column is not null except the first row

            if (i == 0) {
              for (int j = 1; j < 4; j++) {
                assertNull(resultSet.getString(j), resultSet.getString(j));
              }
            } else {
              for (int j = 1; j < 4; j++) {
                assertNotNull(resultSet.getString(j), resultSet.getString(j));
              }
            }
            i = i + 1;
          }
        }
      } finally {
        statement.execute("drop table testSnow4245");
      }
    }
  }

  /** SNOW-4394 - Four bytes UTF-8 characters are not returned correctly. */
  @Test
  public void testSnow4394() throws Throwable {
    String tableName =
        String.format("snow4394_%s", UUID.randomUUID().toString()).replaceAll("-", "_");

    try (Connection connection = getConnection();
        Statement statement = connection.createStatement()) {
      // create test table
      try {
        statement.execute(String.format("CREATE OR REPLACE TABLE %s(str string)", tableName));

        String data = "What is \ud83d\ude12?";
        // insert two rows
        int numRows =
            statement.executeUpdate(
                String.format("INSERT INTO %s(str) values('%s')", tableName, data));
        assertEquals(1, numRows, "Unexpected number of rows inserted: " + numRows);

        try (ResultSet rset =
            statement.executeQuery(String.format("SELECT str FROM %s", tableName))) {
          String ret = null;
          while (rset.next()) {
            ret = rset.getString(1);
          }
          assertEquals(data, ret, "Unexpected string value: " + ret);
        }
      } finally {
        statement.execute(String.format("DROP TABLE if exists %s", tableName));
      }
    }
  }

  private void addBindBatch(PreparedStatement preparedStatement, java.sql.Date sqlDate)
      throws SQLException {
    preparedStatement.setDouble(1, 1.2);
    preparedStatement.setString(2, "hello");
    preparedStatement.setDate(3, sqlDate);
    preparedStatement.setDate(4, sqlDate);
    preparedStatement.setString(5, "h");
    preparedStatement.setDate(6, sqlDate);
    preparedStatement.setString(7, "h");
    preparedStatement.setString(8, "h");
    preparedStatement.setString(9, "h");
    preparedStatement.setString(10, "h");
    preparedStatement.setString(11, "h");
    preparedStatement.setDate(12, sqlDate);
    preparedStatement.setString(13, "h");
    preparedStatement.setDouble(14, 1.2);
    preparedStatement.setString(15, "h");
    preparedStatement.setString(16, "h");
    preparedStatement.setString(17, "h");
    preparedStatement.setString(18, "h");
    preparedStatement.setString(19, "h");
    preparedStatement.setDate(20, sqlDate);
    preparedStatement.setString(21, "h");
    preparedStatement.addBatch();
  }

  @Test
  public void testBind() throws Throwable {
    ResultSetMetaData resultSetMetaData = null;
    Timestamp ts = null;
    Time tm = null;
    java.sql.Date sqlDate = null;
    int[] updateCounts;
    try (Connection connection = getConnection()) {
      try (PreparedStatement preparedStatement = connection.prepareStatement("SELECT ?, ?")) {

        // bind integer
        preparedStatement.setInt(1, 1);
        preparedStatement.setString(2, "hello");
        try (ResultSet resultSet = preparedStatement.executeQuery()) {

          resultSetMetaData = resultSet.getMetaData();

          // assert column count
          assertEquals(2, resultSetMetaData.getColumnCount());
          assertEquals(Types.BIGINT, resultSetMetaData.getColumnType(1));
          assertEquals(Types.VARCHAR, resultSetMetaData.getColumnType(2));

          // assert we get 1 rows
          assertTrue(resultSet.next());

          assertEquals(1, resultSet.getInt(1), "integer");
          assertEquals("hello", resultSet.getString(2), "string");
        }
        // bind float
        preparedStatement.setDouble(1, 1.2);
        try (ResultSet resultSet = preparedStatement.executeQuery()) {
          resultSetMetaData = resultSet.getMetaData();

          // assert column count
          assertEquals(2, resultSetMetaData.getColumnCount());
          assertEquals(Types.DOUBLE, resultSetMetaData.getColumnType(1));

          // assert we get 1 rows
          assertTrue(resultSet.next());
          assertEquals(1.2, resultSet.getDouble(1), 0, "double");
          assertEquals("hello", resultSet.getString(2), "string");
        }
        // bind string
        preparedStatement.setString(1, "hello");
        try (ResultSet resultSet = preparedStatement.executeQuery()) {
          resultSetMetaData = resultSet.getMetaData();

          // assert column count
          assertEquals(2, resultSetMetaData.getColumnCount());
          assertEquals(Types.VARCHAR, resultSetMetaData.getColumnType(1));

          // assert we get 1 rows
          assertTrue(resultSet.next());
          assertEquals("hello", resultSet.getString(1), "string1");
          assertEquals("hello", resultSet.getString(2), "string2");
        }
        // bind date
        sqlDate = java.sql.Date.valueOf("2014-08-26");
        preparedStatement.setDate(1, sqlDate);
        try (ResultSet resultSet = preparedStatement.executeQuery()) {
          resultSetMetaData = resultSet.getMetaData();

          // assert column count
          assertEquals(2, resultSetMetaData.getColumnCount());
          assertEquals(Types.DATE, resultSetMetaData.getColumnType(1));

          // assert we get 1 rows
          assertTrue(resultSet.next());
          assertEquals("2014-08-26", resultSet.getString(1), "string");
          assertEquals("hello", resultSet.getString(2), "string");
        }
        // bind timestamp
        ts = buildTimestamp(2014, 7, 26, 3, 52, 0, 0);
        preparedStatement.setTimestamp(1, ts);
        try (ResultSet resultSet = preparedStatement.executeQuery()) {

          resultSetMetaData = resultSet.getMetaData();

          // assert column count
          assertEquals(2, resultSetMetaData.getColumnCount());
          assertEquals(Types.TIMESTAMP, resultSetMetaData.getColumnType(1));

          // assert we get 1 rows
          assertTrue(resultSet.next());
          assertEquals(
              "Mon, 25 Aug 2014 20:52:00 -0700", resultSet.getString(1), "Incorrect timestamp");
          assertEquals("hello", resultSet.getString(2), "string");
        }
        // bind time
        tm = new Time(12345678); // 03:25:45.678
        preparedStatement.setTime(1, tm);
        try (ResultSet resultSet = preparedStatement.executeQuery()) {
          resultSetMetaData = resultSet.getMetaData();

          // assert column count
          assertEquals(2, resultSetMetaData.getColumnCount());
          assertEquals(Types.TIME, resultSetMetaData.getColumnType(1));

          // assert we get 1 rows
          assertTrue(resultSet.next());
          assertEquals("03:25:45", resultSet.getString(1), "Incorrect time");
          assertEquals("hello", resultSet.getString(2), "string");
        }
      }
      // bind in where clause
      try (PreparedStatement preparedStatement =
          connection.prepareStatement("SELECT * FROM orders_jdbc WHERE to_number(c1) = ?")) {

        preparedStatement.setInt(1, 100);
        try (ResultSet resultSet = preparedStatement.executeQuery()) {
          resultSetMetaData = resultSet.getMetaData();

          // assert column count
          assertEquals(9, resultSetMetaData.getColumnCount());
          assertEquals(Types.VARCHAR, resultSetMetaData.getColumnType(1));
          assertEquals(Types.VARCHAR, resultSetMetaData.getColumnType(2));

          // assert we get 1 rows
          assertTrue(resultSet.next());
          assertEquals("100", resultSet.getString(1), "c1");
          assertEquals("147004", resultSet.getString(2), "c2");
        }
      }

      // bind in insert statement
      // create a test table
      try (Statement regularStatement = connection.createStatement()) {
        regularStatement.executeUpdate(
            "create or replace table testBind(a int, b string, c double, d date, "
                + "e timestamp, f time, g date)");

        try (PreparedStatement preparedStatement =
            connection.prepareStatement(
                "insert into testBind(a, b, c, d, e, f) values(?, ?, ?, ?, ?, ?)")) {

          preparedStatement.setInt(1, 1);
          preparedStatement.setString(2, "hello");
          preparedStatement.setDouble(3, 1.2);
          preparedStatement.setDate(4, sqlDate);
          preparedStatement.setTimestamp(5, ts);
          preparedStatement.setTime(6, tm);
          int rowCount = preparedStatement.executeUpdate();

          // update count should be 1
          assertEquals(1, rowCount, "update count");

          // test the inserted rows
          try (ResultSet resultSet = regularStatement.executeQuery("select * from testBind")) {

            // assert we get 1 rows
            assertTrue(resultSet.next());
            assertEquals(1, resultSet.getInt(1), "int");
            assertEquals("hello", resultSet.getString(2), "string");
            assertEquals(1.2, resultSet.getDouble(3), 0, "double");
            assertEquals("2014-08-26", resultSet.getString(4), "date");
            assertEquals("Mon, 25 Aug 2014 20:52:00 -0700", resultSet.getString(5), "timestamp");
            assertEquals("03:25:45", resultSet.getString(6), "time");
            assertNull(resultSet.getString(7), "date");
          }
        }
        // bind in update statement
        try (PreparedStatement preparedStatement =
            connection.prepareStatement("update testBind set b=? where a=?")) {
          preparedStatement.setString(1, "world");
          preparedStatement.setInt(2, 1);
          preparedStatement.execute();
        }

        // test the updated rows
        try (ResultSet resultSet = regularStatement.executeQuery("select * from testBind")) {
          // assert we get 1 rows
          assertTrue(resultSet.next());
          assertEquals(1, resultSet.getInt(1), "int");
          assertEquals("world", resultSet.getString(2), "string");
          assertEquals(1.2, resultSet.getDouble(3), 0, "double");
          assertEquals("2014-08-26", resultSet.getString(4), "date");
          assertEquals("Mon, 25 Aug 2014 20:52:00 -0700", resultSet.getString(5), "timestamp");
          assertEquals("03:25:45", resultSet.getString(6), "time");
          assertNull(resultSet.getString(7), "date");
        }
        // array bind for insert
        try (PreparedStatement preparedStatement =
            connection.prepareStatement(
                "insert into testBind (a, b, c, d, e, f, g) "
                    + "values(?, ?, ?, ?, ?, ?, current_date())")) {

          preparedStatement.setInt(1, 2);
          preparedStatement.setString(2, "hello");
          preparedStatement.setDouble(3, 1.2);
          preparedStatement.setDate(4, sqlDate);
          preparedStatement.setTimestamp(5, ts);
          preparedStatement.setTime(6, tm);
          preparedStatement.addBatch();

          preparedStatement.setInt(1, 3);
          preparedStatement.setString(2, "hello");
          preparedStatement.setDouble(3, 1.2);
          preparedStatement.setDate(4, sqlDate);
          preparedStatement.setTimestamp(5, ts);
          preparedStatement.setTime(6, tm);
          preparedStatement.addBatch();

          updateCounts = preparedStatement.executeBatch();

          // GS optimizes this into one insert execution, but we expand the
          // return count into an array
          assertEquals(2, updateCounts.length, "Number of update counts");

          // update count should be 1 for each
          assertEquals(1, updateCounts[0], "update count");
          assertEquals(1, updateCounts[1], "update count");
        }
        // test the inserted rows
        try (ResultSet resultSet =
            regularStatement.executeQuery("select * from testBind where a = 2")) {

          // assert we get 1 rows
          assertTrue(resultSet.next());
          assertEquals(2, resultSet.getInt(1), "int");
          assertEquals("hello", resultSet.getString(2), "string");
          assertEquals(1.2, resultSet.getDouble(3), 0, "double");
          assertEquals("2014-08-26", resultSet.getString(4), "date");
          assertEquals("Mon, 25 Aug 2014 20:52:00 -0700", resultSet.getString(5), "timestamp");
          assertEquals("03:25:45", resultSet.getString(6), "time");
        }

        try (ResultSet resultSet =
            regularStatement.executeQuery("select * from testBind where a = 3")) {

          // assert we get 1 rows
          assertTrue(resultSet.next());
          assertEquals(3, resultSet.getInt(1), "int");
          assertEquals("hello", resultSet.getString(2), "string");
          assertEquals(1.2, resultSet.getDouble(3), 0, "double");
          assertEquals("2014-08-26", resultSet.getString(4), "date");
          assertEquals("Mon, 25 Aug 2014 20:52:00 -0700", resultSet.getString(5), "timestamp");
          assertEquals("03:25:45", resultSet.getString(6), "time");
        }

        // describe mode
        try (PreparedStatement preparedStatement =
            connection.prepareStatement("select * from testBind WHERE to_number(a) = ?")) {

          resultSetMetaData = preparedStatement.getMetaData();
          assertEquals(7, resultSetMetaData.getColumnCount());
          assertEquals(Types.BIGINT, resultSetMetaData.getColumnType(1));
          assertEquals(Types.VARCHAR, resultSetMetaData.getColumnType(2));
          assertEquals(Types.DOUBLE, resultSetMetaData.getColumnType(3));
          assertEquals(Types.DATE, resultSetMetaData.getColumnType(4));
          assertEquals(Types.TIMESTAMP, resultSetMetaData.getColumnType(5));
          assertEquals(Types.TIME, resultSetMetaData.getColumnType(6));
          assertEquals(Types.DATE, resultSetMetaData.getColumnType(7));
        }

        try (PreparedStatement preparedStatement = connection.prepareStatement("select ?, ?")) {
          resultSetMetaData = preparedStatement.getMetaData();
          assertEquals(2, resultSetMetaData.getColumnCount());
          assertEquals(Types.VARCHAR, resultSetMetaData.getColumnType(1));
          assertEquals(Types.VARCHAR, resultSetMetaData.getColumnType(2));
        }

        try (PreparedStatement preparedStatement = connection.prepareStatement("select ?, ?")) {

          preparedStatement.setInt(1, 1);
          preparedStatement.setString(2, "hello");
          ResultSet result = preparedStatement.executeQuery();

          resultSetMetaData = result.getMetaData();
          assertEquals(2, resultSetMetaData.getColumnCount());
          assertEquals(Types.BIGINT, resultSetMetaData.getColumnType(1));
          assertEquals(Types.VARCHAR, resultSetMetaData.getColumnType(2));
        }

        // test null binding
        try (PreparedStatement preparedStatement = connection.prepareStatement("select ?")) {

          preparedStatement.setNull(1, Types.VARCHAR);
          try (ResultSet resultSet = preparedStatement.executeQuery()) {
            resultSetMetaData = resultSet.getMetaData();

            // assert column count
            assertEquals(1, resultSetMetaData.getColumnCount());
            assertEquals(Types.VARCHAR, resultSetMetaData.getColumnType(1));

            // assert we get 1 rows
            assertTrue(resultSet.next());
            assertNull(resultSet.getObject(1));
          }
          preparedStatement.setNull(1, Types.INTEGER);
          try (ResultSet resultSet = preparedStatement.executeQuery()) {
            resultSetMetaData = resultSet.getMetaData();

            // assert column count
            assertEquals(1, resultSetMetaData.getColumnCount());

            // assert we get 1 rows
            assertTrue(resultSet.next());
            assertNull(resultSet.getObject(1));
          }
        }
      }

      // bind in insert statement
      // create a test table
      try (Statement regularStatement = connection.createStatement()) {
        regularStatement.executeUpdate(
            "create or replace table testBind1(c1 double, c2 string, c3 date, "
                + "c4 date, c5 string, c6 date, c7 string, c8 string, "
                + "c9 string, c10 string, c11 string, c12 date, c13 string, "
                + "c14 float, c15 string, c16 string, c17 string, c18 string,"
                + "c19 string, c20 date, c21 string)");

        // array bind for insert
        try (PreparedStatement preparedStatement =
            connection.prepareStatement(
                "insert into testBind1 (c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, "
                    + "c12, c13, c14, c15, c16, c17, c18, c19, c20, c21) values "
                    + "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?,"
                    + " ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")) {

          for (int idx = 0; idx < 16; idx++) {
            addBindBatch(preparedStatement, sqlDate);
          }

          updateCounts = preparedStatement.executeBatch();

          // GS optimizes this into one insert execution
          assertEquals(16, updateCounts.length, "Number of update counts");

          for (int idx = 0; idx < 16; idx++) {
            assertEquals(1, updateCounts[idx], "update count");
          }
        }
      }
      connection.createStatement().execute("DROP TABLE testBind");
    }
  }

  @Test
  public void testTableBind() throws Throwable {
    ResultSetMetaData resultSetMetaData = null;

    try (Connection connection = getConnection();
        Statement regularStatement = connection.createStatement()) {
      try {
        // select * from table(?)
        try (PreparedStatement preparedStatement =
            connection.prepareStatement("SELECT * from table(?)")) {
          resultSetMetaData = preparedStatement.getMetaData();
          // we do not have any metadata, without a specified table
          assertEquals(0, resultSetMetaData.getColumnCount());

          preparedStatement.setString(1, ORDERS_JDBC);
          try (ResultSet resultSet = preparedStatement.executeQuery()) {
            resultSetMetaData = resultSet.getMetaData();
            assertEquals(9, resultSetMetaData.getColumnCount());
            // assert we have 73 rows
            for (int i = 0; i < 73; i++) {
              assertTrue(resultSet.next());
            }
            assertFalse(resultSet.next());
          }
        }

        // select * from table(?) where c1 = 1
        try (PreparedStatement preparedStatement =
            connection.prepareStatement("SELECT * from table(?) where c1 = 1")) {
          preparedStatement.setString(1, ORDERS_JDBC);
          try (ResultSet resultSet = preparedStatement.executeQuery()) {
            resultSetMetaData = resultSet.getMetaData();

            assertEquals(9, resultSetMetaData.getColumnCount());
            assertTrue(resultSet.next());
            assertFalse(resultSet.next());
          }
        }

        // select * from table(?) where c1 = 2 order by c3
        try (PreparedStatement preparedStatement =
            connection.prepareStatement("SELECT * from table(?) order by c3")) {
          preparedStatement.setString(1, ORDERS_JDBC);
          try (ResultSet resultSet = preparedStatement.executeQuery()) {
            resultSetMetaData = resultSet.getMetaData();

            assertEquals(9, resultSetMetaData.getColumnCount());
            // assert we have 73 rows
            for (int i = 0; i < 73; i++) {
              assertTrue(resultSet.next());
            }
            assertFalse(resultSet.next());
          }
        }

        regularStatement.execute("create or replace table testTableBind(c integer, d string)");
        // insert into table
        regularStatement.executeUpdate("insert into testTableBind (c, d) values (1, 'one')");
        // select c1, c from table(?), testTableBind
        try (PreparedStatement preparedStatement =
            connection.prepareStatement("SELECT * from table(?), testTableBind")) {
          preparedStatement.setString(1, ORDERS_JDBC);
          try (ResultSet resultSet = preparedStatement.executeQuery()) {
            resultSetMetaData = resultSet.getMetaData();

            assertEquals(11, resultSetMetaData.getColumnCount());
            // assert we have 73 rows
            for (int i = 0; i < 73; i++) {
              assertTrue(resultSet.next());
            }
            assertFalse(resultSet.next());
          }
        }

        // select * from table(?), table(?)
        try (PreparedStatement preparedStatement =
            connection.prepareStatement("SELECT * from table(?), table(?)")) {
          preparedStatement.setString(1, ORDERS_JDBC);
          preparedStatement.setString(2, "testTableBind");
          try (ResultSet resultSet = preparedStatement.executeQuery()) {
            resultSetMetaData = resultSet.getMetaData();

            assertEquals(11, resultSetMetaData.getColumnCount());
            // assert we have 73 rows
            for (int i = 0; i < 73; i++) {
              assertTrue(resultSet.next());
            }
            assertFalse(resultSet.next());
          }
        }

        // select tab1.c1, tab2.c from table(?) as a, table(?) as b
        try (PreparedStatement preparedStatement =
            connection.prepareStatement("SELECT a.c1, b.c from table(?) as a, table(?) as b")) {
          preparedStatement.setString(1, ORDERS_JDBC);
          preparedStatement.setString(2, "testTableBind");
          try (ResultSet resultSet = preparedStatement.executeQuery()) {
            resultSetMetaData = resultSet.getMetaData();

            assertEquals(2, resultSetMetaData.getColumnCount());
            // assert we have 73 rows
            for (int i = 0; i < 73; i++) {
              assertTrue(resultSet.next());
            }
            assertFalse(resultSet.next());
          }
        }
      } finally {
        regularStatement.execute("DROP TABLE testTableBind");
      }
    }
  }

  @Test
  public void testBindInWithClause() throws Throwable {
    try (Connection connection = getConnection();
        Statement regularStatement = connection.createStatement()) {
      try {
        // create a test table
        regularStatement.execute(
            "create or replace table testBind2(a int, b string, c double, "
                + "d date, e timestamp, f time, g date)");

        // bind in where clause
        try (PreparedStatement preparedStatement =
            connection.prepareStatement(
                "WITH V AS (SELECT * FROM testBind2 WHERE a = ?) " + "SELECT count(*) FROM V")) {

          preparedStatement.setInt(1, 100);
          try (ResultSet resultSet = preparedStatement.executeQuery()) {
            ResultSetMetaData resultSetMetaData = resultSet.getMetaData();

            // assert column count
            assertEquals(1, resultSetMetaData.getColumnCount());

            // assert we get 1 rows
            assertTrue(resultSet.next());
          }
        }
      } finally {
        regularStatement.execute("DROP TABLE testBind2");
      }
    }
  }

  @Test
  public void testBindTimestampNTZ() throws Throwable {

    try (Connection connection = getConnection();
        Statement regularStatement = connection.createStatement()) {
      try {
        // create a test table
        regularStatement.executeUpdate(
            "create or replace table testBindTimestampNTZ(a timestamp_ntz)");

        regularStatement.execute("alter session set client_timestamp_type_mapping='timestamp_ntz'");

        // bind in where clause
        try (PreparedStatement preparedStatement =
            connection.prepareStatement("insert into testBindTimestampNTZ values(?)")) {

          Timestamp ts = buildTimestamp(2014, 7, 26, 3, 52, 0, 0);
          preparedStatement.setTimestamp(1, ts);

          int updateCount = preparedStatement.executeUpdate();

          // update count should be 1
          assertEquals(1, updateCount, "update count");

          // test the inserted rows
          try (ResultSet resultSet =
              regularStatement.executeQuery("select * from testBindTimestampNTZ")) {

            // assert we get 1 rows
            assertTrue(resultSet.next());
            assertEquals("Tue, 26 Aug 2014 03:52:00 Z", resultSet.getString(1), "timestamp");

            regularStatement.executeUpdate("truncate table testBindTimestampNTZ");

            preparedStatement.setTimestamp(
                1, ts, Calendar.getInstance(TimeZone.getTimeZone("America/Los_Angeles")));

            updateCount = preparedStatement.executeUpdate();

            // update count should be 1
            assertEquals(1, updateCount, "update count");
          }
          // test the inserted rows
          try (ResultSet resultSet =
              regularStatement.executeQuery("select * from testBindTimestampNTZ")) {

            // assert we get 1 rows
            assertTrue(resultSet.next());
          }
        }
      } finally {
        regularStatement.execute("DROP TABLE testBindTimestampNTZ");
      }
    }
  }

  @Test
  public void testNullBind() throws Throwable {

    try (Connection connection = getConnection();
        Statement regularStatement = connection.createStatement()) {
      try {
        regularStatement.execute("create or replace table testNullBind(a double)");

        // array bind with nulls
        try (PreparedStatement preparedStatement =
            connection.prepareStatement("insert into testNullBind (a) values(?)")) {
          preparedStatement.setDouble(1, 1.2);
          preparedStatement.addBatch();

          preparedStatement.setObject(1, null);
          preparedStatement.addBatch();

          int[] updateCounts = preparedStatement.executeBatch();

          // GS optimizes this into one insert execution
          assertEquals(2, updateCounts.length, "Number of update counts");

          // update count should be 1
          assertEquals(1, updateCounts[0], "update count");
          assertEquals(1, updateCounts[1], "update count");

          preparedStatement.clearBatch();

          preparedStatement.setObject(1, null);
          preparedStatement.addBatch();

          preparedStatement.setDouble(1, 1.2);
          preparedStatement.addBatch();

          updateCounts = preparedStatement.executeBatch();

          // GS optimizes this into one insert execution
          assertEquals(2, updateCounts.length, "Number of update counts");

          // update count should be 1
          assertEquals(1, updateCounts[0], "update count");
          assertEquals(1, updateCounts[1], "update count");

          preparedStatement.clearBatch();

          preparedStatement.setObject(1, null);
          preparedStatement.addBatch();

          updateCounts = preparedStatement.executeBatch();

          // GS optimizes this into one insert execution
          assertEquals(1, updateCounts.length, "Number of update counts");

          // update count should be 1
          assertEquals(1, updateCounts[0], "update count");

          preparedStatement.clearBatch();

          // this test causes query count in GS not to be decremented because
          // the exception is thrown before registerQC. Discuss with Johnston
          // to resolve the issue before enabling the test.
          preparedStatement.setObject(1, "Null", Types.DOUBLE);
          preparedStatement.addBatch();
          SnowflakeSQLException ex =
              assertThrows(SnowflakeSQLException.class, preparedStatement::executeBatch);
          assertEquals(2086, ex.getErrorCode());

          preparedStatement.clearBatch();

          preparedStatement.setString(1, "hello");
          preparedStatement.addBatch();

          preparedStatement.setDouble(1, 1.2);
          ex = assertThrows(SnowflakeSQLException.class, preparedStatement::addBatch);
          assertEquals(
              (int) ErrorCode.ARRAY_BIND_MIXED_TYPES_NOT_SUPPORTED.getMessageCode(),
              ex.getErrorCode());
        }
      } finally {
        regularStatement.execute("DROP TABLE testNullBind");
      }
    }
  }

  @Test
  public void testSnow12603() throws Throwable {
    ResultSetMetaData resultSetMetaData = null;
    try (Connection connection = getConnection()) {
      try (PreparedStatement preparedStatement =
          connection.prepareStatement("SELECT ?, ?, ?, ?, ?, ?")) {

        java.sql.Date sqlDate = java.sql.Date.valueOf("2014-08-26");

        Timestamp ts = buildTimestamp(2014, 7, 26, 3, 52, 0, 0);

        preparedStatement.setObject(1, 1);
        preparedStatement.setObject(2, "hello");
        preparedStatement.setObject(3, new BigDecimal("1.3"));
        preparedStatement.setObject(4, Float.valueOf("1.3"));
        preparedStatement.setObject(5, sqlDate);
        preparedStatement.setObject(6, ts);
        try (ResultSet resultSet = preparedStatement.executeQuery()) {

          resultSetMetaData = resultSet.getMetaData();

          // assert column count
          assertEquals(6, resultSetMetaData.getColumnCount());
          assertEquals(Types.BIGINT, resultSetMetaData.getColumnType(1));
          assertEquals(Types.VARCHAR, resultSetMetaData.getColumnType(2));
          assertEquals(Types.DECIMAL, resultSetMetaData.getColumnType(3));
          assertEquals(Types.DOUBLE, resultSetMetaData.getColumnType(4));
          assertEquals(Types.DATE, resultSetMetaData.getColumnType(5));
          assertEquals(Types.TIMESTAMP, resultSetMetaData.getColumnType(6));

          // assert we get 1 rows
          assertTrue(resultSet.next());

          assertEquals(1, resultSet.getInt(1), "integer");
          assertEquals("hello", resultSet.getString(2), "string");
          assertEquals(new BigDecimal("1.3"), resultSet.getBigDecimal(3), "decimal");
          assertEquals(1.3, resultSet.getDouble(4), 0, "double");
          assertEquals("2014-08-26", resultSet.getString(5), "date");
          assertEquals("Mon, 25 Aug 2014 20:52:00 -0700", resultSet.getString(6), "timestamp");

          preparedStatement.setObject(1, 1, Types.INTEGER);
          preparedStatement.setObject(2, "hello", Types.VARCHAR);
          preparedStatement.setObject(3, new BigDecimal("1.3"), Types.DECIMAL);
          preparedStatement.setObject(4, Float.valueOf("1.3"), Types.DOUBLE);
          preparedStatement.setObject(5, sqlDate, Types.DATE);
          preparedStatement.setObject(6, ts, Types.TIMESTAMP);
        }
        try (ResultSet resultSet = preparedStatement.executeQuery()) {

          resultSetMetaData = resultSet.getMetaData();

          // assert column count
          assertEquals(6, resultSetMetaData.getColumnCount());
          assertEquals(Types.BIGINT, resultSetMetaData.getColumnType(1));
          assertEquals(Types.VARCHAR, resultSetMetaData.getColumnType(2));
          assertEquals(Types.DECIMAL, resultSetMetaData.getColumnType(3));
          assertEquals(Types.DOUBLE, resultSetMetaData.getColumnType(4));
          assertEquals(Types.DATE, resultSetMetaData.getColumnType(5));
          assertEquals(Types.TIMESTAMP, resultSetMetaData.getColumnType(6));

          // assert we get 1 rows
          assertTrue(resultSet.next());

          assertEquals(1, resultSet.getInt(1), "integer");
          assertEquals("hello", resultSet.getString(2), "string");
          assertEquals(new BigDecimal("1.3"), resultSet.getBigDecimal(3), "decimal");
          assertEquals(1.3, resultSet.getDouble(4), 0, "double");
          assertEquals("2014-08-26", resultSet.getString(5), "date");
          assertEquals("Mon, 25 Aug 2014 20:52:00 -0700", resultSet.getString(6), "timestamp");
        }
      }
    }
  }

  /** SNOW-6290: timestamp value is shifted by local timezone */
  @Test
  public void testSnow6290() throws Throwable {
    try (Connection connection = getConnection();
        Statement statement = connection.createStatement()) {
      try {
        // create test table
        statement.execute("CREATE OR REPLACE TABLE testSnow6290(ts timestamp)");

        PreparedStatement preparedStatement =
            connection.prepareStatement("INSERT INTO testSnow6290(ts) values(?)");

        Timestamp ts = new Timestamp(System.currentTimeMillis());

        preparedStatement.setTimestamp(1, ts);
        preparedStatement.executeUpdate();

        ResultSet res = statement.executeQuery("select ts from testSnow6290");

        assertTrue(res.next(), "expect a row");

        Timestamp tsFromDB = res.getTimestamp(1);

        assertEquals(ts.getTime(), tsFromDB.getTime(), "timestamp mismatch");
      } finally {
        statement.execute("DROP TABLE if exists testSnow6290");
      }
    }
  }

  /** SNOW-6986: null sql shouldn't be allowed */
  @Test
  public void testInvalidSQL() throws Throwable {
    try (Connection connection = getConnection();
        Statement statement = connection.createStatement()) {

      // execute DDLs
      SnowflakeSQLException ex =
          assertThrows(SnowflakeSQLException.class, () -> statement.executeQuery(null));
      assertEquals((int) ErrorCode.INVALID_SQL.getMessageCode(), ex.getErrorCode());
    }
  }

  @Test
  public void testGetObject() throws Throwable {
    ResultSetMetaData resultSetMetaData;

    try (Connection connection = getConnection();
        PreparedStatement preparedStatement = connection.prepareStatement("SELECT ?")) {
      // bind integer
      preparedStatement.setInt(1, 1);
      try (ResultSet resultSet = preparedStatement.executeQuery()) {

        resultSetMetaData = resultSet.getMetaData();

        assertEquals(
            Long.class.getName(),
            resultSetMetaData.getColumnClassName(1),
            "column class name=BigDecimal");

        // assert we get 1 rows
        assertTrue(resultSet.next());

        assertTrue(resultSet.getObject(1) instanceof Long, "integer");
      }
      preparedStatement.setString(1, "hello");
      try (ResultSet resultSet = preparedStatement.executeQuery()) {
        resultSetMetaData = resultSet.getMetaData();

        assertEquals(
            String.class.getName(),
            resultSetMetaData.getColumnClassName(1),
            "column class name=String");

        // assert we get 1 rows
        assertTrue(resultSet.next());

        assertTrue(resultSet.getObject(1) instanceof String, "string");
      }

      preparedStatement.setDouble(1, 1.2);
      try (ResultSet resultSet = preparedStatement.executeQuery()) {

        resultSetMetaData = resultSet.getMetaData();

        assertEquals(
            Double.class.getName(),
            resultSetMetaData.getColumnClassName(1),
            "column class name=Double");

        // assert we get 1 rows
        assertTrue(resultSet.next());

        assertTrue(resultSet.getObject(1) instanceof Double, "double");
      }

      preparedStatement.setTimestamp(1, new Timestamp(0));
      try (ResultSet resultSet = preparedStatement.executeQuery()) {

        resultSetMetaData = resultSet.getMetaData();

        assertEquals(
            Timestamp.class.getName(),
            resultSetMetaData.getColumnClassName(1),
            "column class name=Timestamp");

        // assert we get 1 rows
        assertTrue(resultSet.next());

        assertTrue(resultSet.getObject(1) instanceof Timestamp, "timestamp");
      }

      preparedStatement.setDate(1, new java.sql.Date(0));
      try (ResultSet resultSet = preparedStatement.executeQuery()) {
        resultSetMetaData = resultSet.getMetaData();

        assertEquals(
            Date.class.getName(),
            resultSetMetaData.getColumnClassName(1),
            "column class name=Date");

        // assert we get 1 rows
        assertTrue(resultSet.next());

        assertTrue(resultSet.getObject(1) instanceof Date, "date");
      }
    }
  }

  @Test
  public void testGetDoubleForNull() throws Throwable {
    try (Connection connection = getConnection();
        Statement stmt = connection.createStatement();
        ResultSet resultSet = stmt.executeQuery("select cast(null as int) as null_int")) {
      assertTrue(resultSet.next());
      assertEquals(0, resultSet.getDouble(1), 0.0001, "0 for null");
    }
  }

  // SNOW-27438
  @Test
  public void testGetDoubleForNaN() throws Throwable {
    try (Connection connection = getConnection();
        Statement stmt = connection.createStatement();
        ResultSet resultSet = stmt.executeQuery("select 'nan'::float")) {
      assertTrue(resultSet.next());
      assertThat("NaN for NaN", resultSet.getDouble(1), equalTo(Double.NaN));
    }
  }

  @Test
  public void testPutViaExecuteQuery() throws Throwable {
    try (Connection connection = getConnection();
        Statement statement = connection.createStatement()) {
      try {
        // load file test
        // create a unique data file name by using current timestamp in millis
        // test external table load
        statement.execute("CREATE OR REPLACE TABLE testPutViaExecuteQuery(a number)");

        // put files
        try (ResultSet resultSet =
            statement.executeQuery(
                "PUT file://"
                    + getFullPathFileInResource(TEST_DATA_FILE)
                    + " @%testPutViaExecuteQuery/orders parallel=10")) {

          ResultSetMetaData resultSetMetaData = resultSet.getMetaData();

          // assert column count
          assertTrue(resultSetMetaData.getColumnCount() > 0);
          // assert we get 1 rows
          for (int i = 0; i < 1; i++) {
            assertTrue(resultSet.next());
          }
        }
      } finally {
        statement.execute("DROP TABLE IF EXISTS testPutViaExecuteQuery");
      }
    }
  }

  @Disabled("takes 7 min. enable this for long running tests")
  @Test
  public void testSnow16332() throws Throwable {
    // use v1 query request API and inject 200ms socket timeout for first
    // http request to simulate network failure
    try (Connection conn = getConnection();
        Statement stmt = conn.createStatement()) {
      try {
        // create a table
        stmt.execute("CREATE OR REPLACE TABLE SNOW16332 (i int)");

        // make sure QC is JIT optimized. Change the GS JVM args to include
        // -Xcomp or -XX:CompileThreshold = < a number smaller than the
        // stmtCounter

        int stmtCounter = 2000;
        while (stmtCounter > 0) {
          // insert into it this should start a transaction.
          stmt.executeUpdate("INSERT INTO SNOW16332 VALUES (" + stmtCounter + ")");
          --stmtCounter;
        }

        try (Connection connWithNwError = getConnection(500)) { // inject socket timeout in ms
          try (Statement stmtWithNwError = connWithNwError.createStatement()) {

            // execute dml
            stmtWithNwError.executeUpdate(
                "INSERT INTO SNOW16332 "
                    + "SELECT seq8() "
                    + "FROM table(generator(timeLimit => 1))");

            // and execute another dml
            stmtWithNwError.executeUpdate(
                "INSERT INTO SNOW16332 "
                    + "SELECT seq8() "
                    + "FROM table(generator(timeLimit => 1))");
          }
        }
      } finally {
        stmt.executeQuery("DROP TABLE SNOW16332");
      }
    }
  }

  @Test
  public void testV1Query() throws Throwable {
    ResultSetMetaData resultSetMetaData = null;
    // use v1 query request API and inject 200ms socket timeout for first
    // http request to simulate network failure
    try (Connection connection = getConnection(200); // inject socket timeout = 200m
        Statement statement = connection.createStatement()) {

      // execute query
      try (ResultSet resultSet =
          statement.executeQuery("SELECT count(*) FROM table(generator(rowCount => 100000000))")) {
        resultSetMetaData = resultSet.getMetaData();

        // assert column count
        assertEquals(1, resultSetMetaData.getColumnCount());

        // assert we get 1 row
        for (int i = 0; i < 1; i++) {
          assertTrue(resultSet.next());
          assertTrue(resultSet.getInt(1) > 0);
        }
      }

      // Test parsing for timestamp with timezone value that has new encoding
      // where timezone index follows timestamp value
      try (ResultSet resultSet =
          statement.executeQuery("SELECT 'Fri, 23 Oct 2015 12:35:38 -0700'::timestamp_tz")) {
        resultSetMetaData = resultSet.getMetaData();

        // assert column count
        assertEquals(1, resultSetMetaData.getColumnCount());

        // assert we get 1 row
        for (int i = 0; i < 1; i++) {
          assertTrue(resultSet.next());
          assertEquals("Fri, 23 Oct 2015 12:35:38 -0700", resultSet.getString(1));
        }
      }
    }
  }

  @Test
  public void testCancelQuery() throws Throwable {
    try (Connection connection = getConnection();
        Statement statement = connection.createStatement()) {
      // schedule a cancel in 5 seconds
      Timer timer = new Timer();
      timer.schedule(
          new TimerTask() {
            @Override
            public void run() {
              try {
                statement.cancel();
              } catch (SQLException ex) {
                logger.log(Level.SEVERE, "Cancel failed with exception {}", ex);
              }
            }
          },
          5000);

      SQLException ex =
          assertThrows(
              SQLException.class,
              () ->
                  statement
                      .executeQuery("SELECT count(*) FROM TABLE(generator(timeLimit => 120))")
                      .close());
      // assert the sqlstate is what we expect (QUERY CANCELLED)
      assertEquals(SqlState.QUERY_CANCELED, ex.getSQLState(), "sqlstate mismatch");
    }
  }

  /** SNOW-14774: timestamp_ntz value should use client time zone to adjust the epoch time. */
  @Test
  public void testSnow14774() throws Throwable {
    Calendar calendar = null;
    Timestamp tsInUTC = null;
    Timestamp tsInLA = null;
    SimpleDateFormat sdf = null;
    String tsStrInLA = null;
    String tsStrInUTC = null;
    try (Connection connection = getConnection();
        Statement statement = connection.createStatement()) {
      // 30 minutes past daylight saving change (from 2am to 3am)
      try (ResultSet res = statement.executeQuery("select '2015-03-08 03:30:00'::timestamp_ntz")) {

        assertTrue(res.next());

        // get timestamp in UTC
        calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        tsInUTC = res.getTimestamp(1, calendar);

        sdf = new SimpleDateFormat("yyyy.MM.dd HH:mm:ss");
        sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
        tsStrInUTC = sdf.format(tsInUTC);

        // get timestamp in LA timezone
        calendar.setTimeZone(TimeZone.getTimeZone("America/Los_Angeles"));
        tsInLA = res.getTimestamp(1, calendar);

        sdf.setTimeZone(TimeZone.getTimeZone("America/Los_Angeles"));
        tsStrInLA = sdf.format(tsInLA);

        // the timestamp in LA and in UTC should be the same
        assertEquals(tsStrInUTC, tsStrInLA, "timestamp values not equal");
      }
      // 30 minutes before daylight saving change
      try (ResultSet res = statement.executeQuery("select '2015-03-08 01:30:00'::timestamp_ntz")) {

        assertTrue(res.next());

        // get timestamp in UTC
        calendar.setTimeZone(TimeZone.getTimeZone("UTC"));
        tsInUTC = res.getTimestamp(1, calendar);

        sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
        tsStrInUTC = sdf.format(tsInUTC);

        // get timestamp in LA timezone
        calendar.setTimeZone(TimeZone.getTimeZone("America/Los_Angeles"));
        tsInLA = res.getTimestamp(1, calendar);

        sdf.setTimeZone(TimeZone.getTimeZone("America/Los_Angeles"));
        tsStrInLA = sdf.format(tsInLA);

        // the timestamp in LA and in UTC should be the same
        assertEquals(tsStrInUTC, tsStrInLA, "timestamp values not equal");
      }
    }
  }

  /** SNOW-19172: getMoreResults should return false after executeQuery */
  @Test
  public void testSnow19172() throws SQLException {
    try (Connection connection = getConnection();
        Statement statement = connection.createStatement()) {
      statement.executeQuery("select 1");

      assertTrue(!statement.getMoreResults());
    }
  }

  @Test
  public void testSnow19819() throws Throwable {
    try (Connection connection = getConnection()) {
      try (Statement regularStatement = connection.createStatement()) {
        try {
          regularStatement.execute(
              "create or replace table testSnow19819(\n"
                  + "s string,\n"
                  + "v variant,\n"
                  + "t timestamp_ltz)\n");

          try (PreparedStatement preparedStatement =
              connection.prepareStatement(
                  "insert into testSnow19819 (s, v, t)\n"
                      + "select ?, parse_json(?), to_timestamp(?)")) {

            preparedStatement.setString(1, "foo");
            preparedStatement.setString(2, "{ }");
            preparedStatement.setString(3, "2016-05-12 12:15:00");
            preparedStatement.addBatch();

            preparedStatement.setString(1, "foo2");
            preparedStatement.setString(2, "{ \"a\": 1 }");
            preparedStatement.setString(3, "2016-05-12 12:16:00");
            preparedStatement.addBatch();

            preparedStatement.executeBatch();

            try (ResultSet resultSet =
                connection
                    .createStatement()
                    .executeQuery("SELECT s, v, t FROM testSnow19819 ORDER BY 1")) {
              assertThat("next result", resultSet.next());
              assertThat("String", resultSet.getString(1), equalTo("foo"));
              assertThat("Variant", resultSet.getString(2), equalTo("{}"));
              assertThat("next result", resultSet.next());
              assertThat("String", resultSet.getString(1), equalTo("foo2"));
              assertThat("Variant", resultSet.getString(2), equalTo("{\n  \"a\": 1\n}"));
              assertThat("no more result", !resultSet.next());
            }
          }
        } finally {
          regularStatement.execute("DROP TABLE testSnow19819");
        }
      }
    }
  }

  @Test
  @DontRunOnTestaccount
  public void testClientInfo() throws Throwable {
    System.setProperty(
        "snowflake.client.info",
        "{\"spark.version\":\"3.0.0\", \"spark.snowflakedb.version\":\"2.8.5\", \"spark.app.name\":\"SnowflakeSourceSuite\", \"scala.version\":\"2.12.11\", \"java.version\":\"1.8.0_221\", \"snowflakedb.jdbc.version\":\"3.13.2\"}");
    try (Connection connection = getConnection();
        Statement statement = connection.createStatement();
        ResultSet res = statement.executeQuery("select current_session_client_info()")) {

      assertTrue(res.next(), "result expected");

      String clientInfoJSONStr = res.getString(1);

      JsonNode clientInfoJSON = mapper.readTree(clientInfoJSONStr);

      // assert that spark version and spark app are found
      assertEquals("3.0.0", clientInfoJSON.get("spark.version").asText(), "spark version mismatch");
      assertEquals(
          "2.8.5",
          clientInfoJSON.get("spark.snowflakedb.version").asText(),
          "snowflakedb version mismatch");
      assertEquals(
          "SnowflakeSourceSuite",
          clientInfoJSON.get("spark.app.name").asText(),
          "spark app mismatch");

      closeSQLObjects(res, statement, connection);
    }
    System.clearProperty("snowflake.client.info");
  }

  @Test
  public void testLargeResultSet() throws Throwable {
    try (Connection connection = getConnection();
        // create statement
        Statement statement = connection.createStatement()) {
      String sql =
          "SELECT random()||random(), randstr(1000, random()) FROM table(generator(rowcount =>"
              + " 10000))";
      try (ResultSet result = statement.executeQuery(sql)) {
        int cnt = 0;
        while (result.next()) {
          ++cnt;
        }
        assertEquals(10000, cnt);
      }
    }
  }

  @Test
  @DontRunOnGithubActions
  public void testSnow26503() throws Throwable {
    ResultSetMetaData resultSetMetaData;
    String queryId = null;
    try (Connection connection = getConnection();
        // create a test table
        Statement regularStatement = connection.createStatement()) {
      try {
        regularStatement.execute(
            "create or replace table testBind2(a int) as select * from values(1),(2),(8),(10)");

        // test binds in BETWEEN predicate
        try (PreparedStatement preparedStatement =
            connection.prepareStatement("SELECT * FROM testBind2 WHERE a between ? and ?")) {
          preparedStatement.setInt(1, 3);
          preparedStatement.setInt(2, 9);
          // test that the query succeeds; used to fail with incident
          try (ResultSet resultSet = preparedStatement.executeQuery()) {
            resultSetMetaData = resultSet.getMetaData();

            // assert column count
            assertEquals(1, resultSetMetaData.getColumnCount());

            // assert we get 1 row
            assertTrue(resultSet.next());
          }
        }
        try (PreparedStatement preparedStatement =
                connection.prepareStatement("SELECT last_query_id()");
            ResultSet resultSet = preparedStatement.executeQuery()) {
          assertTrue(resultSet.next());
          queryId = resultSet.getString(1);
        }

        // check that the bind values can be retrieved using system$get_bind_values
        try (Connection snowflakeConnection = getSnowflakeAdminConnection()) {
          try (Statement regularStatementSF = snowflakeConnection.createStatement()) {
            regularStatementSF.execute("create or replace warehouse wh26503 warehouse_size=xsmall");

            try (PreparedStatement preparedStatement =
                snowflakeConnection.prepareStatement(
                    "select bv:\"1\":\"value\"::string, bv:\"2\":\"value\"::string from (select"
                        + " parse_json(system$get_bind_values(?)) bv)")) {
              preparedStatement.setString(1, queryId);
              try (ResultSet resultSet = preparedStatement.executeQuery()) {
                assertTrue(resultSet.next());

                // check that the bind values are correct
                assertEquals(3, resultSet.getInt(1));
                assertEquals(9, resultSet.getInt(2));
              }
            }
          }
          snowflakeConnection.createStatement().execute("DROP warehouse wh26503");
        }
      } finally {
        regularStatement.execute("DROP TABLE testBind2");
      }
    }
  }

  /**
   * Test binding variable when creating view or udf, which currently is not supported in Snowflake.
   * Exception will be thrown and returned back to user
   */
  @Test
  public void testSnow28530() throws Throwable {
    try (Connection connection = getConnection();
        Statement regularStatement = connection.createStatement()) {
      try {
        regularStatement.execute("create or replace table t(a number, b number)");

        /////////////////////////////////////////
        // bind variables in a view definition
        try (PreparedStatement preparedStatement =
            connection.prepareStatement("create or replace view v as select * from t where a=?")) {
          preparedStatement.setInt(1, 1);
          SnowflakeSQLException e =
              assertThrows(
                  SnowflakeSQLException.class,
                  preparedStatement::execute,
                  "Bind variable in view definition did not cause a user error");
          assertEquals(ERROR_CODE_BIND_VARIABLE_NOT_ALLOWED_IN_VIEW_OR_UDF_DEF, e.getErrorCode());
        }

        /////////////////////////////////////////////
        // bind variables in a scalar UDF definition
        try (PreparedStatement preparedStatement =
            connection.prepareStatement(
                "create or replace function f(n number) returns number as " + "'n + ?'")) {
          SnowflakeSQLException e =
              assertThrows(
                  SnowflakeSQLException.class,
                  preparedStatement::execute,
                  "Bind variable in scalar UDF definition did not cause a user error");
          assertEquals(ERROR_CODE_BIND_VARIABLE_NOT_ALLOWED_IN_VIEW_OR_UDF_DEF, e.getErrorCode());
        }

        ///////////////////////////////////////////
        // bind variables in a table UDF definition
        try (PreparedStatement preparedStatement =
            connection.prepareStatement(
                "create or replace function tf(n number) returns table(b number) as"
                    + " 'select b from t where a=?'")) {
          SnowflakeSQLException e =
              assertThrows(
                  SnowflakeSQLException.class,
                  preparedStatement::execute,
                  "Bind variable in table UDF definition did not cause a user error");
          assertEquals(ERROR_CODE_BIND_VARIABLE_NOT_ALLOWED_IN_VIEW_OR_UDF_DEF, e.getErrorCode());
        }
      } finally {
        regularStatement.execute("drop table t");
      }
    }
  }

  /**
   * SNOW-31104 improves the type inference for string constants that need to be coerced to numbers.
   * Verify that the same improvements work when the constant is a bind ref.
   */
  @Test
  public void testSnow31104() throws Throwable {

    Properties paramProperties = new Properties();
    paramProperties.put("TYPESYSTEM_WIDEN_CONSTANTS_EXACTLY", Boolean.TRUE.toString());
    try (Connection connection = getConnection(paramProperties);
        Statement regularStatement = connection.createStatement()) {
      // Repeat a couple of test cases from snow-31104.sql
      // We don't need to repeat all of them; we just need to verify
      // that string bind refs and null bind refs are treated the same as
      // string and null constants.
      try {
        regularStatement.execute("create or replace table t(n number)");

        regularStatement.executeUpdate(
            "insert into t values (1), (90000000000000000000000000000000000000)");

        try (PreparedStatement preparedStatement =
            connection.prepareStatement("select n, n > ? from t order by 1")) {
          preparedStatement.setString(1, "1");

          // this should not produce a user error
          try (ResultSet resultSet = preparedStatement.executeQuery()) {
            assertTrue(resultSet.next());
            assertFalse(resultSet.getBoolean(2));
            assertTrue(resultSet.next());
            assertTrue(resultSet.getBoolean(2));
          }
        }
        try (PreparedStatement preparedStatement =
            connection.prepareStatement("select n, '1' in (?, '256', n, 10) from t order by 1")) {
          preparedStatement.setString(1, null);

          try (ResultSet resultSet = preparedStatement.executeQuery()) {
            assertTrue(resultSet.next());
            assertTrue(resultSet.getBoolean(2));
            assertTrue(resultSet.next());
            assertNull(resultSet.getObject(2));
          }
        }
      } finally {
        regularStatement.execute("drop table t");
      }
    }
  }

  @Test
  @DontRunOnGithubActions
  public void testPutGet() throws Throwable {

    List<String> accounts = Arrays.asList(null, "s3testaccount", "azureaccount", "gcpaccount");
    for (int i = 0; i < accounts.size(); i++) {
      try (Connection connection = getConnection(accounts.get(i));
          Statement statement = connection.createStatement()) {
        try {
          String sourceFilePath = getFullPathFileInResource(TEST_DATA_FILE);

          File destFolder = new File(tmpFolder, "dest");
          destFolder.mkdirs();
          String destFolderCanonicalPath = destFolder.getCanonicalPath();
          String destFolderCanonicalPathWithSeparator = destFolderCanonicalPath + File.separator;

          statement.execute("alter session set ENABLE_GCP_PUT_EXCEPTION_FOR_OLD_DRIVERS=false");
          statement.execute("CREATE OR REPLACE STAGE testPutGet_stage");

          assertTrue(
              statement.execute("PUT file://" + sourceFilePath + " @testPutGet_stage"),
              "Failed to put a file");

          findFile(statement, "ls @testPutGet_stage/");

          // download the file we just uploaded to stage
          assertTrue(
              statement.execute(
                  "GET @testPutGet_stage 'file://" + destFolderCanonicalPath + "' parallel=8"),
              "Failed to get a file");

          // Make sure that the downloaded file exists, it should be gzip compressed
          File downloaded = new File(destFolderCanonicalPathWithSeparator + TEST_DATA_FILE + ".gz");
          assertTrue(downloaded.exists());

          Process p =
              Runtime.getRuntime()
                  .exec("gzip -d " + destFolderCanonicalPathWithSeparator + TEST_DATA_FILE + ".gz");
          p.waitFor();

          File original = new File(sourceFilePath);
          File unzipped = new File(destFolderCanonicalPathWithSeparator + TEST_DATA_FILE);
          assertEquals(original.length(), unzipped.length());
        } finally {
          statement.execute("DROP STAGE IF EXISTS testGetPut_stage");
        }
      }
    }
  }

  /**
   * Tests unencrypted named stages, which don't use client-side encryption to enable data lake
   * scenarios. Unencrypted named stages are specified with encryption=(TYPE='SNOWFLAKE_SSE').
   *
   * @throws Throwable
   */
  @Test
  @DontRunOnGithubActions
  public void testPutGetToUnencryptedStage() throws Throwable {

    List<String> accounts = Arrays.asList(null, "s3testaccount", "azureaccount", "gcpaccount");
    for (int i = 0; i < accounts.size(); i++) {
      try (Connection connection = getConnection(accounts.get(i));
          Statement statement = connection.createStatement()) {
        try {
          String sourceFilePath = getFullPathFileInResource(TEST_DATA_FILE);

          File destFolder = new File(tmpFolder, "dest");
          destFolder.mkdirs();
          String destFolderCanonicalPath = destFolder.getCanonicalPath();
          String destFolderCanonicalPathWithSeparator = destFolderCanonicalPath + File.separator;

          statement.execute("alter session set ENABLE_GCP_PUT_EXCEPTION_FOR_OLD_DRIVERS=false");
          statement.execute(
              "CREATE OR REPLACE STAGE testPutGet_unencstage encryption=(TYPE='SNOWFLAKE_SSE')");

          assertTrue(
              statement.execute("PUT file://" + sourceFilePath + " @testPutGet_unencstage"),
              "Failed to put a file");

          findFile(statement, "ls @testPutGet_unencstage/");

          // download the file we just uploaded to stage
          assertTrue(
              statement.execute(
                  "GET @testPutGet_unencstage 'file://" + destFolderCanonicalPath + "' parallel=8"),
              "Failed to get a file");

          // Make sure that the downloaded file exists, it should be gzip compressed
          File downloaded = new File(destFolderCanonicalPathWithSeparator + TEST_DATA_FILE + ".gz");
          assertTrue(downloaded.exists());

          Process p =
              Runtime.getRuntime()
                  .exec("gzip -d " + destFolderCanonicalPathWithSeparator + TEST_DATA_FILE + ".gz");
          p.waitFor();

          File original = new File(sourceFilePath);
          File unzipped = new File(destFolderCanonicalPathWithSeparator + TEST_DATA_FILE);
          assertEquals(original.length(), unzipped.length());
        } finally {
          statement.execute("DROP STAGE IF EXISTS testPutGet_unencstage");
        }
      }
    }
  }

  /** Prepare statement will fail if the connection is already closed. */
  @Test
  public void testNotClosedSession() throws SQLException {
    Connection connection = getConnection();
    connection.close();
    assertThrows(SnowflakeSQLException.class, () -> connection.prepareStatement("select 1"));
  }

  @Test
  @DontRunOnGithubActions
  public void testToTimestampNullBind() throws Throwable {
    try (Connection connection = getConnection();
        PreparedStatement preparedStatement =
            connection.prepareStatement(
                "select 3 where to_timestamp_ltz(?, 3) = '1970-01-01 00:00:12.345"
                    + " +000'::timestamp_ltz")) {
      // First test, normal usage.
      preparedStatement.setInt(1, 12345);

      try (ResultSet resultSet = preparedStatement.executeQuery()) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        // Assert column count.
        assertEquals(1, resultSetMetaData.getColumnCount());
        // Assert this returned a 3.
        assertTrue(resultSet.next());
        assertEquals(3, resultSet.getInt(1));
        assertFalse(resultSet.next());

        // Second test, input is null.
        preparedStatement.setNull(1, Types.INTEGER);
      }
      try (ResultSet resultSet = preparedStatement.executeQuery()) {
        // Assert no rows returned.
        assertFalse(resultSet.next());
      }
    }
    // NOTE: Don't add new tests here. Instead, add it to other appropriate test class or create a
    // new
    // one. This class is too large to have more tests.
  }
}
