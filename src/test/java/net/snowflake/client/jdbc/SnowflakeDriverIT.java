/*
 * Copyright (c) 2012-2020 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.client.jdbc;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.*;
import java.math.BigDecimal;
import java.nio.channels.FileChannel;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;
import net.snowflake.client.AbstractDriverIT;
import net.snowflake.client.ConditionalIgnoreRule;
import net.snowflake.client.RunningOnGithubAction;
import net.snowflake.client.category.TestCategoryOthers;
import net.snowflake.common.core.SqlState;
import org.apache.commons.io.FileUtils;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

/** General integration tests */
@Category(TestCategoryOthers.class)
public class SnowflakeDriverIT extends BaseJDBCTest {
  private static final int MAX_CONCURRENT_QUERIES_PER_USER = 50;
  private static final String getCurrenTransactionStmt = "SELECT CURRENT_TRANSACTION()";
  private static Logger logger = Logger.getLogger(SnowflakeDriverIT.class.getName());

  private static String ORDERS_JDBC = "ORDERS_JDBC";

  @Rule public TemporaryFolder tmpFolder = new TemporaryFolder();
  private ObjectMapper mapper = new ObjectMapper();

  @Rule public TemporaryFolder tmpFolder2 = new TemporaryFolder();

  public String testStageName =
      String.format("test_stage_%s", UUID.randomUUID().toString()).replaceAll("-", "_");

  @BeforeClass
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
            "Failed to put a file",
            statement.execute(
                "PUT file://" + getFullPathFileInResource(TEST_DATA_FILE) + " @%orders_jdbc"));
        assertTrue(
            "Failed to put a file",
            statement.execute(
                "PUT file://" + getFullPathFileInResource(TEST_DATA_FILE_2) + " @%orders_jdbc"));

        int numRows = statement.executeUpdate("copy into orders_jdbc");

        assertEquals("Unexpected number of rows copied: " + numRows, 73, numRows);
      }
    }
  }

  @AfterClass
  public static void tearDown() throws SQLException {
    try (Connection connection = getConnection()) {
      Statement statement = connection.createStatement();
      statement.execute("drop table if exists clustered_jdbc");
      statement.execute("drop table if exists orders_jdbc");
      statement.close();
    }
  }

  public static Connection getConnection(int injectSocketTimeout) throws SQLException {
    Connection connection = AbstractDriverIT.getConnection(injectSocketTimeout);

    Statement statement = connection.createStatement();
    statement.execute(
        "alter session set "
            + "TIMEZONE='America/Los_Angeles',"
            + "TIMESTAMP_TYPE_MAPPING='TIMESTAMP_LTZ',"
            + "TIMESTAMP_OUTPUT_FORMAT='DY, DD MON YYYY HH24:MI:SS TZHTZM',"
            + "TIMESTAMP_TZ_OUTPUT_FORMAT='DY, DD MON YYYY HH24:MI:SS TZHTZM',"
            + "TIMESTAMP_LTZ_OUTPUT_FORMAT='DY, DD MON YYYY HH24:MI:SS TZHTZM',"
            + "TIMESTAMP_NTZ_OUTPUT_FORMAT='DY, DD MON YYYY HH24:MI:SS TZHTZM'");
    statement.close();
    return connection;
  }

  public static Connection getConnection() throws SQLException {
    return getConnection(AbstractDriverIT.DONT_INJECT_SOCKET_TIMEOUT);
  }

  @Ignore
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
                Connection connection = null;
                Statement statement = null;
                ResultSet resultSet = null;
                ResultSetMetaData resultSetMetaData;

                try {
                  connection = getConnection();
                  statement = connection.createStatement();
                  resultSet = statement.executeQuery("SELECT system$sleep(10) % 1");
                  resultSetMetaData = resultSet.getMetaData();

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

                  statement.close();
                } finally {
                  closeSQLObjects(resultSet, statement, connection);
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
    Connection connection = null;
    Statement statement = null;
    ResultSet resultSet = null;

    try {
      Properties paramProperties = new Properties();
      connection = getConnection(paramProperties);
      statement = connection.createStatement();
      resultSet = statement.executeQuery("show columns in clustered_jdbc");
      assertEquals("number of columns", 2, countRows(resultSet));
    } finally {
      closeSQLObjects(resultSet, statement, connection);
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
    Connection connection = null;
    Statement statement = null;
    ResultSet resultSet = null;
    try {
      connection = getConnection();
      connection.createStatement().execute("alter session set rows_per_resultset=2048");

      statement = connection.createStatement();
      resultSet = statement.executeQuery("SELECT * FROM orders_jdbc");
      ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
      int numColumns = resultSetMetaData.getColumnCount();
      assertEquals(9, numColumns);
      assertEquals("number of columns", 73, countRows(resultSet));
      statement.close();
    } finally {
      closeSQLObjects(resultSet, statement, connection);
    }
  }

  @Test
  public void testDDLs() throws Throwable {
    Connection connection = null;
    Statement statement = null;
    try {
      connection = getConnection();

      statement = connection.createStatement();

      statement.execute("CREATE OR REPLACE TABLE testDDLs(version number, name string)");

    } finally {
      if (statement != null) {
        statement.execute("DROP TABLE testDDLs");
      }
      closeSQLObjects(statement, connection);
    }
  }

  private long getCurrentTransaction(Connection connection) throws SQLException {
    try (Statement statement = connection.createStatement()) {
      statement.execute(getCurrenTransactionStmt);
      ResultSet rs = statement.getResultSet();
      if (rs.next()) {
        String txnId = rs.getString(1);
        return txnId != null ? Long.valueOf(txnId) : 0L;
      }
    }

    throw new SQLException(getCurrenTransactionStmt + " didn't return a result.");
  }

  /** Tests autocommit */
  @Test
  public void testAutocommit() throws Throwable {
    Connection connection = null;
    Statement statement = null;
    ResultSet resultSet = null;

    try {
      connection = getConnection();

      statement = connection.createStatement();

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
      resultSet = statement.executeQuery("SELECT COUNT(*) FROM AUTOCOMMIT_API_TEST WHERE i = 1");
      assertTrue(resultSet.next());
      assertEquals(1, resultSet.getInt(1));
      resultSet.close();

      // 2. test rollback ==
      // delete from the table, should start a transaction.
      statement.executeUpdate("DELETE FROM AUTOCOMMIT_API_TEST");
      assertNotEquals(0, getCurrentTransaction(connection));

      // roll it back using the api
      connection.rollback();
      assertFalse(connection.getAutoCommit());
      assertEquals(0, getCurrentTransaction(connection));
      resultSet = statement.executeQuery("SELECT COUNT(*) FROM AUTOCOMMIT_API_TEST WHERE i = 1");
      assertTrue(resultSet.next());
      assertEquals(1, resultSet.getInt(1));
    } finally {
      if (statement != null) {
        statement.execute("DROP TABLE AUTOCOMMIT_API_TEST");
      }
      closeSQLObjects(resultSet, statement, connection);
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
      assertTrue("get constraint result row count", resultSet.next());

      if (pkTableName != null) {
        assertTrue(
            "get constraint result primary table name",
            pkTableName.equalsIgnoreCase(resultSet.getString(3)));
      }

      if (fkTableName != null) {
        assertTrue(
            "get constraint result foreign table name",
            fkTableName.equalsIgnoreCase(resultSet.getString(7)));
      }
    }
  }

  @Test
  public void testBoolean() throws Throwable {
    Connection connection = null;
    Statement statement = null;
    ResultSet resultSet = null;

    try {
      connection = getConnection();

      statement = connection.createStatement();
      statement.execute("alter SESSION set CLIENT_METADATA_REQUEST_USE_CONNECTION_CTX=true");

      DatabaseMetaData metadata = connection.getMetaData();

      // Create a table with boolean columns
      statement.execute("create or replace table testBooleanT1(c1 boolean)");

      // Insert values into the table
      statement.execute("insert into testBooleanT1 values(true), (false), (null)");

      // Get values from the table
      PreparedStatement preparedStatement =
          connection.prepareStatement("select c1 from testBooleanT1");

      // I. Test ResultSetMetaData interface
      resultSet = preparedStatement.executeQuery();

      ResultSetMetaData resultSetMetaData = resultSet.getMetaData();

      // Verify the column type is Boolean
      assertEquals(Types.BOOLEAN, resultSetMetaData.getColumnType(1));

      // II. Test DatabaseMetadata interface
      ResultSet columnMetaDataResultSet =
          metadata.getColumns(
              null, // catalog
              null, // schema
              "TESTBOOLEANT1", // table
              null // column
              );

      resultSetMetaData = columnMetaDataResultSet.getMetaData();

      // assert column count
      assertEquals(24, resultSetMetaData.getColumnCount());

      assertTrue(columnMetaDataResultSet.next());
      assertEquals(Types.BOOLEAN, columnMetaDataResultSet.getInt(5));
    } finally // cleanup
    {
      // drop the table
      if (statement != null) {
        statement.execute("drop table testBooleanT1");
      }
      closeSQLObjects(resultSet, statement, connection);
    }
  }

  @Test
  public void testConstraints() throws Throwable {
    Connection connection = null;
    Statement statement = null;
    ResultSet resultSet = null;

    try {
      connection = getConnection();

      statement = connection.createStatement();
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
      resultSet = metadata.getPrimaryKeys(null, null, "TESTCONSTRAINTSP1");

      // primary key for testConstraintsP1 should contain two rows
      assertConstraintResults(resultSet, 2, 6, "testConstraintsP1", null);

      resultSet = metadata.getPrimaryKeys(null, null, "TESTCONSTRAINTSP2");

      // primary key for testConstraintsP2 contains 1 row
      assertConstraintResults(resultSet, 1, 6, "testConstraintsP2", null);
      resultSet.close();
      resultSet.next();

      // Show imported keys
      resultSet = metadata.getImportedKeys(null, null, "TESTCONSTRAINTSF1");

      assertConstraintResults(resultSet, 2, 14, null, "testConstraintsF1");

      resultSet = metadata.getImportedKeys(null, null, "TESTCONSTRAINTSF2");

      assertConstraintResults(resultSet, 3, 14, null, "testConstraintsF2");
      resultSet.close();
      resultSet.next();

      // show exported keys
      resultSet = metadata.getExportedKeys(null, null, "TESTCONSTRAINTSP1");

      assertConstraintResults(resultSet, 4, 14, "testConstraintsP1", null);

      resultSet = metadata.getExportedKeys(null, null, "TESTCONSTRAINTSP2");

      assertConstraintResults(resultSet, 1, 14, "testConstraintsP2", null);
      resultSet.close();
      resultSet.next();

      // show cross references
      resultSet =
          metadata.getCrossReference(
              null, null, "TESTCONSTRAINTSP1", null, null, "TESTCONSTRAINTSF1");

      assertConstraintResults(resultSet, 2, 14, "testConstraintsP1", "testConstraintsF1");

      resultSet =
          metadata.getCrossReference(
              null, null, "TESTCONSTRAINTSP2", null, null, "TESTCONSTRAINTSF2");

      assertConstraintResults(resultSet, 1, 14, "testConstraintsP2", "testConstraintsF2");

      resultSet =
          metadata.getCrossReference(
              null, null, "TESTCONSTRAINTSP1", null, null, "TESTCONSTRAINTSF2");

      assertConstraintResults(resultSet, 2, 14, "testConstraintsP1", "testConstraintsF2");

      resultSet =
          metadata.getCrossReference(
              null, null, "TESTCONSTRAINTSP2", null, null, "TESTCONSTRAINTSF1");

      assertFalse(
          "cross reference from testConstraintsP2 to " + "testConstraintsF2 should be empty",
          resultSet.next());
      resultSet.close();
      resultSet.next();
    } finally {
      if (statement != null) {
        statement.execute("DROP TABLE TESTCONSTRAINTSF1");
        statement.execute("DROP TABLE TESTCONSTRAINTSF2");
        statement.execute("DROP TABLE TESTCONSTRAINTSP1");
        statement.execute("DROP TABLE TESTCONSTRAINTSP2");
      }
      closeSQLObjects(resultSet, statement, connection);
    }
  }

  @Test
  public void testQueryWithMaxRows() throws Throwable {
    Connection connection = null;
    Statement statement = null;
    ResultSet resultSet = null;
    final int maxRows = 30;

    try {
      connection = getConnection();
      statement = connection.createStatement();
      statement.setMaxRows(maxRows);
      resultSet = statement.executeQuery("SELECT * FROM orders_jdbc");

      // assert column count
      ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
      assertEquals(9, resultSetMetaData.getColumnCount());
      assertEquals(maxRows, countRows(resultSet));
    } finally {
      closeSQLObjects(resultSet, statement, connection);
    }
  }

  @Test
  public void testCancelQueryBySystemFunction() throws Throwable {
    Statement statement = null;
    ResultSet resultSet = null;

    final Connection connection = getConnection();

    try {
      // Get the current session identifier
      Statement getSessionIdStmt = connection.createStatement();
      getSessionIdStmt.setMaxRows(30);
      resultSet = getSessionIdStmt.executeQuery("SELECT current_session()");
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

      // execute a query for 120s
      statement = connection.createStatement();
      statement.setMaxRows(30);

      resultSet = statement.executeQuery("SELECT count(*) FROM TABLE(generator(timeLimit => 120))");

      fail("should raise an exception");
    } catch (SQLException ex) {
      // assert the sqlstate is what we expect (QUERY CANCELLED)
      assertEquals("sqlstate mismatch", SqlState.QUERY_CANCELED, ex.getSQLState());
    } finally {
      closeSQLObjects(resultSet, statement, connection);
    }
  }

  @Test
  public void testDBMetadata() throws Throwable {
    Connection connection = null;
    Statement statement = null;

    try {
      connection = getConnection();

      statement = connection.createStatement();
      statement.execute("alter SESSION set CLIENT_METADATA_REQUEST_USE_CONNECTION_CTX=true");

      // get database metadata
      DatabaseMetaData metaData = connection.getMetaData();

      // the following will issue
      ResultSet databaseSet = metaData.getCatalogs();
      assertTrue("databases shouldn't be empty", databaseSet.next());

      // "show schemas in [databaseName]"
      ResultSet schemaSet = metaData.getSchemas(connection.getCatalog(), connection.getSchema());
      assertTrue("schemas shouldn't be empty", schemaSet.next());
      assertTrue(
          "database should be " + connection.getCatalog(),
          connection.getCatalog().equalsIgnoreCase(schemaSet.getString(2)));
      assertTrue(
          "schema should be " + connection.getSchema(),
          connection.getSchema().equalsIgnoreCase(schemaSet.getString(1)));

      // snow tables in a schema
      ResultSet tableSet =
          metaData.getTables(
              connection.getCatalog(), connection.getSchema(), ORDERS_JDBC, null); // types
      assertTrue(
          String.format(
              "table %s should exists in db: %s, schema: %s",
              ORDERS_JDBC, connection.getCatalog(), connection.getSchema()),
          tableSet.next());
      assertTrue(
          "database should be " + connection.getCatalog(),
          connection.getCatalog().equalsIgnoreCase(schemaSet.getString(2)));
      assertTrue(
          "schema should be " + connection.getSchema(),
          connection.getSchema().equalsIgnoreCase(schemaSet.getString(1)));
      assertTrue(
          "table should be orders_jdbc", ORDERS_JDBC.equalsIgnoreCase(tableSet.getString(3)));

      ResultSet tableMetaDataResultSet =
          metaData.getTables(
              null, // catalog
              null, // schema
              ORDERS_JDBC, // table
              null); // types

      ResultSetMetaData resultSetMetaData = tableMetaDataResultSet.getMetaData();

      assertEquals(10, resultSetMetaData.getColumnCount());

      // assert we get 1 rows
      int cnt = 0;
      while (tableMetaDataResultSet.next()) {
        assertTrue(ORDERS_JDBC.equalsIgnoreCase(tableMetaDataResultSet.getString(3)));
        ++cnt;
      }
      assertEquals("number of tables", 1, cnt);

      tableMetaDataResultSet.close();

      // test pattern
      tableMetaDataResultSet =
          metaData.getTables(
              null, // catalog
              null, // schema
              "%", // table
              null); // types

      resultSetMetaData = tableMetaDataResultSet.getMetaData();

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
      assertTrue("orders_jdbc not found", found);

      tableMetaDataResultSet.close();

      // get column metadata
      ResultSet columnMetaDataResultSet = metaData.getColumns(null, null, ORDERS_JDBC, null);

      resultSetMetaData = columnMetaDataResultSet.getMetaData();

      // assert column count
      assertEquals(24, resultSetMetaData.getColumnCount());

      // assert we get 9 rows
      cnt = 0;
      while (columnMetaDataResultSet.next()) {
        // SNOW-16881: assert database name
        assertTrue(connection.getCatalog().equalsIgnoreCase(columnMetaDataResultSet.getString(1)));

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

      columnMetaDataResultSet.close();

      // create a table with mix cases
      statement = connection.createStatement();
      statement.execute("create or replace table \"testDBMetadata\" (a timestamp_ltz)");
      columnMetaDataResultSet = metaData.getColumns(null, null, "testDBMetadata", null);

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
    } finally {
      if (statement != null) {
        statement.execute("DROP TABLE IF EXISTS \"testDBMetadata\"");
      }
      closeSQLObjects(statement, connection);
    }
  }

  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testPutWithWildcardGCP() throws Throwable {
    Properties _connectionProperties = new Properties();
    _connectionProperties.put("inject_wait_in_put", 5);
    _connectionProperties.put("ssl", "off");
    Connection connection =
        getConnection(
            DONT_INJECT_SOCKET_TIMEOUT, _connectionProperties, false, false, "gcpaccount");
    Statement statement = connection.createStatement();

    String sourceFilePath = getFullPathFileInResource(TEST_DATA_FILE);
    // replace file name with wildcard character
    sourceFilePath = sourceFilePath.replace("orders_100.csv", "orders_10*.csv");

    File destFolder = tmpFolder.newFolder();
    String destFolderCanonicalPath = destFolder.getCanonicalPath();
    String destFolderCanonicalPathWithSeparator = destFolderCanonicalPath + File.separator;

    try {
      statement.execute("CREATE OR REPLACE STAGE wildcard_stage");
      assertTrue(
          "Failed to put a file",
          statement.execute("PUT file://" + sourceFilePath + " @wildcard_stage"));

      findFile(statement, "ls @wildcard_stage/");

      assertTrue(
          "Failed to get files",
          statement.execute(
              "GET @wildcard_stage 'file://" + destFolderCanonicalPath + "' parallel=8"));

      File downloaded;
      // download the files we just uploaded to stage
      for (int i = 0; i < fileNames.length; i++) {
        // Make sure that the downloaded file exists, it should be gzip compressed
        downloaded = new File(destFolderCanonicalPathWithSeparator + fileNames[i] + ".gz");
        assert (downloaded.exists());

        Process p =
            Runtime.getRuntime()
                .exec("gzip -d " + destFolderCanonicalPathWithSeparator + fileNames[i] + ".gz");
        p.waitFor();

        String individualFilePath = sourceFilePath.replace("orders_10*.csv", fileNames[i]);

        File original = new File(individualFilePath);
        File unzipped = new File(destFolderCanonicalPathWithSeparator + fileNames[i]);
        assert (original.length() == unzipped.length());
        assert (FileUtils.contentEquals(original, unzipped));
      }

    } finally {
      statement.execute("DROP STAGE IF EXISTS wildcard_stage");
      statement.close();
      connection.close();
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
    FileChannel fIn = inputStream.getChannel();
    FileChannel fOut = outputStream.getChannel();
    fOut.transferFrom(fIn, 0, fIn.size());
    fIn.position(0);
    fOut.transferFrom(fIn, fIn.size(), fIn.size());
    fOut.close();
    fIn.close();
  }

  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testPutGetLargeFileGCP() throws Throwable {
    Connection connection = getConnection("gcpaccount");
    Statement statement = connection.createStatement();

    File destFolder = tmpFolder.newFolder();
    String destFolderCanonicalPath = destFolder.getCanonicalPath();
    String destFolderCanonicalPathWithSeparator = destFolderCanonicalPath + File.separator;

    File largeTempFile = tmpFolder.newFile("largeFile.csv");
    BufferedWriter bw = new BufferedWriter(new FileWriter(largeTempFile));
    bw.write("Creating large test file for GCP PUT/GET test");
    bw.write(System.lineSeparator());
    bw.write("Creating large test file for GCP PUT/GET test");
    bw.write(System.lineSeparator());
    bw.close();
    File largeTempFile2 = tmpFolder.newFile("largeFile2.csv");

    String sourceFilePath = largeTempFile.getCanonicalPath();

    try {
      // copy info from 1 file to another and continue doubling file size until we reach ~1.5GB,
      // which is a large file
      for (int i = 0; i < 12; i++) {
        copyContentFrom(largeTempFile, largeTempFile2);
        copyContentFrom(largeTempFile2, largeTempFile);
      }

      // create a stage to put the file in
      statement.execute("CREATE OR REPLACE STAGE largefile_stage");
      assertTrue(
          "Failed to put a file",
          statement.execute("PUT file://" + sourceFilePath + " @largefile_stage"));

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
          "Failed to get files",
          statement.execute(
              "GET @extra_stage 'file://" + destFolderCanonicalPath + "' parallel=8"));

      // Make sure that the downloaded file exists; it should be gzip compressed
      File downloaded = new File(destFolderCanonicalPathWithSeparator + "bigFile.csv.gz");
      assert (downloaded.exists());

      // unzip the file
      Process p =
          Runtime.getRuntime()
              .exec("gzip -d " + destFolderCanonicalPathWithSeparator + "bigFile.csv.gz");
      p.waitFor();

      // compare the original file with the file that's been uploaded, copied into a table, copied
      // back into a stage,
      // downloaded, and unzipped
      File unzipped = new File(destFolderCanonicalPathWithSeparator + "bigFile.csv");
      assert (largeTempFile.length() == unzipped.length());
      assert (FileUtils.contentEquals(largeTempFile, unzipped));
    } finally {
      statement.execute("DROP STAGE IF EXISTS largefile_stage");
      statement.execute("DROP STAGE IF EXISTS extra_stage");
      statement.execute("DROP TABLE IF EXISTS large_table");
      statement.close();
      connection.close();
    }
  }

  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testPutOverwrite() throws Throwable {
    Connection connection = null;
    Statement statement = null;

    // create 2 files: an original, and one that will overwrite the original
    File file1 = tmpFolder.newFile("testfile.csv");
    BufferedWriter bw = new BufferedWriter(new FileWriter(file1));
    bw.write("Writing original file content. This should get overwritten.");
    bw.close();

    File file2 = tmpFolder2.newFile("testfile.csv");
    bw = new BufferedWriter(new FileWriter(file2));
    bw.write("This is all new! This should be the result of the overwriting.");
    bw.close();

    String sourceFilePathOriginal = file1.getCanonicalPath();
    String sourceFilePathOverwrite = file2.getCanonicalPath();

    File destFolder = tmpFolder.newFolder();
    String destFolderCanonicalPath = destFolder.getCanonicalPath();
    String destFolderCanonicalPathWithSeparator = destFolderCanonicalPath + File.separator;

    List<String> accounts = Arrays.asList(null, "s3testaccount", "azureaccount", "gcpaccount");
    for (int i = 0; i < accounts.size(); i++) {
      try {
        connection = getConnection(accounts.get(i));

        statement = connection.createStatement();

        // create a stage to put the file in
        statement.execute("CREATE OR REPLACE STAGE testing_stage");
        assertTrue(
            "Failed to put a file",
            statement.execute("PUT file://" + sourceFilePathOriginal + " @testing_stage"));
        // check that file exists in stage after PUT
        findFile(statement, "ls @testing_stage/");

        // put another file in same stage with same filename with overwrite = true
        assertTrue(
            "Failed to put a file",
            statement.execute(
                "PUT file://" + sourceFilePathOverwrite + " @testing_stage overwrite=true"));

        // check that file exists in stage after PUT
        findFile(statement, "ls @testing_stage/");

        // get file from new stage
        assertTrue(
            "Failed to get files",
            statement.execute(
                "GET @testing_stage 'file://" + destFolderCanonicalPath + "' parallel=8"));

        // Make sure that the downloaded file exists; it should be gzip compressed
        File downloaded = new File(destFolderCanonicalPathWithSeparator + "testfile.csv.gz");
        assert (downloaded.exists());

        // unzip the file
        Process p =
            Runtime.getRuntime()
                .exec("gzip -d " + destFolderCanonicalPathWithSeparator + "testfile.csv.gz");
        p.waitFor();

        File unzipped = new File(destFolderCanonicalPathWithSeparator + "testfile.csv");
        assert (FileUtils.contentEqualsIgnoreEOL(file2, unzipped, null));
      } finally {
        statement.execute("DROP TABLE IF EXISTS testLoadToLocalFS");
        statement.close();
      }
    }
  }

  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testPut() throws Throwable {
    Connection connection = null;
    Statement statement = null;
    ResultSet resultSet = null;

    List<String> accounts = Arrays.asList(null, "s3testaccount", "azureaccount", "gcpaccount");
    for (int i = 0; i < accounts.size(); i++) {
      try {
        connection = getConnection(accounts.get(i));

        statement = connection.createStatement();

        // load file test
        // create a unique data file name by using current timestamp in millis
        try {
          // test external table load
          statement.execute("CREATE OR REPLACE TABLE testLoadToLocalFS(a number)");

          // put files
          assertTrue(
              "Failed to put a file",
              statement.execute(
                  "PUT file://"
                      + getFullPathFileInResource(TEST_DATA_FILE)
                      + " @%testLoadToLocalFS/orders parallel=10"));

          resultSet = statement.getResultSet();

          ResultSetMetaData resultSetMetaData = resultSet.getMetaData();

          // assert column count
          assertTrue(resultSetMetaData.getColumnCount() > 0);

          assertTrue(resultSet.next()); // one row
          assertFalse(resultSet.next());

          findFile(
              statement, "ls @%testLoadToLocalFS/ pattern='.*orders/" + TEST_DATA_FILE + ".g.*'");

          // remove files
          resultSet =
              statement.executeQuery(
                  "rm @%testLoadToLocalFS/ pattern='.*orders/" + TEST_DATA_FILE + ".g.*'");

          resultSetMetaData = resultSet.getMetaData();

          // assert column count
          assertTrue(resultSetMetaData.getColumnCount() >= 1);

          // assert we get 1 row for the file we copied
          assertTrue(resultSet.next());
          assertNotNull(resultSet.getString(1));
          assertFalse(resultSet.next());
          try {
            resultSet.getString(1); // no more row
            fail("must fail");
          } catch (SQLException ex) {
            assertEquals((int) ErrorCode.COLUMN_DOES_NOT_EXIST.getMessageCode(), ex.getErrorCode());
          }

          Thread.sleep(100);

          // show files again
          resultSet = statement.executeQuery("ls @%testLoadToLocalFS/ pattern='.*orders/orders.*'");

          // assert we get 0 row
          assertFalse(resultSet.next());

        } finally {
          statement.execute("DROP TABLE IF EXISTS testLoadToLocalFS");
          statement.close();
        }

      } finally {
        closeSQLObjects(resultSet, statement, connection);
      }
    }
  }

  static void findFile(Statement statement, String checkSQL) throws Throwable {
    boolean fileFound = false;
    ResultSet resultSet = null;

    // tolerate at most 60 tries for the following loop
    for (int numSecs = 0; numSecs <= 60; numSecs++) {
      // show files
      resultSet = statement.executeQuery(checkSQL);

      if (resultSet.next()) {
        fileFound = true;
        break;
      }
      // give enough time for s3 eventual consistency for US region
      Thread.sleep(1000);
    }
    assertTrue("Could not find a file", fileFound);

    // assert the first column not null
    assertNotNull("Null result", resultSet.getString(1));
  }

  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testSQLError42S02() throws SQLException {
    Connection connection = null;
    Statement statement = null;
    ResultSet resultSet = null;

    try {
      connection = getConnection();

      statement = connection.createStatement();

      // execute a bad query
      try {
        resultSet = statement.executeQuery("SELECT * FROM nonexistence");

        fail("SQL exception not raised");
      } catch (SQLException ex1) {
        // assert the sqlstate "42S02" which means BASE_TABLE_OR_VIEW_NOT_FOUND
        assertEquals("sqlstate mismatch", "42S02", ex1.getSQLState());
      }
    } finally {
      closeSQLObjects(resultSet, statement, connection);
    }
  }

  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testExplainPlan() throws Throwable {
    Connection connection = null;
    Statement statement = null;
    ResultSet resultSet = null;

    try {
      connection = getConnection();
      statement = connection.createStatement();

      // test explain plan: sorry not available for general but debugging purpose only
      resultSet = statement.executeQuery("EXPLAIN PLAN FOR SELECT c1 FROM orders_jdbc");

      ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
      assertTrue("must return more than 4 columns", resultSetMetaData.getColumnCount() >= 4);
      assertTrue("must return more than 3 rows", countRows(resultSet) > 3);

      statement.close();

    } finally {
      closeSQLObjects(resultSet, statement, connection);
    }
  }

  @Test
  public void testTimestampParsing() throws Throwable {
    Connection connection = null;
    Statement statement = null;
    ResultSet resultSet = null;

    try {
      connection = getConnection();

      statement = connection.createStatement();
      resultSet =
          statement.executeQuery(
              "select to_timestamp('2013-05-08T15:39:20.123-07:00') from orders_jdbc");

      assertTrue(resultSet.next());
      assertEquals("Wed, 08 May 2013 15:39:20 -0700", resultSet.getString(1));
    } finally {
      closeSQLObjects(resultSet, statement, connection);
    }
  }

  @Test
  public void testDateParsing() throws Throwable {
    Connection connection = null;
    Statement statement = null;
    ResultSet resultSet = null;

    try {
      connection = getConnection();
      statement = connection.createStatement();
      resultSet = statement.executeQuery("select to_date('0001-01-01')");

      assertTrue(resultSet.next());
      assertEquals("0001-01-01", resultSet.getString(1));
    } finally {
      closeSQLObjects(resultSet, statement, connection);
    }
  }

  @Test
  public void testTimeParsing() throws Throwable {
    Connection connection = null;
    Statement statement = null;
    ResultSet resultSet = null;

    try {
      connection = getConnection();
      statement = connection.createStatement();
      resultSet = statement.executeQuery("select to_time('15:39:20.123') from orders_jdbc");

      assertTrue(resultSet.next());
      assertEquals("15:39:20", resultSet.getString(1));
    } finally {
      closeSQLObjects(resultSet, statement, connection);
    }
  }

  @Test
  public void testClientSideSorting() throws Throwable {
    Connection connection = null;
    Statement statement = null;
    ResultSet resultSet = null;

    try {
      connection = getConnection();

      statement = connection.createStatement();

      // turn on sorting mode
      statement.execute("set-sf-property sort on");

      resultSet = statement.executeQuery("SELECT c3 FROM orders_jdbc");

      ResultSetMetaData resultSetMetaData = resultSet.getMetaData();

      // assert column count
      assertEquals(1, resultSetMetaData.getColumnCount());

      // assert the values for the first 5 rows
      for (int i = 0; i < 5; i++) {
        assertTrue(resultSet.next());

        // assert each column is 'F'
        assertEquals("F", resultSet.getString(1));
      }

      // turn off sorting mode
      statement.execute("set-sf-property sort off");

      resultSet = statement.executeQuery("SELECT c3 FROM orders_jdbc order by c3 desc");

      resultSetMetaData = resultSet.getMetaData();

      // assert column count
      assertEquals(1, resultSetMetaData.getColumnCount());

      // assert the values for the first 4 rows
      for (int i = 0; i < 4; i++) {
        assertTrue(resultSet.next());

        // assert each column is 'P'
        assertEquals("P", resultSet.getString(1));
      }
    } finally {
      closeSQLObjects(resultSet, statement, connection);
    }
  }

  @Test
  public void testUpdateCount() throws Throwable {
    Connection connection = null;
    Statement statement = null;

    try {
      connection = getConnection();

      statement = connection.createStatement();

      // create test table
      statement.execute("CREATE OR REPLACE TABLE testUpdateCount(version number, name string)");

      // insert two rows
      int numRows =
          statement.executeUpdate("INSERT INTO testUpdateCount values (1, 'a'), (2, 'b')");

      assertEquals("Unexpected number of rows inserted: " + numRows, 2, numRows);
    } finally {
      if (statement != null) {
        statement.execute("DROP TABLE if exists testUpdateCount");
      }
      closeSQLObjects(null, statement, connection);
    }
  }

  @Test
  public void testSnow4245() throws Throwable {
    Connection connection = null;
    Statement statement = null;
    ResultSet resultSet = null;

    try {
      connection = getConnection();

      statement = connection.createStatement();
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
      assertEquals("Unexpected number of rows inserted: " + numRows, 3, numRows);

      // query the data
      resultSet =
          statement.executeQuery(
              "SELECT * FROM testSnow4245 order by 1 "
                  + "nulls first, 2 nulls first, 3 nulls first");

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
    } finally {
      if (statement != null) {
        statement.execute("drop table testSnow4245");
      }
      closeSQLObjects(resultSet, statement, connection);
    }
  }

  /** SNOW-4394 - Four bytes UTF-8 characters are not returned correctly. */
  @Test
  public void testSnow4394() throws Throwable {
    Connection connection = null;
    Statement statement = null;

    String tableName =
        String.format("snow4394_%s", UUID.randomUUID().toString()).replaceAll("-", "_");

    try {
      connection = getConnection();

      statement = connection.createStatement();

      // create test table
      statement.execute(String.format("CREATE OR REPLACE TABLE %s(str string)", tableName));

      String data = "What is \ud83d\ude12?";
      // insert two rows
      int numRows =
          statement.executeUpdate(
              String.format("INSERT INTO %s(str) values('%s')", tableName, data));
      assertEquals("Unexpected number of rows inserted: " + numRows, 1, numRows);

      ResultSet rset = statement.executeQuery(String.format("SELECT str FROM %s", tableName));
      String ret = null;
      while (rset.next()) {
        ret = rset.getString(1);
      }
      rset.close();
      assertEquals("Unexpected string value: " + ret, data, ret);
    } finally {
      if (statement != null) {
        statement.execute(String.format("DROP TABLE if exists %s", tableName));
        statement.close();
      }
      closeSQLObjects(null, statement, connection);
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
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void test31448() throws Throwable {
    Connection connection = getConnection();

    Statement statement = connection.createStatement();

    statement.execute("alter session set enable_fix_31448_2=2, " + "error_on_generic_pruner=true;");

    statement.execute("alter session set timestamp_type_mapping=timestamp_ntz");

    statement.execute("create or replace table " + "bug56658(iv number, tsv timestamp_ntz)");
    statement.execute(
        "insert into bug56658 select seq8(), "
            + "timestampadd(day, seq8(), '1970-01-13 00:00:00'::timestamp_ntz)\n"
            + "from table(generator(rowcount=>20))");

    connection
        .unwrap(SnowflakeConnectionV1.class)
        .getSfSession()
        .setTimestampMappedType(SnowflakeType.TIMESTAMP_NTZ);
    Timestamp ts = buildTimestamp(1970, 0, 15, 10, 14, 30, 0);
    PreparedStatement preparedStatement =
        connection.prepareStatement(
            "select iv, tsv from bug56658 where tsv" + " >= ? and tsv <= ? order by iv;");
    statement.execute("alter session set timestamp_type_mapping=timestamp_ntz");
    Timestamp ts2 = buildTimestamp(1970, 0, 18, 10, 14, 30, 0);
    preparedStatement.setTimestamp(1, ts);
    preparedStatement.setTimestamp(2, ts2);
    preparedStatement.executeQuery();
  }

  @Test
  public void testBind() throws Throwable {
    Connection connection = null;
    PreparedStatement preparedStatement = null;
    Statement regularStatement = null;
    ResultSet resultSet = null;

    try {
      connection = getConnection();

      preparedStatement = connection.prepareStatement("SELECT ?, ?");

      // bind integer
      preparedStatement.setInt(1, 1);
      preparedStatement.setString(2, "hello");
      resultSet = preparedStatement.executeQuery();

      ResultSetMetaData resultSetMetaData = resultSet.getMetaData();

      // assert column count
      assertEquals(2, resultSetMetaData.getColumnCount());
      assertEquals(Types.BIGINT, resultSetMetaData.getColumnType(1));
      assertEquals(Types.VARCHAR, resultSetMetaData.getColumnType(2));

      // assert we get 1 rows
      assertTrue(resultSet.next());

      assertEquals("integer", 1, resultSet.getInt(1));
      assertEquals("string", "hello", resultSet.getString(2));

      // bind float
      preparedStatement.setDouble(1, 1.2);
      resultSet = preparedStatement.executeQuery();

      resultSetMetaData = resultSet.getMetaData();

      // assert column count
      assertEquals(2, resultSetMetaData.getColumnCount());
      assertEquals(Types.DOUBLE, resultSetMetaData.getColumnType(1));

      // assert we get 1 rows
      assertTrue(resultSet.next());
      assertEquals("double", 1.2, resultSet.getDouble(1), 0);
      assertEquals("string", "hello", resultSet.getString(2));

      // bind string
      preparedStatement.setString(1, "hello");
      resultSet = preparedStatement.executeQuery();

      resultSetMetaData = resultSet.getMetaData();

      // assert column count
      assertEquals(2, resultSetMetaData.getColumnCount());
      assertEquals(Types.VARCHAR, resultSetMetaData.getColumnType(1));

      // assert we get 1 rows
      assertTrue(resultSet.next());
      assertEquals("string1", "hello", resultSet.getString(1));
      assertEquals("string2", "hello", resultSet.getString(2));

      // bind date
      java.sql.Date sqlDate = java.sql.Date.valueOf("2014-08-26");
      preparedStatement.setDate(1, sqlDate);
      resultSet = preparedStatement.executeQuery();

      resultSetMetaData = resultSet.getMetaData();

      // assert column count
      assertEquals(2, resultSetMetaData.getColumnCount());
      assertEquals(Types.DATE, resultSetMetaData.getColumnType(1));

      // assert we get 1 rows
      assertTrue(resultSet.next());
      assertEquals("string", "2014-08-26", resultSet.getString(1));
      assertEquals("string", "hello", resultSet.getString(2));

      // bind timestamp
      Timestamp ts = buildTimestamp(2014, 7, 26, 3, 52, 0, 0);
      preparedStatement.setTimestamp(1, ts);
      resultSet = preparedStatement.executeQuery();

      resultSetMetaData = resultSet.getMetaData();

      // assert column count
      assertEquals(2, resultSetMetaData.getColumnCount());
      assertEquals(Types.TIMESTAMP, resultSetMetaData.getColumnType(1));

      // assert we get 1 rows
      assertTrue(resultSet.next());
      assertEquals(
          "Incorrect timestamp", "Mon, 25 Aug 2014 20:52:00 -0700", resultSet.getString(1));
      assertEquals("string", "hello", resultSet.getString(2));

      // bind time
      Time tm = new Time(12345678); // 03:25:45.678
      preparedStatement.setTime(1, tm);
      resultSet = preparedStatement.executeQuery();

      resultSetMetaData = resultSet.getMetaData();

      // assert column count
      assertEquals(2, resultSetMetaData.getColumnCount());
      assertEquals(Types.TIME, resultSetMetaData.getColumnType(1));

      // assert we get 1 rows
      assertTrue(resultSet.next());
      assertEquals("Incorrect time", "03:25:45", resultSet.getString(1));
      assertEquals("string", "hello", resultSet.getString(2));

      preparedStatement.close();

      // bind in where clause
      preparedStatement =
          connection.prepareStatement("SELECT * FROM orders_jdbc WHERE to_number(c1) = ?");

      preparedStatement.setInt(1, 100);
      resultSet = preparedStatement.executeQuery();
      resultSetMetaData = resultSet.getMetaData();

      // assert column count
      assertEquals(9, resultSetMetaData.getColumnCount());
      assertEquals(Types.VARCHAR, resultSetMetaData.getColumnType(1));
      assertEquals(Types.VARCHAR, resultSetMetaData.getColumnType(2));

      // assert we get 1 rows
      assertTrue(resultSet.next());
      assertEquals("c1", "100", resultSet.getString(1));
      assertEquals("c2", "147004", resultSet.getString(2));

      preparedStatement.close();

      // bind in insert statement
      // create a test table
      regularStatement = connection.createStatement();
      regularStatement.executeUpdate(
          "create or replace table testBind(a int, b string, c double, d date, "
              + "e timestamp, f time, g date)");

      preparedStatement =
          connection.prepareStatement(
              "insert into testBind(a, b, c, d, e, f) values(?, ?, ?, ?, ?, ?)");

      preparedStatement.setInt(1, 1);
      preparedStatement.setString(2, "hello");
      preparedStatement.setDouble(3, 1.2);
      preparedStatement.setDate(4, sqlDate);
      preparedStatement.setTimestamp(5, ts);
      preparedStatement.setTime(6, tm);
      int rowCount = preparedStatement.executeUpdate();

      // update count should be 1
      assertEquals("update count", 1, rowCount);

      // test the inserted rows
      resultSet = regularStatement.executeQuery("select * from testBind");

      // assert we get 1 rows
      assertTrue(resultSet.next());
      assertEquals("int", 1, resultSet.getInt(1));
      assertEquals("string", "hello", resultSet.getString(2));
      assertEquals("double", 1.2, resultSet.getDouble(3), 0);
      assertEquals("date", "2014-08-26", resultSet.getString(4));
      assertEquals("timestamp", "Mon, 25 Aug 2014 20:52:00 -0700", resultSet.getString(5));
      assertEquals("time", "03:25:45", resultSet.getString(6));
      assertNull("date", resultSet.getString(7));

      // bind in update statement
      preparedStatement = connection.prepareStatement("update testBind set b=? where a=?");

      preparedStatement.setString(1, "world");
      preparedStatement.setInt(2, 1);
      preparedStatement.execute();

      preparedStatement.close();

      // test the updated rows
      resultSet = regularStatement.executeQuery("select * from testBind");

      // assert we get 1 rows
      assertTrue(resultSet.next());
      assertEquals("int", 1, resultSet.getInt(1));
      assertEquals("string", "world", resultSet.getString(2));
      assertEquals("double", 1.2, resultSet.getDouble(3), 0);
      assertEquals("date", "2014-08-26", resultSet.getString(4));
      assertEquals("timestamp", "Mon, 25 Aug 2014 20:52:00 -0700", resultSet.getString(5));
      assertEquals("time", "03:25:45", resultSet.getString(6));
      assertNull("date", resultSet.getString(7));

      // array bind for insert
      preparedStatement =
          connection.prepareStatement(
              "insert into testBind (a, b, c, d, e, f, g) "
                  + "values(?, ?, ?, ?, ?, ?, current_date())");

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

      int[] updateCounts = preparedStatement.executeBatch();

      // GS optimizes this into one insert execution, but we expand the
      // return count into an array
      assertEquals("Number of update counts", 2, updateCounts.length);

      // update count should be 1 for each
      assertEquals("update count", 1, updateCounts[0]);
      assertEquals("update count", 1, updateCounts[1]);

      // test the inserted rows
      resultSet = regularStatement.executeQuery("select * from testBind where a = 2");

      // assert we get 1 rows
      assertTrue(resultSet.next());
      assertEquals("int", 2, resultSet.getInt(1));
      assertEquals("string", "hello", resultSet.getString(2));
      assertEquals("double", 1.2, resultSet.getDouble(3), 0);
      assertEquals("date", "2014-08-26", resultSet.getString(4));
      assertEquals("timestamp", "Mon, 25 Aug 2014 20:52:00 -0700", resultSet.getString(5));
      assertEquals("time", "03:25:45", resultSet.getString(6));

      resultSet = regularStatement.executeQuery("select * from testBind where a = 3");

      // assert we get 1 rows
      assertTrue(resultSet.next());
      assertEquals("int", 3, resultSet.getInt(1));
      assertEquals("string", "hello", resultSet.getString(2));
      assertEquals("double", 1.2, resultSet.getDouble(3), 0);
      assertEquals("date", "2014-08-26", resultSet.getString(4));
      assertEquals("timestamp", "Mon, 25 Aug 2014 20:52:00 -0700", resultSet.getString(5));
      assertEquals("time", "03:25:45", resultSet.getString(6));

      // describe mode
      preparedStatement =
          connection.prepareStatement("select * from testBind WHERE to_number(a) = ?");

      resultSetMetaData = preparedStatement.getMetaData();
      assertEquals(7, resultSetMetaData.getColumnCount());
      assertEquals(Types.BIGINT, resultSetMetaData.getColumnType(1));
      assertEquals(Types.VARCHAR, resultSetMetaData.getColumnType(2));
      assertEquals(Types.DOUBLE, resultSetMetaData.getColumnType(3));
      assertEquals(Types.DATE, resultSetMetaData.getColumnType(4));
      assertEquals(Types.TIMESTAMP, resultSetMetaData.getColumnType(5));
      assertEquals(Types.TIME, resultSetMetaData.getColumnType(6));
      assertEquals(Types.DATE, resultSetMetaData.getColumnType(7));

      preparedStatement.close();
      preparedStatement = connection.prepareStatement("select ?, ?");

      resultSetMetaData = preparedStatement.getMetaData();
      assertEquals(2, resultSetMetaData.getColumnCount());
      assertEquals(Types.VARCHAR, resultSetMetaData.getColumnType(1));
      assertEquals(Types.VARCHAR, resultSetMetaData.getColumnType(2));

      preparedStatement.close();
      preparedStatement = connection.prepareStatement("select ?, ?");

      preparedStatement.setInt(1, 1);
      preparedStatement.setString(2, "hello");
      ResultSet result = preparedStatement.executeQuery();

      resultSetMetaData = result.getMetaData();
      assertEquals(2, resultSetMetaData.getColumnCount());
      assertEquals(Types.BIGINT, resultSetMetaData.getColumnType(1));
      assertEquals(Types.VARCHAR, resultSetMetaData.getColumnType(2));

      preparedStatement.close();

      // test null binding
      preparedStatement = connection.prepareStatement("select ?");

      preparedStatement.setNull(1, Types.VARCHAR);
      resultSet = preparedStatement.executeQuery();
      resultSetMetaData = resultSet.getMetaData();

      // assert column count
      assertEquals(1, resultSetMetaData.getColumnCount());
      assertEquals(Types.VARCHAR, resultSetMetaData.getColumnType(1));

      // assert we get 1 rows
      assertTrue(resultSet.next());
      assertNull(resultSet.getObject(1));

      preparedStatement.setNull(1, Types.INTEGER);
      resultSet = preparedStatement.executeQuery();
      resultSetMetaData = resultSet.getMetaData();

      // assert column count
      assertEquals(1, resultSetMetaData.getColumnCount());

      // assert we get 1 rows
      assertTrue(resultSet.next());
      assertNull(resultSet.getObject(1));

      preparedStatement.close();

      // bind in insert statement
      // create a test table
      regularStatement = connection.createStatement();
      regularStatement.executeUpdate(
          "create or replace table testBind1(c1 double, c2 string, c3 date, "
              + "c4 date, c5 string, c6 date, c7 string, c8 string, "
              + "c9 string, c10 string, c11 string, c12 date, c13 string, "
              + "c14 float, c15 string, c16 string, c17 string, c18 string,"
              + "c19 string, c20 date, c21 string)");

      // array bind for insert
      preparedStatement =
          connection.prepareStatement(
              "insert into testBind1 (c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, "
                  + "c12, c13, c14, c15, c16, c17, c18, c19, c20, c21) values "
                  + "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?,"
                  + " ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");

      for (int idx = 0; idx < 16; idx++) addBindBatch(preparedStatement, sqlDate);

      updateCounts = preparedStatement.executeBatch();

      // GS optimizes this into one insert execution
      assertEquals("Number of update counts", 16, updateCounts.length);

      for (int idx = 0; idx < 16; idx++) assertEquals("update count", 1, updateCounts[idx]);
    } finally {
      if (regularStatement != null) {
        regularStatement.execute("DROP TABLE testBind");
        regularStatement.close();
      }

      closeSQLObjects(resultSet, preparedStatement, connection);
    }
  }

  @Test
  public void testTableBind() throws Throwable {
    Connection connection = null;
    PreparedStatement preparedStatement = null;
    Statement regularStatement = null;
    ResultSet resultSet = null;

    try {
      connection = getConnection();

      // select * from table(?)
      preparedStatement = connection.prepareStatement("SELECT * from table(?)");
      ResultSetMetaData resultSetMetaData = preparedStatement.getMetaData();
      // we do not have any metadata, without a specified table
      assertEquals(0, resultSetMetaData.getColumnCount());

      preparedStatement.setString(1, ORDERS_JDBC);
      resultSet = preparedStatement.executeQuery();
      resultSetMetaData = resultSet.getMetaData();
      assertEquals(9, resultSetMetaData.getColumnCount());
      // assert we have 73 rows
      for (int i = 0; i < 73; i++) {
        assertTrue(resultSet.next());
      }
      assertFalse(resultSet.next());

      preparedStatement.close();

      // select * from table(?) where c1 = 1
      preparedStatement = connection.prepareStatement("SELECT * from table(?) where c1 = 1");
      preparedStatement.setString(1, ORDERS_JDBC);
      resultSet = preparedStatement.executeQuery();
      resultSetMetaData = resultSet.getMetaData();

      assertEquals(9, resultSetMetaData.getColumnCount());
      assertTrue(resultSet.next());
      assertFalse(resultSet.next());

      preparedStatement.close();

      // select * from table(?) where c1 = 2 order by c3
      preparedStatement = connection.prepareStatement("SELECT * from table(?) order by c3");
      preparedStatement.setString(1, ORDERS_JDBC);
      resultSet = preparedStatement.executeQuery();
      resultSetMetaData = resultSet.getMetaData();

      assertEquals(9, resultSetMetaData.getColumnCount());
      // assert we have 73 rows
      for (int i = 0; i < 73; i++) {
        assertTrue(resultSet.next());
      }
      assertFalse(resultSet.next());

      preparedStatement.close();

      regularStatement = connection.createStatement();
      regularStatement.execute("create or replace table testTableBind(c integer, d string)");

      // insert into table
      regularStatement = connection.createStatement();
      regularStatement.executeUpdate("insert into testTableBind (c, d) values (1, 'one')");

      // select c1, c from table(?), testTableBind
      preparedStatement = connection.prepareStatement("SELECT * from table(?), testTableBind");
      preparedStatement.setString(1, ORDERS_JDBC);
      resultSet = preparedStatement.executeQuery();
      resultSetMetaData = resultSet.getMetaData();

      assertEquals(11, resultSetMetaData.getColumnCount());
      // assert we have 73 rows
      for (int i = 0; i < 73; i++) {
        assertTrue(resultSet.next());
      }
      assertFalse(resultSet.next());

      preparedStatement.close();

      // select * from table(?), table(?)
      preparedStatement = connection.prepareStatement("SELECT * from table(?), table(?)");
      preparedStatement.setString(1, ORDERS_JDBC);
      preparedStatement.setString(2, "testTableBind");
      resultSet = preparedStatement.executeQuery();
      resultSetMetaData = resultSet.getMetaData();

      assertEquals(11, resultSetMetaData.getColumnCount());
      // assert we have 73 rows
      for (int i = 0; i < 73; i++) {
        assertTrue(resultSet.next());
      }
      assertFalse(resultSet.next());

      preparedStatement.close();

      // select tab1.c1, tab2.c from table(?) as a, table(?) as b
      preparedStatement =
          connection.prepareStatement("SELECT a.c1, b.c from table(?) as a, table(?) as b");
      preparedStatement.setString(1, ORDERS_JDBC);
      preparedStatement.setString(2, "testTableBind");
      resultSet = preparedStatement.executeQuery();
      resultSetMetaData = resultSet.getMetaData();

      assertEquals(2, resultSetMetaData.getColumnCount());
      // assert we have 73 rows
      for (int i = 0; i < 73; i++) {
        assertTrue(resultSet.next());
      }
      assertFalse(resultSet.next());

      preparedStatement.close();

    } finally {
      if (regularStatement != null) {
        regularStatement.execute("DROP TABLE testTableBind");
      }
      closeSQLObjects(resultSet, preparedStatement, connection);
    }
  }

  @Test
  public void testBindInWithClause() throws Throwable {
    Connection connection = null;
    PreparedStatement preparedStatement = null;
    Statement regularStatement = null;
    ResultSet resultSet = null;

    try {
      connection = getConnection();

      // create a test table
      regularStatement = connection.createStatement();
      regularStatement.execute(
          "create or replace table testBind2(a int, b string, c double, "
              + "d date, e timestamp, f time, g date)");

      // bind in where clause
      preparedStatement =
          connection.prepareStatement(
              "WITH V AS (SELECT * FROM testBind2 WHERE a = ?) " + "SELECT count(*) FROM V");

      preparedStatement.setInt(1, 100);
      resultSet = preparedStatement.executeQuery();
      ResultSetMetaData resultSetMetaData = resultSet.getMetaData();

      // assert column count
      assertEquals(1, resultSetMetaData.getColumnCount());

      // assert we get 1 rows
      assertTrue(resultSet.next());
      preparedStatement.close();
    } finally {
      if (regularStatement != null) {
        regularStatement.execute("DROP TABLE testBind2");
        regularStatement.close();
      }

      closeSQLObjects(resultSet, preparedStatement, connection);
    }
  }

  @Test
  public void testBindTimestampNTZ() throws Throwable {
    Connection connection = null;
    PreparedStatement preparedStatement = null;
    Statement regularStatement = null;
    ResultSet resultSet = null;

    try {
      connection = getConnection();

      // create a test table
      regularStatement = connection.createStatement();
      regularStatement.executeUpdate(
          "create or replace table testBindTimestampNTZ(a timestamp_ntz)");

      regularStatement.execute("alter session set client_timestamp_type_mapping='timestamp_ntz'");

      // bind in where clause
      preparedStatement = connection.prepareStatement("insert into testBindTimestampNTZ values(?)");

      Timestamp ts = buildTimestamp(2014, 7, 26, 3, 52, 0, 0);
      preparedStatement.setTimestamp(1, ts);

      int updateCount = preparedStatement.executeUpdate();

      // update count should be 1
      assertEquals("update count", 1, updateCount);

      // test the inserted rows
      resultSet = regularStatement.executeQuery("select * from testBindTimestampNTZ");

      // assert we get 1 rows
      assertTrue(resultSet.next());
      assertEquals("timestamp", "Tue, 26 Aug 2014 03:52:00 Z", resultSet.getString(1));

      regularStatement.executeUpdate("truncate table testBindTimestampNTZ");

      preparedStatement.setTimestamp(
          1, ts, Calendar.getInstance(TimeZone.getTimeZone("America/Los_Angeles")));

      updateCount = preparedStatement.executeUpdate();

      // update count should be 1
      assertEquals("update count", 1, updateCount);

      // test the inserted rows
      resultSet = regularStatement.executeQuery("select * from testBindTimestampNTZ");

      // assert we get 1 rows
      assertTrue(resultSet.next());

      preparedStatement.close();
    } finally {
      if (regularStatement != null) {
        regularStatement.execute("DROP TABLE testBindTimestampNTZ");
        regularStatement.close();
      }

      closeSQLObjects(resultSet, preparedStatement, connection);
    }
  }

  @Test
  public void testNullBind() throws Throwable {
    Connection connection = null;
    PreparedStatement preparedStatement = null;
    Statement regularStatement = null;

    try {
      connection = getConnection();

      regularStatement = connection.createStatement();
      regularStatement.execute("create or replace table testNullBind(a double)");

      // array bind with nulls
      preparedStatement = connection.prepareStatement("insert into testNullBind (a) values(?)");

      preparedStatement.setDouble(1, 1.2);
      preparedStatement.addBatch();

      preparedStatement.setObject(1, null);
      preparedStatement.addBatch();

      int[] updateCounts = preparedStatement.executeBatch();

      // GS optimizes this into one insert execution
      assertEquals("Number of update counts", 2, updateCounts.length);

      // update count should be 1
      assertEquals("update count", 1, updateCounts[0]);
      assertEquals("update count", 1, updateCounts[1]);

      preparedStatement.clearBatch();

      preparedStatement.setObject(1, null);
      preparedStatement.addBatch();

      preparedStatement.setDouble(1, 1.2);
      preparedStatement.addBatch();

      updateCounts = preparedStatement.executeBatch();

      // GS optimizes this into one insert execution
      assertEquals("Number of update counts", 2, updateCounts.length);

      // update count should be 1
      assertEquals("update count", 1, updateCounts[0]);
      assertEquals("update count", 1, updateCounts[1]);

      preparedStatement.clearBatch();

      preparedStatement.setObject(1, null);
      preparedStatement.addBatch();

      updateCounts = preparedStatement.executeBatch();

      // GS optimizes this into one insert execution
      assertEquals("Number of update counts", 1, updateCounts.length);

      // update count should be 1
      assertEquals("update count", 1, updateCounts[0]);

      preparedStatement.clearBatch();

      // this test causes query count in GS not to be decremented because
      // the exception is thrown before registerQC. Discuss with Johnston
      // to resolve the issue before enabling the test.
      try {
        preparedStatement.setObject(1, "Null", Types.DOUBLE);
        preparedStatement.addBatch();
        preparedStatement.executeBatch();
        fail("must fail in executeBatch()");
      } catch (SnowflakeSQLException ex) {
        assertEquals(2086, ex.getErrorCode());
      }

      preparedStatement.clearBatch();

      try {
        preparedStatement.setString(1, "hello");
        preparedStatement.addBatch();

        preparedStatement.setDouble(1, 1.2);
        preparedStatement.addBatch();
        fail("must fail");
      } catch (SnowflakeSQLException ex) {
        assertEquals(
            (int) ErrorCode.ARRAY_BIND_MIXED_TYPES_NOT_SUPPORTED.getMessageCode(),
            ex.getErrorCode());
      }
    } finally {
      if (regularStatement != null) {
        regularStatement.execute("DROP TABLE testNullBind");
        regularStatement.close();
      }

      closeSQLObjects(preparedStatement, connection);
    }
  }

  @Test
  public void testSnow12603() throws Throwable {
    Connection connection = null;
    PreparedStatement preparedStatement = null;
    ResultSet resultSet = null;

    try {
      connection = getConnection();

      preparedStatement = connection.prepareStatement("SELECT ?, ?, ?, ?, ?, ?");

      java.sql.Date sqlDate = java.sql.Date.valueOf("2014-08-26");

      Timestamp ts = buildTimestamp(2014, 7, 26, 3, 52, 0, 0);

      preparedStatement.setObject(1, 1);
      preparedStatement.setObject(2, "hello");
      preparedStatement.setObject(3, new BigDecimal("1.3"));
      preparedStatement.setObject(4, Float.valueOf("1.3"));
      preparedStatement.setObject(5, sqlDate);
      preparedStatement.setObject(6, ts);
      resultSet = preparedStatement.executeQuery();

      ResultSetMetaData resultSetMetaData = resultSet.getMetaData();

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

      assertEquals("integer", 1, resultSet.getInt(1));
      assertEquals("string", "hello", resultSet.getString(2));
      assertEquals("decimal", new BigDecimal("1.3"), resultSet.getBigDecimal(3));
      assertEquals("double", 1.3, resultSet.getDouble(4), 0);
      assertEquals("date", "2014-08-26", resultSet.getString(5));
      assertEquals("timestamp", "Mon, 25 Aug 2014 20:52:00 -0700", resultSet.getString(6));

      preparedStatement.setObject(1, 1, Types.INTEGER);
      preparedStatement.setObject(2, "hello", Types.VARCHAR);
      preparedStatement.setObject(3, new BigDecimal("1.3"), Types.DECIMAL);
      preparedStatement.setObject(4, Float.valueOf("1.3"), Types.DOUBLE);
      preparedStatement.setObject(5, sqlDate, Types.DATE);
      preparedStatement.setObject(6, ts, Types.TIMESTAMP);

      resultSet = preparedStatement.executeQuery();

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

      assertEquals("integer", 1, resultSet.getInt(1));
      assertEquals("string", "hello", resultSet.getString(2));
      assertEquals("decimal", new BigDecimal("1.3"), resultSet.getBigDecimal(3));
      assertEquals("double", 1.3, resultSet.getDouble(4), 0);
      assertEquals("date", "2014-08-26", resultSet.getString(5));
      assertEquals("timestamp", "Mon, 25 Aug 2014 20:52:00 -0700", resultSet.getString(6));
    } finally {
      closeSQLObjects(resultSet, preparedStatement, connection);
    }
  }

  /** SNOW-6290: timestamp value is shifted by local timezone */
  @Test
  public void testSnow6290() throws Throwable {
    Connection connection = null;
    Statement statement = null;

    try {
      connection = getConnection();

      statement = connection.createStatement();

      // create test table
      statement.execute("CREATE OR REPLACE TABLE testSnow6290(ts timestamp)");

      PreparedStatement preparedStatement =
          connection.prepareStatement("INSERT INTO testSnow6290(ts) values(?)");

      Timestamp ts = new Timestamp(System.currentTimeMillis());

      preparedStatement.setTimestamp(1, ts);
      preparedStatement.executeUpdate();

      ResultSet res = statement.executeQuery("select ts from testSnow6290");

      assertTrue("expect a row", res.next());

      Timestamp tsFromDB = res.getTimestamp(1);

      assertEquals("timestamp mismatch", ts.getTime(), tsFromDB.getTime());
    } finally {
      if (statement != null) {
        statement.execute("DROP TABLE if exists testSnow6290");
        statement.close();
      }
      closeSQLObjects(statement, connection);
    }
  }

  /** SNOW-6986: null sql shouldn't be allowed */
  @Test
  public void testInvalidSQL() throws Throwable {
    Connection connection = null;
    Statement statement = null;

    try {
      connection = getConnection();

      statement = connection.createStatement();

      // execute DDLs
      statement.executeQuery(null);
      statement.close();

      fail("expected exception, but no exception");

    } catch (SnowflakeSQLException ex) {
      assertEquals((int) ErrorCode.INVALID_SQL.getMessageCode(), ex.getErrorCode());
    } finally {
      closeSQLObjects(statement, connection);
    }
  }

  @Test
  public void testGetObject() throws Throwable {
    Connection connection = null;
    PreparedStatement preparedStatement = null;
    ResultSet resultSet = null;
    ResultSetMetaData resultSetMetaData;

    try {
      connection = getConnection();

      preparedStatement = connection.prepareStatement("SELECT ?");

      // bind integer
      preparedStatement.setInt(1, 1);
      resultSet = preparedStatement.executeQuery();

      resultSetMetaData = resultSet.getMetaData();

      assertEquals(
          "column class name=BigDecimal",
          Long.class.getName(),
          resultSetMetaData.getColumnClassName(1));

      // assert we get 1 rows
      assertTrue(resultSet.next());

      assertTrue("integer", resultSet.getObject(1) instanceof Long);

      preparedStatement.setString(1, "hello");
      resultSet = preparedStatement.executeQuery();

      resultSetMetaData = resultSet.getMetaData();

      assertEquals(
          "column class name=String",
          String.class.getName(),
          resultSetMetaData.getColumnClassName(1));

      // assert we get 1 rows
      assertTrue(resultSet.next());

      assertTrue("string", resultSet.getObject(1) instanceof String);

      preparedStatement.setDouble(1, 1.2);
      resultSet = preparedStatement.executeQuery();

      resultSetMetaData = resultSet.getMetaData();

      assertEquals(
          "column class name=Double",
          Double.class.getName(),
          resultSetMetaData.getColumnClassName(1));

      // assert we get 1 rows
      assertTrue(resultSet.next());

      assertTrue("double", resultSet.getObject(1) instanceof Double);

      preparedStatement.setTimestamp(1, new Timestamp(0));
      resultSet = preparedStatement.executeQuery();

      resultSetMetaData = resultSet.getMetaData();

      assertEquals(
          "column class name=Timestamp",
          Timestamp.class.getName(),
          resultSetMetaData.getColumnClassName(1));

      // assert we get 1 rows
      assertTrue(resultSet.next());

      assertTrue("timestamp", resultSet.getObject(1) instanceof Timestamp);

      preparedStatement.setDate(1, new java.sql.Date(0));
      resultSet = preparedStatement.executeQuery();

      resultSetMetaData = resultSet.getMetaData();

      assertEquals(
          "column class name=Date",
          java.sql.Date.class.getName(),
          resultSetMetaData.getColumnClassName(1));

      // assert we get 1 rows
      assertTrue(resultSet.next());

      assertTrue("date", resultSet.getObject(1) instanceof java.sql.Date);

      preparedStatement.close();

    } finally {
      closeSQLObjects(resultSet, preparedStatement, connection);
    }
  }

  @Test
  public void testGetDoubleForNull() throws Throwable {
    Connection connection = null;
    Statement stmt = null;
    ResultSet resultSet = null;

    try {
      connection = getConnection();

      stmt = connection.createStatement();
      resultSet = stmt.executeQuery("select cast(null as int) as null_int");
      assertTrue(resultSet.next());
      assertEquals("0 for null", 0, resultSet.getDouble(1), 0.0001);
    } finally {
      closeSQLObjects(resultSet, stmt, connection);
    }
  }

  // SNOW-27438
  @Test
  public void testGetDoubleForNaN() throws Throwable {
    Connection connection = null;
    Statement stmt = null;
    ResultSet resultSet = null;

    try {
      connection = getConnection();
      stmt = connection.createStatement();
      resultSet = stmt.executeQuery("select 'nan'::float");
      assertTrue(resultSet.next());
      assertThat("NaN for NaN", resultSet.getDouble(1), equalTo(Double.NaN));
    } finally {
      closeSQLObjects(resultSet, stmt, connection);
    }
  }

  @Test
  public void testPutViaExecuteQuery() throws Throwable {
    Connection connection = null;
    Statement statement = null;
    ResultSet resultSet = null;

    try {
      connection = getConnection();

      statement = connection.createStatement();

      // load file test
      // create a unique data file name by using current timestamp in millis
      try {
        // test external table load
        statement.execute("CREATE OR REPLACE TABLE testPutViaExecuteQuery(a number)");

        // put files
        resultSet =
            statement.executeQuery(
                "PUT file://"
                    + getFullPathFileInResource(TEST_DATA_FILE)
                    + " @%testPutViaExecuteQuery/orders parallel=10");

        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();

        // assert column count
        assertTrue(resultSetMetaData.getColumnCount() > 0);

        // assert we get 1 rows
        for (int i = 0; i < 1; i++) {
          assertTrue(resultSet.next());
        }
      } finally {
        statement.execute("DROP TABLE IF EXISTS testPutViaExecuteQuery");
        statement.close();
      }
    } finally {
      closeSQLObjects(resultSet, statement, connection);
    }
  }

  @Ignore("takes 7 min. enable this for long running tests")
  @Test
  public void testSnow16332() throws Throwable {
    Connection conn = null;
    Connection connWithNwError = null;
    Statement stmt = null;
    Statement stmtWithNwError = null;

    try {
      // use v1 query request API and inject 200ms socket timeout for first
      // http request to simulate network failure
      conn = getConnection();
      stmt = conn.createStatement();

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

      connWithNwError = getConnection(500); // inject socket timeout in ms
      stmtWithNwError = connWithNwError.createStatement();

      // execute dml
      stmtWithNwError.executeUpdate(
          "INSERT INTO SNOW16332 " + "SELECT seq8() " + "FROM table(generator(timeLimit => 1))");

      // and execute another dml
      stmtWithNwError.executeUpdate(
          "INSERT INTO SNOW16332 " + "SELECT seq8() " + "FROM table(generator(timeLimit => 1))");
    } finally {
      if (stmt != null) {
        stmt.executeQuery("DROP TABLE SNOW16332");
      }
      closeSQLObjects(stmt, conn);
      closeSQLObjects(stmtWithNwError, connWithNwError);
    }
  }

  @Test
  public void testV1Query() throws Throwable {
    Connection connection = null;
    Statement statement = null;
    ResultSet resultSet = null;

    try {
      // use v1 query request API and inject 200ms socket timeout for first
      // http request to simulate network failure
      connection = getConnection(200); // inject socket timeout = 200ms

      statement = connection.createStatement();

      // execute query
      resultSet =
          statement.executeQuery("SELECT count(*) FROM table(generator(rowCount => 100000000))");
      ResultSetMetaData resultSetMetaData = resultSet.getMetaData();

      // assert column count
      assertEquals(1, resultSetMetaData.getColumnCount());

      // assert we get 1 row
      for (int i = 0; i < 1; i++) {
        assertTrue(resultSet.next());
        assertTrue(resultSet.getInt(1) > 0);
      }

      // Test parsing for timestamp with timezone value that has new encoding
      // where timezone index follows timestamp value
      resultSet = statement.executeQuery("SELECT 'Fri, 23 Oct 2015 12:35:38 -0700'::timestamp_tz");
      resultSetMetaData = resultSet.getMetaData();

      // assert column count
      assertEquals(1, resultSetMetaData.getColumnCount());

      // assert we get 1 row
      for (int i = 0; i < 1; i++) {
        assertTrue(resultSet.next());
        assertEquals("Fri, 23 Oct 2015 12:35:38 -0700", resultSet.getString(1));
      }
    } finally {
      closeSQLObjects(resultSet, statement, connection);
    }
  }

  @Test
  public void testCancelQuery() throws Throwable {
    ResultSet resultSet = null;

    final Connection connection = getConnection();

    final Statement statement = connection.createStatement();

    // schedule a cancel in 5 seconds
    try {
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

      // now run a query for 120 seconds
      resultSet = statement.executeQuery("SELECT count(*) FROM TABLE(generator(timeLimit => 120))");
      fail("should be canceled");
    } catch (SQLException ex) {
      // assert the sqlstate is what we expect (QUERY CANCELLED)
      assertEquals("sqlstate mismatch", SqlState.QUERY_CANCELED, ex.getSQLState());
    } finally {
      closeSQLObjects(resultSet, statement, connection);
    }
  }

  /** SNOW-14774: timestamp_ntz value should use client time zone to adjust the epoch time. */
  @Test
  public void testSnow14774() throws Throwable {
    Connection connection = null;
    Statement statement = null;

    try {
      connection = getConnection();

      statement = connection.createStatement();

      // 30 minutes past daylight saving change (from 2am to 3am)
      ResultSet res = statement.executeQuery("select '2015-03-08 03:30:00'::timestamp_ntz");

      res.next();

      // get timestamp in UTC
      Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
      Timestamp tsInUTC = res.getTimestamp(1, calendar);

      SimpleDateFormat sdf = new SimpleDateFormat("yyyy.MM.dd HH:mm:ss");
      sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
      String tsStrInUTC = sdf.format(tsInUTC);

      // get timestamp in LA timezone
      calendar.setTimeZone(TimeZone.getTimeZone("America/Los_Angeles"));
      Timestamp tsInLA = res.getTimestamp(1, calendar);

      sdf.setTimeZone(TimeZone.getTimeZone("America/Los_Angeles"));
      String tsStrInLA = sdf.format(tsInLA);

      // the timestamp in LA and in UTC should be the same
      assertEquals("timestamp values not equal", tsStrInUTC, tsStrInLA);

      // 30 minutes before daylight saving change
      res = statement.executeQuery("select '2015-03-08 01:30:00'::timestamp_ntz");

      res.next();

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
      assertEquals("timestamp values not equal", tsStrInUTC, tsStrInLA);
    } finally {
      closeSQLObjects(null, statement, connection);
    }
  }

  /** SNOW-19172: getMoreResults should return false after executeQuery */
  @Test
  public void testSnow19172() throws SQLException {
    Connection connection = null;
    Statement statement = null;

    try {
      connection = getConnection();

      statement = connection.createStatement();

      statement.executeQuery("select 1");

      assertTrue(!statement.getMoreResults());

    } finally {
      closeSQLObjects(statement, connection);
    }
  }

  @Test
  public void testSnow19819() throws Throwable {
    Connection connection;
    PreparedStatement preparedStatement = null;
    Statement regularStatement = null;
    ResultSet resultSet = null;
    connection = getConnection();
    try {
      regularStatement = connection.createStatement();
      regularStatement.execute(
          "create or replace table testSnow19819(\n"
              + "s string,\n"
              + "v variant,\n"
              + "t timestamp_ltz)\n");

      preparedStatement =
          connection.prepareStatement(
              "insert into testSnow19819 (s, v, t)\n" + "select ?, parse_json(?), to_timestamp(?)");

      preparedStatement.setString(1, "foo");
      preparedStatement.setString(2, "{ }");
      preparedStatement.setString(3, "2016-05-12 12:15:00");
      preparedStatement.addBatch();

      preparedStatement.setString(1, "foo2");
      preparedStatement.setString(2, "{ \"a\": 1 }");
      preparedStatement.setString(3, "2016-05-12 12:16:00");
      preparedStatement.addBatch();

      preparedStatement.executeBatch();

      resultSet =
          connection.createStatement().executeQuery("SELECT s, v, t FROM testSnow19819 ORDER BY 1");
      assertThat("next result", resultSet.next());
      assertThat("String", resultSet.getString(1), equalTo("foo"));
      assertThat("Variant", resultSet.getString(2), equalTo("{}"));
      assertThat("next result", resultSet.next());
      assertThat("String", resultSet.getString(1), equalTo("foo2"));
      assertThat("Variant", resultSet.getString(2), equalTo("{\n  \"a\": 1\n}"));
      assertThat("no more result", !resultSet.next());
    } finally {
      if (regularStatement != null) {
        regularStatement.execute("DROP TABLE testSnow19819");
      }

      closeSQLObjects(resultSet, preparedStatement, connection);
    }
  }

  @Test
  public void testClientInfo() throws Throwable {
    Connection connection = null;
    Statement statement = null;
    ResultSet res = null;

    try {
      System.setProperty(
          "snowflake.client.info", "{\"sparkVersion\":\"1.2.0\", \"sparkApp\":\"mySparkApp\"}");

      connection = getConnection();

      statement = connection.createStatement();

      res = statement.executeQuery("select current_session_client_info()");

      assertTrue("result expected", res.next());

      String clientInfoJSONStr = res.getString(1);

      JsonNode clientInfoJSON = mapper.readTree(clientInfoJSONStr);

      // assert that spart version and spark app are found
      assertEquals("spark version mismatch", "1.2.0", clientInfoJSON.get("sparkVersion").asText());

      assertEquals("spark app mismatch", "mySparkApp", clientInfoJSON.get("sparkApp").asText());
    } finally {
      closeSQLObjects(res, statement, connection);
    }
  }

  @Test
  public void testLargeResultSet() throws Throwable {
    Connection connection = null;
    Statement statement = null;
    try {
      connection = getConnection();

      // create statement
      statement = connection.createStatement();

      String sql =
          "SELECT random()||random(), randstr(1000, random()) FROM table(generator(rowcount =>"
              + " 10000))";
      ResultSet result = statement.executeQuery(sql);

      int cnt = 0;
      while (result.next()) {
        ++cnt;
      }
      assertEquals(10000, cnt);
    } finally {
      closeSQLObjects(null, statement, connection);
    }
  }

  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testSnow26503() throws Throwable {
    Connection connection = null;
    Connection snowflakeConnection = null;
    PreparedStatement preparedStatement = null;
    Statement regularStatement = null;
    Statement regularStatementSF = null;
    ResultSet resultSet = null;
    ResultSetMetaData resultSetMetaData;

    try {
      connection = getConnection();

      // create a test table
      regularStatement = connection.createStatement();
      regularStatement.execute(
          "create or replace table testBind2(a int) as select * from values(1),(2),(8),(10)");

      // test binds in BETWEEN predicate
      preparedStatement =
          connection.prepareStatement("SELECT * FROM testBind2 WHERE a between ? and ?");

      preparedStatement.setInt(1, 3);
      preparedStatement.setInt(2, 9);
      // test that the query succeeds; used to fail with incident
      resultSet = preparedStatement.executeQuery();
      resultSetMetaData = resultSet.getMetaData();

      // assert column count
      assertEquals(1, resultSetMetaData.getColumnCount());

      // assert we get 1 row
      assertTrue(resultSet.next());

      resultSet.close();
      preparedStatement.close();
      preparedStatement = connection.prepareStatement("SELECT last_query_id()");
      resultSet = preparedStatement.executeQuery();
      resultSet.next();
      String queryId = resultSet.getString(1);

      resultSet.close();
      preparedStatement.close();

      // check that the bind values can be retrieved using system$get_bind_values
      snowflakeConnection = getSnowflakeAdminConnection();

      regularStatementSF = snowflakeConnection.createStatement();
      regularStatementSF.execute("create or replace warehouse wh26503 warehouse_size=xsmall");

      preparedStatement =
          snowflakeConnection.prepareStatement(
              "select bv:\"1\":\"value\"::string, bv:\"2\":\"value\"::string from (select"
                  + " parse_json(system$get_bind_values(?)) bv)");
      preparedStatement.setString(1, queryId);
      resultSet = preparedStatement.executeQuery();
      resultSet.next();

      // check that the bind values are correct
      assertEquals(3, resultSet.getInt(1));
      assertEquals(9, resultSet.getInt(2));

    } finally {
      if (regularStatement != null) {
        regularStatement.execute("DROP TABLE testBind2");
        regularStatement.close();
      }

      if (regularStatementSF != null) {
        regularStatementSF.execute("DROP warehouse wh26503");
        regularStatementSF.close();
      }

      closeSQLObjects(resultSet, preparedStatement, connection);

      if (snowflakeConnection != null) {
        snowflakeConnection.close();
      }
    }
  }

  /**
   * Test binding variable when creating view or udf, which currently is not supported in Snowflake.
   * Exception will be thrown and returned back to user
   */
  @Test
  public void testSnow28530() throws Throwable {
    Connection connection = null;
    PreparedStatement preparedStatement = null;
    Statement regularStatement = null;

    try {
      connection = getConnection();
      regularStatement = connection.createStatement();
      regularStatement.execute("create or replace table t(a number, b number)");

      /////////////////////////////////////////
      // bind variables in a view definition
      try {
        preparedStatement =
            connection.prepareStatement("create or replace view v as select * from t where a=?");
        preparedStatement.setInt(1, 1);
        preparedStatement.execute();

        // we shouldn't reach here
        fail("Bind variable in view definition did not cause a user error");
      } catch (SnowflakeSQLException e) {
        assertEquals(ERROR_CODE_BIND_VARIABLE_NOT_ALLOWED_IN_VIEW_OR_UDF_DEF, e.getErrorCode());
      }

      /////////////////////////////////////////////
      // bind variables in a scalar UDF definition
      try {
        preparedStatement =
            connection.prepareStatement(
                "create or replace function f(n number) returns number as " + "'n + ?'");
        preparedStatement.execute();
        fail("Bind variable in scalar UDF definition did not cause a user " + "error");
      } catch (SnowflakeSQLException e) {
        assertEquals(ERROR_CODE_BIND_VARIABLE_NOT_ALLOWED_IN_VIEW_OR_UDF_DEF, e.getErrorCode());
      }

      ///////////////////////////////////////////
      // bind variables in a table UDF definition
      try {
        preparedStatement =
            connection.prepareStatement(
                "create or replace function tf(n number) returns table(b number) as"
                    + " 'select b from t where a=?'");
        preparedStatement.execute();
        fail("Bind variable in table UDF definition did not cause a user " + "error");
      } catch (SnowflakeSQLException e) {
        assertEquals(ERROR_CODE_BIND_VARIABLE_NOT_ALLOWED_IN_VIEW_OR_UDF_DEF, e.getErrorCode());
      }
    } finally {
      if (regularStatement != null) {
        regularStatement.execute("drop table t");
        regularStatement.close();
      }

      closeSQLObjects(null, preparedStatement, connection);
    }
  }

  /**
   * SNOW-31104 improves the type inference for string constants that need to be coerced to numbers.
   * Verify that the same improvements work when the constant is a bind ref.
   */
  @Test
  public void testSnow31104() throws Throwable {
    Connection connection = null;
    PreparedStatement preparedStatement = null;
    Statement regularStatement = null;
    ResultSet resultSet = null;

    try {
      Properties paramProperties = new Properties();
      paramProperties.put("TYPESYSTEM_WIDEN_CONSTANTS_EXACTLY", Boolean.TRUE.toString());
      connection = getConnection(paramProperties);

      regularStatement = connection.createStatement();

      // Repeat a couple of test cases from snow-31104.sql
      // We don't need to repeat all of them; we just need to verify
      // that string bind refs and null bind refs are treated the same as
      // string and null constants.

      regularStatement.execute("create or replace table t(n number)");

      regularStatement.executeUpdate(
          "insert into t values (1), (90000000000000000000000000000000000000)");

      preparedStatement = connection.prepareStatement("select n, n > ? from t order by 1");
      preparedStatement.setString(1, "1");

      // this should not produce a user error
      resultSet = preparedStatement.executeQuery();
      resultSet.next();
      assertFalse(resultSet.getBoolean(2));
      resultSet.next();
      assertTrue(resultSet.getBoolean(2));

      preparedStatement =
          connection.prepareStatement("select n, '1' in (?, '256', n, 10) from t order by 1");
      preparedStatement.setString(1, null);

      resultSet = preparedStatement.executeQuery();
      resultSet.next();
      assertTrue(resultSet.getBoolean(2));
      resultSet.next();
      assertNull(resultSet.getObject(2));
    } finally {
      if (regularStatement != null) {
        regularStatement.execute("drop table t");
        regularStatement.close();
      }

      closeSQLObjects(resultSet, preparedStatement, connection);
    }
  }

  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testPutGet() throws Throwable {

    Connection connection = null;
    Statement statement = null;
    List<String> accounts = Arrays.asList(null, "s3testaccount", "azureaccount", "gcpaccount");
    for (int i = 0; i < accounts.size(); i++) {
      try {
        connection = getConnection(accounts.get(i));

        statement = connection.createStatement();

        String sourceFilePath = getFullPathFileInResource(TEST_DATA_FILE);

        File destFolder = tmpFolder.newFolder();
        String destFolderCanonicalPath = destFolder.getCanonicalPath();
        String destFolderCanonicalPathWithSeparator = destFolderCanonicalPath + File.separator;

        try {
          statement.execute("CREATE OR REPLACE STAGE testPutGet_stage");

          assertTrue(
              "Failed to put a file",
              statement.execute("PUT file://" + sourceFilePath + " @testPutGet_stage"));

          findFile(statement, "ls @testPutGet_stage/");

          // download the file we just uploaded to stage
          assertTrue(
              "Failed to get a file",
              statement.execute(
                  "GET @testPutGet_stage 'file://" + destFolderCanonicalPath + "' parallel=8"));

          // Make sure that the downloaded file exists, it should be gzip compressed
          File downloaded = new File(destFolderCanonicalPathWithSeparator + TEST_DATA_FILE + ".gz");
          assert (downloaded.exists());

          Process p =
              Runtime.getRuntime()
                  .exec("gzip -d " + destFolderCanonicalPathWithSeparator + TEST_DATA_FILE + ".gz");
          p.waitFor();

          File original = new File(sourceFilePath);
          File unzipped = new File(destFolderCanonicalPathWithSeparator + TEST_DATA_FILE);
          assert (original.length() == unzipped.length());
        } finally {
          statement.execute("DROP STAGE IF EXISTS testGetPut_stage");
          statement.close();
        }
      } finally {
        closeSQLObjects(null, statement, connection);
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
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testPutGetToUnencryptedStage() throws Throwable {

    Connection connection = null;
    Statement statement = null;
    List<String> accounts = Arrays.asList(null, "s3testaccount", "azureaccount", "gcpaccount");
    for (int i = 0; i < accounts.size(); i++) {
      try {
        connection = getConnection(accounts.get(i));

        statement = connection.createStatement();

        String sourceFilePath = getFullPathFileInResource(TEST_DATA_FILE);

        File destFolder = tmpFolder.newFolder();
        String destFolderCanonicalPath = destFolder.getCanonicalPath();
        String destFolderCanonicalPathWithSeparator = destFolderCanonicalPath + File.separator;

        try {
          statement.execute("alter session set ENABLE_UNENCRYPTED_INTERNAL_STAGES=true");
          statement.execute(
              "CREATE OR REPLACE STAGE testPutGet_unencstage encryption=(TYPE='SNOWFLAKE_SSE')");

          assertTrue(
              "Failed to put a file",
              statement.execute("PUT file://" + sourceFilePath + " @testPutGet_unencstage"));

          findFile(statement, "ls @testPutGet_unencstage/");

          // download the file we just uploaded to stage
          assertTrue(
              "Failed to get a file",
              statement.execute(
                  "GET @testPutGet_unencstage 'file://"
                      + destFolderCanonicalPath
                      + "' parallel=8"));

          // Make sure that the downloaded file exists, it should be gzip compressed
          File downloaded = new File(destFolderCanonicalPathWithSeparator + TEST_DATA_FILE + ".gz");
          assert (downloaded.exists());

          Process p =
              Runtime.getRuntime()
                  .exec("gzip -d " + destFolderCanonicalPathWithSeparator + TEST_DATA_FILE + ".gz");
          p.waitFor();

          File original = new File(sourceFilePath);
          File unzipped = new File(destFolderCanonicalPathWithSeparator + TEST_DATA_FILE);
          assert (original.length() == unzipped.length());
        } finally {
          statement.execute("DROP STAGE IF EXISTS testPutGet_unencstage");
          statement.close();
        }
      } finally {
        closeSQLObjects(null, statement, connection);
      }
    }
  }

  /** Prepare statement will fail if the connection is already closed. */
  @Test(expected = SQLException.class)
  public void testNotClosedSession() throws Throwable {
    Connection connection = getConnection();
    connection.close();
    connection.prepareStatement("select 1");
  }

  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testToTimestampNullBind() throws Throwable {
    Connection connection = null;
    PreparedStatement preparedStatement = null;

    try {
      connection = getConnection();

      preparedStatement =
          connection.prepareStatement(
              "select 3 where to_timestamp_ltz(?, 3) = '1970-01-01 00:00:12.345"
                  + " +000'::timestamp_ltz");

      // First test, normal usage.
      preparedStatement.setInt(1, 12345);

      ResultSet resultSet = preparedStatement.executeQuery();
      ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
      // Assert column count.
      assertEquals(1, resultSetMetaData.getColumnCount());
      // Assert this returned a 3.
      assertTrue(resultSet.next());
      assertEquals(3, resultSet.getInt(1));
      assertFalse(resultSet.next());

      // Second test, input is null.
      preparedStatement.setNull(1, Types.INTEGER);

      resultSet = preparedStatement.executeQuery();
      // Assert no rows returned.
      assertFalse(resultSet.next());
    } finally {
      closeSQLObjects(preparedStatement, connection);
    }
  }
  // NOTE: Don't add new tests here. Instead, add it to other appropriate test class or create a new
  // one. This class is too large to have more tests.
}
