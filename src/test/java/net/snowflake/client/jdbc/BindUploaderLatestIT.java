/*
 * Copyright (c) 2012-2020 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.client.jdbc;

import static net.snowflake.client.jdbc.BindUploaderIT.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.TimeZone;
import net.snowflake.client.category.TestCategoryOthers;
import net.snowflake.client.core.ParameterBindingDTO;
import net.snowflake.client.core.SFSession;
import net.snowflake.client.core.bind.BindUploader;
import org.junit.*;
import org.junit.experimental.categories.Category;

/**
 * Bind Uploader tests for the latest JDBC driver. This doesn't work for the oldest supported
 * driver. Revisit this tests whenever bumping up the oldest supported driver to examine if the
 * tests still is not applicable. If it is applicable, move tests to BindUploaderIT so that both the
 * latest and oldest supported driver run the tests.
 */
@Category(TestCategoryOthers.class)
public class BindUploaderLatestIT extends BaseJDBCTest {
  BindUploader bindUploader;
  Connection conn;
  SFSession session;
  TimeZone prevTimeZone; // store last time zone and restore after tests

  @BeforeClass
  public static void classSetUp() throws Exception {
    BindUploaderIT.classSetUp();
  }

  @AfterClass
  public static void classTearDown() throws Exception {
    BindUploaderIT.classTearDown();
  }

  @Before
  public void setUp() throws Exception {
    conn = getConnection();
    session = (SFSession) conn.unwrap(SnowflakeConnectionV1.class).getSfSession();
    bindUploader = BindUploader.newInstance(session, STAGE_DIR);
    prevTimeZone = TimeZone.getDefault();
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
  }

  @After
  public void tearDown() throws SQLException {
    conn.close();
    bindUploader.close();
    TimeZone.setDefault(prevTimeZone);
  }

  // Test setting the input stream buffer size to be quite small and ensure that 2 files (1
  // corresponding to each input stream) are created in internal stage.
  @Test
  public void testUploadedResultsMultiple() throws Exception {

    // Get an estimate of how many bytes are in 1 of the rows being uploaded and set this value to
    // be the input stream buffer size. For 2 similarly sized rows, we should now have 2 files.
    int lengthOfOneRow = csv1.getBytes("UTF-8").length;
    bindUploader.setInputStreamBufferSize(lengthOfOneRow);
    bindUploader.upload(getBindings(conn));
    // assert 2 files were created on internal stage
    assertEquals(2, bindUploader.getFileCount());
    Statement stmt = conn.createStatement();
    // assert that the results look proper
    ResultSet rs = stmt.executeQuery(SELECT_FROM_STAGE);
    rs.next();
    assertEquals(csv1, parseRow(rs));
    rs.next();
    assertEquals(csv2, parseRow(rs));
    assertFalse(rs.next());
    rs.close();
    stmt.close();
  }

  // Test single csv upload and successful parsing
  @Test
  public void testUploadedResultsSimple() throws Exception {
    bindUploader.upload(getBindings(conn));

    Statement stmt = conn.createStatement();
    ResultSet rs = stmt.executeQuery(SELECT_FROM_STAGE);
    rs.next();
    assertEquals(csv1, parseRow(rs));
    rs.next();
    assertEquals(csv2, parseRow(rs));
    assertFalse(rs.next());
    rs.close();
    stmt.close();
  }

  @Test
  public void testUploadStreamLargeBatch() throws Exception {
    // create large batch so total bytes transferred are about 10 times the size of input stream
    // buffer
    int batchSize = 1024 * 1024;
    SnowflakePreparedStatementV1 stmt =
        (SnowflakePreparedStatementV1) conn.prepareStatement(dummyInsert);
    for (int i = 0; i < batchSize; i++) {
      bind(stmt, row1);
    }
    Map<String, ParameterBindingDTO> parameterBindings = stmt.getBatchParameterBindings();
    bindUploader.upload(parameterBindings);
    ResultSet rs = stmt.executeQuery(SELECT_FROM_STAGE);
    for (int i = 0; i < batchSize; i++) {
      rs.next();
      assertEquals(csv1, parseRow(rs));
    }
    assertFalse(rs.next());
    rs.close();
    stmt.close();
  }
}
