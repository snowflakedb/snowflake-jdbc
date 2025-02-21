package net.snowflake.client.jdbc;

import static net.snowflake.client.jdbc.BindUploaderIT.SELECT_FROM_STAGE;
import static net.snowflake.client.jdbc.BindUploaderIT.STAGE_DIR;
import static net.snowflake.client.jdbc.BindUploaderIT.bind;
import static net.snowflake.client.jdbc.BindUploaderIT.csv1;
import static net.snowflake.client.jdbc.BindUploaderIT.csv2;
import static net.snowflake.client.jdbc.BindUploaderIT.dummyInsert;
import static net.snowflake.client.jdbc.BindUploaderIT.getBindings;
import static net.snowflake.client.jdbc.BindUploaderIT.parseRow;
import static net.snowflake.client.jdbc.BindUploaderIT.row1;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.TimeZone;
import net.snowflake.client.category.TestTags;
import net.snowflake.client.core.ParameterBindingDTO;
import net.snowflake.client.core.SFSession;
import net.snowflake.client.core.bind.BindUploader;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Bind Uploader tests for the latest JDBC driver. This doesn't work for the oldest supported
 * driver. Revisit this tests whenever bumping up the oldest supported driver to examine if the
 * tests still is not applicable. If it is applicable, move tests to BindUploaderIT so that both the
 * latest and oldest supported driver run the tests.
 */
@Tag(TestTags.OTHERS)
public class BindUploaderLatestIT extends BaseJDBCTest {
  BindUploader bindUploader;
  Connection conn;
  SFSession session;
  TimeZone prevTimeZone; // store last time zone and restore after tests

  @BeforeAll
  public static void classSetUp() throws Exception {
    BindUploaderIT.classSetUp();
  }

  @AfterAll
  public static void classTearDown() throws Exception {
    BindUploaderIT.classTearDown();
  }

  @BeforeEach
  public void setUp() throws Exception {
    conn = getConnection();
    session = conn.unwrap(SnowflakeConnectionV1.class).getSfSession();
    bindUploader = BindUploader.newInstance(session, STAGE_DIR);
    prevTimeZone = TimeZone.getDefault();
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
  }

  @AfterEach
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
