package net.snowflake.client.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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

@Tag(TestTags.OTHERS)
public class BindUploaderIT extends BaseJDBCTest {
  BindUploader bindUploader;
  Connection conn;
  SFSession session;

  TimeZone prevTimeZone; // store last time zone and restore after tests

  // We don't insert to a table here, but the binds we test the upload with
  // are constructed by SnowflakePreparedStatementV1.
  // The class first typechecks the query to determine whether array bind is
  // supported, so we need this to force the class to construct array binds
  private static final String createTableSQL =
      "create or replace table test_binduploader(col1 INTEGER, "
          + "col2 INTEGER, col3 DOUBLE, col4 DOUBLE, col5 DOUBLE, col6 VARCHAR, col7 BINARY, "
          + "col8 DATE, col9 TIME, col10 TIMESTAMP)";
  static final String dummyInsert = "insert into test_binduploader VALUES(?,?,?,?,?,?,?,?,?,?)";
  private static final String deleteTableSQL = "drop table if exists test_binduploader";

  static final Object[] row1 = {
    42,
    1234L,
    12.34f,
    12.34d,
    new BigDecimal(42),
    "row1",
    new byte[] {-128, 127},
    new Date(0),
    new Time(0),
    new Timestamp(0)
  };
  private static final Object[] row2 = {
    420,
    12340L,
    120.34f,
    120.34d,
    new BigDecimal(420),
    "row2",
    new byte[] {127, -128},
    new Date(1),
    new Time(1),
    new Timestamp(1)
  };

  static final String csv1 =
      "42,1234,12.34,12.34,42,row1,807F,1970-01-01,00:00:00.000000000,1970-01-01 00:00:00.000000000 Z";
  static final String csv2 =
      "420,12340,120.34,120.34,420,row2,7F80,1970-01-01,00:00:00.001000000,1970-01-01 00:00:00.001000000 Z";

  static final String STAGE_DIR = "binduploaderit";
  static final String SELECT_FROM_STAGE =
      "select $1, $2, $3, $4, $5, $6, $7, $8, $9, $10 from '@SYSTEM$BIND/"
          + STAGE_DIR
          + "' ORDER BY $1 ASC";

  @BeforeAll
  public static void classSetUp() throws Exception {
    Connection connection = getConnection();
    connection.createStatement().execute(createTableSQL);
    connection.close();
  }

  @AfterAll
  public static void classTearDown() throws Exception {
    Connection connection = getConnection();
    connection.createStatement().execute(deleteTableSQL);
    connection.close();
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

  static Map<String, ParameterBindingDTO> getBindings(Connection conn) throws SQLException {
    SnowflakePreparedStatementV1 stmt =
        (SnowflakePreparedStatementV1) conn.prepareStatement(dummyInsert);
    bind(stmt, row1);
    bind(stmt, row2);
    return stmt.getBatchParameterBindings();
  }

  static void bind(SnowflakePreparedStatementV1 stmt, Object[] row) throws SQLException {
    stmt.setInt(1, (int) row[0]);
    stmt.setLong(2, (long) row[1]);
    stmt.setFloat(3, (float) row[2]);
    stmt.setDouble(4, (double) row[3]);
    stmt.setBigDecimal(5, (BigDecimal) row[4]);
    stmt.setString(6, (String) row[5]);
    stmt.setBytes(7, (byte[]) row[6]);
    stmt.setDate(8, (Date) row[7]);
    stmt.setTime(9, (Time) row[8]);
    stmt.setTimestamp(10, (Timestamp) row[9]);
    stmt.addBatch();
  }

  static String parseRow(ResultSet rs) throws Exception {
    StringBuilder sb = new StringBuilder();
    for (int i = 1; i <= 10; i++) {
      sb.append(rs.getString(i));
      if (i != 10) {
        sb.append(',');
      }
    }
    return sb.toString();
  }

  @Test
  public void testIsArrayBindEmpty() {
    Map<String, ParameterBindingDTO> map = new HashMap<>();
    assertFalse(BindUploader.isArrayBind(map));
  }

  @Test
  public void testIsArrayBindNull() {
    assertFalse(BindUploader.isArrayBind(null));
  }

  @Test
  public void testIsArrayBindNonArray() {
    Map<String, ParameterBindingDTO> map = new HashMap<>();
    map.put("1", new ParameterBindingDTO("", "string value"));
    assertFalse(BindUploader.isArrayBind(map));
  }

  @Test
  public void testIsArrayBindEmptyArray() {
    Map<String, ParameterBindingDTO> map = new HashMap<>();
    map.put("1", new ParameterBindingDTO("", new ArrayList<String>()));
    assertTrue(BindUploader.isArrayBind(map));
  }

  @Test
  public void testIsArrayBindNonEmptyArray() {
    Map<String, ParameterBindingDTO> map = new HashMap<>();
    List<String> list = new ArrayList<>();
    list.add("bind value");
    map.put("1", new ParameterBindingDTO("", list));
    list.add("another bind value");
    assertTrue(BindUploader.isArrayBind(map));
  }

  @Test
  public void tetArrayBindValueCountEmpty() {
    Map<String, ParameterBindingDTO> map = new HashMap<>();
    assertEquals(0, BindUploader.arrayBindValueCount(map));
  }

  @Test
  public void testArrayBindValueCountNull() {
    assertEquals(0, BindUploader.arrayBindValueCount(null));
  }

  @Test
  public void testArrayBindValueCountNonArray() {
    Map<String, ParameterBindingDTO> map = new HashMap<>();
    map.put("1", new ParameterBindingDTO("", "string value"));
    assertEquals(0, BindUploader.arrayBindValueCount(map));
  }

  @Test
  public void testArrayBindValueCountEmptyArray() {
    Map<String, ParameterBindingDTO> map = new HashMap<>();
    map.put("1", new ParameterBindingDTO("", new ArrayList<String>()));
    assertEquals(0, BindUploader.arrayBindValueCount(map));
  }

  @Test
  public void testArrayBindValueCountNonEmptyArray() {
    Map<String, ParameterBindingDTO> map = new HashMap<>();
    List<String> list = new ArrayList<>();
    list.add("bind value");
    list.add("another bind value");
    map.put("1", new ParameterBindingDTO("", list));
    assertEquals(2, BindUploader.arrayBindValueCount(map));
  }
}
