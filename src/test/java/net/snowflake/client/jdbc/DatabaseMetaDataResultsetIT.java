package net.snowflake.client.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Arrays;
import java.util.List;
import net.snowflake.client.category.TestTags;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(TestTags.OTHERS)
public class DatabaseMetaDataResultsetIT extends BaseJDBCWithSharedConnectionIT {
  private static final int columnCount = 9;
  private static final int INT_DATA = 1;
  private static final String TEXT_DATA = "TEST";
  private static final String EPOCH_DATE = "1970-01-01";
  private static final double DOUBLE_DATA = 9.5555555;
  private static final double DELTA = 0.0001;
  private static final long NOW = System.currentTimeMillis();
  private static final Time TIME_DATA = new Time(NOW);
  private static final Timestamp TIMESTAMP_DATA = new Timestamp(NOW);
  private static final boolean BOOLEAN_DATA = true;
  private static final BigDecimal DECIMAL_DATA = new BigDecimal(0.01);
  private static final long BIGINT_DATA = 10100000L;
  private static final List<String> columnNames =
      Arrays.asList(
          "int", "text", "date", "double", "time", "timestamp", "bool", "decimal", "bigint");
  private static final List<String> columnTypeNames =
      Arrays.asList(
          "INTEGER", "TEXT", "DATA", "DOUBLE", "TIME", "TIMESTAMP", "BOOLEAN", "DECIMAL", "BIGINT");
  private static final List<Integer> columnTypes =
      Arrays.asList(
          Types.INTEGER,
          Types.VARCHAR,
          Types.DATE,
          Types.DOUBLE,
          Types.TIME,
          Types.TIMESTAMP,
          Types.BOOLEAN,
          Types.DECIMAL,
          Types.BIGINT);
  private static final Object[][] rows = {
    {
      INT_DATA,
      TEXT_DATA,
      Date.valueOf(EPOCH_DATE),
      DOUBLE_DATA,
      TIME_DATA,
      TIMESTAMP_DATA,
      BOOLEAN_DATA,
      DECIMAL_DATA,
      BIGINT_DATA
    },
    {0, null, null, 0, null, null, false, null, 0}
  };

  @Test
  public void testRowIndex() throws SQLException {
    try (ResultSet resultSet = getResultSet(false)) {

      assertEquals(columnCount, resultSet.getMetaData().getColumnCount());
      assertEquals(-1, resultSet.getRow());
      assertTrue(resultSet.isBeforeFirst());
      assertFalse(resultSet.isFirst());

      assertTrue(resultSet.next());
      assertEquals(0, resultSet.getRow());

      assertFalse(resultSet.isBeforeFirst());
      assertTrue(resultSet.isFirst());

      assertTrue(resultSet.next());
      assertEquals(1, resultSet.getRow());

      assertTrue(resultSet.isLast());
      assertFalse(resultSet.isAfterLast());

      assertFalse(resultSet.next());
      assertEquals(2, resultSet.getRow());

      assertFalse(resultSet.isLast());
      assertTrue(resultSet.isAfterLast());
    }
  }

  private ResultSet getResultSet(boolean doNext) throws SQLException {
    Statement st = connection.createStatement();
    ResultSet resultSet =
        new SnowflakeDatabaseMetaDataResultSet(columnNames, columnTypeNames, columnTypes, rows, st);
    if (doNext) {
      resultSet.next();
    }
    return resultSet;
  }

  @Test
  public void testGetInt() throws SQLException {
    try (ResultSet resultSet = getResultSet(true)) {
      assertEquals(INT_DATA, resultSet.getInt("int"));
    }
  }

  @Test
  public void testGetString() throws SQLException {
    try (ResultSet resultSet = getResultSet(true)) {
      assertEquals(TEXT_DATA, resultSet.getString("text"));
    }
  }

  @Test
  public void testGetDate() throws SQLException {
    try (ResultSet resultSet = getResultSet(true)) {
      assertEquals(Date.valueOf(EPOCH_DATE), resultSet.getDate("date"));
    }
  }

  @Test
  public void testGetDouble() throws SQLException {
    try (ResultSet resultSet = getResultSet(true)) {
      assertEquals(DOUBLE_DATA, resultSet.getDouble("double"), DELTA);
    }
  }

  @Test
  public void testGetTime() throws SQLException {
    try (ResultSet resultSet = getResultSet(true)) {
      assertEquals(TIME_DATA, resultSet.getTime("time"));
    }
  }

  @Test
  public void testGetTimestamp() throws SQLException {
    try (ResultSet resultSet = getResultSet(true)) {
      assertEquals(TIMESTAMP_DATA, resultSet.getTimestamp("timestamp"));
    }
  }

  @Test
  public void testGetBoolean() throws SQLException {
    try (ResultSet resultSet = getResultSet(true)) {
      assertEquals(BOOLEAN_DATA, resultSet.getBoolean("bool"));
    }
  }

  @Test
  public void testGetObject() throws SQLException {
    try (ResultSet resultSet = getResultSet(true)) {
      assertEquals(INT_DATA, resultSet.getObject(1));
      assertEquals(TEXT_DATA, resultSet.getObject(2));
      assertEquals(Date.valueOf(EPOCH_DATE), resultSet.getObject(3));
      assertEquals(DOUBLE_DATA, resultSet.getObject(4));
      assertEquals(TIME_DATA, resultSet.getObject(5));
      assertEquals(TIMESTAMP_DATA, resultSet.getObject(6));
      assertEquals(BOOLEAN_DATA, resultSet.getObject(7));
      assertEquals(DECIMAL_DATA, resultSet.getObject(8));
      assertEquals(BIGINT_DATA, resultSet.getObject(9));
    }
  }
}
