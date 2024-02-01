package net.snowflake.client.jdbc;

import static org.junit.Assert.*;

import java.math.BigDecimal;
import java.sql.*;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;
import net.snowflake.client.category.TestCategoryResultSet;
import net.snowflake.client.core.SFSqlInput;
import net.snowflake.client.core.structs.SnowflakeObjectTypeFactories;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(TestCategoryResultSet.class)
public class ResultSetStructuredTypesLatestIT {
  private final String queryResultFormat;

  public ResultSetStructuredTypesLatestIT() {
    this("JSON");
  }

  protected ResultSetStructuredTypesLatestIT(String queryResultFormat) {
    this.queryResultFormat = queryResultFormat;
  }

  public Connection init() throws SQLException {
    Connection conn = BaseJDBCTest.getConnection(BaseJDBCTest.DONT_INJECT_SOCKET_TIMEOUT);
    Statement stmt = conn.createStatement();
    stmt.execute("alter session set jdbc_query_result_format = '" + queryResultFormat + "'");
    stmt.close();
    return conn;
  }

  public static class SimpleClass implements SQLData {
    private String string;

    public SimpleClass() {}

    @Override
    public String getSQLTypeName() throws SQLException {
      return null;
    }

    @Override
    public void readSQL(SQLInput stream, String typeName) throws SQLException {
      string = stream.readString();
    }

    @Override
    public void writeSQL(SQLOutput stream) throws SQLException {
      stream.writeString(string);
    }
  }

  public static class AllTypesClass implements SQLData {
    private String string;
    private Byte b;
    private Short s;
    private Integer i;
    private Long l;
    private Float f;
    private Double d;
    private BigDecimal bd;
    private Boolean bool;
    private Timestamp timestampLtz;
    private Timestamp timestampNtz;
    private Timestamp timestampTz;
    private Timestamp timestampLtzInSpecificTz;
    private Date date;
    private Time time;
    private byte[] binary;
    private SimpleClass simpleClass;

    @Override
    public String getSQLTypeName() throws SQLException {
      return null;
    }

    @Override
    public void readSQL(SQLInput sqlInput, String typeName) throws SQLException {
      string = sqlInput.readString();
      b = sqlInput.readByte();
      s = sqlInput.readShort();
      i = sqlInput.readInt();
      l = sqlInput.readLong();
      f = sqlInput.readFloat();
      d = sqlInput.readDouble();
      bd = sqlInput.readBigDecimal();
      bool = sqlInput.readBoolean();
      timestampLtz = sqlInput.readTimestamp();
      timestampNtz = sqlInput.readTimestamp();
      timestampTz = sqlInput.readTimestamp();
      timestampLtzInSpecificTz =
          SFSqlInput.unwrap(sqlInput).readTimestamp(TimeZone.getTimeZone("Pacific/Honolulu"));
      date = sqlInput.readDate();
      time = sqlInput.readTime();
      binary = sqlInput.readBytes();
      simpleClass = sqlInput.readObject(SimpleClass.class);
    }

    @Override
    public void writeSQL(SQLOutput stream) throws SQLException {}
  }

  @Test
  public void testMapStructToObjectWithFactory() throws SQLException {
    testMapJson(true);
  }

  @Test
  public void testMapStructToObjectWithReflection() throws SQLException {
    testMapJson(false);
  }

  private void testMapJson(boolean registerFactory) throws SQLException {
    if (registerFactory) {
      SnowflakeObjectTypeFactories.register(SimpleClass.class, SimpleClass::new);
    } else {
      SnowflakeObjectTypeFactories.unregister(SimpleClass.class);
    }
    Connection connection = init();
    Statement statement = connection.createStatement();
    ResultSet resultSet = statement.executeQuery("select {'string':'a'}::OBJECT(string VARCHAR)");
    resultSet.next();
    SimpleClass object = resultSet.getObject(1, SimpleClass.class);
    assertEquals("a", object.string);
    statement.close();
    connection.close();
  }

  @Test
  public void testMapAllTypesOfFields() throws SQLException {
    SnowflakeObjectTypeFactories.register(AllTypesClass.class, AllTypesClass::new);
    Connection connection = init();
    Statement statement = connection.createStatement();
    statement.execute("ALTER SESSION SET TIMEZONE = 'Europe/Warsaw'");
    ResultSet resultSet =
        statement.executeQuery(
            "select {"
                + "'string': 'a', "
                + "'b': 1, "
                + "'s': 2, "
                + "'i': 3, "
                + "'l': 4, "
                + "'f': 1.1, "
                + "'d': 2.2, "
                + "'bd': 3.3, "
                + "'bool': true, "
                + "'timestamp_ltz': '2021-12-22 09:43:44'::TIMESTAMP_LTZ, "
                + "'timestamp_ntz': '2021-12-23 09:44:44'::TIMESTAMP_NTZ, "
                + "'timestamp_tz': '2021-12-24 09:45:45 +0800'::TIMESTAMP_TZ, "
                + "'timestamp_ltz_in_specific_tz': '2021-12-25 09:46:46'::TIMESTAMP_TZ, "
                + "'date': '2023-12-24'::DATE, "
                + "'time': '12:34:56'::TIME, "
                + "'binary': TO_BINARY('616263', 'HEX'), "
                + "'simpleClass': {'string': 'b'}"
                + "}::OBJECT("
                + "string VARCHAR, "
                + "b TINYINT, "
                + "s SMALLINT, "
                + "i INTEGER, "
                + "l BIGINT, "
                + "f FLOAT, "
                + "d DOUBLE, "
                + "bd DOUBLE, "
                + "bool BOOLEAN, "
                + "timestamp_ltz TIMESTAMP_LTZ, "
                + "timestamp_ntz TIMESTAMP_NTZ, "
                + "timestamp_tz TIMESTAMP_TZ, "
                + "timestamp_ltz_in_specific_tz TIMESTAMP_LTZ, "
                + "date DATE, "
                + "time TIME, "
                + "binary BINARY, "
                + "simpleClass OBJECT(string VARCHAR)"
                + ")");
    resultSet.next();
    AllTypesClass object = resultSet.getObject(1, AllTypesClass.class);
    assertEquals("a", object.string);
    assertEquals(1, (long) object.b);
    assertEquals(2, (long) object.s);
    assertEquals(3, (long) object.i);
    assertEquals(4, (long) object.l);
    assertEquals(1.1, (double) object.f, 0.01);
    assertEquals(2.2, (double) object.d, 0.01);
    assertEquals(BigDecimal.valueOf(3.3), object.bd);
    assertEquals(Timestamp.valueOf(LocalDateTime.of(2021, 12, 22, 9, 43, 44)), object.timestampLtz);
    assertEquals(
        Timestamp.valueOf(LocalDateTime.of(2021, 12, 23, 10, 44, 44)), object.timestampNtz);
    assertEquals(Timestamp.valueOf(LocalDateTime.of(2021, 12, 24, 2, 45, 45)), object.timestampTz);
    assertEquals(Date.valueOf(LocalDate.of(2023, 12, 24)), object.date);
    assertEquals(Time.valueOf(LocalTime.of(12, 34, 56)), object.time);
    assertEquals(
        Timestamp.valueOf(LocalDateTime.of(2021, 12, 25, 9, 46, 46)),
        object.timestampLtzInSpecificTz);
    assertArrayEquals(new byte[] {'a', 'b', 'c'}, object.binary);
    assertTrue(object.bool);
    assertEquals("b", object.simpleClass.string);
    statement.close();
    connection.close();
  }

  @Test
  public void testMapStructsFromChunks() throws SQLException {
    Connection connection = init();
    Statement statement = connection.createStatement();
    ResultSet resultSet =
        statement.executeQuery(
            "select {'string':'a'}::OBJECT(string VARCHAR) FROM TABLE(GENERATOR(ROWCOUNT=>30000))");
    int i = 0;
    while (resultSet.next()) {
      SimpleClass object = resultSet.getObject(1, SimpleClass.class);
      assertEquals("a", object.string);
    }
    statement.close();
    connection.close();
  }

  @Test
  public void testMapArrays() throws SQLException {
    testMapIntegerArray(true);
    testMapVarcharArray(true);
    testMapDatesArray(true);
    testMapBinaryArray(true);
    testMapTimeArray(true);
    testMapTimestampArray(true);
    testMapBooleanArray(true);
    testMapStructArray(true);
    testMapArrayOfArrays(true);
  }

  private void testMapIntegerArray(boolean registerFactory) throws SQLException {
    Connection connection = init();
    Statement statement = connection.createStatement();
    ResultSet resultSet = statement.executeQuery("SELECT ARRAY_CONSTRUCT(10, 20, 30)::ARRAY(INTEGER)");
    resultSet.next();
    Long[] resultArray = (Long[])resultSet.getArray(1).getArray();
    assertEquals(resultArray[0], Long.valueOf(10));
    assertEquals(resultArray[1], Long.valueOf(20));
    assertEquals(resultArray[2], Long.valueOf(30));
    statement.close();
    connection.close();
  }

  private void testMapVarcharArray(boolean registerFactory) throws SQLException {
    Connection connection = init();
    Statement statement = connection.createStatement();
    ResultSet resultSet = statement.executeQuery("SELECT ARRAY_CONSTRUCT('10', '20','30')::ARRAY(VARCHAR)");
    resultSet.next();
    String[] resultArray = (String[]) resultSet.getArray(1).getArray();
    assertEquals(resultArray[0], "10");
    assertEquals(resultArray[1],"20");
    assertEquals(resultArray[2], "30");
    statement.close();
    connection.close();
  }

  private void testMapDatesArray(boolean registerFactory) throws SQLException {
    Connection connection = init();
    Statement statement = connection.createStatement();
    ResultSet resultSet = statement.executeQuery("SELECT ARRAY_CONSTRUCT(to_date('2023-12-24', 'YYYY-MM-DD'), to_date('2023-12-25', 'YYYY-MM-DD'))::ARRAY(DATE)");
    resultSet.next();
    Date[] resultArray = (Date[]) resultSet.getArray(1).getArray();
    assertEquals(resultArray[0], Date.valueOf(LocalDate.of(2023, 12, 24)));
    assertEquals(resultArray[1], Date.valueOf(LocalDate.of(2023, 12, 25)));
    statement.close();
    connection.close();
  }
  private void testMapTimeArray(boolean registerFactory) throws SQLException {
    Connection connection = init();
    Statement statement = connection.createStatement();
    ResultSet resultSet = statement.executeQuery("SELECT ARRAY_CONSTRUCT(to_time('15:39:20.123'), to_time('15:39:20.123'))::ARRAY(TIME)");
    resultSet.next();
    Time[] resultArray = (Time[]) resultSet.getArray(1).getArray();
    assertEquals(resultArray[0], Time.valueOf(LocalTime.of(15, 39, 20)));
    assertEquals(resultArray[1], Time.valueOf(LocalTime.of(15, 39, 20)));
    statement.close();
    connection.close();
  }
  private void testMapTimestampArray(boolean registerFactory) throws SQLException {
    Connection connection = init();
    Statement statement = connection.createStatement();
    ResultSet resultSet = statement.executeQuery("SELECT ARRAY_CONSTRUCT(TO_TIMESTAMP_NTZ('2021-12-23 09:44:44'), TO_TIMESTAMP_NTZ('2021-12-24 09:55:55'))::ARRAY(TIMESTAMP)");
    resultSet.next();
    Timestamp[] resultArray = (Timestamp[]) resultSet.getArray(1).getArray();
    assertEquals(resultArray[0], Timestamp.valueOf(LocalDateTime.of(2021, 12, 23, 10, 44, 44)));
    assertEquals(resultArray[1], Timestamp.valueOf(LocalDateTime.of(2021, 12, 24, 10, 55, 55)));
    //TODO test timestamps LTZ and TZ
    statement.close();
    connection.close();
  }
  private void testMapBooleanArray(boolean registerFactory) throws SQLException {
    Connection connection = init();
    Statement statement = connection.createStatement();
    ResultSet resultSet = statement.executeQuery("SELECT ARRAY_CONSTRUCT(true,false)::ARRAY(BOOLEAN)");
    resultSet.next();
    Boolean[] resultArray = (Boolean[]) resultSet.getArray(1).getArray();
    assertEquals(resultArray[0], true);
    assertEquals(resultArray[1], false);
    statement.close();
    connection.close();
  }
  private void testMapBinaryArray(boolean registerFactory) throws SQLException {
    Connection connection = init();
    Statement statement = connection.createStatement();
    ResultSet resultSet = statement.executeQuery("SELECT ARRAY_CONSTRUCT(TO_BINARY('616263', 'HEX'),TO_BINARY('616263', 'HEX'))::ARRAY(BINARY)");
    resultSet.next();
    Object[] resultArray = (Object[]) resultSet.getArray(1).getArray();
    assertArrayEquals((byte[]) resultArray[0], new byte[] {'a', 'b', 'c'});
    assertArrayEquals((byte[]) resultArray[1], new byte[] {'a', 'b', 'c'});
    statement.close();
    connection.close();
  }
  private void testMapStructArray(boolean registerFactory) throws SQLException {
    Connection connection = init();
    Statement statement = connection.createStatement();
    ResultSet resultSet = statement.executeQuery("SELECT ARRAY_CONSTRUCT({'x': 'abc', 'y': 1}, {'x': 'def', 'y': 2} )::ARRAY(OBJECT(x VARCHAR, y Integer))");
    resultSet.next();
    Map[] resultArray = (Map[]) resultSet.getArray(1).getArray();
    assertEquals(resultArray[0].toString(), "{x=abc, y=1}");
    assertEquals(resultArray[1].toString(), "{x=def, y=2}");
    statement.close();
    connection.close();
  }
  private void testMapArrayOfArrays(boolean registerFactory) throws SQLException {
    Connection connection = init();
    Statement statement = connection.createStatement();
    ResultSet resultSet = statement.executeQuery("SELECT ARRAY_CONSTRUCT(ARRAY_CONSTRUCT({'x': 'abc', 'y': 1}, {'x': 'def', 'y': 2}) )::ARRAY(ARRAY(OBJECT(x VARCHAR, y Integer)))");
    resultSet.next();
    HashMap[][] resultArray =   (HashMap[][])resultSet.getArray(1).getArray();
    assertEquals(resultArray[0][0].toString(), "{x=abc, y=1}");
    assertEquals(resultArray[0][1].toString(), "{x=def, y=2}");
    statement.close();
    connection.close();
  }
}
