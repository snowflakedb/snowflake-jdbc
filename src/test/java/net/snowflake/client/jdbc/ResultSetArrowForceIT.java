package net.snowflake.client.jdbc;

import net.snowflake.client.ConditionalIgnoreRule;
import net.snowflake.client.RunningOnTravisCI;
import net.snowflake.common.core.SFBinary;
import org.apache.arrow.vector.BigIntVector;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Completely compare json and arrow resultSet behaviors
 */
@RunWith(Parameterized.class)
public class ResultSetArrowForceIT extends BaseJDBCTest
{
  @Parameterized.Parameters(name = "format={0}")
  public static Object[][] data()
  {
    // all tests in this class need to run for both query result formats json and arrow
    if (BaseJDBCTest.isArrowTestsEnabled())
    {
      return new Object[][]{
          {"JSON"},
          {"Arrow_force"}
      };
    }
    else
    {
      return new Object[][]{
          {"JSON"}
      };
    }
  }

  protected static String queryResultFormat;

  public ResultSetArrowForceIT(String queryResultFormat)
  {
    this.queryResultFormat = queryResultFormat;
  }


  public static Connection getConnection()
  throws SQLException
  {
    Connection conn = getConnection(BaseJDBCTest.DONT_INJECT_SOCKET_TIMEOUT);
    if (isArrowTestsEnabled())
    {
      conn.createStatement().execute("alter session set query_result_format = '" + queryResultFormat + "'");
    }
    return conn;
  }

  @Test
  public void testSNOW89737() throws SQLException
  {
    Connection con = getConnection();
    Statement statement = con.createStatement();

    statement.execute("create or replace table test_types(c1 number, c2 integer, c3 float, c4 varchar, c5 char, c6 " +
                      "binary, c7 boolean, c8 date, c9 datetime, c10 time, c11 timestamp_ltz, c12 timestamp_tz, c13 " +
                      "variant, c14 object, c15 array)");
    statement.execute("insert into test_types values (null, null, null, null, null, null, null, null, null, null, " +
                      "null, null, null, null, null)");
    statement.execute("insert into test_types (c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12) values(5, 5, 5.0," +
                      "'hello', 'h', '48454C4C4F', true, '1994-12-27', " +
                      "'1994-12-27 05:05:05', '05:05:05', '1994-12-27 05:05:05 +00:05', '1994-12-27 05:05:05')");
    statement.execute("insert into test_types(c13) select parse_json(' { \"key1\\x00\":\"value1\" } ')");
    statement.execute("insert into test_types(c14) select parse_json(' { \"key1\\x00\":\"value1\" } ')");
    statement.execute("insert into test_types(c15) select parse_json('{\"fruits\" : [\"apples\", \"pears\", " +
                      "\"oranges\"]}')");
    ResultSet resultSet = statement.executeQuery("select * from test_types");
    // test first row of result set against all "get" methods
    assertTrue(resultSet.next());
    // test getString method against all other data types
    assertEquals(null, resultSet.getString(1));
    assertEquals(null, resultSet.getString(2));
    assertEquals(null, resultSet.getString(3));
    assertEquals(null, resultSet.getString(4));
    assertEquals(null, resultSet.getString(5));
    assertEquals(null, resultSet.getString(6));
    assertEquals(null, resultSet.getString(7));
    assertEquals(null, resultSet.getString(8));
    assertEquals(null, resultSet.getString(9));
    assertEquals(null, resultSet.getString(10));
    assertEquals(null, resultSet.getString(11));
    assertEquals(null, resultSet.getString(12));
    assertEquals(null, resultSet.getString(13));
    assertEquals(null, resultSet.getString(14));
    assertEquals(null, resultSet.getString(15));
  }

  /**
   * Note: Arrow format does not include space and \n in the string values
   *
   * @throws SQLException
   */
  @Test
  public void testSemiStructuredData() throws SQLException
  {
    Connection con = getConnection();
    ResultSet rs = con.createStatement().executeQuery(
        "select array_construct(10, 20, 30), " +
        "array_construct(null, 'hello', 3::double, 4, 5), " +
        "array_construct(), " +
        "object_construct('a',1,'b','BBBB', 'c',null)," +
        "object_construct('Key_One', parse_json('NULL'), 'Key_Two', null, 'Key_Three', 'null')," +
        "to_variant(3.2)," +
        "parse_json('{ \"a\": null}')," +
        " 100::variant;");
    while (rs.next())
    {
      assertEquals("[\n" +
                   "  10,\n" +
                   "  20,\n" +
                   "  30\n" +
                   "]", rs.getString(1));
      assertEquals("[\n" +
                   "  undefined,\n" +
                   "  \"hello\",\n" +
                   "  3.000000000000000e+00,\n" +
                   "  4,\n" +
                   "  5\n" +
                   "]", rs.getString(2));
      assertEquals("{\n" +
                   "  \"a\": 1,\n" +
                   "  \"b\": \"BBBB\"\n" +
                   "}", rs.getString(4));
      assertEquals("{\n" +
                   "  \"Key_One\": null,\n" +
                   "  \"Key_Three\": \"null\"\n" +
                   "}", rs.getString(5));
      assertEquals("{\n" +
                   "  \"a\": null\n" +
                   "}", rs.getString(7));
      assertEquals("[]", rs.getString(3));
      assertEquals("3.2", rs.getString(6));
      assertEquals("100", rs.getString(8));
    }
    con.close();
  }

  private Connection init(String table, String column, String values) throws SQLException
  {
    Connection con = getConnection();
    con.createStatement().execute("create or replace table " + table + " " + column);
    con.createStatement().execute("insert into " + table + " values " + values);
    return con;
  }

  private boolean isJSON()
  {
    return queryResultFormat.equalsIgnoreCase("json");
  }

  private void finish(String table, Connection con) throws SQLException
  {
    con.createStatement().execute("drop table " + table);
    con.close();
  }

  /**
   * compare behaviors (json vs arrow)
   * <p>
   * VALUE_IS_NULL      Yes                                          No
   * -----------------------------------------------------------------------
   * getColumnType      BIGINT                                       0
   * getInt             same                                         0
   * getShort           same                                         0
   * getLong            same                                         0
   * getString          same                                         null
   * getFloat           same                                         0
   * getDouble          same                                         0
   * getBigDecimal      same                                         null
   * getObject          same                                         null
   * getByte            same                                         0
   * getBytes           INTERNAL_ERROR vs SUCCESS                    null
   * -------------------------------------------------------------------------
   *
   * @throws SQLException
   */
  @Test
  public void testTinyInt() throws SQLException
  {
    int[] cases = {0, 1, -1, 127, -128};
    String table = "test_arrow_tiny_int";
    String column = "(a int)";
    String values = "(" + StringUtils.join(ArrayUtils.toObject(cases), "),(") + "), (NULL)";
    Connection con = init(table, column, values);

    ResultSet rs = con.createStatement().executeQuery("select * from " + table);
    double delta = 0.1;
    int columnType = rs.getMetaData().getColumnType(1);
    assertEquals(Types.BIGINT, columnType);
    for (int i = 0; i < cases.length; i++)
    {
      rs.next();
      assertEquals(cases[i], rs.getInt(1));
      assertEquals((short) cases[i], rs.getShort(1));
      assertEquals((long) cases[i], rs.getLong(1));
      assertEquals((Integer.toString(cases[i])), rs.getString(1));
      assertEquals((float) cases[i], rs.getFloat(1), delta);
      double val = cases[i];
      assertEquals(val, rs.getDouble(1), delta);
      assertEquals(new BigDecimal(Integer.toString(cases[i])), rs.getBigDecimal(1));
      assertEquals(rs.getLong(1), rs.getObject(1));
      assertEquals(cases[i], rs.getByte(1));
      if (isJSON())
      {
        try
        {
          rs.getBytes(1);
          fail();
        }
        catch (Exception e)
        {
          SQLException se = (SQLException) e;
          assertEquals((int) ErrorCode.INTERNAL_ERROR.getMessageCode(), se.getErrorCode());
          assertEquals(ErrorCode.INTERNAL_ERROR.getSqlState(), se.getSQLState());
        }
      }
      else
      {
        byte[] bytes = new byte[1];
        bytes[0] = (byte) cases[i];
        assertArrayEquals(bytes, rs.getBytes(1));
      }
    }
    rs.next();
    assertEquals(0, rs.getInt(1));
    assertEquals((short) 0, rs.getShort(1));
    assertEquals((long) 0, rs.getLong(1));
    assertNull(rs.getString(1));
    assertEquals((float) 0, rs.getFloat(1), delta);
    double val = 0;
    assertEquals(val, rs.getDouble(1), delta);
    assertNull(rs.getBigDecimal(1));
    assertNull(rs.getObject(1));
    assertEquals(0, rs.getByte(1));
    assertNull(rs.getBytes(1));
    assertTrue(rs.wasNull());
    finish(table, con);
  }

  /**
   * compare behaviors (json vs arrow)
   * <p>
   * VALUE_IS_NULL      Yes                                                                   No
   * ------------------------------------------------------------------------------------------------
   * getColumnType      DECIMAL                                                               0
   * getInt             java.NumberFormatException vs SQLException.INVALID_VALUE_CONVERT      0
   * getShort           java.NumberFormatException vs SQLException.INVALID_VALUE_CONVERT      0
   * getLong            same                                                                  0
   * getString          same                                                                  null
   * getFloat           same                                                                  0
   * getDouble          same                                                                  0
   * getBigDecimal      same                                                                  null
   * getObject          same                                                                  null
   * getByte            INTERNAL_ERROR vs SUCCESS                                             0
   * getBytes           INTERNAL_ERROR vs SUCCESS                                             null
   * --------------------------------------------------------------------------------------------------
   *
   * @throws SQLException
   */
  @Test
  public void testScaledTinyInt() throws SQLException
  {
    float[] cases = {0.0f, 0.11f, -0.11f, 1.27f, -1.28f};
    String table = "test_arrow_tiny_int";
    String column = "(a number(3,2))";
    String values = "(" + StringUtils.join(ArrayUtils.toObject(cases), "),(") + "), (null)";
    Connection con = init(table, column, values);

    ResultSet rs = con.createStatement().executeQuery(
        "select * from test_arrow_tiny_int");
    double delta = 0.001;
    int columnType = rs.getMetaData().getColumnType(1);
    assertEquals(Types.DECIMAL, columnType);

    for (int i = 0; i < cases.length; i++)
    {
      rs.next();
      try
      {
        rs.getInt(1);
        fail();
      }
      catch (Exception e)
      {
          SQLException se = (SQLException) e;
          assertEquals((int) ErrorCode.INVALID_VALUE_CONVERT.getMessageCode(), se.getErrorCode());
          assertEquals(ErrorCode.INVALID_VALUE_CONVERT.getSqlState(), se.getSQLState());
      }
      try
      {
        rs.getShort(1);
        fail();
      }
      catch (Exception e)
      {
          SQLException se = (SQLException) e;
          assertEquals((int) ErrorCode.INVALID_VALUE_CONVERT.getMessageCode(), se.getErrorCode());
          assertEquals(ErrorCode.INVALID_VALUE_CONVERT.getSqlState(), se.getSQLState());
      }
      try
      {
        rs.getLong(1);
        fail();
      }
      catch (Exception e)
      {
        SQLException se = (SQLException) e;
        assertEquals((int) ErrorCode.INVALID_VALUE_CONVERT.getMessageCode(), se.getErrorCode());
        assertEquals(ErrorCode.INVALID_VALUE_CONVERT.getSqlState(), se.getSQLState());
      }

      assertEquals((String.format("%.2f", cases[i])), rs.getString(1));
      assertEquals(cases[i], rs.getFloat(1), delta);
      double val = cases[i];
      assertEquals(val, rs.getDouble(1), delta);
      assertEquals(new BigDecimal(rs.getString(1)), rs.getBigDecimal(1));
      assertEquals(rs.getBigDecimal(1), rs.getObject(1));
      if (isJSON())
      {
        try
        {
          rs.getByte(1);
          fail();
        }
        catch (Exception e)
        {
          // Note: not caught by SQLException!
          assertTrue(e.toString().contains("NumberFormatException"));
        }
      }
      else
      {
        assertEquals(((byte) (cases[i] * 100)), rs.getByte(1));
      }
      if (isJSON())
      {
        try
        {
          rs.getBytes(1);
          fail();
        }
        catch (Exception e)
        {
          SQLException se = (SQLException) e;
          assertEquals((int) ErrorCode.INTERNAL_ERROR.getMessageCode(), se.getErrorCode());
          assertEquals(ErrorCode.INTERNAL_ERROR.getSqlState(), se.getSQLState());
        }
      }
      else
      {
        byte[] bytes = new byte[1];
        bytes[0] = rs.getByte(1);
        assertArrayEquals(bytes, rs.getBytes(1));
      }
    }

    // null value
    rs.next();
    assertEquals(0, rs.getInt(1));
    assertEquals((short) 0, rs.getShort(1));
    assertEquals((long) 0, rs.getLong(1));
    assertNull(rs.getString(1));
    assertEquals((float) 0, rs.getFloat(1), delta);
    double val = 0;
    assertEquals(val, rs.getDouble(1), delta);
    assertNull(rs.getBigDecimal(1));
    assertNull(rs.getObject(1));
    assertEquals(0, rs.getByte(1));
    assertNull(rs.getBytes(1));
    assertTrue(rs.wasNull());
    finish(table, con);
  }


  /**
   * compare behaviors (json vs arrow)
   * <p>
   * VALUE_IS_NULL      Yes                                          No
   * -----------------------------------------------------------------------
   * getColumnType      BIGINT                                       0
   * getInt             same                                         0
   * getShort           same                                         0
   * getLong            same                                         0
   * getString          same                                         null
   * getFloat           same                                         0
   * getDouble          same                                         0
   * getBigDecimal      same                                         null
   * getObject          same                                         null
   * getByte            INTERNAL_ERROR vs INVALID_VALUE_CONVERT      0
   * getBytes           INTERNAL_ERROR vs SUCCESS                    null
   * -------------------------------------------------------------------------
   *
   * @throws SQLException
   */
  @Test
  public void testSmallInt() throws SQLException
  {
    short[] cases = {0, 1, -1, 127, -128, 128, -129, 32767, -32768};
    String table = "test_arrow_small_int";
    String column = "(a int)";
    String values = "(" + StringUtils.join(ArrayUtils.toObject(cases), "),(") + "), (NULL)";
    Connection con = init(table, column, values);

    ResultSet rs = con.createStatement().executeQuery("select * from " + table);
    double delta = 0.1;
    int columnType = rs.getMetaData().getColumnType(1);
    assertEquals(Types.BIGINT, columnType);
    for (int i = 0; i < cases.length; i++)
    {
      rs.next();
      assertEquals(cases[i], rs.getInt(1));
      assertEquals(cases[i], rs.getShort(1));
      assertEquals((long) cases[i], rs.getLong(1));
      assertEquals((Integer.toString(cases[i])), rs.getString(1));
      assertEquals((float) cases[i], rs.getFloat(1), delta);
      double val = cases[i];
      assertEquals(val, rs.getDouble(1), delta);
      assertEquals(new BigDecimal(Integer.toString(cases[i])), rs.getBigDecimal(1));
      assertEquals(rs.getLong(1), rs.getObject(1));
      if (cases[i] <= 127 && cases[i] >= -128)
      {
        assertEquals(cases[i], rs.getByte(1));
      }
      else
      {
        try
        {
          rs.getByte(1);
          fail();
        }
        catch (Exception e)
        {
          if (isJSON())
          {
            // Note: not caught by SQLException!
            assertTrue(e.toString().contains("NumberFormatException"));
          }
          else
          {
            SQLException se = (SQLException) e;
            assertEquals((int) ErrorCode.INVALID_VALUE_CONVERT.getMessageCode(), se.getErrorCode());
            assertEquals(ErrorCode.INVALID_VALUE_CONVERT.getSqlState(), se.getSQLState());
          }
        }
      }
      if (isJSON())
      {
        try
        {
          rs.getBytes(1);
          fail();
        }
        catch (Exception e)
        {
          SQLException se = (SQLException) e;
          assertEquals((int) ErrorCode.INTERNAL_ERROR.getMessageCode(), se.getErrorCode());
          assertEquals(ErrorCode.INTERNAL_ERROR.getSqlState(), se.getSQLState());
        }
      }
      else
      {
        ByteBuffer bb = ByteBuffer.allocate(2);
        bb.putShort(cases[i]);
        assertArrayEquals(bb.array(), rs.getBytes(1));
      }
    }
    rs.next();
    assertEquals(0, rs.getInt(1));
    assertEquals((short) 0, rs.getShort(1));
    assertEquals((long) 0, rs.getLong(1));
    assertNull(rs.getString(1));
    assertEquals((float) 0, rs.getFloat(1), delta);
    double val = 0;
    assertEquals(val, rs.getDouble(1), delta);
    assertNull(rs.getBigDecimal(1));
    assertNull(rs.getObject(1));
    assertEquals(0, rs.getByte(1));
    assertNull(rs.getBytes(1));
    assertTrue(rs.wasNull());
    finish(table, con);
  }

  /**
   * compare behaviors (json vs arrow)
   * <p>
   * VALUE_IS_NULL      Yes                                                 No
   * -----------------------------------------------------------------------------
   * getColumnType      DECIMAL                                             0
   * getInt             NumberFormatException vs INVALID_VALUE_CONVERT      0
   * getShort           NumberFormatException vs INVALID_VALUE_CONVERT      0
   * getLong            same                                                0
   * getString          same                                                null
   * getFloat           same                                                0
   * getDouble          same                                                0
   * getBigDecimal      same                                                null
   * getObject          same                                                null
   * getByte            NumberFormatException vs INVALID_VALUE_CONVERT      0
   * getBytes           INTERNAL_ERROR        vs SUCCESS                    null
   * -------------------------------------------------------------------------------
   *
   * @throws SQLException
   */
  @Test
  public void testScaledSmallInt() throws SQLException
  {
    float[] cases = {0, 2.0f, -2.0f, 32.767f, -32.768f};
    short[] shortCompact = {0, 2000, -2000, 32767, -32768};
    String table = "test_arrow_small_int";
    String column = "(a number(5,3))";
    String values = "(" + StringUtils.join(ArrayUtils.toObject(cases), "),(") + "), (null)";
    Connection con = init(table, column, values);

    ResultSet rs = con.createStatement().executeQuery(
        "select * from test_arrow_small_int");
    double delta = 0.0001;
    int columnType = rs.getMetaData().getColumnType(1);
    assertEquals(Types.DECIMAL, columnType);

    for (int i = 0; i < cases.length; i++)
    {
      rs.next();
      try
      {
        rs.getInt(1);
        fail();
      }
      catch (Exception e)
      {
          SQLException se = (SQLException) e;
          assertEquals((int) ErrorCode.INVALID_VALUE_CONVERT.getMessageCode(), se.getErrorCode());
          assertEquals(ErrorCode.INVALID_VALUE_CONVERT.getSqlState(), se.getSQLState());
      }
      try
      {
        rs.getShort(1);
        fail();
      }
      catch (Exception e)
      {
          SQLException se = (SQLException) e;
          assertEquals((int) ErrorCode.INVALID_VALUE_CONVERT.getMessageCode(), se.getErrorCode());
          assertEquals(ErrorCode.INVALID_VALUE_CONVERT.getSqlState(), se.getSQLState());
      }
      try
      {
        rs.getLong(1);
        fail();
      }
      catch (Exception e)
      {
        SQLException se = (SQLException) e;
        assertEquals((int) ErrorCode.INVALID_VALUE_CONVERT.getMessageCode(), se.getErrorCode());
        assertEquals(ErrorCode.INVALID_VALUE_CONVERT.getSqlState(), se.getSQLState());
      }

      assertEquals((String.format("%.3f", cases[i])), rs.getString(1));
      assertEquals(cases[i], rs.getFloat(1), delta);
      double val = cases[i];
      assertEquals(val, rs.getDouble(1), delta);
      assertEquals(new BigDecimal(rs.getString(1)), rs.getBigDecimal(1));
      assertEquals(rs.getBigDecimal(1), rs.getObject(1));
      try
      {
        rs.getByte(1);
        fail();
      }
      catch (Exception e)
      {
        if (isJSON())
        {
          // Note: not caught by SQLException!
          assertTrue(e.toString().contains("NumberFormatException"));
        }
        else
        {
          SQLException se = (SQLException) e;
          assertEquals((int) ErrorCode.INVALID_VALUE_CONVERT.getMessageCode(), se.getErrorCode());
          assertEquals(ErrorCode.INVALID_VALUE_CONVERT.getSqlState(), se.getSQLState());
        }
      }
      if (isJSON())
      {
        try
        {
          rs.getBytes(1);
          fail();
        }
        catch (Exception e)
        {
          SQLException se = (SQLException) e;
          assertEquals((int) ErrorCode.INTERNAL_ERROR.getMessageCode(), se.getErrorCode());
          assertEquals(ErrorCode.INTERNAL_ERROR.getSqlState(), se.getSQLState());
        }
      }
      else
      {
        ByteBuffer byteBuffer = ByteBuffer.allocate(2);
        byteBuffer.putShort(shortCompact[i]);
        assertArrayEquals(byteBuffer.array(), rs.getBytes(1));
      }
    }

    // null value
    rs.next();
    assertEquals(0, rs.getInt(1));
    assertEquals((short) 0, rs.getShort(1));
    assertEquals((long) 0, rs.getLong(1));
    assertNull(rs.getString(1));
    assertEquals((float) 0, rs.getFloat(1), delta);
    double val = 0;
    assertEquals(val, rs.getDouble(1), delta);
    assertNull(rs.getBigDecimal(1));
    assertEquals(null, rs.getObject(1));
    assertEquals(0, rs.getByte(1));
    assertNull(rs.getBytes(1));
    assertTrue(rs.wasNull());
    finish(table, con);
  }

  /**
   * compare behaviors (json vs arrow)
   * <p>
   * VALUE_IS_NULL      Yes                                               No
   * --------------------------------------------------------------------------
   * getColumnType      BIGINT                                            0
   * getInt             same                                              0
   * getShort           same                                              0
   * getLong            same                                              0
   * getString          same                                              null
   * getFloat           same                                              0
   * getDouble          same                                              0
   * getBigDecimal      same                                              null
   * getObject          same                                              null
   * getByte            INTERNAL_ERROR         vs INVALID_VALUE_CONVERT   0
   * getBytes           INTERNAL_ERROR OR      vs SUCCESS                 null
   * Return wrong result
   * -------------------------------------------------------------------------
   *
   * @throws SQLException
   */
  @Test
  public void testInt() throws SQLException
  {
    int[] cases = {0, 1, -1, 127, -128, 128, -129, 32767, -32768, 32768, -32769, 2147483647, -2147483648};
    String table = "test_arrow_int";
    String column = "(a int)";
    String values = "(" + StringUtils.join(ArrayUtils.toObject(cases), "),(") + "), (NULL)";
    Connection con = init(table, column, values);

    ResultSet rs = con.createStatement().executeQuery("select * from " + table);
    double delta = 0.1;
    int columnType = rs.getMetaData().getColumnType(1);
    assertEquals(Types.BIGINT, columnType);
    for (int i = 0; i < cases.length; i++)
    {
      rs.next();
      assertEquals(cases[i], rs.getInt(1));
      if (cases[i] >= Short.MIN_VALUE && cases[i] <= Short.MAX_VALUE)
      {
        assertEquals((short) cases[i], rs.getShort(1));
      }
      else
      {
        try
        {
          assertEquals((short) cases[i], rs.getShort(1));
          fail();
        }
        catch (Exception e)
        {
          {
              SQLException se = (SQLException) e;
              assertEquals((int) ErrorCode.INVALID_VALUE_CONVERT.getMessageCode(), se.getErrorCode());
              assertEquals(ErrorCode.INVALID_VALUE_CONVERT.getSqlState(), se.getSQLState());
          }
        }
      }
      assertEquals((long) cases[i], rs.getLong(1));
      assertEquals((Integer.toString(cases[i])), rs.getString(1));
      assertEquals((float) cases[i], rs.getFloat(1), delta);
      double val = cases[i];
      assertEquals(val, rs.getDouble(1), delta);
      assertEquals(new BigDecimal(Integer.toString(cases[i])), rs.getBigDecimal(1));
      assertEquals(rs.getLong(1), rs.getObject(1));
      if (cases[i] <= 127 && cases[i] >= -128)
      {
        assertEquals(cases[i], rs.getByte(1));
      }
      else
      {
        try
        {
          rs.getByte(1);
          fail();
        }
        catch (Exception e)
        {
          if (isJSON())
          {
            // Note: not caught by SQLException!
            assertTrue(e.toString().contains("NumberFormatException"));
          }
          else
          {
            SQLException se = (SQLException) e;
            assertEquals((int) ErrorCode.INVALID_VALUE_CONVERT.getMessageCode(), se.getErrorCode());
            assertEquals(ErrorCode.INVALID_VALUE_CONVERT.getSqlState(), se.getSQLState());
          }
        }
      }
      ByteBuffer bb = ByteBuffer.allocate(4);
      bb.putInt(cases[i]);
      if (isJSON())
      {
        try
        {
          byte[] bytes = rs.getBytes(1);
          assertFalse(Arrays.equals(bb.array(), bytes));
        }
        catch (Exception e)
        {
          SQLException se = (SQLException) e;
          assertEquals((int) ErrorCode.INTERNAL_ERROR.getMessageCode(), se.getErrorCode());
          assertEquals(ErrorCode.INTERNAL_ERROR.getSqlState(), se.getSQLState());
        }
      }
      else
      {
        assertArrayEquals(bb.array(), rs.getBytes(1));
      }
    }
    rs.next();
    assertEquals(0, rs.getInt(1));
    assertEquals((short) 0, rs.getShort(1));
    assertEquals((long) 0, rs.getLong(1));
    assertNull(rs.getString(1));
    assertEquals((float) 0, rs.getFloat(1), delta);
    double val = 0;
    assertEquals(val, rs.getDouble(1), delta);
    assertNull(rs.getBigDecimal(1));
    assertNull(rs.getObject(1));
    assertEquals(0, rs.getByte(1));
    assertNull(rs.getBytes(1));
    assertTrue(rs.wasNull());
    finish(table, con);
  }

  /**
   * compare behaviors (json vs arrow)
   * <p>
   * VALUE_IS_NULL      Yes                                                 No
   * -----------------------------------------------------------------------------
   * getColumnType      DECIMAL                                             0
   * getInt             NumberFormatException vs INVALID_VALUE_CONVERT      0
   * getShort           NumberFormatException vs INVALID_VALUE_CONVERT      0
   * getLong            same                                                0
   * getString          same                                                null
   * getFloat           same                                                0
   * getDouble          same                                                0
   * getBigDecimal      same                                                null
   * getObject          same                                                null
   * getByte            NumberFormatException vs INVALID_VALUE_CONVERT      0
   * getBytes           INTERNAL_ERROR        vs SUCCESS                    null
   * -------------------------------------------------------------------------------
   *
   * @throws SQLException
   */
  @Test
  public void testScaledInt() throws SQLException
  {
    int scale = 9;
    int[] intCompacts = {
        0, 123456789, -123456789, 2147483647, -2147483647
    };
    List<BigDecimal> caseList = Arrays.stream(intCompacts)
        .mapToObj(x -> BigDecimal.valueOf(x, scale)).collect(Collectors.toList());

    BigDecimal[] cases = caseList.stream().toArray(BigDecimal[]::new);

    String table = "test_arrow_int";

    String column = String.format("(a number(10,%d))", scale);
    String values = "(" + StringUtils.join(cases, "),(") + "), (null)";
    Connection con = init(table, column, values);

    ResultSet rs = con.createStatement().executeQuery(
        "select * from test_arrow_int");
    double delta = 0.0000000001;
    int columnType = rs.getMetaData().getColumnType(1);
    assertEquals(Types.DECIMAL, columnType);

    for (int i = 0; i < cases.length; i++)
    {
      rs.next();
      try
      {
        rs.getInt(1);
        fail();
      }
      catch (Exception e)
      {
          SQLException se = (SQLException) e;
          assertEquals((int) ErrorCode.INVALID_VALUE_CONVERT.getMessageCode(), se.getErrorCode());
          assertEquals(ErrorCode.INVALID_VALUE_CONVERT.getSqlState(), se.getSQLState());
      }
      try
      {
        rs.getShort(1);
        fail();
      }
      catch (Exception e)
      {
          SQLException se = (SQLException) e;
          assertEquals((int) ErrorCode.INVALID_VALUE_CONVERT.getMessageCode(), se.getErrorCode());
          assertEquals(ErrorCode.INVALID_VALUE_CONVERT.getSqlState(), se.getSQLState());
      }
      try
      {
        rs.getLong(1);
        fail();
      }
      catch (Exception e)
      {
        SQLException se = (SQLException) e;
        assertEquals((int) ErrorCode.INVALID_VALUE_CONVERT.getMessageCode(), se.getErrorCode());
        assertEquals(ErrorCode.INVALID_VALUE_CONVERT.getSqlState(), se.getSQLState());
      }

      assertEquals(cases[i].toPlainString(), rs.getString(1));
      assertEquals(Float.parseFloat(cases[i].toString()), rs.getFloat(1), delta);
      double val = Double.parseDouble(cases[i].toString());
      assertEquals(val, rs.getDouble(1), delta);
      assertEquals(new BigDecimal(rs.getString(1)), rs.getBigDecimal(1));
      assertEquals(rs.getBigDecimal(1), rs.getObject(1));
      try
      {
        rs.getByte(1);
        fail();
      }
      catch (Exception e)
      {
        if (isJSON())
        {
          // Note: not caught by SQLException!
          assertTrue(e.toString().contains("NumberFormatException"));
        }
        else
        {
          SQLException se = (SQLException) e;
          assertEquals((int) ErrorCode.INVALID_VALUE_CONVERT.getMessageCode(), se.getErrorCode());
          assertEquals(ErrorCode.INVALID_VALUE_CONVERT.getSqlState(), se.getSQLState());
        }
      }
      if (isJSON())
      {
        try
        {
          rs.getBytes(1);
          fail();
        }
        catch (Exception e)
        {
          SQLException se = (SQLException) e;
          assertEquals((int) ErrorCode.INTERNAL_ERROR.getMessageCode(), se.getErrorCode());
          assertEquals(ErrorCode.INTERNAL_ERROR.getSqlState(), se.getSQLState());
        }
      }
      else
      {
        ByteBuffer byteBuffer = ByteBuffer.allocate(4);
        byteBuffer.putInt(intCompacts[i]);
        assertArrayEquals(byteBuffer.array(), rs.getBytes(1));
      }
    }

    // null value
    rs.next();
    assertEquals(0, rs.getInt(1));
    assertEquals((short) 0, rs.getShort(1));
    assertEquals((long) 0, rs.getLong(1));
    assertNull(rs.getString(1));
    assertEquals((float) 0, rs.getFloat(1), delta);
    double val = 0;
    assertEquals(val, rs.getDouble(1), delta);
    assertNull(rs.getBigDecimal(1));
    assertEquals(null, rs.getObject(1));
    assertEquals(0, rs.getByte(1));
    assertNull(rs.getBytes(1));
    assertTrue(rs.wasNull());
    finish(table, con);
  }

  /**
   * compare behaviors (json vs arrow)
   * <p>
   * VALUE_IS_NULL      Yes                                               No
   * --------------------------------------------------------------------------
   * getColumnType      BIGINT                                            0
   * getInt             same                                              0
   * getShort           same                                              0
   * getLong            same                                              0
   * getString          same                                              null
   * getFloat           same                                              0
   * getDouble          same                                              0
   * getBigDecimal      same                                              null
   * getObject          same                                              null
   * getByte            INTERNAL_ERROR         vs INVALID_VALUE_CONVERT   0
   * getBytes           INTERNAL_ERROR OR      vs SUCCESS                 null
   * Return wrong result
   * -------------------------------------------------------------------------
   *
   * @throws SQLException
   */
  @Test
  public void testBigInt() throws SQLException
  {
    long[] cases = {0, 1, -1, 127, -128, 128, -129, 32767, -32768, 32768, -32769, 2147483647, -2147483648,
                    2147483648l, -2147483649l, Long.MAX_VALUE, Long.MIN_VALUE};
    String table = "test_arrow_big_int";
    String column = "(a int)";
    String values = "(" + StringUtils.join(ArrayUtils.toObject(cases), "),(") + "), (NULL)";
    Connection con = init(table, column, values);

    ResultSet rs = con.createStatement().executeQuery("select * from " + table);
    double delta = 0.1;
    int columnType = rs.getMetaData().getColumnType(1);
    assertEquals(Types.BIGINT, columnType);
    for (int i = 0; i < cases.length; i++)
    {
      rs.next();

      if (cases[i] >= Integer.MIN_VALUE && cases[i] <= Integer.MAX_VALUE)
      {
        assertEquals(cases[i], rs.getInt(1));
      }
      else
      {
        try
        {
          assertEquals(cases[i], rs.getInt(1));
          fail();
        }
        catch (Exception e)
        {
          {
              SQLException se = (SQLException) e;
              assertEquals((int) ErrorCode.INVALID_VALUE_CONVERT.getMessageCode(), se.getErrorCode());
              assertEquals(ErrorCode.INVALID_VALUE_CONVERT.getSqlState(), se.getSQLState());
          }
        }
      }
      if (cases[i] >= Short.MIN_VALUE && cases[i] <= Short.MAX_VALUE)
      {
        assertEquals((short) cases[i], rs.getShort(1));
      }
      else
      {
        try
        {
          assertEquals((short) cases[i], rs.getShort(1));
          fail();
        }
        catch (Exception e)
        {
          {
              SQLException se = (SQLException) e;
              assertEquals((int) ErrorCode.INVALID_VALUE_CONVERT.getMessageCode(), se.getErrorCode());
              assertEquals(ErrorCode.INVALID_VALUE_CONVERT.getSqlState(), se.getSQLState());
          }
        }
      }
      assertEquals(cases[i], rs.getLong(1));
      assertEquals((Long.toString(cases[i])), rs.getString(1));
      assertEquals((float) cases[i], rs.getFloat(1), delta);
      double val = cases[i];
      assertEquals(val, rs.getDouble(1), delta);
      assertEquals(new BigDecimal(Long.toString(cases[i])), rs.getBigDecimal(1));
      assertEquals(rs.getLong(1), rs.getObject(1));
      if (cases[i] <= 127 && cases[i] >= -128)
      {
        assertEquals(cases[i], rs.getByte(1));
      }
      else
      {
        try
        {
          rs.getByte(1);
          fail();
        }
        catch (Exception e)
        {
          if (isJSON())
          {
            // Note: not caught by SQLException!
            assertTrue(e.toString().contains("NumberFormatException"));
          }
          else
          {
            SQLException se = (SQLException) e;
            assertEquals((int) ErrorCode.INVALID_VALUE_CONVERT.getMessageCode(), se.getErrorCode());
            assertEquals(ErrorCode.INVALID_VALUE_CONVERT.getSqlState(), se.getSQLState());
          }
        }
      }
      ByteBuffer bb = ByteBuffer.allocate(8);
      bb.putLong(cases[i]);
      if (isJSON())
      {
        try
        {
          byte[] bytes = rs.getBytes(1);
          assertFalse(Arrays.equals(bb.array(), bytes));
        }
        catch (Exception e)
        {
          SQLException se = (SQLException) e;
          assertEquals((int) ErrorCode.INTERNAL_ERROR.getMessageCode(), se.getErrorCode());
          assertEquals(ErrorCode.INTERNAL_ERROR.getSqlState(), se.getSQLState());
        }
      }
      else
      {
        assertArrayEquals(bb.array(), rs.getBytes(1));
      }
    }
    rs.next();
    assertEquals(0, rs.getInt(1));
    assertEquals((short) 0, rs.getShort(1));
    assertEquals((long) 0, rs.getLong(1));
    assertNull(rs.getString(1));
    assertEquals((float) 0, rs.getFloat(1), delta);
    double val = 0;
    assertEquals(val, rs.getDouble(1), delta);
    assertNull(rs.getBigDecimal(1));
    assertEquals(null, rs.getObject(1));
    assertEquals(0, rs.getByte(1));
    assertNull(rs.getBytes(1));
    assertTrue(rs.wasNull());
    finish(table, con);
  }

  /**
   * compare behaviors (json vs arrow)
   * <p>
   * VALUE_IS_NULL      Yes                                                 No
   * -----------------------------------------------------------------------------
   * getColumnType      DECIMAL                                             0
   * getInt             NumberFormatException vs INVALID_VALUE_CONVERT      0
   * getShort           NumberFormatException vs INVALID_VALUE_CONVERT      0
   * getLong            same                                                0
   * getString          same                                                null
   * getFloat           same                                                0
   * getDouble          same                                                0
   * getBigDecimal      same                                                null
   * getObject          same                                                null
   * getByte            NumberFormatException vs INVALID_VALUE_CONVERT      0
   * getBytes           INTERNAL_ERROR        vs SUCCESS                    null
   * -------------------------------------------------------------------------------
   *
   * @throws SQLException
   */
  @Test
  public void testScaledBigInt() throws SQLException
  {
    int scale = 18;
    long[] longCompacts = {
        0, 123456789, -123456789, 2147483647, -2147483647, Long.MIN_VALUE, Long.MAX_VALUE
    };
    List<BigDecimal> caseList = Arrays.stream(longCompacts)
        .mapToObj(x -> BigDecimal.valueOf(x, scale)).collect(Collectors.toList());

    BigDecimal[] cases = caseList.stream().toArray(BigDecimal[]::new);

    String table = "test_arrow_big_int";

    String column = String.format("(a number(38,%d))", scale);
    String values = "(" + StringUtils.join(cases, "),(") + "), (null)";
    Connection con = init(table, column, values);

    ResultSet rs = con.createStatement().executeQuery("select * from " + table);

    double delta = 0.0000000000000000001;
    int columnType = rs.getMetaData().getColumnType(1);
    assertEquals(Types.DECIMAL, columnType);

    for (int i = 0; i < cases.length; i++)
    {
      rs.next();
      try
      {
        rs.getInt(1);
        fail();
      }
      catch (Exception e)
      {
          SQLException se = (SQLException) e;
          assertEquals((int) ErrorCode.INVALID_VALUE_CONVERT.getMessageCode(), se.getErrorCode());
          assertEquals(ErrorCode.INVALID_VALUE_CONVERT.getSqlState(), se.getSQLState());
      }
      try
      {
        rs.getShort(1);
        fail();
      }
      catch (Exception e)
      {
          SQLException se = (SQLException) e;
          assertEquals((int) ErrorCode.INVALID_VALUE_CONVERT.getMessageCode(), se.getErrorCode());
          assertEquals(ErrorCode.INVALID_VALUE_CONVERT.getSqlState(), se.getSQLState());
      }
      try
      {
        rs.getLong(1);
        fail();
      }
      catch (Exception e)
      {
        SQLException se = (SQLException) e;
        assertEquals((int) ErrorCode.INVALID_VALUE_CONVERT.getMessageCode(), se.getErrorCode());
        assertEquals(ErrorCode.INVALID_VALUE_CONVERT.getSqlState(), se.getSQLState());
      }

      assertEquals(cases[i].toPlainString(), rs.getString(1));
      assertEquals(Float.parseFloat(cases[i].toString()), rs.getFloat(1), delta);
      double val = Double.parseDouble(cases[i].toString());
      assertEquals(val, rs.getDouble(1), delta);
      assertEquals(new BigDecimal(rs.getString(1)), rs.getBigDecimal(1));
      assertEquals(rs.getBigDecimal(1), rs.getObject(1));
      try
      {
        rs.getByte(1);
        fail();
      }
      catch (Exception e)
      {
        if (isJSON())
        {
          // Note: not caught by SQLException!
          assertTrue(e.toString().contains("NumberFormatException"));
        }
        else
        {
          SQLException se = (SQLException) e;
          assertEquals((int) ErrorCode.INVALID_VALUE_CONVERT.getMessageCode(), se.getErrorCode());
          assertEquals(ErrorCode.INVALID_VALUE_CONVERT.getSqlState(), se.getSQLState());
        }
      }
      if (isJSON())
      {
        try
        {
          rs.getBytes(1);
          fail();
        }
        catch (Exception e)
        {
          SQLException se = (SQLException) e;
          assertEquals((int) ErrorCode.INTERNAL_ERROR.getMessageCode(), se.getErrorCode());
          assertEquals(ErrorCode.INTERNAL_ERROR.getSqlState(), se.getSQLState());
        }
      }
      else
      {
        ByteBuffer byteBuffer = ByteBuffer.allocate(BigIntVector.TYPE_WIDTH);
        byteBuffer.putLong(longCompacts[i]);
        assertArrayEquals(byteBuffer.array(), rs.getBytes(1));
      }
    }

    // null value
    rs.next();
    assertEquals(0, rs.getInt(1));
    assertEquals((short) 0, rs.getShort(1));
    assertEquals((long) 0, rs.getLong(1));
    assertNull(rs.getString(1));
    assertEquals((float) 0, rs.getFloat(1), delta);
    double val = 0;
    assertEquals(val, rs.getDouble(1), delta);
    assertNull(rs.getBigDecimal(1));
    assertEquals(null, rs.getObject(1));
    assertEquals(0, rs.getByte(1));
    assertNull(rs.getBytes(1));
    assertTrue(rs.wasNull());
    finish(table, con);
  }

  /**
   * compare behaviors (json vs arrow)
   * <p>
   * VALUE_IS_NULL      Yes                                                 No
   * -----------------------------------------------------------------------------
   * getColumnType      DECIMAL                                             0
   * getInt             NumberFormatException vs INVALID_VALUE_CONVERT      0
   * getShort           NumberFormatException vs INVALID_VALUE_CONVERT      0
   * getLong            same                                                0
   * getString          same                                                null
   * getFloat           same                                                0
   * getDouble          same                                                0
   * getBigDecimal      same                                                null
   * getObject          INTERNAL_ERROR        vs SUCCESS                    null
   * getByte            NumberFormatException vs INVALID_VALUE_CONVERT      0
   * getBytes           INTERNAL_ERROR        vs SUCCESS                    null
   * -------------------------------------------------------------------------------
   *
   * @throws SQLException
   */
  @Test
  public void testDecimalNoScale() throws SQLException
  {
    int scale = 0;
    String[] longCompacts = {
        "10000000000000000000000000000000000000",
        "12345678901234567890123456789012345678",
        "99999999999999999999999999999999999999"
    };
    List<BigDecimal> caseList = Arrays.stream(longCompacts)
        .map(x -> new BigDecimal(x)).collect(Collectors.toList());

    BigDecimal[] cases = caseList.stream().toArray(BigDecimal[]::new);

    String table = "test_arrow_decimal";

    String column = String.format("(a number(38,%d))", scale);
    String values = "(" + StringUtils.join(cases, "),(") + "), (null)";
    Connection con = init(table, column, values);

    ResultSet rs = con.createStatement().executeQuery("select * from " + table);

    double delta = 0.1;
    int columnType = rs.getMetaData().getColumnType(1);

    assertEquals(Types.BIGINT, columnType);

    for (int i = 0; i < cases.length; i++)
    {
      rs.next();
      try
      {
        rs.getInt(1);
        fail();
      }
      catch (Exception e)
      {
          SQLException se = (SQLException) e;
          assertEquals((int) ErrorCode.INVALID_VALUE_CONVERT.getMessageCode(), se.getErrorCode());
          assertEquals(ErrorCode.INVALID_VALUE_CONVERT.getSqlState(), se.getSQLState());
      }
      try
      {
        rs.getShort(1);
        fail();
      }
      catch (Exception e)
      {
          SQLException se = (SQLException) e;
          assertEquals((int) ErrorCode.INVALID_VALUE_CONVERT.getMessageCode(), se.getErrorCode());
          assertEquals(ErrorCode.INVALID_VALUE_CONVERT.getSqlState(), se.getSQLState());
      }
      try
      {
        rs.getLong(1);
        fail();
      }
      catch (Exception e)
      {
        SQLException se = (SQLException) e;
        assertEquals((int) ErrorCode.INVALID_VALUE_CONVERT.getMessageCode(), se.getErrorCode());
        assertEquals(ErrorCode.INVALID_VALUE_CONVERT.getSqlState(), se.getSQLState());
      }

      assertEquals(cases[i].toPlainString(), rs.getString(1));
      assertEquals(Float.parseFloat(cases[i].toString()), rs.getFloat(1), delta);
      double val = Double.parseDouble(cases[i].toString());
      assertEquals(val, rs.getDouble(1), delta);
      assertEquals(new BigDecimal(rs.getString(1)), rs.getBigDecimal(1));
      if (isJSON())
      {
        try
        {
          rs.getObject(1);
          fail();
        }
        catch (Exception e)
        {
          SQLException se = (SQLException) e;
          assertEquals((int) ErrorCode.INVALID_VALUE_CONVERT.getMessageCode(), se.getErrorCode());
          assertEquals(ErrorCode.INVALID_VALUE_CONVERT.getSqlState(), se.getSQLState());
        }
      }
      else
      {
        assertEquals(rs.getBigDecimal(1), rs.getObject(1));
      }
      try
      {
        rs.getByte(1);
        fail();
      }
      catch (Exception e)
      {
        if (isJSON())
        {
          // Note: not caught by SQLException!
          assertTrue(e.toString().contains("NumberFormatException"));
        }
        else
        {
          SQLException se = (SQLException) e;
          assertEquals((int) ErrorCode.INVALID_VALUE_CONVERT.getMessageCode(), se.getErrorCode());
          assertEquals(ErrorCode.INVALID_VALUE_CONVERT.getSqlState(), se.getSQLState());
        }
      }
      if (isJSON())
      {
        // wrong value
        assertArrayEquals(SFBinary.fromHex(rs.getBigDecimal(1).toString()).getBytes(),
                          rs.getBytes(1));
      }
      else
      {
        assertArrayEquals(cases[i].toBigInteger().toByteArray(), rs.getBytes(1));
      }
    }

    // null value
    rs.next();
    assertEquals(0, rs.getInt(1));
    assertEquals((short) 0, rs.getShort(1));
    assertEquals((long) 0, rs.getLong(1));
    assertNull(rs.getString(1));
    assertEquals((float) 0, rs.getFloat(1), delta);
    double val = 0;
    assertEquals(val, rs.getDouble(1), delta);
    assertNull(rs.getBigDecimal(1));
    assertEquals(null, rs.getObject(1));
    assertEquals(0, rs.getByte(1));
    assertNull(rs.getBytes(1));
    assertTrue(rs.wasNull());
    finish(table, con);
  }

  /**
   * compare behaviors (json vs arrow)
   * <p>
   * VALUE_IS_NULL      Yes                                                 No
   * -----------------------------------------------------------------------------
   * getColumnType      DECIMAL                                             0
   * getInt             NumberFormatException vs INVALID_VALUE_CONVERT      0
   * getShort           NumberFormatException vs INVALID_VALUE_CONVERT      0
   * getLong            same                                                0
   * getString          same                                                null
   * getFloat           same                                                0
   * getDouble          same                                                0
   * getBigDecimal      same                                                null
   * getObject          same                                                null
   * getByte            NumberFormatException vs INVALID_VALUE_CONVERT      0
   * getBytes           INTERNAL_ERROR        vs SUCCESS                    null
   * -------------------------------------------------------------------------------
   *
   * @throws SQLException
   */
  @Test
  public void testDecimalWithLargeScale() throws SQLException
  {
    int scale = 37;
    String[] longCompacts = {
        "1.0000000000000000000000000000000000000",
        "1.2345678901234567890123456789012345678",
        "9.9999999999999999999999999999999999999"
    };
    List<BigDecimal> caseList = Arrays.stream(longCompacts)
        .map(x -> new BigDecimal(x)).collect(Collectors.toList());

    BigDecimal[] cases = caseList.stream().toArray(BigDecimal[]::new);

    String table = "test_arrow_decimal";

    String column = String.format("(a number(38,%d))", scale);
    String values = "(" + StringUtils.join(cases, "),(") + "), (null)";
    Connection con = init(table, column, values);

    ResultSet rs = con.createStatement().executeQuery("select * from " + table);

    double delta = 0.00000000000000000000000000000000000001;
    int columnType = rs.getMetaData().getColumnType(1);
    assertEquals(Types.DECIMAL, columnType);

    for (int i = 0; i < cases.length; i++)
    {
      rs.next();
      try
      {
        rs.getInt(1);
        fail();
      }
      catch (Exception e)
      {
          SQLException se = (SQLException) e;
          assertEquals((int) ErrorCode.INVALID_VALUE_CONVERT.getMessageCode(), se.getErrorCode());
          assertEquals(ErrorCode.INVALID_VALUE_CONVERT.getSqlState(), se.getSQLState());
      }
      try
      {
        rs.getShort(1);
        fail();
      }
      catch (Exception e)
      {
          SQLException se = (SQLException) e;
          assertEquals((int) ErrorCode.INVALID_VALUE_CONVERT.getMessageCode(), se.getErrorCode());
          assertEquals(ErrorCode.INVALID_VALUE_CONVERT.getSqlState(), se.getSQLState());
      }
      try
      {
        rs.getLong(1);
        fail();
      }
      catch (Exception e)
      {
        SQLException se = (SQLException) e;
        assertEquals((int) ErrorCode.INVALID_VALUE_CONVERT.getMessageCode(), se.getErrorCode());
        assertEquals(ErrorCode.INVALID_VALUE_CONVERT.getSqlState(), se.getSQLState());
      }

      assertEquals(cases[i].toPlainString(), rs.getString(1));
      assertEquals(Float.parseFloat(cases[i].toString()), rs.getFloat(1), delta);
      double val = Double.parseDouble(cases[i].toString());
      assertEquals(val, rs.getDouble(1), delta);
      assertEquals(new BigDecimal(rs.getString(1)), rs.getBigDecimal(1));
      assertEquals(rs.getBigDecimal(1), rs.getObject(1));
      try
      {
        rs.getByte(1);
        fail();
      }
      catch (Exception e)
      {
        if (isJSON())
        {
          // Note: not caught by SQLException!
          assertTrue(e.toString().contains("NumberFormatException"));
        }
        else
        {
          SQLException se = (SQLException) e;
          assertEquals((int) ErrorCode.INVALID_VALUE_CONVERT.getMessageCode(), se.getErrorCode());
          assertEquals(ErrorCode.INVALID_VALUE_CONVERT.getSqlState(), se.getSQLState());
        }
      }
      if (isJSON())
      {
        try
        {
          rs.getBytes(1);
          fail();
        }
        catch (Exception e)
        {
          SQLException se = (SQLException) e;
          assertEquals((int) ErrorCode.INTERNAL_ERROR.getMessageCode(), se.getErrorCode());
          assertEquals(ErrorCode.INTERNAL_ERROR.getSqlState(), se.getSQLState());
        }
      }
      else
      {
        assertArrayEquals(cases[i].toBigInteger().toByteArray(), rs.getBytes(1));
      }
    }

    // null value
    rs.next();
    assertEquals(0, rs.getInt(1));
    assertEquals((short) 0, rs.getShort(1));
    assertEquals((long) 0, rs.getLong(1));
    assertNull(rs.getString(1));
    assertEquals((float) 0, rs.getFloat(1), delta);
    double val = 0;
    assertEquals(val, rs.getDouble(1), delta);
    assertNull(rs.getBigDecimal(1));
    assertEquals(null, rs.getObject(1));
    assertEquals(0, rs.getByte(1));
    assertNull(rs.getBytes(1));
    assertTrue(rs.wasNull());
    finish(table, con);
  }

  /**
   * compare behaviors (json vs arrow)
   * <p>
   * VALUE_IS_NULL      Yes                                                 No
   * -----------------------------------------------------------------------------
   * getColumnType      DECIMAL                                             0
   * getInt             NumberFormatException vs INVALID_VALUE_CONVERT      0
   * getShort           NumberFormatException vs INVALID_VALUE_CONVERT      0
   * getLong            same                                                0
   * getString          same                                                null
   * getFloat           same                                                0
   * getDouble          same                                                0
   * getBigDecimal      same                                                null
   * getObject          same                                                null
   * getByte            NumberFormatException vs INVALID_VALUE_CONVERT      0
   * getBytes           INTERNAL_ERROR        vs SUCCESS                    null
   * -------------------------------------------------------------------------------
   *
   * @throws SQLException
   */
  @Test
  public void testDecimal() throws SQLException
  {
    int scale = 37;
    long[] longCompacts = {
        0, 123456789, -123456789, 2147483647, -2147483647, Long.MIN_VALUE, Long.MAX_VALUE
    };
    List<BigDecimal> caseList = Arrays.stream(longCompacts)
        .mapToObj(x -> BigDecimal.valueOf(x, scale)).collect(Collectors.toList());

    BigDecimal[] cases = caseList.stream().toArray(BigDecimal[]::new);

    String table = "test_arrow_big_int";

    String column = String.format("(a number(38,%d))", scale);
    String values = "(" + StringUtils.join(cases, "),(") + "), (null)";
    Connection con = init(table, column, values);

    ResultSet rs = con.createStatement().executeQuery("select * from " + table);

    double delta = 0.00000000000000000000000000000000000001;
    int columnType = rs.getMetaData().getColumnType(1);
    assertEquals(Types.DECIMAL, columnType);

    for (int i = 0; i < cases.length; i++)
    {
      rs.next();
      try
      {
        rs.getInt(1);
        fail();
      }
      catch (Exception e)
      {
          SQLException se = (SQLException) e;
          assertEquals((int) ErrorCode.INVALID_VALUE_CONVERT.getMessageCode(), se.getErrorCode());
          assertEquals(ErrorCode.INVALID_VALUE_CONVERT.getSqlState(), se.getSQLState());
      }
      try
      {
        rs.getShort(1);
        fail();
      }
      catch (Exception e)
      {
          SQLException se = (SQLException) e;
          assertEquals((int) ErrorCode.INVALID_VALUE_CONVERT.getMessageCode(), se.getErrorCode());
          assertEquals(ErrorCode.INVALID_VALUE_CONVERT.getSqlState(), se.getSQLState());
      }
      try
      {
        rs.getLong(1);
        fail();
      }
      catch (Exception e)
      {
        SQLException se = (SQLException) e;
        assertEquals((int) ErrorCode.INVALID_VALUE_CONVERT.getMessageCode(), se.getErrorCode());
        assertEquals(ErrorCode.INVALID_VALUE_CONVERT.getSqlState(), se.getSQLState());
      }

      assertEquals(cases[i].toPlainString(), rs.getString(1));
      assertEquals(Float.parseFloat(cases[i].toString()), rs.getFloat(1), delta);
      double val = Double.parseDouble(cases[i].toString());
      assertEquals(val, rs.getDouble(1), delta);
      assertEquals(new BigDecimal(rs.getString(1)), rs.getBigDecimal(1));
      assertEquals(rs.getBigDecimal(1), rs.getObject(1));
      try
      {
        rs.getByte(1);
        fail();
      }
      catch (Exception e)
      {
        if (isJSON())
        {
          // Note: not caught by SQLException!
          assertTrue(e.toString().contains("NumberFormatException"));
        }
        else
        {
          SQLException se = (SQLException) e;
          assertEquals((int) ErrorCode.INVALID_VALUE_CONVERT.getMessageCode(), se.getErrorCode());
          assertEquals(ErrorCode.INVALID_VALUE_CONVERT.getSqlState(), se.getSQLState());
        }
      }
      if (isJSON())
      {
        try
        {
          rs.getBytes(1);
          fail();
        }
        catch (Exception e)
        {
          SQLException se = (SQLException) e;
          assertEquals((int) ErrorCode.INTERNAL_ERROR.getMessageCode(), se.getErrorCode());
          assertEquals(ErrorCode.INTERNAL_ERROR.getSqlState(), se.getSQLState());
        }
      }
      else
      {
        assertArrayEquals(cases[i].toBigInteger().toByteArray(), rs.getBytes(1));
      }
    }

    // null value
    rs.next();
    assertEquals(0, rs.getInt(1));
    assertEquals((short) 0, rs.getShort(1));
    assertEquals((long) 0, rs.getLong(1));
    assertNull(rs.getString(1));
    assertEquals((float) 0, rs.getFloat(1), delta);
    double val = 0;
    assertEquals(val, rs.getDouble(1), delta);
    assertNull(rs.getBigDecimal(1));
    assertEquals(null, rs.getObject(1));
    assertEquals(0, rs.getByte(1));
    assertNull(rs.getBytes(1));
    assertTrue(rs.wasNull());
    finish(table, con);
  }

  /**
   * Arrow can make sure no precision loss for double values
   *
   * @throws SQLException
   */
  @Test
  public void testDoublePrecision() throws SQLException
  {
    String[] cases = {
        // SNOW-31249
        "-86.6426540296895",
        "3.14159265359",
        // SNOW-76269
        "1.7976931348623157E308",
        "1.7E308",
        "1.7976931348623151E308",
        "-1.7976931348623151E308",
        "-1.7E308",
        "-1.7976931348623157E308"
    };

    // json results have been truncated to maxScale = 9
    String[] json_results = {
        "-86.64265403",
        "3.141592654",
        "Infinity",
        "1.7E308",
        "Infinity",
        "-Infinity",
        "-1.7E308",
        "-Infinity"
    };
    String table = "test_arrow_double";

    String column = "(a double)";
    String values = "(" + StringUtils.join(cases, "),(") + ")";
    Connection con = init(table, column, values);
    ResultSet rs = con.createStatement().executeQuery("select * from " + table);
    int i = 0;
    if (isJSON())
    {
      while (rs.next())
      {
        assertEquals(json_results[i++], Double.toString(rs.getDouble(1)));
      }
    }
    else
    {
      // Arrow results has no precision loss
      while (rs.next())
      {
        assertEquals(cases[i++], Double.toString(rs.getDouble(1)));
      }
    }
    finish(table, con);
  }

  @Test
  public void testBoolean() throws SQLException
  {
    String table = "test_arrow_boolean";
    String column = "(a boolean)";
    String values = "(true),(null),(false)";
    Connection conn = init(table, column, values);
    Statement statement = conn.createStatement();
    ResultSet rs = statement.executeQuery("select * from " + table);
    assertTrue(rs.next());
    assertTrue(rs.getBoolean(1));
    assertEquals("TRUE", rs.getString(1));
    assertTrue(rs.next());
    assertFalse(rs.getBoolean(1));
    assertTrue(rs.next());
    assertFalse(rs.getBoolean(1));
    assertEquals("FALSE", rs.getString(1));
    assertFalse(rs.next());
    finish(table, conn);
  }

  @Test
  public void testClientSideSorting() throws SQLException
  {
    String table = "test_arrow_sort_on";
    String column = "( a int, b double, c string)";
    String values = "(1,2.0,'test'),(0,2.0, 'test'),(1,2.0,'abc')";
    Connection conn = init(table, column, values);
    Statement statement = conn.createStatement();
    // turn on sorting mode
    statement.execute("set-sf-property sort on");

    ResultSet rs = statement.executeQuery("select * from " + table);
    rs.next();
    assertEquals("0", rs.getString(1));
    rs.next();
    assertEquals("1", rs.getString(1));
    rs.next();
    assertEquals("test", rs.getString(3));
    finish(table, conn);
  }

  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnTravisCI.class)
  public void testClientSideSortingOnBatchedChunk() throws SQLException
  {
    // in this test, the first chunk contains multiple batches when the format is Arrow
    String[] queries = {
        "set-sf-property sort on",
        "alter session set populate_change_tracking_columns = true;",
        "alter session set create_change_tracking_columns = true;",
        "alter session set enable_stream = true;",
        "alter session set qa_mode=true;  -- used for row_id",
        "create or replace schema stream_get_table_timestamp;",
        "create or replace  table T(id int);",
        "create stream S on table T;",
        "select system$stream_get_table_timestamp('S');",
        "select system$last_change_commit_time('T');",
        "insert into T values (1);",
        "insert into T values (2);",
        "insert into T values (3);",
        };

    Connection conn = getConnection();
    Statement stat = conn.createStatement();
    for (String q : queries)
    {
      stat.execute(q);
    }

    ResultSet rs = stat.executeQuery("select * from S");
    assertTrue(rs.next());
    assertEquals(1, rs.getInt(1));
    assertTrue(rs.next());
    assertEquals(2, rs.getInt(1));
    assertTrue(rs.next());
    assertEquals(3, rs.getInt(1));
    assertFalse(rs.next());
    stat.execute("drop stream S");
    stat.execute("drop table T");
    stat.close();
  }

  @Test
  public void testTimestampNTZAreAllNulls() throws SQLException
  {
    Connection con = getConnection();
    Statement statement = con.createStatement();
    statement.executeQuery(
        "create or replace table test_null_ts_ntz (a timestampntz(9)) as select null from table(generator" +
        "(rowcount => 1000000)) v " +
        "order by 1;");
    ResultSet rs = statement.executeQuery("select * from test_null_ts_ntz");
    while (rs.next())
    {
      rs.getObject(1);
    }
    statement.executeQuery("drop table if exists test_null_ts_ntz");
    statement.close();
  }
}
