package net.snowflake.client.jdbc;

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
  @Parameterized.Parameters
  public static Object[][] data()
  {
    // all tests in this class need to run for both query result formats json and arrow
    if (BaseJDBCTest.isArrowTestsEnabled())
    {
      return new Object[][]{
          {"JSON"}
          , {"Arrow_force"}
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

  /**
   * Note: Arrow format does not include space and \n in the string values
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
      if (isJSON())
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
      }
      else
      {
        assertEquals("[10,20,30]", rs.getString(1));
        assertEquals("[undefined,\"hello\",3.000000000000000e+00,4,5]", rs.getString(2));
        assertEquals("{\"a\":1,\"b\":\"BBBB\"}", rs.getString(4));
        assertEquals("{\"Key_One\":null,\"Key_Three\":\"null\"}", rs.getString(5));
        assertEquals("{\"a\":null}", rs.getString(7));
      }
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
   *
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
   *-------------------------------------------------------------------------
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
    for (int i =0;i < cases.length;i++)
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
   *
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
   *--------------------------------------------------------------------------------------------------
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

    for (int i =0;i < cases.length;i++)
    {
      rs.next();
      try
      {
        rs.getInt(1);
        fail();
      }
      catch (Exception e)
      {
        if (isJSON())
        {
          assertTrue(e.toString().contains("NumberFormatException"));
        }
        else
        {
          SQLException se = (SQLException) e;
          assertEquals((int) ErrorCode.INVALID_VALUE_CONVERT.getMessageCode(), se.getErrorCode());
          assertEquals(ErrorCode.INVALID_VALUE_CONVERT.getSqlState(), se.getSQLState());
        }
      }
      try
      {
        rs.getShort(1);
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
   *
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
   *-------------------------------------------------------------------------
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
    for (int i =0;i < cases.length;i++)
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
   *
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
   *-------------------------------------------------------------------------------
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

    for (int i =0;i < cases.length;i++)
    {
      rs.next();
      try
      {
        rs.getInt(1);
        fail();
      }
      catch (Exception e)
      {
        if (isJSON())
        {
          assertTrue(e.toString().contains("NumberFormatException"));
        }
        else
        {
          SQLException se = (SQLException) e;
          assertEquals((int) ErrorCode.INVALID_VALUE_CONVERT.getMessageCode(), se.getErrorCode());
          assertEquals(ErrorCode.INVALID_VALUE_CONVERT.getSqlState(), se.getSQLState());
        }
      }
      try
      {
        rs.getShort(1);
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
   *
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
   *                    Return wrong result
   *-------------------------------------------------------------------------
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
    for (int i =0;i < cases.length;i++)
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
   *
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
   *-------------------------------------------------------------------------------
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

    for (int i =0;i < cases.length;i++)
    {
      rs.next();
      try
      {
        rs.getInt(1);
        fail();
      }
      catch (Exception e)
      {
        if (isJSON())
        {
          assertTrue(e.toString().contains("NumberFormatException"));
        }
        else
        {
          SQLException se = (SQLException) e;
          assertEquals((int) ErrorCode.INVALID_VALUE_CONVERT.getMessageCode(), se.getErrorCode());
          assertEquals(ErrorCode.INVALID_VALUE_CONVERT.getSqlState(), se.getSQLState());
        }
      }
      try
      {
        rs.getShort(1);
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
   *
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
   *                    Return wrong result
   *-------------------------------------------------------------------------
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
    for (int i =0;i < cases.length;i++)
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
   *
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
   *-------------------------------------------------------------------------------
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

    for (int i =0;i < cases.length;i++)
    {
      rs.next();
      try
      {
        rs.getInt(1);
        fail();
      }
      catch (Exception e)
      {
        if (isJSON())
        {
          assertTrue(e.toString().contains("NumberFormatException"));
        }
        else
        {
          SQLException se = (SQLException) e;
          assertEquals((int) ErrorCode.INVALID_VALUE_CONVERT.getMessageCode(), se.getErrorCode());
          assertEquals(ErrorCode.INVALID_VALUE_CONVERT.getSqlState(), se.getSQLState());
        }
      }
      try
      {
        rs.getShort(1);
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
   *
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
   *-------------------------------------------------------------------------------
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

    for (int i =0;i < cases.length;i++)
    {
      rs.next();
      try
      {
        rs.getInt(1);
        fail();
      }
      catch (Exception e)
      {
        if (isJSON())
        {
          assertTrue(e.toString().contains("NumberFormatException"));
        }
        else
        {
          SQLException se = (SQLException) e;
          assertEquals((int) ErrorCode.INVALID_VALUE_CONVERT.getMessageCode(), se.getErrorCode());
          assertEquals(ErrorCode.INVALID_VALUE_CONVERT.getSqlState(), se.getSQLState());
        }
      }
      try
      {
        rs.getShort(1);
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
   *
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
   *-------------------------------------------------------------------------------
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

    for (int i =0;i < cases.length;i++)
    {
      rs.next();
      try
      {
        rs.getInt(1);
        fail();
      }
      catch (Exception e)
      {
        if (isJSON())
        {
          assertTrue(e.toString().contains("NumberFormatException"));
        }
        else
        {
          SQLException se = (SQLException) e;
          assertEquals((int) ErrorCode.INVALID_VALUE_CONVERT.getMessageCode(), se.getErrorCode());
          assertEquals(ErrorCode.INVALID_VALUE_CONVERT.getSqlState(), se.getSQLState());
        }
      }
      try
      {
        rs.getShort(1);
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
   *
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
   *-------------------------------------------------------------------------------
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

    for (int i =0;i < cases.length;i++)
    {
      rs.next();
      try
      {
        rs.getInt(1);
        fail();
      }
      catch (Exception e)
      {
        if (isJSON())
        {
          assertTrue(e.toString().contains("NumberFormatException"));
        }
        else
        {
          SQLException se = (SQLException) e;
          assertEquals((int) ErrorCode.INVALID_VALUE_CONVERT.getMessageCode(), se.getErrorCode());
          assertEquals(ErrorCode.INVALID_VALUE_CONVERT.getSqlState(), se.getSQLState());
        }
      }
      try
      {
        rs.getShort(1);
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
      while(rs.next())
      {
        assertEquals(json_results[i++], Double.toString(rs.getDouble(1)));
      }
    }
    else
    {
      // Arrow results has no precision loss
      while(rs.next())
      {
        assertEquals(cases[i++], Double.toString(rs.getDouble(1)));
      }
    }
    finish(table, con);
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
}
