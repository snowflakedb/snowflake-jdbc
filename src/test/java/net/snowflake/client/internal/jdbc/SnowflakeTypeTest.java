package net.snowflake.client.internal.jdbc;

import static net.snowflake.client.internal.jdbc.util.SnowflakeTypeHelper.convertStringToType;
import static net.snowflake.client.internal.jdbc.util.SnowflakeTypeUtil.getJavaType;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Time;
import java.sql.Types;
import net.snowflake.client.api.exception.SnowflakeSQLException;
import net.snowflake.client.api.resultset.SnowflakeType;
import net.snowflake.client.internal.exception.SnowflakeSQLLoggedException;
import net.snowflake.client.internal.jdbc.util.SnowflakeTypeHelper;
import net.snowflake.client.internal.jdbc.util.SnowflakeTypeUtil;
import org.junit.jupiter.api.Test;

public class SnowflakeTypeTest {

  @Test
  public void testSnowflakeType() {
    assertEquals(
        getJavaType(SnowflakeType.CHAR, false), SnowflakeTypeHelper.JavaDataType.JAVA_STRING);
    assertEquals(
        getJavaType(SnowflakeType.INTEGER, false), SnowflakeTypeHelper.JavaDataType.JAVA_LONG);
    assertEquals(
        getJavaType(SnowflakeType.FIXED, false), SnowflakeTypeHelper.JavaDataType.JAVA_BIGDECIMAL);
    assertEquals(
        getJavaType(SnowflakeType.TIMESTAMP, false),
        SnowflakeTypeHelper.JavaDataType.JAVA_TIMESTAMP);
    assertEquals(
        getJavaType(SnowflakeType.TIME, false), SnowflakeTypeHelper.JavaDataType.JAVA_TIMESTAMP);
    assertEquals(
        getJavaType(SnowflakeType.TIMESTAMP_LTZ, false),
        SnowflakeTypeHelper.JavaDataType.JAVA_TIMESTAMP);
    assertEquals(
        getJavaType(SnowflakeType.TIMESTAMP_NTZ, false),
        SnowflakeTypeHelper.JavaDataType.JAVA_TIMESTAMP);
    assertEquals(
        getJavaType(SnowflakeType.TIMESTAMP_TZ, false),
        SnowflakeTypeHelper.JavaDataType.JAVA_TIMESTAMP);
    assertEquals(
        getJavaType(SnowflakeType.DATE, false), SnowflakeTypeHelper.JavaDataType.JAVA_TIMESTAMP);
    assertEquals(
        getJavaType(SnowflakeType.BOOLEAN, false), SnowflakeTypeHelper.JavaDataType.JAVA_BOOLEAN);
    assertEquals(
        getJavaType(SnowflakeType.VECTOR, false), SnowflakeTypeHelper.JavaDataType.JAVA_STRING);
    assertEquals(
        getJavaType(SnowflakeType.BINARY, false), SnowflakeTypeHelper.JavaDataType.JAVA_BYTES);
    assertEquals(
        getJavaType(SnowflakeType.ANY, false), SnowflakeTypeHelper.JavaDataType.JAVA_OBJECT);
    assertEquals(
        getJavaType(SnowflakeType.OBJECT, true), SnowflakeTypeHelper.JavaDataType.JAVA_OBJECT);
    assertEquals(
        getJavaType(SnowflakeType.OBJECT, false), SnowflakeTypeHelper.JavaDataType.JAVA_STRING);
    assertEquals(
        getJavaType(SnowflakeType.GEOMETRY, false), SnowflakeTypeHelper.JavaDataType.JAVA_STRING);
  }

  @Test
  public void testConvertStringToType() {
    assertEquals(convertStringToType(null), Types.NULL);
    assertEquals(convertStringToType("decimal"), Types.DECIMAL);
    assertEquals(convertStringToType("int"), Types.INTEGER);
    assertEquals(convertStringToType("integer"), Types.INTEGER);
    assertEquals(convertStringToType("byteint"), Types.INTEGER);
    assertEquals(convertStringToType("smallint"), Types.SMALLINT);
    assertEquals(convertStringToType("bigint"), Types.BIGINT);
    assertEquals(convertStringToType("double"), Types.DOUBLE);
    assertEquals(convertStringToType("double precision"), Types.DOUBLE);
    assertEquals(convertStringToType("real"), Types.REAL);
    assertEquals(convertStringToType("char"), Types.CHAR);
    assertEquals(convertStringToType("character"), Types.CHAR);
    assertEquals(convertStringToType("varbinary"), Types.VARBINARY);
    assertEquals(convertStringToType("boolean"), Types.BOOLEAN);
    assertEquals(convertStringToType("date"), Types.DATE);
    assertEquals(convertStringToType("time"), Types.TIME);
    assertEquals(convertStringToType("timestamp"), Types.TIMESTAMP);
    assertEquals(convertStringToType("datetime"), Types.TIMESTAMP);
    assertEquals(convertStringToType("timestamp_ntz"), Types.TIMESTAMP);
    assertEquals(convertStringToType("timestamp_ltz"), Types.TIMESTAMP_WITH_TIMEZONE);
    assertEquals(convertStringToType("timestamp_tz"), Types.TIMESTAMP_WITH_TIMEZONE);
    assertEquals(convertStringToType("variant"), Types.OTHER);
    assertEquals(convertStringToType("object"), Types.JAVA_OBJECT);
    assertEquals(convertStringToType("vector"), SnowflakeUtil.EXTRA_TYPES_VECTOR);
    assertEquals(convertStringToType("array"), Types.ARRAY);
    assertEquals(convertStringToType("default"), Types.OTHER);
  }

  @Test
  public void testJavaSQLTypeFind() {
    assertNull(SnowflakeTypeHelper.JavaSQLType.find(200000));
  }

  @Test
  public void testJavaSQLTypeLexicalValue() {
    assertEquals(SnowflakeTypeUtil.lexicalValue(1.0f, null, null, null, null), "0x1.0p0");
    assertEquals(
        SnowflakeTypeUtil.lexicalValue(new BigDecimal(100.0), null, null, null, null), "100");
    assertEquals(
        SnowflakeTypeUtil.lexicalValue("random".getBytes(), null, null, null, null),
        "72616E646F6D");
  }

  @Test
  public void testJavaTypeToSFType() throws SnowflakeSQLException {
    assertEquals(SnowflakeTypeUtil.javaTypeToSFType(0, null), SnowflakeType.ANY);
    assertThrows(
        SnowflakeSQLLoggedException.class,
        () -> {
          SnowflakeTypeUtil.javaTypeToSFType(2000000, null);
        });
  }

  @Test
  public void testJavaTypeToClassName() throws SQLException {
    assertEquals(SnowflakeTypeUtil.javaTypeToClassName(Types.DECIMAL), BigDecimal.class.getName());
    assertEquals(SnowflakeTypeUtil.javaTypeToClassName(Types.TIME), Time.class.getName());
    assertEquals(SnowflakeTypeUtil.javaTypeToClassName(Types.BOOLEAN), Boolean.class.getName());
    assertThrows(
        SQLFeatureNotSupportedException.class,
        () -> {
          SnowflakeTypeUtil.javaTypeToClassName(-2000000);
        });
  }
}
