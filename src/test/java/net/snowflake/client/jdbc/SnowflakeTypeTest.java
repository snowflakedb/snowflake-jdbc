package net.snowflake.client.jdbc;

import static net.snowflake.client.jdbc.SnowflakeType.convertStringToType;
import static net.snowflake.client.jdbc.SnowflakeType.getJavaType;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Time;
import java.sql.Types;
import org.junit.jupiter.api.Test;

public class SnowflakeTypeTest {

  @Test
  public void testSnowflakeType() {
    assertEquals(getJavaType(SnowflakeType.CHAR, false), SnowflakeType.JavaDataType.JAVA_STRING);
    assertEquals(getJavaType(SnowflakeType.INTEGER, false), SnowflakeType.JavaDataType.JAVA_LONG);
    assertEquals(
        getJavaType(SnowflakeType.FIXED, false), SnowflakeType.JavaDataType.JAVA_BIGDECIMAL);
    assertEquals(
        getJavaType(SnowflakeType.TIMESTAMP, false), SnowflakeType.JavaDataType.JAVA_TIMESTAMP);
    assertEquals(getJavaType(SnowflakeType.TIME, false), SnowflakeType.JavaDataType.JAVA_TIMESTAMP);
    assertEquals(
        getJavaType(SnowflakeType.TIMESTAMP_LTZ, false), SnowflakeType.JavaDataType.JAVA_TIMESTAMP);
    assertEquals(
        getJavaType(SnowflakeType.TIMESTAMP_NTZ, false), SnowflakeType.JavaDataType.JAVA_TIMESTAMP);
    assertEquals(
        getJavaType(SnowflakeType.TIMESTAMP_TZ, false), SnowflakeType.JavaDataType.JAVA_TIMESTAMP);
    assertEquals(getJavaType(SnowflakeType.DATE, false), SnowflakeType.JavaDataType.JAVA_TIMESTAMP);
    assertEquals(
        getJavaType(SnowflakeType.BOOLEAN, false), SnowflakeType.JavaDataType.JAVA_BOOLEAN);
    assertEquals(getJavaType(SnowflakeType.VECTOR, false), SnowflakeType.JavaDataType.JAVA_STRING);
    assertEquals(getJavaType(SnowflakeType.BINARY, false), SnowflakeType.JavaDataType.JAVA_BYTES);
    assertEquals(getJavaType(SnowflakeType.ANY, false), SnowflakeType.JavaDataType.JAVA_OBJECT);
    assertEquals(getJavaType(SnowflakeType.OBJECT, true), SnowflakeType.JavaDataType.JAVA_OBJECT);
    assertEquals(getJavaType(SnowflakeType.OBJECT, false), SnowflakeType.JavaDataType.JAVA_STRING);
    assertEquals(
        getJavaType(SnowflakeType.GEOMETRY, false), SnowflakeType.JavaDataType.JAVA_STRING);
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
    assertNull(SnowflakeType.JavaSQLType.find(200000));
  }

  @Test
  public void testJavaSQLTypeLexicalValue() {
    assertEquals(SnowflakeType.lexicalValue(1.0f, null, null, null, null), "0x1.0p0");
    assertEquals(SnowflakeType.lexicalValue(new BigDecimal(100.0), null, null, null, null), "100");
    assertEquals(
        SnowflakeType.lexicalValue("random".getBytes(), null, null, null, null), "72616E646F6D");
  }

  @Test
  public void testJavaTypeToSFType() throws SnowflakeSQLException {
    assertEquals(SnowflakeType.javaTypeToSFType(0, null), SnowflakeType.ANY);
    assertThrows(
        SnowflakeSQLLoggedException.class,
        () -> {
          SnowflakeType.javaTypeToSFType(2000000, null);
        });
  }

  @Test
  public void testJavaTypeToClassName() throws SQLException {
    assertEquals(SnowflakeType.javaTypeToClassName(Types.DECIMAL), BigDecimal.class.getName());
    assertEquals(SnowflakeType.javaTypeToClassName(Types.TIME), Time.class.getName());
    assertEquals(SnowflakeType.javaTypeToClassName(Types.BOOLEAN), Boolean.class.getName());
    assertThrows(
        SQLFeatureNotSupportedException.class,
        () -> {
          SnowflakeType.javaTypeToClassName(-2000000);
        });
  }
}
