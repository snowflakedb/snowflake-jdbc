package net.snowflake.client.jdbc;

import static net.snowflake.client.jdbc.SnowflakeType.convertStringToType;
import static net.snowflake.client.jdbc.SnowflakeType.getJavaType;
import static org.junit.jupiter.api.Assertions.assertThrows;
import java.math.BigDecimal;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Time;
import java.sql.Types;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class SnowflakeTypeTest {

  @Test
  public void testSnowflakeType() {
    Assertions.assertEquals(getJavaType(SnowflakeType.CHAR, false), SnowflakeType.JavaDataType.JAVA_STRING);
    Assertions.assertEquals(getJavaType(SnowflakeType.INTEGER, false), SnowflakeType.JavaDataType.JAVA_LONG);
    Assertions.assertEquals(getJavaType(SnowflakeType.FIXED, false), SnowflakeType.JavaDataType.JAVA_BIGDECIMAL);
    Assertions.assertEquals(getJavaType(SnowflakeType.TIMESTAMP, false), SnowflakeType.JavaDataType.JAVA_TIMESTAMP);
    Assertions.assertEquals(getJavaType(SnowflakeType.TIME, false), SnowflakeType.JavaDataType.JAVA_TIMESTAMP);
    Assertions.assertEquals(getJavaType(SnowflakeType.TIMESTAMP_LTZ, false), SnowflakeType.JavaDataType.JAVA_TIMESTAMP);
    Assertions.assertEquals(getJavaType(SnowflakeType.TIMESTAMP_NTZ, false), SnowflakeType.JavaDataType.JAVA_TIMESTAMP);
    Assertions.assertEquals(getJavaType(SnowflakeType.TIMESTAMP_TZ, false), SnowflakeType.JavaDataType.JAVA_TIMESTAMP);
    Assertions.assertEquals(getJavaType(SnowflakeType.DATE, false), SnowflakeType.JavaDataType.JAVA_TIMESTAMP);
    Assertions.assertEquals(getJavaType(SnowflakeType.BOOLEAN, false), SnowflakeType.JavaDataType.JAVA_BOOLEAN);
    Assertions.assertEquals(getJavaType(SnowflakeType.VECTOR, false), SnowflakeType.JavaDataType.JAVA_STRING);
    Assertions.assertEquals(getJavaType(SnowflakeType.BINARY, false), SnowflakeType.JavaDataType.JAVA_BYTES);
    Assertions.assertEquals(getJavaType(SnowflakeType.ANY, false), SnowflakeType.JavaDataType.JAVA_OBJECT);
    Assertions.assertEquals(getJavaType(SnowflakeType.OBJECT, true), SnowflakeType.JavaDataType.JAVA_OBJECT);
    Assertions.assertEquals(getJavaType(SnowflakeType.OBJECT, false), SnowflakeType.JavaDataType.JAVA_STRING);
    Assertions.assertEquals(getJavaType(SnowflakeType.GEOMETRY, false), SnowflakeType.JavaDataType.JAVA_STRING);
  }

  @Test
  public void testConvertStringToType() {
    Assertions.assertEquals(convertStringToType(null), Types.NULL);
    Assertions.assertEquals(convertStringToType("decimal"), Types.DECIMAL);
    Assertions.assertEquals(convertStringToType("int"), Types.INTEGER);
    Assertions.assertEquals(convertStringToType("integer"), Types.INTEGER);
    Assertions.assertEquals(convertStringToType("byteint"), Types.INTEGER);
    Assertions.assertEquals(convertStringToType("smallint"), Types.SMALLINT);
    Assertions.assertEquals(convertStringToType("bigint"), Types.BIGINT);
    Assertions.assertEquals(convertStringToType("double"), Types.DOUBLE);
    Assertions.assertEquals(convertStringToType("double precision"), Types.DOUBLE);
    Assertions.assertEquals(convertStringToType("real"), Types.REAL);
    Assertions.assertEquals(convertStringToType("char"), Types.CHAR);
    Assertions.assertEquals(convertStringToType("character"), Types.CHAR);
    Assertions.assertEquals(convertStringToType("varbinary"), Types.VARBINARY);
    Assertions.assertEquals(convertStringToType("boolean"), Types.BOOLEAN);
    Assertions.assertEquals(convertStringToType("date"), Types.DATE);
    Assertions.assertEquals(convertStringToType("time"), Types.TIME);
    Assertions.assertEquals(convertStringToType("timestamp"), Types.TIMESTAMP);
    Assertions.assertEquals(convertStringToType("datetime"), Types.TIMESTAMP);
    Assertions.assertEquals(convertStringToType("timestamp_ntz"), Types.TIMESTAMP);
    Assertions.assertEquals(convertStringToType("timestamp_ltz"), Types.TIMESTAMP_WITH_TIMEZONE);
    Assertions.assertEquals(convertStringToType("timestamp_tz"), Types.TIMESTAMP_WITH_TIMEZONE);
    Assertions.assertEquals(convertStringToType("variant"), Types.OTHER);
    Assertions.assertEquals(convertStringToType("object"), Types.JAVA_OBJECT);
    Assertions.assertEquals(convertStringToType("vector"), SnowflakeUtil.EXTRA_TYPES_VECTOR);
    Assertions.assertEquals(convertStringToType("array"), Types.ARRAY);
    Assertions.assertEquals(convertStringToType("default"), Types.OTHER);
  }

  @Test
  public void testJavaSQLTypeFind() {
    Assertions.assertNull(SnowflakeType.JavaSQLType.find(200000));
  }

  @Test
  public void testJavaSQLTypeLexicalValue() {
    Assertions.assertEquals(SnowflakeType.lexicalValue(1.0f, null, null, null, null), "0x1.0p0");
    Assertions.assertEquals(SnowflakeType.lexicalValue(new BigDecimal(100.0), null, null, null, null), "100");
    Assertions.assertEquals(SnowflakeType.lexicalValue("random".getBytes(), null, null, null, null), "72616E646F6D");
  }

  @Test
  public void testJavaTypeToSFType() throws SnowflakeSQLException {
    Assertions.assertEquals(SnowflakeType.javaTypeToSFType(0, null), SnowflakeType.ANY);
    assertThrows(
        SnowflakeSQLLoggedException.class,
        () -> {
          SnowflakeType.javaTypeToSFType(2000000, null);
        });
  }

  @Test
  public void testJavaTypeToClassName() throws SQLException {
    Assertions.assertEquals(SnowflakeType.javaTypeToClassName(Types.DECIMAL), BigDecimal.class.getName());
    Assertions.assertEquals(SnowflakeType.javaTypeToClassName(Types.TIME), Time.class.getName());
    Assertions.assertEquals(SnowflakeType.javaTypeToClassName(Types.BOOLEAN), Boolean.class.getName());
    assertThrows(
        SQLFeatureNotSupportedException.class,
        () -> {
          SnowflakeType.javaTypeToClassName(-2000000);
        });
  }
}
