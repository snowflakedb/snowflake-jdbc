package net.snowflake.client.internal.api.implementation.metadata;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.Types;
import java.util.stream.Stream;
import net.snowflake.client.internal.jdbc.SnowflakeColumnMetadata;
import net.snowflake.client.internal.jdbc.SnowflakeUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

public class SnowflakeDatabaseMetaDataImplColumnSizeTest {

  static Stream<Arguments> lengthBasedTypes() {
    return Stream.of(
        Arguments.of(Types.VARCHAR, 100),
        Arguments.of(Types.CHAR, 50),
        Arguments.of(Types.BINARY, 200),
        Arguments.of(Types.VARBINARY, 250));
  }

  @ParameterizedTest
  @MethodSource("lengthBasedTypes")
  public void testGetColumnSizeByLength(int type, int length) {
    SnowflakeColumnMetadata metadata = mock(SnowflakeColumnMetadata.class);
    when(metadata.getType()).thenReturn(type);
    when(metadata.getLength()).thenReturn(length);

    assertEquals(length, SnowflakeDatabaseMetaDataImpl.getColumnSize(metadata));
  }

  static Stream<Integer> precisionBasedTypes() {
    return Stream.of(
        Types.DECIMAL,
        Types.NUMERIC,
        Types.BIGINT,
        Types.INTEGER,
        Types.SMALLINT,
        Types.TINYINT,
        Types.FLOAT,
        Types.DOUBLE,
        Types.REAL,
        SnowflakeUtil.EXTRA_TYPES_DECFLOAT,
        Types.DATE,
        Types.TIME,
        Types.TIMESTAMP,
        Types.TIMESTAMP_WITH_TIMEZONE,
        SnowflakeUtil.EXTRA_TYPES_TIMESTAMP_LTZ,
        SnowflakeUtil.EXTRA_TYPES_TIMESTAMP_TZ,
        SnowflakeUtil.EXTRA_TYPES_TIMESTAMP_NTZ);
  }

  @ParameterizedTest
  @MethodSource("precisionBasedTypes")
  public void testGetColumnSizeByPrecision(int type) {
    int precision = 38;
    SnowflakeColumnMetadata metadata = mock(SnowflakeColumnMetadata.class);
    when(metadata.getType()).thenReturn(type);
    when(metadata.getPrecision()).thenReturn(precision);

    assertEquals(precision, SnowflakeDatabaseMetaDataImpl.getColumnSize(metadata));
  }

  @Test
  public void testGetColumnSizeVector() {
    SnowflakeColumnMetadata metadata = mock(SnowflakeColumnMetadata.class);
    when(metadata.getType()).thenReturn(SnowflakeUtil.EXTRA_TYPES_VECTOR);
    when(metadata.getDimension()).thenReturn(128);

    assertEquals(128, SnowflakeDatabaseMetaDataImpl.getColumnSize(metadata));
  }

  @ParameterizedTest
  @ValueSource(ints = {Types.BOOLEAN, Types.ARRAY, Types.STRUCT})
  public void testGetColumnSizeOther(int type) {
    SnowflakeColumnMetadata metadata = mock(SnowflakeColumnMetadata.class);
    when(metadata.getType()).thenReturn(type);
    assertNull(SnowflakeDatabaseMetaDataImpl.getColumnSize(metadata));
  }
}
