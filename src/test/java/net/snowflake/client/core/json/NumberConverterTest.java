package net.snowflake.client.core.json;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.math.BigDecimal;
import java.sql.Types;
import net.snowflake.client.core.SFException;
import org.junit.jupiter.api.Test;

public class NumberConverterTest {
  private final NumberConverter numberConverter = new NumberConverter();

  @Test
  public void testByteFromNumber() {
    assertThat(numberConverter.getByte(1), equalTo((byte) 1));
    assertThat(numberConverter.getByte(258), equalTo((byte) 2));
    assertThat(numberConverter.getByte(-1), equalTo((byte) -1));
  }

  @Test
  public void testByteFromString() {
    assertThat(numberConverter.getByte("1"), equalTo((byte) 1));
    assertThat(numberConverter.getByte("-1"), equalTo((byte) -1));
  }

  @Test
  public void testShortFromNumber() throws SFException {
    assertThat(numberConverter.getShort(1, Types.INTEGER), equalTo((short) 1));
    assertThat(numberConverter.getShort(2.5, Types.DOUBLE), equalTo((short) 2));
    assertThat(numberConverter.getShort(3.4f, Types.FLOAT), equalTo((short) 3));
  }

  @Test
  public void testShortFromString() throws SFException {
    assertThat(numberConverter.getShort("1", Types.INTEGER), equalTo((short) 1));
    assertThat(numberConverter.getShort("2.5", Types.DOUBLE), equalTo((short) 2));
    assertThat(numberConverter.getShort("3.4", Types.FLOAT), equalTo((short) 3));
    assertThat(numberConverter.getShort("4.5.6", Types.FLOAT), equalTo((short) 4));
  }

  @Test
  public void testIntFromNumber() throws SFException {
    assertThat(numberConverter.getInt(1, Types.INTEGER), equalTo(1));
    assertThat(numberConverter.getInt(2.5, Types.DOUBLE), equalTo(2));
    assertThat(numberConverter.getInt(3.4f, Types.FLOAT), equalTo(3));
  }

  @Test
  public void testIntFromString() throws SFException {
    assertThat(numberConverter.getInt("1", Types.INTEGER), equalTo(1));
    assertThat(numberConverter.getInt("2.5", Types.DOUBLE), equalTo(2));
    assertThat(numberConverter.getInt("3.4", Types.FLOAT), equalTo(3));
    assertThat(numberConverter.getInt("4.5.6", Types.FLOAT), equalTo(4));
  }

  @Test
  public void testLongFromNumber() throws SFException {
    assertThat(numberConverter.getLong(1, Types.INTEGER), equalTo(1L));
    assertThat(numberConverter.getLong(2.5, Types.DOUBLE), equalTo(2L));
    assertThat(numberConverter.getLong(3.4f, Types.FLOAT), equalTo(3L));
  }

  @Test
  public void testLongFromString() throws SFException {
    assertThat(numberConverter.getLong("1", Types.INTEGER), equalTo(1L));
    assertThat(numberConverter.getLong("2.5", Types.DOUBLE), equalTo(2L));
    assertThat(numberConverter.getLong("3.4", Types.FLOAT), equalTo(3L));
    assertThat(numberConverter.getLong("4.5.6", Types.FLOAT), equalTo(4L));
  }

  @Test
  public void testBigDecimalFromNumber() throws SFException {
    assertThat(numberConverter.getBigDecimal(1, Types.INTEGER), equalTo(BigDecimal.ONE));
    assertThat(numberConverter.getBigDecimal(1, Types.BIGINT), equalTo(BigDecimal.ONE));
    assertThat(numberConverter.getBigDecimal(1.5, Types.FLOAT), equalTo(new BigDecimal("1.5")));
  }

  @Test
  public void testBigDecimalFromString() throws SFException {
    assertThat(numberConverter.getBigDecimal("1", Types.INTEGER), equalTo(BigDecimal.ONE));
    assertThat(numberConverter.getBigDecimal("1", Types.BIGINT), equalTo(BigDecimal.ONE));
    assertThat(numberConverter.getBigDecimal("1.5", Types.FLOAT), equalTo(new BigDecimal("1.5")));
  }

  @Test
  public void testBigDecimalWithScaleFromNumber() throws SFException {
    assertThat(numberConverter.getBigDecimal(1, Types.INTEGER), equalTo(BigDecimal.ONE));
    assertThat(numberConverter.getBigDecimal(1, Types.BIGINT), equalTo(BigDecimal.ONE));
    assertThat(numberConverter.getBigDecimal(1.5, Types.FLOAT), equalTo(new BigDecimal("1.5")));
    assertThat(
        numberConverter.getBigDecimal(1.50001, Types.FLOAT, 1), equalTo(new BigDecimal("1.5")));
    assertThat(
        numberConverter.getBigDecimal(1.50001, Types.FLOAT, 5), equalTo(new BigDecimal("1.50001")));
  }

  @Test
  public void testBigDecimalWithScaleFromString() throws SFException {
    assertThat(numberConverter.getBigDecimal("1", Types.INTEGER), equalTo(BigDecimal.ONE));
    assertThat(numberConverter.getBigDecimal("1", Types.BIGINT), equalTo(BigDecimal.ONE));
    assertThat(numberConverter.getBigDecimal("1.5", Types.FLOAT), equalTo(new BigDecimal("1.5")));
    assertThat(
        numberConverter.getBigDecimal("1.50001", Types.FLOAT, 1), equalTo(new BigDecimal("1.5")));
    assertThat(
        numberConverter.getBigDecimal("1.50001", Types.FLOAT, 5),
        equalTo(new BigDecimal("1.50001")));
  }

  @Test
  public void testFloatFromNumber() throws SFException {
    assertThat(numberConverter.getFloat(1, Types.INTEGER), equalTo(1.0f));
    assertThat(numberConverter.getFloat(1, Types.BIGINT), equalTo(1.0f));
    assertThat(numberConverter.getFloat(1.5, Types.FLOAT), equalTo(1.5f));
    assertThat(numberConverter.getFloat(1.5, Types.DOUBLE), equalTo(1.5f));
  }

  @Test
  public void testFloatFromString() throws SFException {
    assertThat(numberConverter.getFloat("1", Types.INTEGER), equalTo(1.0f));
    assertThat(numberConverter.getFloat("1", Types.BIGINT), equalTo(1.0f));
    assertThat(numberConverter.getFloat("1.5", Types.FLOAT), equalTo(1.5f));
    assertThat(numberConverter.getFloat("1.5", Types.DOUBLE), equalTo(1.5f));
    assertThat(numberConverter.getFloat("inf", Types.FLOAT), equalTo(Float.POSITIVE_INFINITY));
    assertThat(numberConverter.getFloat("-inf", Types.FLOAT), equalTo(Float.NEGATIVE_INFINITY));
  }

  @Test
  public void testDoubleFromNumber() throws SFException {
    assertThat(numberConverter.getDouble(1, Types.INTEGER), equalTo(1.0));
    assertThat(numberConverter.getDouble(1, Types.BIGINT), equalTo(1.0));
    assertThat(numberConverter.getDouble(1.5, Types.FLOAT), equalTo(1.5));
    assertThat(numberConverter.getDouble(1.5, Types.DOUBLE), equalTo(1.5));
  }

  @Test
  public void testDoubleFromString() throws SFException {
    assertThat(numberConverter.getDouble("1", Types.INTEGER), equalTo(1.0));
    assertThat(numberConverter.getDouble("1", Types.BIGINT), equalTo(1.0));
    assertThat(numberConverter.getDouble("1.5", Types.FLOAT), equalTo(1.5));
    assertThat(numberConverter.getDouble("1.5", Types.DOUBLE), equalTo(1.5));
    assertThat(numberConverter.getDouble("inf", Types.FLOAT), equalTo(Double.POSITIVE_INFINITY));
    assertThat(numberConverter.getDouble("-inf", Types.FLOAT), equalTo(Double.NEGATIVE_INFINITY));
  }

  @Test
  public void testBigIntFromNumber() throws SFException {
    assertThat(numberConverter.getBigInt(1, Types.BIGINT), equalTo(1L));
    assertThat(
        numberConverter.getBigInt(9_223_372_036_854_775_807L, Types.BIGINT),
        equalTo(9_223_372_036_854_775_807L));
  }

  @Test
  public void testBigIntFromString() throws SFException {
    assertThat(numberConverter.getBigInt("1", Types.BIGINT), equalTo(1L));
    assertThat(
        numberConverter.getBigInt("9223372036854775807", Types.BIGINT),
        equalTo(9_223_372_036_854_775_807L));
    assertThat(
        numberConverter.getBigInt("9223372036854775808", Types.BIGINT),
        equalTo(new BigDecimal("9223372036854775808")));
  }
}
