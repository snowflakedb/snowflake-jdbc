package net.snowflake.client.core.json;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.sql.Types;
import net.snowflake.client.core.SFException;
import org.junit.jupiter.api.Test;

public class BooleanConverterTest {
  private final BooleanConverter booleanConverter = new BooleanConverter();

  @Test
  public void testConvertBoolean() throws SFException {
    assertThat(booleanConverter.getBoolean(true, Types.BOOLEAN), equalTo(true));
    assertThat(booleanConverter.getBoolean(false, Types.BOOLEAN), equalTo(false));
  }

  @Test
  public void testConvertNumeric() throws SFException {
    assertThat(booleanConverter.getBoolean(1, Types.INTEGER), equalTo(true));
    assertThat(booleanConverter.getBoolean(1, Types.SMALLINT), equalTo(true));
    assertThat(booleanConverter.getBoolean(1, Types.TINYINT), equalTo(true));
    assertThat(booleanConverter.getBoolean(1, Types.BIGINT), equalTo(true));
    assertThat(booleanConverter.getBoolean(1, Types.BIT), equalTo(true));
    assertThat(booleanConverter.getBoolean(1, Types.DECIMAL), equalTo(true));
    assertThat(booleanConverter.getBoolean(0, Types.INTEGER), equalTo(false));
    assertThat(booleanConverter.getBoolean(0, Types.SMALLINT), equalTo(false));
    assertThat(booleanConverter.getBoolean(0, Types.TINYINT), equalTo(false));
    assertThat(booleanConverter.getBoolean(0, Types.BIGINT), equalTo(false));
    assertThat(booleanConverter.getBoolean(0, Types.BIT), equalTo(false));
    assertThat(booleanConverter.getBoolean(0, Types.DECIMAL), equalTo(false));
  }

  @Test
  public void testConvertString() throws SFException {
    assertThat(booleanConverter.getBoolean("1", Types.VARCHAR), equalTo(true));
    assertThat(booleanConverter.getBoolean("1", Types.CHAR), equalTo(true));
    assertThat(booleanConverter.getBoolean("true", Types.VARCHAR), equalTo(true));
    assertThat(booleanConverter.getBoolean("TRUE", Types.CHAR), equalTo(true));
    assertThat(booleanConverter.getBoolean("0", Types.VARCHAR), equalTo(false));
    assertThat(booleanConverter.getBoolean("0", Types.CHAR), equalTo(false));
    assertThat(booleanConverter.getBoolean("false", Types.VARCHAR), equalTo(false));
    assertThat(booleanConverter.getBoolean("FALSE", Types.CHAR), equalTo(false));
  }

  @Test
  public void testConvertOtherType() {
    assertThrows(SFException.class, () -> booleanConverter.getBoolean("1", Types.BINARY));
  }
}
