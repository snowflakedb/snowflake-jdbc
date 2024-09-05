package net.snowflake.client.core.arrow.tostringhelpers;

import net.snowflake.client.core.SnowflakeJdbcInternalApi;
import net.snowflake.client.jdbc.SnowflakeType;

@SnowflakeJdbcInternalApi
public class ArrowArrayStringRepresentationBuilder extends ArrowStringRepresentationBuilderBase {

  private final SnowflakeType valueType;

  public ArrowArrayStringRepresentationBuilder(SnowflakeType valueType) {
    super(",", "[", "]");
    this.valueType = valueType;
  }

  public ArrowStringRepresentationBuilderBase appendValue(String value) {
    return add(quoteIfNeeded(value, valueType));
  }
}
