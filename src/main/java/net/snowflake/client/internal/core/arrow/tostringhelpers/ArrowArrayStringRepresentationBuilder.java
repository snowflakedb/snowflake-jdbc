package net.snowflake.client.internal.core.arrow.tostringhelpers;

import net.snowflake.client.api.resultset.SnowflakeType;

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
