package net.snowflake.client.core.arrow.tostringhelpers;

import net.snowflake.client.core.SnowflakeJdbcInternalApi;
import net.snowflake.client.jdbc.SnowflakeType;

@SnowflakeJdbcInternalApi
public class ArrowObjectStringRepresentationBuilder extends ArrowStringRepresentationBuilderBase {

  public ArrowObjectStringRepresentationBuilder() {
    super("{", "}");
  }

  public ArrowStringRepresentationBuilderBase appendKeyValue(
      String key, String value, SnowflakeType valueType) {
    addCommaIfNeeded();
    appendQuoted(key).append(": ");
    return appendQuotedIfNeeded(value, valueType);
  }
}
