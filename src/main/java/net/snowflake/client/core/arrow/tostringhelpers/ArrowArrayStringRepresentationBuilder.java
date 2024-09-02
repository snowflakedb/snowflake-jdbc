package net.snowflake.client.core.arrow.tostringhelpers;

import net.snowflake.client.core.SnowflakeJdbcInternalApi;
import net.snowflake.client.jdbc.SnowflakeType;

@SnowflakeJdbcInternalApi
public class ArrowArrayStringRepresentationBuilder extends ArrowStringRepresentationBuilderBase {

  public ArrowArrayStringRepresentationBuilder() {
    super();
    this.append('[');
  }

  public ArrowStringRepresentationBuilderBase appendValue(String value, SnowflakeType valueType) {
    super.addCommaIfNeeded();
    if (shouldQuoteValue(valueType)) {
      return this.appendQuoted(value);
    }
    return this.append(value);
  }

  @Override
  public String toString() {
    this.append(']');
    return super.toString();
  }
}
