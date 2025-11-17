package net.snowflake.client.internal.core.arrow.tostringhelpers;

import java.util.StringJoiner;
import net.snowflake.client.api.resultset.SnowflakeType;

public class ArrowObjectStringRepresentationBuilder extends ArrowStringRepresentationBuilderBase {

  public ArrowObjectStringRepresentationBuilder() {
    super(",", "{", "}");
  }

  public ArrowStringRepresentationBuilderBase appendKeyValue(
      String key, String value, SnowflakeType valueType) {
    StringJoiner joiner = new StringJoiner(": ");
    joiner.add('"' + key + '"');
    joiner.add(quoteIfNeeded(value, valueType));
    return add(joiner.toString());
  }
}
