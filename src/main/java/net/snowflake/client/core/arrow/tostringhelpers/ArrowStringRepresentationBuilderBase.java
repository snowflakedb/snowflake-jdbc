package net.snowflake.client.core.arrow.tostringhelpers;

import java.util.HashSet;
import java.util.Set;
import java.util.StringJoiner;
import net.snowflake.client.core.SnowflakeJdbcInternalApi;
import net.snowflake.client.jdbc.SnowflakeType;

@SnowflakeJdbcInternalApi
public abstract class ArrowStringRepresentationBuilderBase {
  private final StringJoiner joiner;
  private static final Set<SnowflakeType> quotableTypes;

  static {
    quotableTypes = new HashSet<>();
    quotableTypes.add(SnowflakeType.ANY);
    quotableTypes.add(SnowflakeType.CHAR);
    quotableTypes.add(SnowflakeType.TEXT);
    quotableTypes.add(SnowflakeType.VARIANT);
    quotableTypes.add(SnowflakeType.BINARY);
    quotableTypes.add(SnowflakeType.DATE);
    quotableTypes.add(SnowflakeType.TIME);
    quotableTypes.add(SnowflakeType.TIMESTAMP_LTZ);
    quotableTypes.add(SnowflakeType.TIMESTAMP_NTZ);
    quotableTypes.add(SnowflakeType.TIMESTAMP_TZ);
  }

  public ArrowStringRepresentationBuilderBase(String delimiter, String prefix, String suffix) {
    joiner = new StringJoiner(delimiter, prefix, suffix);
  }

  protected ArrowStringRepresentationBuilderBase add(String string) {
    joiner.add(string);
    return this;
  }

  private boolean shouldQuoteValue(SnowflakeType type) {
    return quotableTypes.contains(type);
  }

  protected String quoteIfNeeded(String string, SnowflakeType type) {
    if (shouldQuoteValue(type)) {
      return '"' + string + '"';
    }

    return string;
  }

  @Override
  public String toString() {
    return joiner.toString();
  }
}
